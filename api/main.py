"""
main.py – GutBiomeDB FastAPI backend
主后端：差异分析、筛选选项、数据统计 API
"""

import logging
import os
import json
import math
import re
import tempfile
import threading
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Optional

from fastapi import UploadFile, File

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from scipy import stats
from scipy.spatial.distance import cdist
from dotenv import load_dotenv
from starlette.requests import Request
from starlette.responses import RedirectResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from analysis import (
    NETWORK_METHOD_NOTES,
    available_network_methods,
    compare_network_edges,
    compute_network_topology,
    fastspar_cooccurrence,
    sample_similarity_search,
    spearman_cooccurrence,
    wilcoxon_marker_test,
)
from compare_utils import aggregate_by_level, relative_abundance_matrix, run_compare_analysis, run_spearman_analysis
from disease_utils import (
    build_disease_profile,
    build_disease_studies,
    build_lollipop_result,
    compute_genus_statistics,
    log2fc_interval,
    matched_disease_control,
)

# Configure logging / 配置日志
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Load environment variables / 加载环境变量 ─────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env.local", override=True)
load_dotenv(Path(__file__).parent.parent / ".env", override=False)

METADATA_PATH = os.getenv("METADATA_PATH", "")  # set via .env.local
ABUNDANCE_PATH = os.getenv("ABUNDANCE_PATH", "")  # set via .env.local
ADMIN_TOKEN    = os.getenv("ADMIN_TOKEN", "")

# ── 缓存版本号：自动使用 main.py 文件哈希，代码一改缓存自动失效 ────────────
import hashlib as _hashlib
CACHE_BUST = _hashlib.md5(open(__file__, "rb").read()).hexdigest()[:8]

# Validate at startup — use logging so warnings are never silently swallowed
# 启动时校验：使用 logging 确保警告不会被静默丢弃
if not METADATA_PATH:
    logging.warning("METADATA_PATH not set — data endpoints will fail. Set it in .env.local")
if not ABUNDANCE_PATH:
    logging.warning("ABUNDANCE_PATH not set — diff-analysis endpoints will fail. Set it in .env.local")
if not ADMIN_TOKEN:
    logging.warning("ADMIN_TOKEN not set — admin endpoints will reject all requests")

app = FastAPI(
    title="GutBiomeDB API",
    version="2.0.0",
    description="""
# GutBiomeDB — RESTful API

A comprehensive analysis platform for the human gut microbiome, integrating **168,464 samples** across **4,680 genera**, **72 countries**, and **224 condition categories** (223 non-NC condition labels plus one NC category).

## Features
- **Differential Analysis**: Wilcoxon rank-sum test, t-test, LEfSe (LDA effect size), PERMANOVA
- **Species Profiling**: Genus-level abundance across diseases, countries, age groups, and sex
- **Disease Biomarker Discovery**: Wilcoxon + BH FDR correction + LDA effect size estimation
- **Co-occurrence Network**: Spearman correlation-based microbial interaction networks
- **Sample Similarity Search**: Bray-Curtis / Jaccard distance-based sample matching
- **Lifecycle Atlas**: Age-stratified microbiome composition across 7 named life stages (Infant to Centenarian) plus Unknown
- **Data Export**: CSV/JSON/TSV download for all analysis results

## Citation
If you use this API in your research, please cite:
> Zhai J, Li Y, Liu J, Su X, Cui R, Zheng D, Sun Y, Yu J, Dai C. GutBiomeDB: An Integrated Human Gut Microbiome Database.

## Contact
- Correspondence: cdai@cmu.edu.cn
""",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    openapi_tags=[
        {"name": "Overview", "description": "Health check and system statistics"},
        {"name": "Species", "description": "Species/genus search and profiling"},
        {"name": "Disease", "description": "Disease browsing and biomarker discovery"},
        {"name": "Metabolism", "description": "Literature-curated metabolic function browser"},
        {"name": "Analysis", "description": "Differential analysis and statistical tests"},
        {"name": "Network", "description": "Co-occurrence networks and chord diagrams"},
        {"name": "Lifecycle", "description": "Age-stratified microbiome composition"},
        {"name": "Similarity", "description": "Sample similarity search"},
        {"name": "Download", "description": "Data export endpoints"},
        {"name": "Admin", "description": "Administration endpoints"},
    ],
)

# ── Rate limiting / 速率限制 ─────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS: allow all origins in dev, restrict to frontend URL in production
# 跨域：开发模式允许所有来源，生产模式必须设置 FRONTEND_URL
_FRONTEND_URL = os.getenv("FRONTEND_URL", "")
_DEBUG = os.getenv("DEBUG", "true").lower() == "true"

if not _DEBUG and not _FRONTEND_URL:
    raise RuntimeError(
        "FRONTEND_URL must be set when DEBUG=false. "
        "Without it, CORS will block all frontend requests. "
        "Set FRONTEND_URL in .env.local (e.g. http://localhost:5173)"
    )

_ALLOWED_ORIGINS = ["*"] if _DEBUG else [_FRONTEND_URL]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Security headers / 安全响应头 ────────────────────────────────────────────
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

app.add_middleware(SecurityHeadersMiddleware)


# ── Startup warmup / 启动预热 ─────────────────────────────────────────────────

@app.on_event("startup")
def warmup_data():
    """Pre-load data into memory at startup to avoid cold-start latency.
    ????????????????????"""
    import threading

    def _warmup_endpoints():
        import asyncio
        import time
        time.sleep(2)  # wait for server to be ready
        import urllib.request
        lightweight_requests = [
            {"url": "http://127.0.0.1:8000/api/filter-options"},
            {"url": "http://127.0.0.1:8000/api/data-stats"},
            {"url": "http://127.0.0.1:8000/api/disease-list"},
            {"url": "http://127.0.0.1:8000/api/health-index/reference"},
        ]
        for spec in lightweight_requests:
            url = spec["url"]
            try:
                data = spec.get("data")
                payload = None if data is None else json.dumps(data).encode("utf-8")
                request = urllib.request.Request(
                    url,
                    data=payload,
                    method=spec.get("method", "GET"),
                    headers=spec.get("headers", {}),
                )
                urllib.request.urlopen(request, timeout=180)
                logging.info(f"Warmup OK: {request.method} {url.split('/api/')[-1]}")
            except Exception as e:
                logging.warning(f"Warmup failed: {url} -> {e}")

        direct_warmups = [
            ("network default", lambda: getattr(microbe_disease_network, "__wrapped__", microbe_disease_network)(None, 12, 15)),
            ("disease-profile NC", lambda: getattr(disease_profile, "__wrapped__", disease_profile)(None, "NC", 40)),
            ("lifecycle global", lambda: getattr(lifecycle_atlas, "__wrapped__", lifecycle_atlas)(None, "", "", 10)),
            ("lifecycle compare IBD", lambda: getattr(lifecycle_compare, "__wrapped__", lifecycle_compare)(None, "IBD", "", 10)),
            ("metabolism overview", lambda: getattr(metabolism_overview, "__wrapped__", metabolism_overview)(None)),
            ("metabolism category scfa_producers", lambda: getattr(metabolism_category_profile, "__wrapped__", metabolism_category_profile)(None, "scfa_producers")),
            ("biomarker discovery IBD", lambda: getattr(biomarker_discovery, "__wrapped__", biomarker_discovery)(None, "IBD", 2.0, 0.05)),
            ("lollipop IBD", lambda: getattr(lollipop_data, "__wrapped__", lollipop_data)(None, "IBD", 40)),
            (
                "cross-study CD",
                lambda: asyncio.run(
                    getattr(cross_study_analysis, "__wrapped__", cross_study_analysis)(
                        None,
                        CrossStudyRequest(
                            project_ids=["PRJNA414072", "PRJNA237362", "PRJNA398187", "PRJNA431126"],
                            disease="CD",
                            method="wilcoxon",
                            taxonomy_level="genus",
                            p_threshold=0.05,
                            min_studies=2,
                        ),
                    )
                ),
            ),
        ]
        # 并行预热：所有重计算 endpoint 同时开始，总预热时间 ≈ 最慢单项（而非串行累加）
        import concurrent.futures as _cf
        def _run_warmup(item):
            label, func = item
            try:
                func()
                logging.info(f"Warmup OK: direct {label}")
            except Exception as e:
                logging.warning(f"Warmup failed: direct {label} -> {e}")
        with _cf.ThreadPoolExecutor(max_workers=4) as _pool:
            list(_pool.map(_run_warmup, direct_warmups))
        logging.info("All direct warmups completed")

    def _preload_data():
        # Avoid blocking startup on the full abundance matrix so health checks
        # and public API probes can recover immediately after code updates.
        try:
            if METADATA_PATH:
                get_metadata()
                logging.info("Metadata pre-loaded into cache")
            if ABUNDANCE_PATH:
                get_abundance()
                logging.info("Abundance pre-loaded into cache")
        except Exception as e:
            logging.warning(f"Warmup failed (non-fatal): {e}")

        _warmup_endpoints()

    threading.Thread(target=_preload_data, daemon=True).start()
    logging.info("Background data warmup started")


# ── Response cache for compute-heavy endpoints / 计算密集端点结果缓存 ─────────

_RESULT_CACHE: dict[str, tuple[float, float, dict]] = {}
_CACHE_TTL = 3600  # 1 hour

# 磁盘持久化缓存目录（重启后无需重算）
_DISK_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_disk_cache")
os.makedirs(_DISK_CACHE_DIR, exist_ok=True)
_DISK_CACHE_TTL = 86400 * 7  # 7 天

def _disk_cache_path(key: str) -> str:
    safe = key.replace(":", "_").replace("/", "_")
    return os.path.join(_DISK_CACHE_DIR, f"{safe}_v{CACHE_BUST}.json")

def get_disk_cached(key: str):
    """Load from disk cache if fresh; returns None if missing/expired."""
    path = _disk_cache_path(key)
    try:
        if not os.path.exists(path):
            return None
        age = datetime.now().timestamp() - os.path.getmtime(path)
        if age > _DISK_CACHE_TTL:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def set_disk_cached(key: str, val: dict):
    """Persist result to disk cache."""
    path = _disk_cache_path(key)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(val, f, ensure_ascii=False)
    except Exception as e:
        logging.warning(f"Disk cache write failed for {key}: {e}")

def get_cached(key: str):
    """Return cached result if exists and not expired."""
    if key in _RESULT_CACHE:
        ts, ttl, val = _RESULT_CACHE[key]
        if (datetime.now().timestamp() - ts) < ttl:
            return val
        del _RESULT_CACHE[key]
    return None

def set_cached(key: str, val: dict, ttl: int | None = None):
    """Store result in cache."""
    _RESULT_CACHE[key] = (datetime.now().timestamp(), float(ttl or _CACHE_TTL), val)
    # Evict old entries if cache too large
    if len(_RESULT_CACHE) > 500:
        oldest = sorted(_RESULT_CACHE, key=lambda k: _RESULT_CACHE[k][0])
        for k in oldest[:100]:
            del _RESULT_CACHE[k]


# ── Disease ontology / 疾病本体映射 ───────────────────────────────────────────
ONTOLOGY_PATH = os.path.join(os.path.dirname(__file__), "disease_ontology.json")
with open(ONTOLOGY_PATH, "r", encoding="utf-8") as f:
    DISEASE_ONTOLOGY: dict = json.load(f)
logging.info(f"Disease ontology loaded: {len(DISEASE_ONTOLOGY)} entries")

# ── Data loading (cached) / 数据加载（缓存） ──────────────────────────────────

_METADATA_LOCK = threading.Lock()
_ABUNDANCE_LOCK = threading.Lock()


@lru_cache(maxsize=1)
def _load_metadata_cached() -> pd.DataFrame:
    """Load and clean metadata CSV. / 加载并清理元数据CSV"""
    logging.info(f"Loading metadata from {METADATA_PATH}...")
    df = _read_csv_with_fallbacks(METADATA_PATH, on_bad_lines="skip", low_memory=False)

    # Normalize column names / 规范化列名
    df.columns = [c.strip() for c in df.columns]

    inform_cols = [f"inform{i}" for i in range(12)]
    for col in inform_cols + ["inform-all"]:
        if col not in df.columns:
            df[col] = pd.NA

    if "sex" in df.columns:
        sex_series = df["sex"].fillna("").astype(str).str.strip().str.lower()
        sex_series = sex_series.replace("", "unknown")
        sex_series = sex_series.where(sex_series.isin(["male", "female", "unknown"]), "unknown")
        df["sex"] = sex_series

    # Use iso column for country (matches frontend data model)
    # 使用 iso 列作为国家代码（与前端一致，如 US/CN/JP）
    if "iso" in df.columns:
        iso_series = df["iso"].fillna("").astype(str).str.strip().replace("", "unknown")
        iso_series = iso_series.replace({"TW": "CN", "HK": "CN", "MO": "CN"})
        df["iso"] = iso_series
        df["country"] = iso_series
    elif "geo_loc_name" in df.columns:
        df["country"] = df["geo_loc_name"].fillna("").astype(str).str.split(":").str[0].str.strip().str.lower()
        df["country"] = df["country"].replace("", "unknown")
    else:
        df["country"] = "unknown"

    _apply_control_group_remap(df, inform_cols)

    inform_all = df["inform-all"].fillna("").astype(str).str.strip()
    inform0 = df["inform0"].fillna("").astype(str).str.strip()
    fill_mask = inform_all.ne("") & inform0.eq("") & inform_all.map(_normalize_inform_label).ne("NC")
    if fill_mask.any():
        df.loc[fill_mask, "inform0"] = inform_all.loc[fill_mask]

    for col in inform_cols + ["inform-all"]:
        df[col] = df[col].map(_normalize_inform_label)

    # Legacy: keep "disease" as inform-all for backward compatibility
    if "inform-all" in df.columns:
        df["disease"] = df["inform-all"].fillna("").astype(str).str.strip()
        df.loc[df["disease"] == "", "disease"] = "unknown"
    else:
        df["disease"] = "unknown"

    # Build composite sample key matching abundance matrix rownames
    # 构建与丰度矩阵行名匹配的复合样本键
    # Abundance rownames: "PROJECT_SRR"
    if "srr" in df.columns and "project" in df.columns:
        df["sample_key"] = df["project"].astype(str) + "_" + df["srr"].astype(str)
    else:
        df["sample_key"] = df.index.astype(str)

    if "pubdate" in df.columns:
        df["year"] = pd.to_datetime(df["pubdate"], errors="coerce").dt.year

    logging.info(f"Metadata loaded: {len(df)} rows")
    return df


def get_metadata() -> pd.DataFrame:
    with _METADATA_LOCK:
        return _load_metadata_cached()


def _clear_metadata_cache() -> None:
    with _METADATA_LOCK:
        _load_metadata_cached.cache_clear()


get_metadata.cache_clear = _clear_metadata_cache


@lru_cache(maxsize=1)
def _load_abundance_cached() -> pd.DataFrame:
    """Load abundance CSV (large ~1.5 GB). / 加载丰度CSV（约1.5GB大文件）"""
    logging.info(f"Loading abundance from {ABUNDANCE_PATH}...")
    # First column is sample_id (rownames from R)
    # 第一列是样本ID（来自R的行名）
    df = pd.read_csv(ABUNDANCE_PATH, index_col=0, low_memory=False)
    logging.info(f"Abundance loaded: {df.shape}")
    return df


def get_abundance() -> pd.DataFrame:
    with _ABUNDANCE_LOCK:
        return _load_abundance_cached()


def _clear_abundance_cache() -> None:
    with _ABUNDANCE_LOCK:
        _load_abundance_cached.cache_clear()


get_abundance.cache_clear = _clear_abundance_cache


# ISO 3166-1 alpha-2 → country name mapping / ISO国家代码→国名映射
COUNTRY_NAMES = {
    "AE": "UAE", "AF": "Afghanistan", "AL": "Albania", "AM": "Armenia", "AT": "Austria",
    "AU": "Australia", "AZ": "Azerbaijan", "BD": "Bangladesh", "BE": "Belgium",
    "BF": "Burkina Faso", "BG": "Bulgaria", "BR": "Brazil", "BW": "Botswana",
    "CA": "Canada", "CF": "Central African Rep.", "CH": "Switzerland", "CM": "Cameroon",
    "CN": "China", "CO": "Colombia", "CZ": "Czechia", "DE": "Germany", "DK": "Denmark",
    "EC": "Ecuador", "EE": "Estonia", "EG": "Egypt", "ES": "Spain", "ET": "Ethiopia",
    "FI": "Finland", "FJ": "Fiji", "FR": "France", "GA": "Gabon", "GB": "United Kingdom",
    "GH": "Ghana", "GR": "Greece", "GT": "Guatemala", "HK": "Hong Kong", "HN": "Honduras",
    "HR": "Croatia", "HU": "Hungary", "ID": "Indonesia", "IE": "Ireland", "IL": "Israel",
    "IN": "India", "IR": "Iran", "IS": "Iceland", "IT": "Italy", "JM": "Jamaica",
    "JO": "Jordan", "JP": "Japan", "KE": "Kenya", "KR": "South Korea", "KZ": "Kazakhstan",
    "LK": "Sri Lanka", "LT": "Lithuania", "LV": "Latvia", "MA": "Morocco", "MD": "Moldova", "MT": "Malta",
    "MG": "Madagascar", "ML": "Mali", "MM": "Myanmar", "MN": "Mongolia", "MW": "Malawi",
    "MX": "Mexico", "MY": "Malaysia", "MZ": "Mozambique", "NG": "Nigeria", "NL": "Netherlands",
    "NO": "Norway", "NP": "Nepal", "NZ": "New Zealand", "PE": "Peru", "PG": "Papua New Guinea",
    "PH": "Philippines", "PK": "Pakistan", "PL": "Poland", "PT": "Portugal", "RO": "Romania",
    "RS": "Serbia", "RU": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia", "SE": "Sweden",
    "SD": "Sudan", "SG": "Singapore", "SI": "Slovenia", "SK": "Slovakia", "SN": "Senegal", "SV": "El Salvador",
    "TH": "Thailand", "TN": "Tunisia", "TR": "Turkey", "TW": "Taiwan", "TZ": "Tanzania",
    "UA": "Ukraine", "UG": "Uganda", "US": "United States", "UZ": "Uzbekistan",
    "VE": "Venezuela", "VN": "Vietnam", "ZA": "South Africa", "ZM": "Zambia", "ZW": "Zimbabwe", "AO": "Angola",
}

SPECIAL_POPULATION_LABELS = {
    "Healthy first-degree relatives of Crohn's disease patients",
    "preterm infants",
    "Low birth weight infant",
    "Very low birth weight preterm infants",
}
SPECIAL_POPULATION_LABELS_LOWER = {label.lower() for label in SPECIAL_POPULATION_LABELS}
HEALTHY_CONTROL_ALIASES = {"nc", "healthy control", "helminth uninfected control"}
CONTROL_GROUP_REMAP_LABELS = {
    "No_Fecal occult blood positive,without severe underlying bowel disease",
}
CONTROL_GROUP_REMAP_LABELS_LOWER = {label.lower() for label in CONTROL_GROUP_REMAP_LABELS}
EXCLUDED_DISEASE_ALIASES = {"dss colitis"}


def _read_csv_with_fallbacks(path: str, **kwargs) -> pd.DataFrame:
    """Read CSV with UTF-8 preference and legacy fallbacks."""
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "gbk", "latin1"):
        try:
            return pd.read_csv(path, encoding=encoding, **kwargs)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return pd.read_csv(path, **kwargs)


def _normalize_inform_label(value: object) -> str:
    """Normalize disease/special-population labels across inform fields."""
    if pd.isna(value):
        return ""
    label = str(value).strip()
    if not label:
        return ""
    lower = label.lower()
    if lower in {"nan", "unknown"}:
        return ""
    if lower in CONTROL_GROUP_REMAP_LABELS_LOWER:
        return "NC"
    if lower in HEALTHY_CONTROL_ALIASES:
        return "NC"
    if lower in EXCLUDED_DISEASE_ALIASES:
        return ""
    return label


def _apply_control_group_remap(df: pd.DataFrame, inform_cols: list[str]) -> None:
    """Normalize known mixed-control rows to the canonical NC representation."""
    if "inform-all" not in df.columns:
        return
    inform_all = df["inform-all"].fillna("").astype(str).str.strip()
    control_mask = inform_all.str.lower().isin(CONTROL_GROUP_REMAP_LABELS_LOWER)
    if not control_mask.any():
        return
    df.loc[control_mask, "inform-all"] = "NC"
    for col in inform_cols:
        if col in df.columns:
            df.loc[control_mask, col] = ""


def _label_kind(label: object) -> str:
    normalized = _normalize_inform_label(label)
    if not normalized:
        return "unknown"
    if normalized.lower() == "nc":
        return "healthy_control"
    if normalized.lower() in SPECIAL_POPULATION_LABELS_LOWER:
        return "special_population"
    return "disease"


def iso_to_name(code: str) -> str:
    """Convert ISO code to human-readable name. / ISO代码转可读国名"""
    return COUNTRY_NAMES.get(code, code)


# ── Disease name i18n / 疾病名称中英文映射 ────────────────────────────────────
_DISEASE_ZH_PATH = Path(__file__).parent / "disease_names_zh.json"
DISEASE_NAMES_ZH: dict[str, str] = {}
if _DISEASE_ZH_PATH.exists():
    with open(_DISEASE_ZH_PATH, encoding="utf-8") as f:
        DISEASE_NAMES_ZH = json.load(f)

DISEASE_NAMES_EN_FALLBACK: dict[str, str] = {
    "NC": "Non-disease Control",
    "Healthy first-degree relatives of Crohn's disease patients": "Healthy First-degree Relatives of Crohn's Disease Patients",
    "End-stage renal disease": "End-stage Renal Disease",
    "Low birth weight infant": "Low Birth Weight Infant",
    "Very low birth weight preterm infants": "Very Low Birth Weight Preterm Infants",
    "Enterobacterales infection": "Enterobacterales Infection",
    "Cancer": "Cancer",
    "Nonrecurrent Clostridioides difficile infection": "Nonrecurrent Clostridioides difficile Infection",
    "Recurrent Clostridioides difficile infection": "Recurrent Clostridioides difficile Infection",
}


def disease_to_zh(name: str) -> str:
    """Return Chinese name if available, else original. / 返回中文疾病名（如有）"""
    return DISEASE_NAMES_ZH.get(name, name)


def _humanize_disease_name(name: str) -> str:
    """Build a readable English fallback for unmapped disease keys."""
    cleaned = str(name).strip().replace("_", " ")
    if not cleaned:
        return ""
    return re.sub(
        r"\b([A-Za-z][A-Za-z'-]*)\b",
        lambda match: match.group(1) if match.group(1).isupper() else match.group(1)[0].upper() + match.group(1)[1:],
        cleaned,
    )


def disease_to_en(name: str) -> str:
    """Return a canonical English display name for a disease/control key."""
    manual = DISEASE_NAMES_EN_FALLBACK.get(name, "")
    if manual:
        return manual
    onto = DISEASE_ONTOLOGY.get(name, {})
    standard = str(onto.get("standard_name", "")).strip()
    if standard:
        return standard
    return _humanize_disease_name(name)


def _disease_sort_key(name: str) -> tuple[int, str]:
    return (0 if str(name).upper() == "NC" else 1, disease_to_en(str(name)).casefold())


def extract_genus(col_name: str) -> str:
    """
    Extract genus from full taxonomy string.
    从完整分类字符串中提取属名
    e.g. "Bacteria.Bacillota.Clostridia.Lachnospirales.Lachnospiraceae.Blautia" → "Blautia"
    """
    parts = col_name.split(".")
    return parts[-1] if parts else col_name


def extract_phylum(col_name: str) -> str:
    """Extract phylum from full taxonomy. / 提取门名"""
    parts = col_name.split(".")
    return parts[1] if len(parts) > 1 else col_name


def get_project_column(meta: pd.DataFrame) -> str | None:
    """Return the project column name if available."""
    for candidate in ("BioProject", "project"):
        if candidate in meta.columns:
            return candidate
    return None


def count_unique_projects(meta: pd.DataFrame) -> int:
    """Count unique public projects from metadata."""
    project_col = get_project_column(meta)
    if project_col is None:
        return 0
    series = meta[project_col].dropna().astype(str).str.strip().replace("", pd.NA).dropna()
    return int(series.nunique())


def count_unique_genera_from_abundance() -> int:
    """Count unique genera from abundance matrix headers without loading the full matrix."""
    if not ABUNDANCE_PATH or not os.path.exists(ABUNDANCE_PATH):
        return 0

    columns = pd.read_csv(ABUNDANCE_PATH, nrows=0).columns.tolist()
    if not columns:
        return 0

    data_columns = columns[1:] if columns[0].strip().lower() in {"sample_id", "sampleid", "sample", "rownames"} else columns
    return len(data_columns)


def build_genus_phylum_map(columns: list[str]) -> dict[str, str]:
    """Build a genus → phylum lookup from taxonomy columns."""
    phylum_map: dict[str, str] = {}
    for col in columns:
        genus = extract_genus(col).strip()
        phylum = extract_phylum(col).strip()
        if genus and genus not in phylum_map:
            phylum_map[genus] = phylum
    return phylum_map


METABOLISM_MAPPING_PATH = Path(__file__).parent.parent / "public" / "data" / "metabolism_mapping.json"
METABOLISM_INFORM_COLS = [f"inform{i}" for i in range(12)]
METABOLISM_MIN_SAMPLES = 5
METABOLISM_PROFILE_DISEASE_LIMIT = 30
METABOLISM_OVERVIEW_DISEASE_LIMIT = 20
METABOLISM_OVERVIEW_TTL = 24 * 3600
METABOLISM_PSEUDOCOUNT = 1e-6


def _strict_nc_mask(meta: pd.DataFrame, inform_cols: list[str] | None = None) -> pd.Series:
    """Return healthy-control mask using inform-all as the source-of-truth field."""
    columns = inform_cols or [f"inform{i}" for i in range(12)]
    if "inform-all" in meta.columns:
        inform_all = meta["inform-all"].fillna("").astype(str).str.strip().map(_normalize_inform_label)
        return inform_all == "NC"
    if not columns or columns[0] not in meta.columns:
        return pd.Series(False, index=meta.index, dtype=bool)

    mask = meta[columns[0]].fillna("").astype(str).str.strip().str.lower() == "nc"
    for col in columns[1:]:
        if col not in meta.columns:
            continue
        values = meta[col].fillna("").astype(str).str.strip()
        mask &= values == ""
    return mask


def _primary_condition_series(meta: pd.DataFrame) -> pd.Series:
    """Return normalized primary cohort labels from inform-all."""
    if "inform-all" not in meta.columns:
        return pd.Series("", index=meta.index, dtype=object)
    return meta["inform-all"].fillna("").astype(str).str.strip().map(_normalize_inform_label)


def _primary_condition_mask(meta: pd.DataFrame, label: str) -> pd.Series:
    """Match samples by primary inform-all cohort label."""
    normalized = _normalize_inform_label(label)
    if not normalized:
        return pd.Series(False, index=meta.index, dtype=bool)
    primary = _primary_condition_series(meta)
    return primary.str.lower() == normalized.lower()


def _primary_condition_counts(meta: pd.DataFrame, *, include_nc: bool = True) -> dict[str, int]:
    """Count primary cohort labels from inform-all."""
    primary = _primary_condition_series(meta)
    primary = primary[primary != ""]
    if not include_nc:
        primary = primary[primary.str.lower() != "nc"]
    counts = primary.value_counts()
    return {str(name): int(count) for name, count in counts.items()}


def _inform_label_mask(meta: pd.DataFrame, label: str, inform_cols: list[str] | None = None) -> pd.Series:
    """Match disease groups by inform0-11, but keep NC on strict inform-all controls."""
    normalized = _normalize_inform_label(label)
    if not normalized:
        return pd.Series(False, index=meta.index, dtype=bool)
    columns = inform_cols or [f"inform{i}" for i in range(12)]
    if normalized.lower() == "nc":
        return _strict_nc_mask(meta, columns)

    mask = pd.Series(False, index=meta.index, dtype=bool)
    for col in columns:
        if col not in meta.columns:
            continue
        values = meta[col].fillna("").astype(str).str.strip().map(_normalize_inform_label)
        mask |= values.str.lower() == normalized.lower()
    return mask


def _inform_label_counts(meta: pd.DataFrame, inform_cols: list[str] | None = None, *, include_nc: bool = True) -> dict[str, int]:
    """Count labels from inform0-11, collapsing strict NC rows to a single NC label."""
    counts: dict[str, int] = {}
    for label in _iter_inform_labels(meta, inform_cols):
        if not include_nc and label.lower() == "nc":
            continue
        counts[label] = counts.get(label, 0) + 1
    return counts


def _clean_text_series(series: pd.Series) -> pd.Series:
    """Normalize string series by dropping empty / nan-like values."""
    values = series.dropna().astype(str).str.strip()
    return values[(values != "") & (values.str.lower() != "nan")]


def _iter_inform_labels(meta: pd.DataFrame, inform_cols: list[str] | None = None):
    """Yield inform0-11 labels, while sourcing NC only from strict inform-all controls."""
    columns = inform_cols or [f"inform{i}" for i in range(12)]
    available_cols = [col for col in columns if col in meta.columns]
    strict_nc = _strict_nc_mask(meta, available_cols)
    for idx in meta.index:
        if bool(strict_nc.loc[idx]):
            yield "NC"
            continue
        for col in available_cols:
            label = _normalize_inform_label(meta.at[idx, col])
            if not label or label.lower() == "nc":
                continue
            yield label


def _non_nc_disease_mask(meta: pd.DataFrame, inform_cols: list[str] | None = None) -> pd.Series:
    """Return mask for samples whose primary inform-all cohort is non-empty and non-NC."""
    primary = _primary_condition_series(meta)
    return (primary != "") & (primary.str.lower() != "nc")


def _collect_project_diseases(meta: pd.DataFrame, inform_cols: list[str] | None = None) -> list[str]:
    """Collect unique non-NC labels from inform0-11 after NC normalization."""
    counts = _inform_label_counts(meta, inform_cols, include_nc=False)
    return sorted(counts.keys(), key=_disease_sort_key)


def _infer_region_16s(instrument: str) -> str:
    """Heuristic 16S region estimate from sequencer name."""
    instrument_lower = instrument.lower()
    if any(token in instrument_lower for token in ("illumina", "nextseq", "miseq")):
        return "V3-V4 (est.)"
    if "ion torrent" in instrument_lower or "pgm" in instrument_lower:
        return "V1-V2 (est.)"
    if "454" in instrument_lower:
        return "V1-V3 (est.)"
    return "Unknown"


def _project_mode_value(series: pd.Series, default: str = "Unknown", max_len: int | None = None) -> str:
    """Return the most common cleaned string value."""
    values = _clean_text_series(series)
    if values.empty:
        return default
    result = str(values.mode().iloc[0])
    return result[:max_len] if max_len else result


def _sorted_count_rows(counts: dict[str, int], key_name: str, limit: int | None = None) -> list[dict[str, int | str]]:
    items = sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))
    if limit is not None:
        items = items[:limit]
    return [{key_name: name, "count": int(count)} for name, count in items]


def _series_count_rows(
    series: pd.Series,
    key_name: str,
    limit: int | None = None,
) -> list[dict[str, int | str]]:
    values = _clean_text_series(series)
    counts = values.value_counts()
    if limit is not None:
        counts = counts.head(limit)
    return [{key_name: str(name), "count": int(count)} for name, count in counts.items()]


def _disease_count_rows(meta: pd.DataFrame, inform_cols: list[str] | None = None, limit: int | None = None) -> list[dict[str, int | str]]:
    """Count non-NC labels from inform0-11 for detail panels."""
    counts = _inform_label_counts(meta, inform_cols, include_nc=False)
    return _sorted_count_rows(counts, "disease", limit=limit)


def _project_overview(proj: str, grp: pd.DataFrame, inform_cols: list[str] | None = None) -> dict:
    """Build aggregated project-level summary used by studies endpoints."""
    columns = inform_cols or [f"inform{i}" for i in range(12)]
    disease_values = _collect_project_diseases(grp, columns)
    nc_mask = _strict_nc_mask(grp, columns)
    disease_mask = _non_nc_disease_mask(grp, columns)

    countries = _clean_text_series(grp["country"]) if "country" in grp.columns else pd.Series(dtype=str)
    country_counts = countries.value_counts()
    country_main = str(country_counts.index[0]) if not country_counts.empty else "Unknown"
    country_list = [str(name) for name in country_counts.index.tolist()]

    instrument_val = _project_mode_value(grp["instrument"], default="Unknown", max_len=40) if "instrument" in grp.columns else "Unknown"
    region_16s = _infer_region_16s(instrument_val)

    year_val: int | None = None
    if "year" in grp.columns:
        years = pd.to_numeric(grp["year"], errors="coerce").dropna().astype(int)
        if not years.empty:
            year_val = int(years.mode().iloc[0])

    return {
        "project_id": str(proj),
        "sample_count": int(len(grp)),
        "nc_count": int(nc_mask.sum()),
        "disease_count": int(disease_mask.sum()),
        "n_diseases": int(len(disease_values)),
        "diseases": disease_values,
        "has_control": bool(nc_mask.any()),
        "country": country_main,
        "country_list": country_list,
        "year": year_val,
        "instrument": instrument_val,
        "region_16s": region_16s,
    }


@lru_cache(maxsize=1)
def load_metabolism_mapping() -> dict:
    """Load curated metabolism mapping JSON."""
    with open(METABOLISM_MAPPING_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _normalize_name_list(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        name = str(value).strip()
        if not name or name in seen:
            continue
        normalized.append(name)
        seen.add(name)
    return normalized


def _metabolism_exact_names(category: dict) -> list[str]:
    values = category.get("genus_exact_names") or category.get("taxa") or []
    return _normalize_name_list(list(values))


def _metabolism_disease_mask(meta: pd.DataFrame, disease: str) -> pd.Series:
    return _inform_label_mask(meta, disease, METABOLISM_INFORM_COLS)


def _metabolism_strict_nc_mask(meta: pd.DataFrame) -> pd.Series:
    return _strict_nc_mask(meta, METABOLISM_INFORM_COLS)


def _prepare_metabolism_context(meta: pd.DataFrame, abundance_df: pd.DataFrame) -> dict:
    mapping = load_metabolism_mapping()
    all_exact_names = sorted(
        {
            genus
            for category in mapping.get("categories", [])
            for genus in _metabolism_exact_names(category)
        }
    )

    abundance_lookup = abundance_df.copy(deep=False)
    abundance_lookup.index = abundance_lookup.index.astype(str)

    matched_meta = meta.copy()
    matched_meta["sample_key"] = matched_meta["sample_key"].fillna("").astype(str)
    matched_meta = matched_meta.loc[matched_meta["sample_key"].isin(set(abundance_lookup.index))]
    matched_meta = matched_meta.drop_duplicates(subset="sample_key").reset_index(drop=True)
    matched_keys = matched_meta["sample_key"].tolist()

    selected_columns = [
        col
        for col in abundance_lookup.columns
        if extract_genus(str(col)).strip() in all_exact_names
    ]

    if not matched_keys or not selected_columns:
        return {
            "meta": matched_meta,
            "genus_matrix": np.zeros((len(matched_keys), 0), dtype=float),
            "genus_labels": [],
            "genus_index": {},
        }

    totals = pd.Series(
        abundance_lookup.sum(axis=1).astype(float).to_numpy(dtype=float),
        index=abundance_lookup.index,
    )
    denom = totals.reindex(matched_keys).fillna(0.0).to_numpy(dtype=float, copy=True)
    denom[denom == 0] = 1.0

    raw_subset = abundance_lookup.loc[matched_keys, selected_columns].to_numpy(dtype=float)
    rel_subset = raw_subset / denom[:, None] * 100.0
    genus_matrix, genus_labels, _ = aggregate_by_level(rel_subset, selected_columns, "genus")

    return {
        "meta": matched_meta,
        "genus_matrix": genus_matrix,
        "genus_labels": genus_labels,
        "genus_index": {label: idx for idx, label in enumerate(genus_labels)},
    }


def _metabolism_top_diseases(meta: pd.DataFrame, limit: int) -> list[dict]:
    counts = _inform_label_counts(meta, METABOLISM_INFORM_COLS, include_nc=False)
    rows = [{"name": name, "sample_count": count} for name, count in counts.items()]
    rows.sort(key=lambda item: item["sample_count"], reverse=True)
    return rows[:limit]


def _metabolism_category_result(category: dict, context: dict, disease_limit: int) -> dict:
    meta = context["meta"]
    genus_index = context["genus_index"]
    genus_matrix = context["genus_matrix"]

    exact_names = _metabolism_exact_names(category)
    matched_genera = [name for name in exact_names if name in genus_index]
    unmatched_genera = [name for name in exact_names if name not in genus_index]
    if unmatched_genera:
        logging.info(
            "Metabolism unmatched genera for %s: %s",
            category["id"],
            ", ".join(unmatched_genera),
        )

    result = {
        "category_id": category["id"],
        "category_name_en": category.get("name_en", category["id"]),
        "category_name_zh": category.get("name_zh", category.get("name_en", category["id"])),
        "n_matched": len(matched_genera),
        "matched_genera": matched_genera,
        "unmatched_genera": unmatched_genera,
        "control_count": 0,
        "disease_profiles": [],
    }

    if not matched_genera or genus_matrix.size == 0:
        result["warning"] = "No exact abundance matches found for this category"
        return result

    chosen_indices = [genus_index[name] for name in matched_genera]
    category_scores = genus_matrix[:, chosen_indices].mean(axis=1)
    nc_mask = _metabolism_strict_nc_mask(meta)
    nc_scores = category_scores[nc_mask.to_numpy()]
    result["control_count"] = int(len(nc_scores))

    if len(nc_scores) < METABOLISM_MIN_SAMPLES:
        result["warning"] = "Insufficient strict NC samples"
        return result

    rows: list[dict] = []
    p_values: list[float] = []
    for disease_row in _metabolism_top_diseases(meta, disease_limit):
        disease = disease_row["name"]
        disease_mask = _metabolism_disease_mask(meta, disease)
        disease_scores = category_scores[disease_mask.to_numpy()]
        if len(disease_scores) < METABOLISM_MIN_SAMPLES:
            continue

        try:
            stat = stats.mannwhitneyu(disease_scores, nc_scores, alternative="two-sided")
            u_stat = float(stat.statistic)
            p_value = float(stat.pvalue)
        except Exception:
            u_stat = 0.0
            p_value = 1.0

        mean_disease = float(np.mean(disease_scores))
        mean_nc = float(np.mean(nc_scores))
        effect_size = float(1 - 2 * u_stat / (len(disease_scores) * len(nc_scores)))
        log2fc = float(
            math.log2(
                (mean_disease + METABOLISM_PSEUDOCOUNT)
                / (mean_nc + METABOLISM_PSEUDOCOUNT)
            )
        )
        rows.append(
            {
                "disease": disease,
                "sample_count": int(len(disease_scores)),
                "control_count": int(len(nc_scores)),
                "mean_disease": round(mean_disease, 6),
                "mean_nc": round(mean_nc, 6),
                "log2fc": round(log2fc, 6),
                "p_value": round(p_value, 8),
                "adjusted_p": 1.0,
                "effect_size": round(effect_size, 6),
                "direction": "enriched" if log2fc > 0 else ("depleted" if log2fc < 0 else "neutral"),
            }
        )
        p_values.append(p_value)

    adjusted = bh_correction(p_values)
    for idx, row in enumerate(rows):
        row["adjusted_p"] = round(float(adjusted[idx]), 8)

    result["disease_profiles"] = rows
    return result


def _build_metabolism_overview(mapping: dict, context: dict) -> dict:
    meta = context["meta"]
    nc_count = int(_metabolism_strict_nc_mask(meta).sum())
    diseases = _metabolism_top_diseases(meta, METABOLISM_OVERVIEW_DISEASE_LIMIT)

    if nc_count < METABOLISM_MIN_SAMPLES:
        return {
            "diseases": diseases,
            "categories": [],
            "warning": "Insufficient strict NC samples for overview",
            "generated_at": datetime.now().isoformat(),
        }

    categories = []
    for category in mapping.get("categories", []):
        profile = _metabolism_category_result(category, context, METABOLISM_OVERVIEW_DISEASE_LIMIT)
        row_lookup = {row["disease"]: row["log2fc"] for row in profile["disease_profiles"]}
        categories.append(
            {
                "category_id": category["id"],
                "name_en": category.get("name_en", category["id"]),
                "name_zh": category.get("name_zh", category.get("name_en", category["id"])),
                "icon": category.get("icon", ""),
                "n_matched": profile["n_matched"],
                "matched_genera": profile["matched_genera"],
                "values": [row_lookup.get(disease["name"]) for disease in diseases],
            }
        )

    return {
        "diseases": diseases,
        "categories": categories,
        "generated_at": datetime.now().isoformat(),
    }


# ── Pydantic models / 请求/响应模型 ────────────────────────────────────────────

class GroupFilter(BaseModel):
    """Filter conditions for a sample group. / 样本组筛选条件"""
    country: Optional[str] = None   # e.g. "china"
    disease: Optional[str] = None   # e.g. "IBD"
    age_group: Optional[str] = None # e.g. "Adult"
    sex: Optional[str] = None       # e.g. "female"


class DiffAnalysisRequest(BaseModel):
    group_a_filter: GroupFilter
    group_b_filter: GroupFilter
    taxonomy_level: str = "genus"   # genus / family / phylum
    method: str = "wilcoxon"        # wilcoxon / t-test / lefse / permanova


class SimilarityRequest(BaseModel):
    """样本相似性搜索请求模型"""
    abundances: dict[str, float]  # genus_name -> abundance_value / 属名 -> 丰度值
    metric: str = "braycurtis"    # braycurtis or jaccard / 距离度量
    top_k: int = 10               # 返回最相似样本数量
    filter_disease: str = ""
    filter_country: str = ""
    filter_age_group: str = ""


class CrossStudyRequest(BaseModel):
    """跨研究元分析请求模型"""
    project_ids: list[str]        # 项目ID列表
    disease: str                  # 目标疾病
    method: str = "wilcoxon"      # wilcoxon / t-test
    taxonomy_level: str = "genus" # genus / family / phylum
    p_threshold: float = 0.05     # 显著性阈值
    min_studies: int = 2          # 最少一致队列数


class SampleCountRequest(BaseModel):
    group_a_filter: GroupFilter
    group_b_filter: GroupFilter


class SpearmanAnalysisRequest(BaseModel):
    group_a_filter: GroupFilter
    group_b_filter: GroupFilter
    taxonomy_level: str = "genus"
    max_taxa: int = 18


class HealthIndexRequest(BaseModel):
    """微生物组健康指数请求模型"""
    abundances: dict[str, float]  # genus_name -> abundance_value
    age_group: str = ""           # 可选年龄段, 用于参考范围


# ── Helper functions / 辅助函数 ────────────────────────────────────────────────

def apply_filter(df: pd.DataFrame, f: GroupFilter) -> pd.DataFrame:
    """Filter metadata dataframe by group conditions. / 按条件筛选元数据"""
    result = df.copy()
    if f.country:
        result = result[result["country"].str.lower() == f.country.lower()]
    if f.disease:
        # Match if ANY of inform0-11 contains the disease
        # 只要 inform0-11 中任一列匹配即算有该疾病
        INFORM_COLS = [f"inform{i}" for i in range(12)]
        disease_lower = f.disease.strip().lower()
        mask = pd.Series(False, index=result.index)
        for col in INFORM_COLS:
            if col in result.columns:
                mask |= result[col].fillna("").astype(str).str.strip().str.lower() == disease_lower
        result = result[mask]
    if f.age_group:
        if "age_group" in result.columns:
            result = result[result["age_group"].str.lower() == f.age_group.lower()]
    if f.sex:
        if "sex" in result.columns:
            result = result[result["sex"].str.lower() == f.sex.lower()]
    return result


def apply_filter(df: pd.DataFrame, f: GroupFilter) -> pd.DataFrame:
    """Filter metadata using inform0-11 disease groups and strict NC controls."""
    result = df.copy()
    if f.country:
        result = result[result["country"].str.lower() == f.country.lower()]
    if f.disease:
        result = result[_inform_label_mask(result, f.disease)]
    if f.age_group and "age_group" in result.columns:
        result = result[result["age_group"].str.lower() == f.age_group.lower()]
    if f.sex and "sex" in result.columns:
        result = result[result["sex"].str.lower() == f.sex.lower()]
    return result


def bh_correction(p_values: list[float]) -> list[float]:
    """
    Benjamini-Hochberg FDR correction.
    BH多重检验校正（FDR）
    """
    n = len(p_values)
    if n == 0:
        return []
    indexed = sorted(enumerate(p_values), key=lambda x: x[1])
    adjusted = [0.0] * n
    prev_adj = 1.0
    for rank, (orig_idx, p) in enumerate(reversed(indexed)):
        adj = min(prev_adj, p * n / (n - rank))
        adjusted[orig_idx] = min(adj, 1.0)
        prev_adj = adj
    return adjusted


def shannon_diversity(row: np.ndarray) -> float:
    """Calculate Shannon diversity index. / 计算Shannon多样性指数"""
    row = row[row > 0]
    if len(row) == 0:
        return 0.0
    p = row / row.sum()
    return float(-np.sum(p * np.log(p)))


def simpson_diversity(row: np.ndarray) -> float:
    """Calculate Simpson diversity index (1-D). / 计算Simpson多样性指数"""
    row = row[row > 0]
    if len(row) == 0:
        return 0.0
    p = row / row.sum()
    return float(1 - np.sum(p ** 2))


def bray_curtis_pcoa(matrix_a: np.ndarray, matrix_b: np.ndarray,
                     max_samples: int = 200):
    """
    Compute Bray-Curtis distance matrix and PCoA for two groups.
    计算两组的Bray-Curtis距离矩阵和PCoA坐标
    Subsamples to max_samples per group for performance.
    为性能考虑，每组最多采样max_samples个样本
    """
    # Set random seed for reproducible subsampling / 设置种子保证可重复性
    np.random.seed(42)

    # Subsample for performance / 为性能随机抽样
    if len(matrix_a) > max_samples:
        idx = np.random.choice(len(matrix_a), max_samples, replace=False)
        matrix_a = matrix_a[idx]
    if len(matrix_b) > max_samples:
        idx = np.random.choice(len(matrix_b), max_samples, replace=False)
        matrix_b = matrix_b[idx]

    n_a, n_b = len(matrix_a), len(matrix_b)
    combined = np.vstack([matrix_a, matrix_b])
    n = len(combined)

    # Bray-Curtis distance via scipy C implementation (much faster than Python loop)
    # 使用scipy的C实现计算Bray-Curtis距离（远比Python循环快）
    bc = cdist(combined, combined, metric="braycurtis")
    bc = np.nan_to_num(bc)  # replace any NaN from all-zero rows

    # Classical MDS (PCoA) / 主坐标分析
    # Double centering / 双中心化
    n_pts = bc.shape[0]
    H = np.eye(n_pts) - np.ones((n_pts, n_pts)) / n_pts
    B = -0.5 * H @ (bc ** 2) @ H

    eigenvalues, eigenvectors = np.linalg.eigh(B)
    # Sort descending / 降序排列
    idx_sort = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[idx_sort]
    eigenvectors = eigenvectors[:, idx_sort]

    # Take first 2 PCoA axes / 取前两个主坐标轴
    pos_mask = eigenvalues > 0
    if pos_mask.sum() < 2:
        coords = np.zeros((n_pts, 2))
    else:
        coords = eigenvectors[:, :2] * np.sqrt(np.maximum(eigenvalues[:2], 0))

    return [
        {"x": float(coords[i, 0]), "y": float(coords[i, 1]),
         "group": "A" if i < n_a else "B"}
        for i in range(n_pts)
    ]


def lefse_analysis(
    agg_a: np.ndarray,
    agg_b: np.ndarray,
    taxa: list[str],
    lda_threshold: float = 2.0,
    p_threshold: float = 0.05,
) -> list[dict]:
    """
    Simplified LEfSe: Kruskal-Wallis test + LDA effect size estimation.
    简化版 LEfSe：Kruskal-Wallis 检验 + LDA 效应值估计

    Steps:
    1. Kruskal-Wallis test per taxon (non-parametric ANOVA)
    2. Estimate LDA score from between/within group variance ratio
    3. Filter by p-value and LDA threshold
    """
    results = []
    for i, taxon in enumerate(taxa):
        vals_a = agg_a[:, i]
        vals_b = agg_b[:, i]

        # Skip taxa with no variation / 跳过无变异的分类
        if np.std(vals_a) == 0 and np.std(vals_b) == 0:
            continue

        # Kruskal-Wallis test (non-parametric one-way ANOVA)
        try:
            kw_stat, kw_p = stats.kruskal(vals_a, vals_b)
        except Exception:
            continue

        if kw_p >= p_threshold:
            continue

        # LDA effect size estimation / LDA 效应值估计
        # Approximation: log10(1 + abs(mean_diff) * scaling_factor)
        mean_a = float(np.mean(vals_a))
        mean_b = float(np.mean(vals_b))
        grand_mean = float(np.mean(np.concatenate([vals_a, vals_b])))

        # Between-class variance / 组间方差
        n_a, n_b = len(vals_a), len(vals_b)
        between_var = (n_a * (mean_a - grand_mean) ** 2 +
                       n_b * (mean_b - grand_mean) ** 2) / (n_a + n_b)

        # Within-class variance / 组内方差
        within_var = (n_a * float(np.var(vals_a)) +
                      n_b * float(np.var(vals_b))) / (n_a + n_b)

        # LDA score approximation / LDA 分数近似
        if within_var > 0:
            lda_score = math.log10(1 + abs(between_var / within_var) * abs(mean_a - mean_b) * 1e6)
        else:
            lda_score = math.log10(1 + abs(mean_a - mean_b) * 1e6)

        if lda_score < lda_threshold:
            continue

        enriched = "A" if mean_a > mean_b else "B"
        results.append({
            "taxon": taxon,
            "lda_score": round(lda_score, 4),
            "p_value": round(float(kw_p), 6),
            "enriched_group": enriched,
        })

    # Sort by LDA score descending / 按 LDA 分数降序
    results.sort(key=lambda x: x["lda_score"], reverse=True)
    return results[:100]  # top 100


def permanova_test(
    agg_a: np.ndarray,
    agg_b: np.ndarray,
    n_permutations: int = 999,
    max_samples: int = 300,
) -> dict:
    """
    PERMANOVA (Permutational Multivariate Analysis of Variance).
    Tests if group centroids differ in Bray-Curtis distance space.
    检验两组在 Bray-Curtis 距离空间中的质心是否显著不同

    Returns F-statistic, p-value, R² (effect size).
    """
    np.random.seed(42)

    # Subsample for performance / 抽样以提高性能
    if len(agg_a) > max_samples:
        idx = np.random.choice(len(agg_a), max_samples, replace=False)
        agg_a = agg_a[idx]
    if len(agg_b) > max_samples:
        idx = np.random.choice(len(agg_b), max_samples, replace=False)
        agg_b = agg_b[idx]

    n_a, n_b = len(agg_a), len(agg_b)
    combined = np.vstack([agg_a, agg_b])
    n = n_a + n_b
    labels = np.array([0] * n_a + [1] * n_b)

    # Compute Bray-Curtis distance matrix / 计算 Bray-Curtis 距离矩阵
    bc_dist = cdist(combined, combined, metric="braycurtis")
    bc_dist = np.nan_to_num(bc_dist)

    # Calculate pseudo-F statistic / 计算伪 F 统计量
    def calc_pseudo_f(dist_matrix: np.ndarray, group_labels: np.ndarray) -> tuple[float, float]:
        """Calculate pseudo-F and R² from distance matrix and group labels."""
        n_total = len(group_labels)
        groups = np.unique(group_labels)
        k = len(groups)

        # Total sum of squared distances / 总距离平方和
        ss_total = np.sum(dist_matrix ** 2) / (2 * n_total)

        # Within-group sum of squares / 组内平方和
        ss_within = 0.0
        for g in groups:
            mask = group_labels == g
            n_g = np.sum(mask)
            if n_g > 1:
                sub_dist = dist_matrix[np.ix_(mask, mask)]
                ss_within += np.sum(sub_dist ** 2) / (2 * n_g)

        ss_between = ss_total - ss_within

        # Pseudo-F
        df_between = k - 1
        df_within = n_total - k
        if df_within <= 0 or ss_within == 0:
            return 0.0, 0.0

        f_stat = (ss_between / df_between) / (ss_within / df_within)
        r_squared = ss_between / ss_total if ss_total > 0 else 0.0
        return float(f_stat), float(r_squared)

    observed_f, r_squared = calc_pseudo_f(bc_dist, labels)

    # Permutation test / 置换检验
    count_ge = 0
    for _ in range(n_permutations):
        perm_labels = np.random.permutation(labels)
        perm_f, _ = calc_pseudo_f(bc_dist, perm_labels)
        if perm_f >= observed_f:
            count_ge += 1

    p_value = (count_ge + 1) / (n_permutations + 1)

    return {
        "f_statistic": round(observed_f, 4),
        "p_value": round(float(p_value), 4),
        "r_squared": round(r_squared, 4),
        "permutations": n_permutations,
        "n_a": n_a,
        "n_b": n_b,
    }


# ── API endpoints / API端点 ───────────────────────────────────────────────────

@app.get("/api/health", summary="Health check",
         description="Returns API status and current timestamp.")
@limiter.limit("120/minute")
def health(request: Request):
    """Health check / 健康检查"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/filter-options",
         summary="Get filter options",
         description="Returns available filter values for countries, diseases, age groups, and sexes.")
@limiter.limit("120/minute")
def filter_options(request: Request):
    """
    Return available filter option values from metadata.
    返回元数据中可用的筛选选项值
    """
    cached = get_cached("filter_options")
    if cached:
        return cached
    meta = get_metadata()

    countries = sorted(meta["country"].dropna().astype(str).str.strip().unique().tolist())

    disease_counts = _inform_label_counts(meta, include_nc=True)
    diseases = sorted(disease_counts.keys(), key=_disease_sort_key)

    age_groups: list[str] = []
    if "age_group" in meta.columns:
        age_groups = sorted(meta["age_group"].dropna().unique().tolist())

    sexes: list[str] = []
    if "sex" in meta.columns:
        sexes = sorted(
            value
            for value in meta["sex"].dropna().astype(str).str.strip().unique().tolist()
            if value in {"male", "female"}
        )

    result = {
        "countries": countries,
        "diseases": diseases[:500],       # limit for payload size / 限制响应大小
        "age_groups": age_groups,
        "sexes": sexes,
    }
    set_cached("filter_options", result)
    return result


@app.get("/api/data-stats",
         summary="Dataset statistics",
         description="Returns total sample count, country count, condition-label counts, and data version.")
@limiter.limit("120/minute")
def data_stats(request: Request):
    """
    Return current dataset statistics for homepage display.
    返回当前数据集统计数据供首页展示
    """
    cached = get_cached("data_stats")
    if cached:
        return cached
    meta = get_metadata()

    # Read version info if exists / 读取版本信息
    version_file = Path(__file__).parent / "data_version.json"
    version_info = {}
    if version_file.exists():
        with open(version_file) as f:
            version_info = json.load(f)

    unique_labels = set(_iter_inform_labels(meta))
    non_nc_condition_labels = {
        label
        for label in unique_labels
        if _label_kind(label) != "healthy_control"
    }
    has_nc_category = any(_label_kind(label) == "healthy_control" for label in unique_labels)

    project_col = get_project_column(meta)
    country_project_counts: dict[str, int] = {}
    if project_col is not None and "country" in meta.columns:
        project_meta = meta[[project_col, "country"]].copy()
        project_meta[project_col] = project_meta[project_col].astype(str).str.strip()
        project_meta["country"] = project_meta["country"].astype(str).str.strip()
        project_meta = project_meta[
            project_meta[project_col].ne("") & project_meta["country"].ne("") & project_meta["country"].ne("unknown")
        ]
        country_project_counts = {
            str(country): int(count)
            for country, count in project_meta.groupby("country")[project_col].nunique().items()
        }

    result = {
        "total_samples": int(len(meta)),
        "total_countries": int(meta.loc[meta["country"] != "unknown", "country"].nunique()) if "country" in meta.columns else 0,
        # Legacy alias kept for compatibility: non-NC condition labels only.
        "total_diseases": len(non_nc_condition_labels),
        "total_non_nc_condition_labels": len(non_nc_condition_labels),
        "total_condition_categories": len(non_nc_condition_labels) + int(has_nc_category),
        "total_projects": count_unique_projects(meta),
        "total_genera": count_unique_genera_from_abundance(),
        "country_project_counts": country_project_counts,
        "last_updated": version_info.get("last_updated", datetime.now().strftime("%Y-%m-%d")),
        "version": version_info.get("version", f"v1.0_{datetime.now().strftime('%Y%m%d')}"),
    }
    set_cached("data_stats", result)
    return result


@app.get("/api/project-timeline",
         summary="Project growth timeline",
         description="Returns yearly sample and project counts for homepage trend views.")
@limiter.limit("120/minute")
def project_timeline(request: Request):
    """Return yearly sample/project counts derived from metadata pubdate."""
    cached = get_cached("project_timeline")
    if cached:
        return cached

    meta = get_metadata().copy()
    project_col = get_project_column(meta)

    if "year" not in meta.columns and "pubdate" in meta.columns:
        meta["year"] = pd.to_datetime(meta["pubdate"], errors="coerce").dt.year

    if "year" not in meta.columns:
        result = {"timeline": []}
        set_cached("project_timeline", result)
        return result

    year_meta = meta.dropna(subset=["year"]).copy()
    if year_meta.empty:
        result = {"timeline": []}
        set_cached("project_timeline", result)
        return result

    year_meta["year"] = year_meta["year"].astype(int)

    if project_col is not None:
        grouped = year_meta.groupby("year").agg(
            n_samples=(project_col, "size"),
            n_projects=(project_col, "nunique"),
        ).reset_index()
    else:
        grouped = year_meta.groupby("year").size().reset_index(name="n_samples")
        grouped["n_projects"] = 0

    result = {
        "timeline": [
            {
                "year": int(row.year),
                "n_samples": int(row.n_samples),
                "n_projects": int(row.n_projects),
            }
            for row in grouped.itertuples(index=False)
        ]
    }
    set_cached("project_timeline", result)
    return result


@app.get("/api/disease-names-zh",
         summary="Disease name translations",
         description="Returns Chinese translations for disease names.")
@limiter.limit("120/minute")
def get_disease_names_zh(request: Request):
    """Return disease name Chinese translations / 返回疾病名称中文翻译字典
    Merges disease_names_zh.json + disease_ontology standard_name_zh"""
    merged = dict(DISEASE_NAMES_ZH)
    # Add translations from ontology that aren't in the manual file
    for key, info in DISEASE_ONTOLOGY.items():
        if key not in merged:
            zh = info.get("standard_name_zh", "")
            if zh:
                merged[key] = zh
    return merged


@app.get("/api/disease-display-names",
         summary="Standardized display names for diseases",
         description="Returns a mapping from raw disease keys to standardized full display names (no abbreviation suffix).")
@limiter.limit("120/minute")
def get_disease_display_names(request: Request):
    """Return standardized display names (full name only, no abbreviation suffix)
    返回标准化疾病显示名称映射（仅全称，不附加缩写括号）"""
    keys = set(DISEASE_ONTOLOGY.keys()) | set(DISEASE_NAMES_EN_FALLBACK.keys())
    keys.update(item["name"] for item in get_disease_list_cached())
    result = {key: disease_to_en(key) for key in sorted(keys, key=_disease_sort_key)}
    return result


@app.post("/api/estimate-sample-count",
          summary="Estimate sample count",
          description="Return matched sample counts for Group A and Group B before running differential analysis.")
@limiter.limit("60/minute")
def estimate_sample_count(request: Request, req: SampleCountRequest):
    meta = get_metadata()
    abund_idx = set(get_abundance().index)

    def summarize(group_filter: GroupFilter) -> dict:
        group_meta = apply_filter(meta, group_filter)
        valid = [key for key in group_meta["sample_key"].values if key in abund_idx]
        return {
            "metadata_n": int(len(group_meta)),
            "abundance_n": int(len(valid)),
        }

    return {
        "group_a": summarize(req.group_a_filter),
        "group_b": summarize(req.group_b_filter),
    }


@app.post("/api/spearman-analysis",
          summary="Spearman correlation analysis",
          description="Compute a top-taxa Spearman correlation matrix for the samples selected by the current Compare workspace filters.")
@limiter.limit("20/minute")
def spearman_analysis(request: Request, req: SpearmanAnalysisRequest):
    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)

    group_a_meta = apply_filter(meta, req.group_a_filter)
    group_b_meta = apply_filter(meta, req.group_b_filter)
    selected_keys = []
    seen: set[str] = set()
    for key in list(group_a_meta["sample_key"].values) + list(group_b_meta["sample_key"].values):
        if key in abund_idx and key not in seen:
            seen.add(key)
            selected_keys.append(key)

    if len(selected_keys) < 6:
        raise HTTPException(400, "Need at least 6 matched samples for Spearman analysis")

    # Cap to 2000 samples to keep computation fast (seed-42 random subsample)
    MAX_SPEARMAN_SAMPLES = 2000
    if len(selected_keys) > MAX_SPEARMAN_SAMPLES:
        import numpy as _np
        _rng = _np.random.default_rng(42)
        selected_keys = [selected_keys[i] for i in sorted(
            _rng.choice(len(selected_keys), MAX_SPEARMAN_SAMPLES, replace=False).tolist()
        )]

    return run_spearman_analysis(
        abundance_df=abund,
        sample_keys=selected_keys,
        taxonomy_level=req.taxonomy_level,
        max_taxa=max(8, min(int(req.max_taxa), 24)),
    )


@app.post("/api/diff-analysis",
          summary="Differential analysis",
          description="Compare microbiome between two groups using Wilcoxon, t-test, LEfSe, or PERMANOVA.")
@limiter.limit("20/minute")
def diff_analysis(request: Request, req: DiffAnalysisRequest):
    """
    Perform differential abundance analysis between two sample groups.
    对两组样本执行差异丰度分析

    Steps:
    1. Filter metadata to get sample IDs for each group
    2. Extract abundance data for those samples
    3. Run Wilcoxon/t-test per taxon
    4. Apply BH correction
    5. Calculate alpha and beta diversity
    """
    meta = get_metadata()
    abund = get_abundance()

    # Get sample subsets / 获取两组样本子集
    group_a_meta = apply_filter(meta, req.group_a_filter)
    group_b_meta = apply_filter(meta, req.group_b_filter)

    if len(group_a_meta) == 0:
        raise HTTPException(400, "Group A: no samples match the given filters")
    if len(group_b_meta) == 0:
        raise HTTPException(400, "Group B: no samples match the given filters")

    # Match sample keys to abundance index / 匹配样本键与丰度矩阵索引
    abund_idx = set(abund.index)
    keys_a = group_a_meta["sample_key"].values
    keys_b = group_b_meta["sample_key"].values

    valid_a = [k for k in keys_a if k in abund_idx]
    valid_b = [k for k in keys_b if k in abund_idx]

    if len(valid_a) == 0:
        raise HTTPException(
            400,
            f"Group A: {len(keys_a)} metadata rows found but none matched "
            f"abundance data. Check sample key format."
        )
    if len(valid_b) == 0:
        raise HTTPException(
            400,
            f"Group B: {len(keys_b)} metadata rows found but none matched "
            f"abundance data."
        )

    def filter_to_label(f: GroupFilter) -> str:
        parts = []
        if f.country:
            parts.append(f.country.title())
        if f.disease:
            parts.append(f.disease)
        if f.age_group:
            parts.append(f.age_group)
        if f.sex:
            parts.append(f.sex.title())
        return "-".join(parts) if parts else "Group"

    return run_compare_analysis(
        abundance_df=abund,
        valid_a=valid_a,
        valid_b=valid_b,
        taxonomy_level=req.taxonomy_level,
        method=req.method,
        group_a_name=filter_to_label(req.group_a_filter),
        group_b_name=filter_to_label(req.group_b_filter),
    )

    # NOTE:
    # The legacy implementation below is unreachable and retained only as historical debt.
    # /api/diff-analysis is contractually served by run_compare_analysis() above.

    # Extract abundance matrices and normalize to relative abundance (%)
    # 提取丰度矩阵并归一化为相对丰度（%）
    raw_a = abund.loc[valid_a].values.astype(float)
    raw_b = abund.loc[valid_b].values.astype(float)
    totals_a = raw_a.sum(axis=1, keepdims=True)
    totals_b = raw_b.sum(axis=1, keepdims=True)
    totals_a[totals_a == 0] = 1
    totals_b[totals_b == 0] = 1
    mat_a = raw_a / totals_a * 100
    mat_b = raw_b / totals_b * 100
    col_names = abund.columns.tolist()

    # ── Aggregate by taxonomy level / 按分类层级聚合 ──────────────────────────
    def group_by_level(matrix: np.ndarray, cols: list[str], level: str):
        """Aggregate columns by taxonomy level. / 按分类层级聚合列"""
        if level == "genus":
            labels = [extract_genus(c) for c in cols]
        elif level == "phylum":
            labels = [extract_phylum(c) for c in cols]
        else:
            labels = [extract_genus(c) for c in cols]

        # Sum columns with same label / 相同标签的列求和
        unique_labels = list(dict.fromkeys(labels))  # preserve order
        agg = np.zeros((matrix.shape[0], len(unique_labels)))
        for i, lbl in enumerate(unique_labels):
            idxs = [j for j, l in enumerate(labels) if l == lbl]
            agg[:, i] = matrix[:, idxs].sum(axis=1)
        return agg, unique_labels

    agg_a, taxa = group_by_level(mat_a, col_names, req.taxonomy_level)
    agg_b, _    = group_by_level(mat_b, col_names, req.taxonomy_level)

    # ── Differential abundance test / 差异丰度统计检验 ─────────────────────────
    # Use wilcoxon as base test for LEfSe/PERMANOVA methods too
    # LEfSe/PERMANOVA 同时运行 wilcoxon 作为基础差异分析
    base_method = req.method if req.method in ("wilcoxon", "t-test") else "wilcoxon"

    diff_results = []
    p_values = []

    for i, taxon in enumerate(taxa):
        vals_a = agg_a[:, i]
        vals_b = agg_b[:, i]

        mean_a = float(np.mean(vals_a))
        mean_b = float(np.mean(vals_b))

        # log2 fold change (add pseudocount to avoid log(0))
        # log2倍数变化（加伪计数避免log(0)）
        pseudo = 1e-6
        log2fc = math.log2((mean_a + pseudo) / (mean_b + pseudo))

        # Statistical test / 统计检验
        u_stat = 0.0
        try:
            if base_method == "wilcoxon":
                mwu = stats.mannwhitneyu(vals_a, vals_b, alternative="two-sided")
                u_stat, p = float(mwu.statistic), float(mwu.pvalue)
            else:
                t_res = stats.ttest_ind(vals_a, vals_b)
                u_stat, p = float(t_res.statistic), float(t_res.pvalue)
        except Exception:
            p = 1.0

        # Effect size / 效应量
        n_a, n_b = len(vals_a), len(vals_b)
        if base_method == "wilcoxon":
            effect_size = float(1 - 2 * u_stat / (n_a * n_b)) if n_a * n_b > 0 else 0.0
        else:
            pooled_std = float(np.std(np.concatenate([vals_a, vals_b])))
            effect_size = float((mean_a - mean_b) / pooled_std) if pooled_std > 0 else 0.0

        p_values.append(float(p))
        diff_results.append({
            "taxon": taxon,
            "mean_a": mean_a,
            "mean_b": mean_b,
            "log2fc": log2fc,
            "p_value": float(p),
            "adjusted_p": 0.0,
            "effect_size": effect_size,
        })

    # BH correction / BH多重校正
    adj_p = bh_correction(p_values)
    for i, row in enumerate(diff_results):
        row["adjusted_p"] = adj_p[i]

    # Sort by adjusted p-value / 按校正p值排序
    diff_results.sort(key=lambda x: x["adjusted_p"])

    # ── LEfSe analysis (if requested) / LEfSe 分析 ───────────────────────────
    lefse_results = None
    if req.method == "lefse":
        lefse_results = lefse_analysis(agg_a, agg_b, taxa)

    # ── PERMANOVA (if requested) / PERMANOVA 分析 ────────────────────────────
    permanova_result = None
    if req.method == "permanova":
        permanova_result = permanova_test(agg_a, agg_b)

    # ── Alpha diversity / Alpha多样性 ─────────────────────────────────────────
    shannon_a = [shannon_diversity(r) for r in agg_a]
    simpson_a = [simpson_diversity(r) for r in agg_a]
    shannon_b = [shannon_diversity(r) for r in agg_b]
    simpson_b = [simpson_diversity(r) for r in agg_b]

    alpha_diversity = {
        "group_a": {
            "shannon": shannon_a[:500],   # limit payload size / 限制响应大小
            "simpson": simpson_a[:500],
        },
        "group_b": {
            "shannon": shannon_b[:500],
            "simpson": simpson_b[:500],
        },
    }

    # ── Beta diversity PCoA / Beta多样性PCoA ──────────────────────────────────
    pcoa_coords = bray_curtis_pcoa(agg_a, agg_b, max_samples=150)

    # Build group name labels / 构建组名
    def filter_to_label(f: GroupFilter) -> str:
        parts = []
        if f.country:
            parts.append(f.country.title())
        if f.disease:
            parts.append(f.disease)
        if f.age_group:
            parts.append(f.age_group)
        if f.sex:
            parts.append(f.sex.title())
        return "-".join(parts) if parts else "Group"

    group_a_name = filter_to_label(req.group_a_filter)
    group_b_name = filter_to_label(req.group_b_filter)

    response = {
        "summary": {
            "group_a_name": group_a_name,
            "group_b_name": group_b_name,
            "group_a_n": len(valid_a),
            "group_b_n": len(valid_b),
            "taxonomy_level": req.taxonomy_level,
            "method": req.method,
            "total_taxa": len(taxa),
        },
        "diff_taxa": diff_results[:200],   # top 200 by significance / 按显著性取前200
        "alpha_diversity": alpha_diversity,
        "beta_diversity": {
            "pcoa_coords": pcoa_coords,
        },
    }

    # Attach LEfSe and PERMANOVA results if computed
    # 附加 LEfSe 和 PERMANOVA 结果
    if lefse_results is not None:
        response["lefse_results"] = lefse_results
    if permanova_result is not None:
        response["permanova"] = permanova_result

    return response


# ── Phenotype Association Analysis / 表型关联分析端点 ─────────────────────────

def _get_samples_by_pheno(meta: pd.DataFrame, dim_type: str, group: str) -> pd.Series:
    """
    Return boolean mask of samples belonging to a phenotype group.
    返回属于某表型分组的样本布尔掩码
    """
    if dim_type == "disease":
        return _inform_label_mask(meta, group)
    elif dim_type == "age":
        if "age_group" in meta.columns:
            return meta["age_group"].str.lower() == group.lower()
        return pd.Series(False, index=meta.index)
    elif dim_type == "sex":
        if "sex" in meta.columns:
            return meta["sex"].str.lower() == group.lower()
        return pd.Series(False, index=meta.index)
    return pd.Series(False, index=meta.index)


# End of unreachable legacy implementation for /api/diff-analysis.
@app.get("/api/phenotype-groups",
         summary="List phenotype groups",
         description="Return all available groups for a dimension type with sample counts.")
@limiter.limit("60/minute")
def phenotype_groups(request: Request, dim_type: str = "disease"):
    """
    Return groups and sample counts for a given dimension type.
    返回某维度下所有分组及其样本量
    """
    meta = get_metadata()
    if dim_type == "disease":
        primary_counts = _inform_label_counts(meta, include_nc=True)
        groups = [
            {"group": disease, "sample_count": int(count)}
            for disease, count in sorted(
                primary_counts.items(),
                key=lambda item: (0 if item[0] == "NC" else 1, -item[1], item[0].lower()),
            )
        ]
        return {"dim_type": dim_type, "groups": groups}
    elif dim_type == "age":
        if "age_group" not in meta.columns:
            return {"dim_type": dim_type, "groups": []}
        vc = meta["age_group"].dropna().astype(str).value_counts()
        groups = [{"group": k, "sample_count": int(v)} for k, v in vc.items() if k.strip()]
        return {"dim_type": dim_type, "groups": groups}
    elif dim_type == "sex":
        if "sex" not in meta.columns:
            return {"dim_type": dim_type, "groups": []}
        vc = meta["sex"].dropna().astype(str).value_counts()
        groups = [{"group": k, "sample_count": int(v)} for k, v in vc.items() if k.strip() and k in {"male", "female"}]
        return {"dim_type": dim_type, "groups": groups}
    return {"dim_type": dim_type, "groups": []}


@app.get("/api/phenotype-association",
         summary="Phenotype association analysis",
         description="Mann-Whitney U + BH-FDR per taxon between two phenotype groups.")
@limiter.limit("10/minute")
def phenotype_association(
    request: Request,
    dim_type: str = "sex",
    group_a: str = "female",
    group_b: str = "male",
    tax_level: str = "genus",      # genus | phylum
    min_prevalence: float = 0.10,  # minimum prevalence filter (10%)
    top_n: int = 100,              # max results to return
):
    """
    Core phenotype association analysis.
    核心表型关联分析：Mann-Whitney U + BH-FDR + 效应量 + 流行率 + 门注释
    """
    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)

    # Get sample masks
    mask_a = _get_samples_by_pheno(meta, dim_type, group_a)
    mask_b = _get_samples_by_pheno(meta, dim_type, group_b)

    if not mask_a.any():
        raise HTTPException(400, f"Group A '{group_a}' has no samples for dim_type='{dim_type}'")
    if not mask_b.any():
        raise HTTPException(400, f"Group B '{group_b}' has no samples for dim_type='{dim_type}'")

    # Match to abundance matrix
    keys_a = meta.loc[mask_a, "sample_key"].values
    keys_b = meta.loc[mask_b, "sample_key"].values
    valid_a = [k for k in keys_a if k in abund_idx]
    valid_b = [k for k in keys_b if k in abund_idx]

    if len(valid_a) == 0:
        raise HTTPException(400, f"Group A: no matched samples in abundance data")
    if len(valid_b) == 0:
        raise HTTPException(400, f"Group B: no matched samples in abundance data")

    # Normalize to relative abundance
    raw_a = abund.loc[valid_a].values.astype(float)
    raw_b = abund.loc[valid_b].values.astype(float)
    totals_a = raw_a.sum(axis=1, keepdims=True); totals_a[totals_a == 0] = 1
    totals_b = raw_b.sum(axis=1, keepdims=True); totals_b[totals_b == 0] = 1
    mat_a = raw_a / totals_a * 100
    mat_b = raw_b / totals_b * 100
    col_names = abund.columns.tolist()

    # Aggregate by taxonomy level
    if tax_level == "phylum":
        labels = [extract_phylum(c) for c in col_names]
    else:
        labels = [extract_genus(c) for c in col_names]

    unique_labels = list(dict.fromkeys(labels))
    n_taxa = len(unique_labels)
    agg_a = np.zeros((mat_a.shape[0], n_taxa))
    agg_b = np.zeros((mat_b.shape[0], n_taxa))
    for i, lbl in enumerate(unique_labels):
        idxs = [j for j, l in enumerate(labels) if l == lbl]
        agg_a[:, i] = mat_a[:, idxs].sum(axis=1)
        agg_b[:, i] = mat_b[:, idxs].sum(axis=1)

    # Build phylum lookup for genus-level (for coloring)
    phylum_map: dict[str, str] = {}
    for col in col_names:
        parts = col.split(".")
        g = parts[-1] if parts else col
        p = parts[1] if len(parts) > 1 else ""
        phylum_map[g] = p

    n_a, n_b = mat_a.shape[0], mat_b.shape[0]
    pseudo = 1e-6
    results = []
    p_values = []

    for i, taxon in enumerate(unique_labels):
        vals_a = agg_a[:, i]
        vals_b = agg_b[:, i]

        # Prevalence filter — skip rare taxa
        prev_a = float((vals_a > 0).mean())
        prev_b = float((vals_b > 0).mean())
        if max(prev_a, prev_b) < min_prevalence:
            continue

        mean_a = float(np.mean(vals_a))
        mean_b = float(np.mean(vals_b))
        median_a = float(np.median(vals_a))
        median_b = float(np.median(vals_b))
        log2fc = float(math.log2((mean_a + pseudo) / (mean_b + pseudo)))

        # Mann-Whitney U
        try:
            mwu = stats.mannwhitneyu(vals_a, vals_b, alternative="two-sided")
            u_stat, p = float(mwu.statistic), float(mwu.pvalue)
        except Exception:
            u_stat, p = 0.0, 1.0

        # Rank-biserial correlation as effect size
        effect_size = float(1 - 2 * u_stat / (n_a * n_b)) if n_a * n_b > 0 else 0.0

        # 95% CI for mean difference (bootstrap approximation via SE)
        pooled_n = n_a + n_b
        se = float(np.std(np.concatenate([vals_a, vals_b])) / math.sqrt(pooled_n)) if pooled_n > 1 else 0.0
        ci_low = float((mean_a - mean_b) - 1.96 * se)
        ci_high = float((mean_a - mean_b) + 1.96 * se)

        phylum = phylum_map.get(taxon, "") if tax_level == "genus" else ""

        p_values.append(p)
        results.append({
            "taxon": taxon,
            "rank": tax_level,
            "phylum": phylum,
            "mean_a": round(mean_a, 6),
            "mean_b": round(mean_b, 6),
            "median_a": round(median_a, 6),
            "median_b": round(median_b, 6),
            "prevalence_a": round(prev_a, 4),
            "prevalence_b": round(prev_b, 4),
            "n_a": n_a,
            "n_b": n_b,
            "log2fc": round(log2fc, 4),
            "lda_score": None,
            "effect_size": round(effect_size, 4),
            "p_value": p,
            "adjusted_p": 0.0,  # filled below
            "enriched_in": "a" if log2fc > 0 else ("b" if log2fc < 0 else "none"),
            "ci_low": round(ci_low, 6),
            "ci_high": round(ci_high, 6),
        })

    # BH correction
    adj_p = bh_correction(p_values)
    for i, row in enumerate(results):
        row["adjusted_p"] = adj_p[i]

    # Sort by adjusted p, then |log2fc|
    results.sort(key=lambda x: (x["adjusted_p"], -abs(x["log2fc"])))

    sig_count = sum(1 for r in results if r["adjusted_p"] < 0.05)
    top_results = results[:top_n]

    return {
        "group_a": group_a,
        "group_b": group_b,
        "dim_type": dim_type,
        "tax_level": tax_level,
        "method": "mannwhitneyu_bh",
        "n_a": n_a,
        "n_b": n_b,
        "total_taxa": len(results),
        "significant_count": sig_count,
        "results": top_results,
    }


@app.get("/api/phenotype-taxa-profile",
         summary="Taxa abundance distribution per group",
         description="Return per-group quantile distributions for a specific taxon (for boxplot).")
@limiter.limit("30/minute")
def phenotype_taxa_profile(
    request: Request,
    taxon: str,
    dim_type: str = "sex",
):
    """
    Return abundance distribution (Q1/Q3/median/whiskers) per phenotype group.
    返回某分类在各表型分组的丰度分布（用于箱线图）
    """
    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)
    col_names = abund.columns.tolist()

    # Find taxon column indices
    taxon_lower = taxon.strip().lower()
    taxon_idxs = [j for j, c in enumerate(col_names) if extract_genus(c).lower() == taxon_lower
                  or extract_phylum(c).lower() == taxon_lower]
    if not taxon_idxs:
        raise HTTPException(404, f"Taxon '{taxon}' not found in abundance data")

    # Get all phenotype groups
    if dim_type == "disease":
        INFORM_COLS = [f"inform{i}" for i in range(12)]
        all_groups: set[str] = set()
        for col in INFORM_COLS:
            if col in meta.columns:
                vals = meta[col].dropna().astype(str).str.strip()
                all_groups.update(v for v in vals if v and v != "nan" and v != "NC")
        groups = sorted(all_groups)[:50]  # limit to top 50 disease groups
    elif dim_type == "age":
        groups = meta["age_group"].dropna().unique().tolist() if "age_group" in meta.columns else []
    elif dim_type == "sex":
        groups = meta["sex"].dropna().unique().tolist() if "sex" in meta.columns else []
    else:
        groups = []

    profile_data = []
    for group in groups:
        mask = _get_samples_by_pheno(meta, dim_type, group)
        keys = meta.loc[mask, "sample_key"].values
        valid_keys = [k for k in keys if k in abund_idx]
        if len(valid_keys) < 5:
            continue

        vals = abund.loc[valid_keys].values[:, taxon_idxs].sum(axis=1).astype(float)
        totals = abund.loc[valid_keys].values.sum(axis=1).astype(float)
        totals[totals == 0] = 1
        rel = vals / totals * 100

        q1, q3 = float(np.percentile(rel, 25)), float(np.percentile(rel, 75))
        iqr = q3 - q1
        profile_data.append({
            "group": group,
            "n": len(valid_keys),
            "median": round(float(np.median(rel)), 6),
            "mean": round(float(np.mean(rel)), 6),
            "q1": round(q1, 6),
            "q3": round(q3, 6),
            "whisker_low": round(max(float(rel.min()), q1 - 1.5 * iqr), 6),
            "whisker_high": round(min(float(rel.max()), q3 + 1.5 * iqr), 6),
            "outliers": [round(float(v), 6) for v in rel if v < q1 - 1.5 * iqr or v > q3 + 1.5 * iqr][:20],
        })

    profile_data.sort(key=lambda x: -x["median"])
    return {"taxon": taxon, "dim_type": dim_type, "groups": profile_data}


# ── Species search & profile endpoints / 物种搜索与画像端点 ──────────────────

# Invalid genus names to filter out (taxonomy parsing artifacts)
# 需过滤的无效属名（分类学解析产生的噪音）
_INVALID_GENERA = {"NA", "group", "Sedis", "Incertae", "unclassified",
                   "uncultured", "Unknown", "unknown", "noname", "002", "sp"}


def is_valid_genus(name: str) -> bool:
    """Check if genus name is a real biological name. / 检查属名是否为真实生物名"""
    if not name or len(name) < 3:
        return False
    if name in _INVALID_GENERA:
        return False
    if name[0].islower():  # real genus names are capitalized / 真实属名首字母大写
        return False
    if name.isdigit():
        return False
    return True


@lru_cache(maxsize=1)
def get_genus_list() -> list[str]:
    """Return sorted list of all valid genus names from abundance data. / 返回丰度数据中所有有效属名"""
    abund = get_abundance()
    genera = sorted(set(g for g in (extract_genus(c) for c in abund.columns) if is_valid_genus(g)))
    return genera


@app.get("/api/species-search",
         summary="Search genera",
         description="Full-text search across genus names, returns up to 20 matches.")
@limiter.limit("120/minute")
def species_search(request: Request, q: str = ""):
    """
    Autocomplete genus search. Returns up to 20 matching genus names.
    属名自动补全搜索，返回最多20个匹配结果
    """
    if not q or len(q.strip()) < 2:
        return {"results": []}
    q_lower = q.strip().lower()
    genera = get_genus_list()
    # Prefix matches first, then contains matches / 前缀匹配优先，其次包含匹配
    prefix = [g for g in genera if g.lower().startswith(q_lower)]
    contains = [g for g in genera if q_lower in g.lower() and g not in prefix]
    return {"results": (prefix + contains)[:20]}


@app.get("/api/species-profile",
         summary="Genus profile",
         description="Detailed abundance profile for a genus across diseases, countries, age groups, and sex.")
@limiter.limit("60/minute")
def species_profile(request: Request, genus: str):
    """
    Return a comprehensive profile for a given genus:
    为给定属返回综合画像：丰度/疾病/国家/年龄/性别分布
    """
    cache_key = f"species_profile:{genus.strip().lower()}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    meta = get_metadata()
    abund = get_abundance()

    if not genus or not genus.strip():
        raise HTTPException(400, "genus parameter is required")

    genus = genus.strip()

    # Find matching columns in abundance matrix / 在丰度矩阵中查找匹配列
    matching_cols = [c for c in abund.columns if extract_genus(c).lower() == genus.lower()]
    if not matching_cols:
        raise HTTPException(404, f"Genus '{genus}' not found in abundance data")

    # Resolve the canonical genus name / 解析标准属名
    canonical_name = extract_genus(matching_cols[0])

    # Sum abundance across all matching columns for the genus
    # 对该属的所有匹配列求和
    genus_raw = abund[matching_cols].sum(axis=1)

    # Convert to relative abundance (%) per sample
    # 转换为每样本相对丰度（%）
    sample_totals = abund.sum(axis=1).replace(0, 1)
    genus_abundance = genus_raw / sample_totals * 100

    # Match to metadata / 与元数据关联
    meta_with_key = meta.set_index("sample_key")
    common_keys = genus_abundance.index.intersection(meta_with_key.index)

    if len(common_keys) == 0:
        raise HTTPException(404, "No matching samples between abundance and metadata")

    ga = genus_abundance.loc[common_keys]
    mm = meta_with_key.loc[common_keys]

    # Overall stats / 总体统计
    present_count = int((ga > 0).sum())
    total_count = len(ga)
    mean_abundance = float(ga.mean())
    median_abundance = float(np.median(ga.values.astype(float))) if total_count > 0 else 0.0
    prevalence = present_count / total_count if total_count > 0 else 0
    phylum = extract_phylum(matching_cols[0]) or "Other"

    # Strict NC baseline / 严格健康对照基线
    inform_cols = [f"inform{i}" for i in range(12)]
    nc_mask = _strict_nc_mask(mm, inform_cols)
    nc_vals = ga.loc[nc_mask].values.astype(float)
    nc_mean_val = float(np.mean(nc_vals)) if len(nc_vals) > 0 else 0.0
    nc_prevalence_val = float((nc_vals > 0).sum() / len(nc_vals)) if len(nc_vals) > 0 else 0.0

    AGE_GROUP_ORDER = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian", "Unknown"]

    # Helper: group mean abundance / 辅助函数：按分组计算平均丰度
    def group_means(col_name: str, top_n: int = 30, order: list[str] | None = None) -> list[dict]:
        if col_name not in mm.columns:
            return []
        grouped = mm.groupby(mm[col_name].fillna("unknown").astype(str).str.strip())
        results = []
        for name, group_idx in grouped.groups.items():
            if not name or name == "nan" or name == "unknown":
                continue
            vals = ga.loc[group_idx]
            display_name = iso_to_name(name) if col_name == "country" else name
            results.append({
                "name": display_name,
                "mean_abundance": round(float(vals.mean()), 8),
                "median_abundance": round(float(np.median(vals)), 8),
                "std_abundance": round(float(np.std(vals)), 8),
                "p25": round(float(np.percentile(vals, 25)), 8),
                "p75": round(float(np.percentile(vals, 75)), 8),
                "prevalence": round(float((vals > 0).sum() / len(vals)), 4),
                "sample_count": len(vals),
            })
        if order:
            order_index = {item: idx for idx, item in enumerate(order)}
            results.sort(key=lambda x: (order_index.get(x["name"], len(order_index)), -x["sample_count"]))
        else:
            results.sort(key=lambda x: x["mean_abundance"], reverse=True)
        return results[:top_n]

    # Disease distribution (from inform0-11) / 疾病分布（来自inform0-11）
    disease_samples: dict[str, set[str]] = {}
    for col in inform_cols:
        if col not in mm.columns:
            continue
        for sample_key, disease_name in mm[col].fillna("").astype(str).str.strip().items():
            disease_name = _normalize_inform_label(disease_name)
            if not disease_name or disease_name.lower() == "nc":
                continue
            disease_samples.setdefault(disease_name, set()).add(str(sample_key))

    pseudo = 1e-6
    by_disease = []
    disease_pvals: list[float] = []
    for disease_name, sample_ids in disease_samples.items():
        valid_ids = ga.index.intersection(list(sample_ids))
        if len(valid_ids) == 0:
            continue
        arr = ga.loc[valid_ids].values.astype(float)
        mean_dis = float(arr.mean())
        log2fc = float(math.log2((mean_dis + pseudo) / (nc_mean_val + pseudo)))
        p = 1.0
        effect_size = 0.0
        if len(arr) >= 5 and len(nc_vals) >= 5:
            try:
                u_stat, p = stats.mannwhitneyu(arr, nc_vals, alternative="two-sided")
                p = float(p)
                effect_size = float(1 - 2 * float(u_stat) / (len(arr) * len(nc_vals)))
            except Exception:
                p = 1.0
                effect_size = 0.0
        by_disease.append({
            "name": disease_name,
            "mean_abundance": round(mean_dis, 8),
            "median_abundance": round(float(np.median(arr)), 8),
            "std_abundance": round(float(np.std(arr)), 8),
            "p25": round(float(np.percentile(arr, 25)), 8),
            "p75": round(float(np.percentile(arr, 75)), 8),
            "prevalence": round(float((arr > 0).sum() / len(arr)), 4),
            "sample_count": len(arr),
            "log2fc": round(log2fc, 4),
            "p_value": round(p, 8),
            "effect_size": round(effect_size, 4),
        })
        disease_pvals.append(p)

    if by_disease:
        adjusted = bh_correction(disease_pvals)
        for idx, entry in enumerate(by_disease):
            entry["adjusted_p"] = round(float(adjusted[idx]), 8)
            entry["significant"] = bool(adjusted[idx] < 0.05)
    by_disease.sort(key=lambda x: abs(float(x.get("log2fc", 0.0))), reverse=True)

    result = {
        "genus": canonical_name,
        "phylum": phylum,
        "total_samples": total_count,
        "present_samples": present_count,
        "prevalence": round(prevalence, 4),
        "mean_abundance": round(mean_abundance, 8),
        "median_abundance": round(median_abundance, 8),
        "nc_mean": round(nc_mean_val, 8),
        "nc_prevalence": round(nc_prevalence_val, 4),
        "by_disease": by_disease[:100],
        "by_country": group_means("country", 30),
        "by_age_group": group_means("age_group" if "age_group" in mm.columns else "age", 10, AGE_GROUP_ORDER),
        "by_sex": group_means("sex", 5),
    }
    set_cached(cache_key, result)
    return result


# ── Biomarker profile endpoint / 跨疾病标志物画像端点 ─────────────────────────

@app.get("/api/biomarker-profile",
         summary="Cross-disease biomarker profile",
         description="For a given genus, compute log2 fold change vs healthy controls across all diseases.",
         tags=["Species"])
@limiter.limit("30/minute")
def biomarker_profile(request: Request, genus: str, min_samples: int = 10):
    """
    Cross-disease biomarker profile: for a given genus, compute its differential
    abundance (log2FC + Wilcoxon p) against healthy controls in every disease.
    跨疾病标志物画像：计算给定属在每种疾病中相对健康对照的差异丰度
    """
    if not genus or not genus.strip():
        raise HTTPException(400, "genus parameter is required")

    cache_key = f"biomarker_profile:{genus.strip().lower()}:{min_samples}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    genus = genus.strip()

    meta = get_metadata()
    abund = get_abundance()

    # Find genus columns / 查找属列
    matching_cols = [c for c in abund.columns if extract_genus(c).lower() == genus.lower()]
    if not matching_cols:
        raise HTTPException(404, f"Genus '{genus}' not found in abundance data")

    canonical_name = extract_genus(matching_cols[0])

    # Compute genus relative abundance / 计算属相对丰度
    genus_raw = abund[matching_cols].sum(axis=1)
    sample_totals = abund.sum(axis=1).replace(0, 1)
    genus_rel = (genus_raw / sample_totals * 100).rename("rel_abund")

    # Link metadata / 关联元数据
    meta_keyed = meta.set_index("sample_key")
    common = genus_rel.index.intersection(meta_keyed.index)
    ga = genus_rel.loc[common]
    mm = meta_keyed.loc[common]

    INFORM_COLS = [f"inform{i}" for i in range(12)]
    nc_mask = _strict_nc_mask(mm, INFORM_COLS)
    nc_vals = ga.loc[nc_mask].values.astype(float)

    if len(nc_vals) < min_samples:
        raise HTTPException(400, f"Too few NC samples ({len(nc_vals)})")

    nc_mean = float(np.mean(nc_vals))

    # Collect disease groups / 收集疾病分组
    disease_samples: dict[str, list[str]] = {}
    for col in INFORM_COLS:
        if col not in mm.columns:
            continue
        for idx, val in mm[col].fillna("").astype(str).str.strip().items():
            normalized = _normalize_inform_label(val)
            if not normalized or normalized.lower() == "nc":
                continue
            disease_samples.setdefault(normalized, []).append(str(idx))

    # Compute log2FC and Wilcoxon p for each disease / 计算每种疾病的log2FC和Wilcoxon p
    pseudo = 1e-6
    results = []
    for disease_name, sample_ids in disease_samples.items():
        unique_ids = list(set(sample_ids))
        valid_ids = ga.index.intersection(unique_ids)
        if len(valid_ids) < min_samples:
            continue

        d_vals = ga.loc[valid_ids].values.astype(float)
        d_mean = float(np.mean(d_vals))
        log2fc = float(math.log2((d_mean + pseudo) / (nc_mean + pseudo)))

        try:
            u_stat, p = stats.mannwhitneyu(d_vals, nc_vals, alternative="two-sided")
            p = float(p)
            effect_size = float(1 - 2 * float(u_stat) / (len(d_vals) * len(nc_vals)))
        except Exception:
            p = 1.0
            effect_size = 0.0

        direction = "enriched" if log2fc > 0 else "depleted"
        results.append({
            "disease": disease_name,
            "n_samples": len(valid_ids),
            "n_control": len(nc_vals),
            "mean_disease": round(d_mean, 6),
            "mean_control": round(nc_mean, 6),
            "log2fc": round(log2fc, 4),
            "p_value": round(p, 8),
            "direction": direction,
            "effect_size": round(effect_size, 4),
            "prevalence_disease": round(float((d_vals > 0).sum() / len(d_vals)), 4),
            "prevalence_control": round(float((nc_vals > 0).sum() / len(nc_vals)), 4),
        })

    # BH FDR correction / BH FDR校正
    if results:
        p_vals = [r["p_value"] for r in results]
        adj_p = bh_correction(p_vals)
        for i, r in enumerate(results):
            r["adjusted_p"] = round(adj_p[i], 8)
            r["significant"] = adj_p[i] < 0.05

    results.sort(key=lambda x: abs(x["log2fc"]), reverse=True)

    n_enriched = sum(1 for r in results if r.get("significant") and r["direction"] == "enriched")
    n_depleted = sum(1 for r in results if r.get("significant") and r["direction"] == "depleted")

    result = {
        "genus": canonical_name,
        "n_diseases_tested": len(results),
        "n_enriched": n_enriched,
        "n_depleted": n_depleted,
        "n_control": len(nc_vals),
        "control_mean": round(nc_mean, 6),
        "profiles": results,
    }
    set_cached(cache_key, result)
    return result


# ── Disease ontology endpoint / 疾病本体端点 ──────────────────────────────────

@app.get("/api/disease-ontology", tags=["Disease"],
         summary="Disease ontology mapping",
         description="Returns standardized disease names, MeSH IDs, ICD-10 codes, and categories for all diseases")
@limiter.limit("120/minute")
def disease_ontology(request: Request):
    return DISEASE_ONTOLOGY


# ── Disease browser endpoints / 疾病浏览端点 ─────────────────────────────────

@lru_cache(maxsize=1)
def get_disease_list_cached() -> list[dict]:
    """
    Build a list of all unique diseases with sample counts.
    构建所有疾病及其样本数的列表
    """
    meta = get_metadata()
    disease_counts = _inform_label_counts(meta, include_nc=True)
    result = [
        {"name": label, "sample_count": count, "kind": _label_kind(label)}
        for label, count in disease_counts.items()
    ]
    result.sort(key=lambda item: (0 if item["name"] == "NC" else 1, -int(item["sample_count"]), str(item["name"]).lower()))
    return result


@app.get("/api/disease-list",
         summary="List all diseases",
         description="Returns all diseases with sample counts, sorted by frequency.")
@limiter.limit("120/minute")
def disease_list(request: Request, q: str = ""):
    """
    Return all diseases with sample counts. Optional search filter.
    返回所有疾病及样本数，可选搜索过滤
    """
    diseases = get_disease_list_cached()
    if q and q.strip():
        q_lower = q.strip().lower()
        diseases = [d for d in diseases if q_lower in d["name"].lower()]
    # Enrich each disease with ontology info / 为每个疾病附加本体信息
    enriched = []
    for d in diseases:
        entry = dict(d)
        onto = DISEASE_ONTOLOGY.get(d["name"], {})
        kind = entry.get("kind", _label_kind(entry["name"]))
        entry.update({
            "standard_name": onto.get("standard_name", ""),
            "standard_name_zh": onto.get("standard_name_zh", ""),
            "abbreviation": onto.get("abbreviation", ""),
            "mesh_id": onto.get("mesh_id", ""),
            "icd10": onto.get("icd10", ""),
            "category": onto.get("category", ""),
            "category_zh": onto.get("category_zh", ""),
            "kind": kind,
        })
        enriched.append(entry)
    return {"diseases": enriched}


@app.get(
    "/api/metabolism-category-profile",
    tags=["Metabolism"],
    summary="Metabolism category disease comparison",
    description="Category abundance score per disease versus strict NC controls.",
)
@limiter.limit("60/minute")
def metabolism_category_profile(request: Request, category_id: str):
    """Return disease-vs-NC statistics for a curated metabolism category."""
    mapping = load_metabolism_mapping()
    category = next((item for item in mapping.get("categories", []) if item.get("id") == category_id), None)
    if category is None:
        raise HTTPException(status_code=404, detail=f"Category '{category_id}' not found")

    cache_key = f"metabolism_profile:{category_id}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    context = _prepare_metabolism_context(get_metadata(), get_abundance())
    result = _metabolism_category_result(category, context, METABOLISM_PROFILE_DISEASE_LIMIT)
    set_cached(cache_key, result)
    return result


@app.get(
    "/api/metabolism-overview",
    tags=["Metabolism"],
    summary="Metabolism overview heatmap",
    description="15-category by top-20-disease overview matrix using category abundance scores.",
)
@limiter.limit("60/minute")
def metabolism_overview(request: Request):
    """Return overview heatmap data for the metabolism module."""
    cache_key = "metabolism_overview"
    cached = get_cached(cache_key)
    if cached:
        return cached

    mapping = load_metabolism_mapping()
    context = _prepare_metabolism_context(get_metadata(), get_abundance())
    result = _build_metabolism_overview(mapping, context)
    set_cached(cache_key, result, ttl=METABOLISM_OVERVIEW_TTL)
    return result


@app.get("/api/disease-profile",
         summary="Disease microbiome profile",
         description="Top genera for a disease vs healthy controls with fold-change and prevalence.")
@limiter.limit("60/minute")
def disease_profile(request: Request, disease: str, top_n: int = 40):
    """
    Return a profile for a given disease:
    为给定疾病返回画像：
    - Top N genera by mean abundance in disease samples
    - Comparison with healthy control (control = samples with no disease annotation)
    - Age group distribution
    - Sex distribution
    - Country distribution
    """
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")
    disease = disease.strip()

    cache_key = f"disease_profile:{disease}:{top_n}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    meta = get_metadata()
    abund = get_abundance()
    onto = DISEASE_ONTOLOGY.get(disease, {})

    try:
        result = build_disease_profile(meta, abund, disease, top_n=top_n)
    except ValueError as exc:
        message = str(exc)
        if "not found" in message.lower():
            raise HTTPException(404, message)
        raise HTTPException(400, message)

    result.update({
        "standard_name": onto.get("standard_name", ""),
        "standard_name_zh": onto.get("standard_name_zh", ""),
        "abbreviation": onto.get("abbreviation", ""),
        "mesh_id": onto.get("mesh_id", ""),
        "icd10": onto.get("icd10", ""),
        "category": onto.get("category", ""),
        "category_zh": onto.get("category_zh", ""),
    })
    set_cached(cache_key, result)
    return result


# ── Microbe-disease network endpoint / 菌群-疾病网络端点 ─────────────────────

@app.get("/api/disease-studies",
         summary="Disease study breakdown",
         description="Returns per-project disease/control counts, dominant country, top marker, and consistency score.")
@limiter.limit("60/minute")
def disease_studies(request: Request, disease: str):
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")
    disease = disease.strip()

    cache_key = f"disease_studies:{disease}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    try:
        result = build_disease_studies(get_metadata(), get_abundance(), disease)
    except ValueError as exc:
        message = str(exc)
        if "not found" in message.lower():
            raise HTTPException(404, message)
        raise HTTPException(400, message)

    set_cached(cache_key, result)
    return result


def _select_network_sample_keys(meta: pd.DataFrame, abund: pd.DataFrame, disease: str, max_samples: int) -> np.ndarray:
    """Select disease-specific or NC sample keys that exist in abundance data."""
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    target = disease.strip()

    mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        values = meta[col].fillna("").astype(str).str.strip()
        if target:
            mask |= values == target
        else:
            mask |= values.str.upper() == "NC"

    sample_keys = meta.loc[mask, "sample_key"].dropna().unique()
    valid_keys = abund.index.intersection(sample_keys).to_numpy()
    if len(valid_keys) < 10:
        raise HTTPException(400, "Too few samples for correlation analysis")

    np.random.seed(42)
    if len(valid_keys) > max_samples:
        idx = np.random.choice(len(valid_keys), max_samples, replace=False)
        valid_keys = valid_keys[idx]
    return valid_keys


def _build_genus_matrix(
    abund: pd.DataFrame,
    valid_keys: np.ndarray,
    top_genera: int,
) -> tuple[np.ndarray, np.ndarray, list[str], dict[str, str], dict[str, float]]:
    """Aggregate abundance columns to genus-level count and relative-abundance matrices."""
    raw = abund.loc[valid_keys].values.astype(float)
    totals = raw.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    rel = raw / totals * 100

    genus_indices: dict[str, list[int]] = {}
    genus_phylum: dict[str, str] = {}
    for idx, col_name in enumerate(abund.columns.tolist()):
        genus = extract_genus(col_name)
        if not is_valid_genus(genus):
            continue
        genus_indices.setdefault(genus, []).append(idx)
        genus_phylum.setdefault(genus, extract_phylum(col_name))

    genus_means: list[tuple[str, float]] = []
    for genus, idxs in genus_indices.items():
        genus_means.append((genus, float(rel[:, idxs].sum(axis=1).mean())))

    genus_means.sort(key=lambda item: item[1], reverse=True)
    top_names = [name for name, _mean in genus_means[:top_genera]]

    genus_count_matrix = np.zeros((len(valid_keys), len(top_names)))
    genus_rel_matrix = np.zeros((len(valid_keys), len(top_names)))
    mean_map: dict[str, float] = {}
    for col_idx, genus in enumerate(top_names):
        count_values = raw[:, genus_indices[genus]].sum(axis=1)
        rel_values = rel[:, genus_indices[genus]].sum(axis=1)
        genus_count_matrix[:, col_idx] = count_values
        genus_rel_matrix[:, col_idx] = rel_values
        mean_map[genus] = float(rel_values.mean())

    top_phylum = {genus: genus_phylum.get(genus, "Other") for genus in top_names}
    return genus_count_matrix, genus_rel_matrix, top_names, top_phylum, mean_map


def _build_cooccurrence_result(
    meta: pd.DataFrame,
    abund: pd.DataFrame,
    disease: str,
    min_r: float,
    top_genera: int,
    max_samples: int,
    method: str,
    fdr_threshold: float,
) -> dict:
    """Build a fully decorated co-occurrence network payload for the frontend."""
    methods = available_network_methods()
    if not methods.get(method, False):
        if method == "sparcc":
            raise HTTPException(
                400,
                "SparCC is unavailable because the FastSpar wrappers under E:\\tools\\fastspar are missing or not executable.",
            )
        raise HTTPException(400, NETWORK_METHOD_NOTES.get(method, "Unsupported network method"))

    valid_keys = _select_network_sample_keys(meta, abund, disease, max_samples)
    genus_count_matrix, genus_rel_matrix, top_names, phylum_map, mean_map = _build_genus_matrix(abund, valid_keys, top_genera)

    try:
        if method == "sparcc":
            correlation_result = fastspar_cooccurrence(
                genus_count_matrix,
                top_names,
                phylum_map=phylum_map,
                min_prevalence=0.1,
                min_abs_r=min_r,
                fdr_threshold=fdr_threshold,
                max_pairs=500,
            )
        else:
            correlation_result = spearman_cooccurrence(
                genus_rel_matrix,
                top_names,
                phylum_map=phylum_map,
                min_prevalence=0.1,
                min_abs_r=min_r,
                fdr_threshold=fdr_threshold,
                max_pairs=500,
            )
    except RuntimeError as exc:
        raise HTTPException(500, str(exc)) from exc

    topology = compute_network_topology(correlation_result["taxa"], correlation_result["edges"])

    nodes = []
    for taxon in correlation_result["taxa"]:
        name = taxon["taxon"]
        nodes.append({
            "id": name,
            "mean_abundance": round(mean_map.get(name, taxon["mean_abundance"]), 4),
            "prevalence": taxon["prevalence"],
            "phylum": taxon["phylum"],
            "degree": topology["degree"].get(name, 0),
            "betweenness": topology["betweenness"].get(name, 0.0),
            "community": topology["community"].get(name, 0),
            "is_hub": name in topology["hub_nodes"],
        })

    return {
        "disease": disease or "Healthy (NC)",
        "n_samples": int(len(valid_keys)),
        "n_genera": len(nodes),
        "n_edges": len(correlation_result["edges"]),
        "min_r": min_r,
        "method": correlation_result["method"],
        "method_note": correlation_result["method_note"],
        "available_methods": methods,
        "fdr_threshold": fdr_threshold,
        "hub_nodes": topology["hub_nodes"],
        "n_communities": topology["n_communities"],
        "network_density": topology["network_density"],
        "positive_edge_count": topology["positive_edge_count"],
        "negative_edge_count": topology["negative_edge_count"],
        "nodes": nodes,
        "edges": correlation_result["edges"],
    }


@app.get("/api/network",
         summary="Disease-genus network",
         description="Force-directed graph data showing associations between diseases and genera.")
@limiter.limit("60/minute")
def microbe_disease_network(request: Request, top_diseases: int = 15, top_genera: int = 30):
    """
    Return nodes (diseases + genera) and edges for a force-directed network.
    返回力导向图所需的节点（疾病 + 属）和边
    Edge weight = mean abundance of genus in disease samples.
    """
    cache_key = f"network:{top_diseases}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    # 磁盘持久化缓存：重启后无需重算（7天有效）
    disk_cached = get_disk_cached(cache_key)
    if disk_cached:
        set_cached(cache_key, disk_cached)   # 同步到内存缓存
        return disk_cached
    meta = get_metadata()
    abund = get_abundance()

    INFORM_COLS = [f"inform{i}" for i in range(12)]

    # Get top diseases by sample count / 获取样本数最多的疾病
    disease_counts: dict[str, set[int]] = {}
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        for idx, val in meta[col].dropna().items():
            normalized = _normalize_inform_label(val)
            if not normalized or normalized.lower() == "nc":
                continue
            disease_counts.setdefault(normalized, set()).add(idx)

    top_d = sorted(disease_counts.items(), key=lambda x: len(x[1]), reverse=True)[:top_diseases]
    disease_names = [d[0] for d in top_d]

    # Build genus map（只保留 is_valid_genus 的属）/ 构建属映射
    genus_map: dict[str, list[str]] = {}
    for col in abund.columns:
        g = extract_genus(col)
        if is_valid_genus(g):
            genus_map.setdefault(g, []).append(col)

    # 预聚合：把列按属合并为 genus_abund（行=样本，列=属）
    # 避免在每个疾病的内层循环里逐属切割，大幅提速
    genus_cols_ordered = list(genus_map.keys())
    genus_abund_full = pd.DataFrame(
        {g: abund[cols].sum(axis=1) for g, cols in genus_map.items()},
        index=abund.index,
    )

    # For each disease, compute mean abundance of each genus
    # 对每个疾病，计算每个属的平均丰度
    edges: list[dict] = []
    genus_set: set[str] = set()

    for disease_name in disease_names:
        sample_indices = disease_counts[disease_name]
        sample_keys = meta.loc[list(sample_indices), "sample_key"].dropna().unique()
        common = genus_abund_full.index.intersection(sample_keys)
        if len(common) == 0:
            continue
        disease_slice = genus_abund_full.loc[common]
        # 行归一化为相对丰度 (%)
        row_totals = disease_slice.sum(axis=1).replace(0, 1)
        disease_rel = disease_slice.div(row_totals, axis=0) * 100

        # 按属均值排名，取 top_genera
        genus_means_s = disease_rel.mean()
        genus_means_s = genus_means_s[genus_means_s > 0].nlargest(top_genera)

        for genus, mean_val in genus_means_s.items():
            edges.append({
                "source": disease_name,
                "target": genus,
                "weight": round(float(mean_val), 6),
            })
            genus_set.add(genus)

    # Build nodes / 构建节点
    nodes = []
    for d in disease_names:
        nodes.append({"id": d, "type": "disease", "size": len(disease_counts[d])})
    for g in genus_set:
        nodes.append({"id": g, "type": "genus", "size": 1})

    result = {"nodes": nodes, "edges": edges}
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)   # 持久化到磁盘，重启后秒加载
    return result


# ── Data management endpoints / 数据管理端点 ──────────────────────────────────

def _check_admin(token: str | None):
    """Reject if ADMIN_TOKEN unset or token doesn't match. / 校验admin token，空token也拒绝"""
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        raise HTTPException(401, "Invalid admin token")


@app.get("/api/admin/check",
         summary="Admin authentication check",
         description="Verify admin token validity.")
@limiter.limit("10/minute")
def admin_check(request: Request, x_admin_token: str | None = Header(None)):
    """Verify admin token. / 验证管理员token"""
    _check_admin(x_admin_token)
    return {"status": "authorized"}


@app.delete("/api/admin/clear-disk-cache",
            summary="Clear disk cache",
            description="Delete all disk-persisted cache files. Call after changing computation logic or uploading new data.",
            tags=["Admin"])
@limiter.limit("10/minute")
def clear_disk_cache_endpoint(request: Request, x_admin_token: str | None = Header(None)):
    """Clear all disk cache files and in-memory cache."""
    _check_admin(x_admin_token)
    deleted = []
    try:
        for fname in os.listdir(_DISK_CACHE_DIR):
            if fname.endswith(".json"):
                os.remove(os.path.join(_DISK_CACHE_DIR, fname))
                deleted.append(fname)
    except Exception as e:
        raise HTTPException(500, f"Failed to clear disk cache: {e}")
    _RESULT_CACHE.clear()
    return {"deleted": len(deleted), "files": deleted,
            "message": "Disk and memory cache cleared. Warmup will re-run on next request."}


@app.post("/api/admin/upload-metadata",
          summary="Upload metadata",
          description="Upload and merge new metadata CSV file (requires admin token).")
@limiter.limit("10/minute")
async def upload_metadata(
    request: Request,
    file: UploadFile = File(...),
    x_admin_token: str | None = Header(None),
):
    """
    Upload new metadata CSV to merge into dataset. Requires admin token.
    上传新元数据CSV合并到数据集（需要admin token）
    """
    _check_admin(x_admin_token)

    # Save to temp file / 保存到临时文件
    suffix = ".csv"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        from data_manager import update_metadata
        result = update_metadata(tmp_path)
        # Clear metadata cache so next request loads fresh data
        # 清除元数据缓存，下次请求加载新数据
        get_metadata.cache_clear()
        return result
    finally:
        os.unlink(tmp_path)


@app.post("/api/admin/validate-metadata",
          summary="Validate metadata",
          description="Validate metadata CSV file format and columns (requires admin token).")
@limiter.limit("10/minute")
async def validate_metadata_endpoint(
    request: Request,
    file: UploadFile = File(...),
    x_admin_token: str | None = Header(None),
):
    """Validate metadata CSV format without merging. / 校验格式但不合并"""
    _check_admin(x_admin_token)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        from data_manager import validate_metadata
        return validate_metadata(tmp_path)
    finally:
        os.unlink(tmp_path)


# ── Data download endpoints / 数据下载端点 ────────────────────────────────────

from fastapi.responses import JSONResponse, StreamingResponse
import io
import csv as csv_mod


DOWNLOAD_FORMATS = {"csv": ",", "tsv": "\t", "json": None}
DOWNLOAD_CITATION_NOTE = (
    "Please cite GutBiomeDB and the original BioProject / repository sources when reusing these aggregated outputs."
)


def _validate_download_format(format_name: str) -> str:
    normalized = (format_name or "csv").strip().lower()
    if normalized not in DOWNLOAD_FORMATS:
        raise HTTPException(400, "format must be one of: csv, tsv, json")
    return normalized


def _slugify_download_part(value: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")


def _download_filename(stem: str, format_name: str) -> str:
    suffix = _slugify_download_part(stem) or "download"
    generated = datetime.now().strftime("%Y%m%d")
    return f"{suffix}_{generated}.{format_name}"


def _download_headers(stem: str, format_name: str, rate_limit_note: str) -> dict[str, str]:
    return {
        "Content-Disposition": f'attachment; filename="{_download_filename(stem, format_name)}"',
        "X-Data-Version": app.version,
        "X-Generated-At": datetime.now().isoformat(timespec="seconds"),
        "X-Rate-Limit-Policy": rate_limit_note,
        "X-Download-Scope": "Aggregated statistics only; no raw sample data.",
        "X-Citation-Note": DOWNLOAD_CITATION_NOTE,
    }


def _normalize_download_value(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _download_response(
    *,
    json_payload,
    rows: list[dict],
    fieldnames: list[str],
    stem: str,
    format_name: str,
    rate_limit_note: str,
):
    normalized_format = _validate_download_format(format_name)
    headers = _download_headers(stem, normalized_format, rate_limit_note)

    if normalized_format == "json":
        return JSONResponse(content=json_payload, headers=headers)

    buffer = io.StringIO()
    writer = csv_mod.DictWriter(
        buffer,
        fieldnames=fieldnames,
        delimiter=DOWNLOAD_FORMATS[normalized_format],
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({name: _normalize_download_value(row.get(name, "")) for name in fieldnames})
    buffer.seek(0)

    media_type = "text/tab-separated-values" if normalized_format == "tsv" else "text/csv"
    return StreamingResponse(iter([buffer.getvalue()]), media_type=media_type, headers=headers)


@app.get("/api/download/summary-stats",
         summary="Download summary statistics",
         description="Export aggregated statistics as CSV, JSON, or TSV.",
         tags=["Download"])
@limiter.limit("30/minute")
def download_summary_stats(request: Request, format: str = "csv"):
    """
    Download aggregated summary statistics (NOT raw sample data).
    下载聚合统计数据（不提供原始样本数据）

    Returns sample counts by country, disease, age group, and sex.
    """
    meta = get_metadata()
    INFORM_COLS = [f"inform{i}" for i in range(12)]

    # Country stats / 国家统计
    country_counts = meta["country"].value_counts().to_dict()
    # Disease stats / 疾病统计
    disease_counts = _inform_label_counts(meta, include_nc=False)
    # Age group stats / 年龄组统计
    age_counts = meta["age_group"].value_counts().to_dict() if "age_group" in meta.columns else {}
    # Sex stats / 性别统计
    sex_counts = meta["sex"].value_counts().to_dict() if "sex" in meta.columns else {}

    payload = {
        "total_samples": int(len(meta)),
        "by_country": country_counts,
        "by_disease": disease_counts,
        "by_age_group": age_counts,
        "by_sex": sex_counts,
    }

    rows: list[dict] = []
    for key, value in country_counts.items():
        rows.append({"category": "country", "name": iso_to_name(key), "iso": key, "count": int(value)})
    for key, value in sorted(disease_counts.items(), key=lambda item: item[1], reverse=True):
        rows.append({"category": "disease", "name": key, "iso": "", "count": int(value)})
    for key, value in age_counts.items():
        rows.append({"category": "age_group", "name": key, "iso": "", "count": int(value)})
    for key, value in sex_counts.items():
        rows.append({"category": "sex", "name": key, "iso": "", "count": int(value)})

    return _download_response(
        json_payload=payload,
        rows=rows,
        fieldnames=["category", "name", "iso", "count"],
        stem="summary_stats",
        format_name=format,
        rate_limit_note="30/minute",
    )


@app.get("/api/download/disease-profile",
         summary="Download disease profile",
         description="Export disease-vs-control genus profile results as CSV, TSV, or JSON.",
         tags=["Download"])
@limiter.limit("30/minute")
def download_disease_profile_data(request: Request, disease: str, format: str = "csv"):
    """Download disease profile data / 下载疾病画像数据"""
    profile = disease_profile(request, disease, top_n=50)
    rows = profile.get("top_genera", [])
    return _download_response(
        json_payload=profile,
        rows=rows,
        fieldnames=[
            "genus",
            "phylum",
            "disease_mean",
            "control_mean",
            "disease_prevalence",
            "control_prevalence",
            "log2fc",
            "p_value",
            "adjusted_p",
            "effect_size",
            "enriched_in",
            "ci_low",
            "ci_high",
        ],
        stem=f"disease_profile_{disease}",
        format_name=format,
        rate_limit_note="30/minute",
    )


@app.get("/api/download/species-profile",
         summary="Download species profile",
         description="Export genus-centric profile tables as CSV, TSV, or JSON.",
         tags=["Download"])
@limiter.limit("30/minute")
def download_species_profile_data(request: Request, genus: str, format: str = "csv"):
    """Download species profile data / 下载物种画像数据"""
    profile = species_profile(request, genus)

    rows = profile.get("by_disease", [])
    return _download_response(
        json_payload=profile,
        rows=rows,
        fieldnames=[
            "name",
            "abundance",
            "prevalence",
            "sample_count",
            "phylum",
        ],
        stem=f"genus_profile_{genus}",
        format_name=format,
        rate_limit_note="30/minute",
    )


@app.get("/api/download/genus-list",
         summary="Download genus list",
         description="Export the full valid genus name list as CSV, TSV, or JSON.",
         tags=["Download"])
@limiter.limit("30/minute")
def download_genus_list(request: Request, format: str = "csv"):
    """Download list of all genera / 下载所有属名列表"""
    genera = get_genus_list()

    payload = {"genera": genera, "count": len(genera)}
    rows = [{"genus": genus} for genus in genera]
    return _download_response(
        json_payload=payload,
        rows=rows,
        fieldnames=["genus"],
        stem="genus_list",
        format_name=format,
        rate_limit_note="30/minute",
    )


@app.get(
    "/api/download/diff-results",
    summary="Download differential abundance results",
    description="Export disease-vs-control differential abundance results as CSV, TSV, or JSON.",
    tags=["Download"],
)
@limiter.limit("20/minute")
def download_diff_results(
    request: Request,
    disease: str,
    top_n: int = 200,
    format: str = "csv",
):
    """Download per-genus differential abundance statistics for one disease."""
    meta = get_metadata()
    abund = get_abundance()
    _, _, disease_keys, control_keys = matched_disease_control(meta, abund, disease)
    if len(disease_keys) < 5:
        raise HTTPException(400, f"Too few disease samples with abundance data: {len(disease_keys)}")
    if len(control_keys) < 5:
        raise HTTPException(400, f"Too few control samples with abundance data: {len(control_keys)}")

    rows = compute_genus_statistics(abund, disease_keys, control_keys)[: max(top_n, 1)]
    payload = {
        "disease": disease,
        "top_n": top_n,
        "n_disease": len(disease_keys),
        "n_control": len(control_keys),
        "results": rows,
    }
    return _download_response(
        json_payload=payload,
        rows=rows,
        fieldnames=[
            "genus",
            "phylum",
            "disease_mean",
            "control_mean",
            "disease_prevalence",
            "control_prevalence",
            "log2fc",
            "p_value",
            "adjusted_p",
            "effect_size",
            "enriched_in",
            "ci_low",
            "ci_high",
        ],
        stem=f"diff_results_{disease}_top_{top_n}",
        format_name=format,
        rate_limit_note="20/minute",
    )


@app.get(
    "/api/download/biomarkers",
    summary="Download biomarker discovery results",
    description="Export LEfSe-style biomarker discovery output as CSV, TSV, or JSON.",
    tags=["Download"],
)
@limiter.limit("20/minute")
def download_biomarkers(
    request: Request,
    disease: str,
    lda_threshold: float = 2.0,
    format: str = "csv",
):
    """Download biomarker discovery markers for one disease."""
    payload = biomarker_discovery(request, disease=disease, lda_threshold=lda_threshold, p_threshold=0.05)
    rows = payload.get("markers", [])
    return _download_response(
        json_payload=payload,
        rows=rows,
        fieldnames=[
            "taxon",
            "phylum",
            "lda_score",
            "p_value",
            "p_adj",
            "disease_mean",
            "control_mean",
            "disease_prevalence",
            "control_prevalence",
            "log2fc",
        ],
        stem=f"biomarkers_{disease}_lda_{lda_threshold}",
        format_name=format,
        rate_limit_note="20/minute",
    )


@app.get(
    "/api/download/cooccurrence",
    summary="Download co-occurrence network edges",
    description="Export the filtered co-occurrence network edge table as CSV, TSV, or JSON.",
    tags=["Download"],
)
@limiter.limit("20/minute")
def download_cooccurrence(
    request: Request,
    disease: str = "",
    min_r: float = 0.3,
    top_genera: int = 50,
    format: str = "csv",
):
    """Download co-occurrence network edges for NC or a disease context."""
    payload = cooccurrence_network(
        request,
        disease=disease,
        min_r=min_r,
        top_genera=top_genera,
        max_samples=3000,
        method="spearman",
        fdr_threshold=0.05,
    )
    rows = payload.get("edges", [])
    label = disease or "nc"
    return _download_response(
        json_payload=payload,
        rows=rows,
        fieldnames=[
            "source",
            "target",
            "source_phylum",
            "target_phylum",
            "r",
            "p_value",
            "adjusted_p",
            "type",
            "method",
        ],
        stem=f"cooccurrence_{label}_minr_{min_r}",
        format_name=format,
        rate_limit_note="20/minute",
    )


@app.get(
    "/api/download/lifecycle",
    summary="Download lifecycle atlas table",
    description="Export lifecycle abundance trajectories as CSV, TSV, or JSON.",
    tags=["Download"],
)
@limiter.limit("20/minute")
def download_lifecycle(
    request: Request,
    disease: str = "",
    country: str = "",
    top_genera: int = 15,
    format: str = "csv",
):
    """Download lifecycle stage abundance tables for NC or disease-filtered cohorts."""
    payload = lifecycle_atlas(request, disease=disease, country=country, top_genera=top_genera)
    rows = payload.get("data", [])
    fieldnames = [
        "age_group",
        "sample_count",
        "shannon_mean",
        "shannon_sd",
        "simpson_mean",
        "simpson_sd",
        *payload.get("genera", []),
    ]
    stem_parts = ["lifecycle", disease or "nc"]
    if country:
        stem_parts.append(country)
    stem_parts.append(f"top_{top_genera}")
    return _download_response(
        json_payload=payload,
        rows=rows,
        fieldnames=fieldnames,
        stem="_".join(stem_parts),
        format_name=format,
        rate_limit_note="20/minute",
    )


@app.get("/api/biomarker-discovery",
         summary="Disease biomarker discovery",
         description="Identifies significant biomarker taxa using Wilcoxon test with BH FDR correction and LDA effect size.")
@limiter.limit("60/minute")
def biomarker_discovery(request: Request, disease: str, lda_threshold: float = 2.0, p_threshold: float = 0.05):
    """
    Discover biomarker taxa for a given disease vs healthy controls.
    发现疾病标志物：疾病组 vs 健康对照的显著差异属
    """
    cache_key = f"biomarker_discovery:{disease.strip() if disease else ''}:{lda_threshold}:{p_threshold}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk_hit = get_disk_cached(cache_key)
    if disk_hit:
        set_cached(cache_key, disk_hit)
        return disk_hit

    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")
    disease = disease.strip()

    meta = get_metadata()
    abund = get_abundance()

    INFORM_COLS = [f"inform{i}" for i in range(12)]
    disease_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col in meta.columns:
            disease_mask |= (meta[col].fillna("").astype(str).str.strip() == disease)
    disease_samples = meta.loc[disease_mask]

    if len(disease_samples) == 0:
        raise HTTPException(404, f"Disease '{disease}' not found")

    control_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col in meta.columns:
            control_mask |= (meta[col].fillna("").astype(str).str.strip() == "NC")
    control_samples = meta.loc[control_mask]

    d_keys = disease_samples["sample_key"].dropna().unique()
    c_keys = control_samples["sample_key"].dropna().unique()
    d_valid = abund.index.intersection(d_keys)
    c_valid = abund.index.intersection(c_keys)

    if len(d_valid) < 5:
        raise HTTPException(400, f"Too few disease samples with abundance data: {len(d_valid)}")
    if len(c_valid) < 5:
        raise HTTPException(400, f"Too few control samples with abundance data: {len(c_valid)}")

    # Subsample control if too large
    np.random.seed(42)
    if len(c_valid) > 5000:
        c_valid = c_valid[np.random.choice(len(c_valid), 5000, replace=False)]

    d_raw = abund.loc[d_valid].values.astype(float)
    c_raw = abund.loc[c_valid].values.astype(float)

    d_totals = d_raw.sum(axis=1, keepdims=True); d_totals[d_totals == 0] = 1
    c_totals = c_raw.sum(axis=1, keepdims=True); c_totals[c_totals == 0] = 1
    d_rel = d_raw / d_totals * 100
    c_rel = c_raw / c_totals * 100

    col_names = abund.columns.tolist()
    genus_labels = [extract_genus(c) for c in col_names]
    unique_genera = list(dict.fromkeys(genus_labels))

    d_agg = np.zeros((d_rel.shape[0], len(unique_genera)))
    c_agg = np.zeros((c_rel.shape[0], len(unique_genera)))
    for i, g in enumerate(unique_genera):
        idxs = [j for j, l in enumerate(genus_labels) if l == g]
        d_agg[:, i] = d_rel[:, idxs].sum(axis=1)
        c_agg[:, i] = c_rel[:, idxs].sum(axis=1)

    valid_mask = [is_valid_genus(g) for g in unique_genera]
    valid_indices = [i for i, v in enumerate(valid_mask) if v]
    valid_genera = [unique_genera[i] for i in valid_indices]
    d_filtered = d_agg[:, valid_indices]
    c_filtered = c_agg[:, valid_indices]
    phylum_lookup: dict[str, str] = {}
    for column in col_names:
        genus = extract_genus(column)
        if genus not in phylum_lookup:
            phylum_lookup[genus] = extract_phylum(column)

    markers = wilcoxon_marker_test(d_filtered, c_filtered, valid_genera, p_threshold)
    markers = [m for m in markers if m["lda_score"] >= lda_threshold]
    genus_to_idx = {genus: idx for idx, genus in enumerate(valid_genera)}
    for marker in markers:
        genus = marker["taxon"]
        marker["phylum"] = phylum_lookup.get(genus, "")
        idx = genus_to_idx.get(genus)
        if idx is None:
            continue
        ci_low, ci_high = log2fc_interval(d_filtered[:, idx], c_filtered[:, idx])
        marker["ci_low"] = ci_low
        marker["ci_high"] = ci_high

    result = {
        "disease": disease,
        "n_disease": len(d_valid),
        "n_control": len(c_valid),
        "n_markers": len(markers),
        "lda_threshold": lda_threshold,
        "p_threshold": p_threshold,
        "markers": markers[:100],
    }
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


@app.get("/api/lollipop-data",
         summary="Differential abundance plot data",
         description="Log2 fold change data for lollipop plot visualization.")
@limiter.limit("60/minute")
def lollipop_data(request: Request, disease: str, top_n: int = 40):
    """
    Return differential abundance data for lollipop plot.
    返回棒棒糖图格式的差异丰度数据
    """
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")
    disease = disease.strip()

    cache_key = f"lollipop:{disease}:{top_n}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk_hit = get_disk_cached(cache_key)
    if disk_hit:
        set_cached(cache_key, disk_hit)
        return disk_hit

    try:
        result = build_lollipop_result(get_metadata(), get_abundance(), disease, top_n=max(top_n, 120))
    except ValueError as exc:
        message = str(exc)
        if "not found" in message.lower() or "insufficient" in message.lower():
            raise HTTPException(404, message)
        raise HTTPException(400, message)

    result["data"] = result["data"][:top_n]
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


@app.get("/api/chord-data",
         summary="Chord diagram data",
         description="Disease-genus association matrix for chord diagram visualization.")
@limiter.limit("60/minute")
def chord_data(request: Request, top_diseases: int = 10, top_genera: int = 12):
    """
    Return disease-genus association matrix for chord diagram.
    返回疾病-属关联矩阵用于弦图
    """
    cache_key = f"chord:{top_diseases}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    meta = get_metadata()
    abund = get_abundance()

    INFORM_COLS = [f"inform{i}" for i in range(12)]

    disease_counts: dict[str, set[int]] = {}
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        for idx, val in meta[col].dropna().items():
            normalized = _normalize_inform_label(val)
            if not normalized or normalized.lower() == "nc":
                continue
            disease_counts.setdefault(normalized, set()).add(idx)

    top_d = sorted(disease_counts.items(), key=lambda x: len(x[1]), reverse=True)[:top_diseases]
    disease_names = [d[0] for d in top_d]

    genus_map: dict[str, list[str]] = {}
    genus_phylum: dict[str, str] = {}
    for col in abund.columns:
        g = extract_genus(col)
        if is_valid_genus(g):
            genus_map.setdefault(g, []).append(col)
            if g not in genus_phylum:
                genus_phylum[g] = extract_phylum(col)

    sample_totals = abund.sum(axis=1).replace(0, 1)
    global_genus_means: list[tuple[str, float]] = []
    for g, cols in genus_map.items():
        mean_val = float((abund[cols].sum(axis=1) / sample_totals * 100).mean())
        global_genus_means.append((g, mean_val))
    global_genus_means.sort(key=lambda x: x[1], reverse=True)
    genus_names = [g[0] for g in global_genus_means[:top_genera]]

    matrix: list[list[float]] = []
    for disease_name in disease_names:
        sample_indices = list(disease_counts[disease_name])
        sample_keys = meta.loc[sample_indices, "sample_key"].dropna().unique()
        common = abund.index.intersection(sample_keys)
        row = []
        if len(common) > 0:
            d_abund = abund.loc[common]
            d_totals = d_abund.sum(axis=1).replace(0, 1)
            for g in genus_names:
                cols = genus_map.get(g, [])
                if cols:
                    mean_val = float((d_abund[cols].sum(axis=1) / d_totals * 100).mean())
                else:
                    mean_val = 0.0
                row.append(round(mean_val, 4))
        else:
            row = [0.0] * len(genus_names)
        matrix.append(row)

    result = {
        "diseases": disease_names,
        "genera": genus_names,
        "phyla": [genus_phylum.get(g, "Unknown") for g in genus_names],
        "matrix": matrix,
    }
    set_cached(cache_key, result)
    return result


@app.get("/api/species-cooccurrence",
         summary="Species co-occurrence partners",
         description="Top co-occurring genera for a given genus, based on Spearman correlation across healthy controls or a selected disease context.",
         tags=["Species"])
@limiter.limit("30/minute")
def species_cooccurrence(request: Request, genus: str, top_k: int = 10, disease: str = ""):
    """
    Return top co-occurring genera for a given genus.
    返回给定属的 Top 共现微生物
    """
    if not genus or not genus.strip():
        raise HTTPException(400, "genus parameter is required")

    disease_name = disease.strip()
    cache_key = f"species_cooccurrence:{genus.strip().lower()}:{top_k}:{disease_name.lower() or '__nc__'}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    meta = get_metadata()
    abund = get_abundance()

    # Find genus column(s)
    matching_cols = [c for c in abund.columns if extract_genus(c).lower() == genus.strip().lower()]
    if not matching_cols:
        raise HTTPException(404, f"Genus '{genus}' not found")

    canonical = extract_genus(matching_cols[0])

    if disease_name:
        disease_mask = _inform_label_mask(meta, disease_name)
        sample_keys = meta.loc[disease_mask, "sample_key"].dropna().unique()
        context_label = disease_name
    else:
        nc_mask = _strict_nc_mask(meta)
        sample_keys = meta.loc[nc_mask, "sample_key"].dropna().unique()
        context_label = "NC (Healthy)"
    valid_keys = abund.index.intersection(sample_keys)

    np.random.seed(42)
    if len(valid_keys) > 3000:
        valid_keys = valid_keys[np.random.choice(len(valid_keys), 3000, replace=False)]

    if len(valid_keys) < 20:
        return {"genus": canonical, "context": context_label, "n_samples": int(len(valid_keys)), "partners": []}

    raw = abund.loc[valid_keys].values.astype(float)
    totals = raw.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    rel = raw / totals * 100

    col_names = abund.columns.tolist()
    genus_indices: dict[str, list[int]] = {}
    genus_phylum: dict[str, str] = {}
    for idx, col_name in enumerate(col_names):
        genus_name = extract_genus(col_name)
        if not is_valid_genus(genus_name):
            continue
        genus_indices.setdefault(genus_name, []).append(idx)
        genus_phylum.setdefault(genus_name, extract_phylum(col_name))

    unique_genera = list(genus_indices.keys())
    genus_matrix = np.zeros((rel.shape[0], len(unique_genera)))
    for i, genus_name in enumerate(unique_genera):
        genus_matrix[:, i] = rel[:, genus_indices[genus_name]].sum(axis=1)

    # Find target genus index
    target_idx = None
    for i, g in enumerate(unique_genera):
        if g.lower() == genus.strip().lower():
            target_idx = i
            break
    if target_idx is None:
        return {"genus": canonical, "context": context_label, "n_samples": int(len(valid_keys)), "partners": []}

    target_vec = genus_matrix[:, target_idx]
    # Skip if target has no variance
    if np.std(target_vec) == 0:
        return {"genus": canonical, "context": context_label, "n_samples": int(len(valid_keys)), "partners": []}

    partners_raw = []
    raw_pvals: list[float] = []
    for i, g in enumerate(unique_genera):
        if i == target_idx or not is_valid_genus(g):
            continue
        other_vec = genus_matrix[:, i]
        if np.std(other_vec) == 0 or (other_vec > 0).sum() < 5:
            continue
        r, p = stats.spearmanr(target_vec, other_vec)
        if not np.isfinite(r) or not np.isfinite(p):
            continue
        if abs(float(r)) >= 0.15:
            partners_raw.append({
                "genus": g,
                "r": round(float(r), 4),
                "p_value": round(float(p), 6),
                "phylum": genus_phylum.get(g, "Other"),
                "type": "positive" if r > 0 else "negative",
            })
            raw_pvals.append(float(p))

    partners = []
    if partners_raw:
        adjusted = bh_correction(raw_pvals)
        for idx, candidate in enumerate(partners_raw):
            adjusted_p = float(adjusted[idx])
            if adjusted_p >= 0.05:
                continue
            candidate["adjusted_p"] = round(adjusted_p, 6)
            candidate["significant"] = True
            partners.append(candidate)

    partners.sort(key=lambda x: abs(x["r"]), reverse=True)
    result = {
        "genus": canonical,
        "context": context_label,
        "n_samples": int(len(valid_keys)),
        "partners": partners[:top_k],
    }
    set_cached(cache_key, result)
    return result


@app.get("/api/cooccurrence",
         summary="Co-occurrence network",
         description="Spearman correlation-based microbial co-occurrence network.")
@limiter.limit("20/minute")
def cooccurrence_network(
    request: Request,
    disease: str = "",
    min_r: float = 0.3,
    top_genera: int = 50,
    max_samples: int = 3000,
    method: str = "sparcc",
    fdr_threshold: float = 0.05,
):
    """
    Compute genus co-occurrence network based on Spearman correlation.
    基于 Spearman 相关性计算属共现网络
    """
    cache_key = f"cooccurrence:{disease}:{min_r}:{top_genera}:{max_samples}:{method}:{fdr_threshold}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    meta = get_metadata()
    abund = get_abundance()
    result = _build_cooccurrence_result(
        meta=meta,
        abund=abund,
        disease=disease,
        min_r=min_r,
        top_genera=top_genera,
        max_samples=max_samples,
        method=method,
        fdr_threshold=fdr_threshold,
    )
    set_cached(cache_key, result)
    return result


@app.get("/api/network-compare",
         summary="Disease vs healthy co-occurrence comparison",
         description="Compare disease-specific and healthy control microbial co-occurrence networks.",
         tags=["Network"])
@limiter.limit("20/minute")
def network_compare(
    request: Request,
    disease: str,
    min_r: float = 0.3,
    top_genera: int = 50,
    max_samples: int = 3000,
    method: str = "sparcc",
    fdr_threshold: float = 0.05,
):
    """Compare disease and healthy-control co-occurrence networks."""
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")

    disease = disease.strip()
    cache_key = f"network_compare:{disease}:{min_r}:{top_genera}:{max_samples}:{method}:{fdr_threshold}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    meta = get_metadata()
    abund = get_abundance()
    disease_network = _build_cooccurrence_result(
        meta=meta,
        abund=abund,
        disease=disease,
        min_r=min_r,
        top_genera=top_genera,
        max_samples=max_samples,
        method=method,
        fdr_threshold=fdr_threshold,
    )
    control_network = _build_cooccurrence_result(
        meta=meta,
        abund=abund,
        disease="",
        min_r=min_r,
        top_genera=top_genera,
        max_samples=max_samples,
        method=method,
        fdr_threshold=fdr_threshold,
    )
    comparison = compare_network_edges(disease_network["edges"], control_network["edges"])
    result = {
        "disease": disease,
        "method": method,
        "disease_network": disease_network,
        "control_network": control_network,
        **comparison,
    }
    set_cached(cache_key, result)
    return result


AGE_GROUP_ORDER = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian", "Unknown"]
AGE_NAMED_ORDER = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian"]


def _lifecycle_filter_meta(meta: pd.DataFrame, disease: str = "", country: str = "") -> pd.DataFrame:
    """Filter metadata for lifecycle views using disease/country selectors."""
    filtered = meta.copy()
    disease_value = str(disease).strip()

    if disease_value.upper() == "NC":
        filtered = filtered[_strict_nc_mask(filtered)]
    elif disease_value:
        filtered = filtered[_inform_label_mask(filtered, disease)]

    if country and country.strip():
        filtered = filtered[filtered["country"].fillna("").astype(str).str.upper() == country.strip().upper()]

    filtered = filtered.dropna(subset=["sample_key"]).drop_duplicates(subset="sample_key")
    return filtered.reset_index(drop=True)


def _lifecycle_top_transitions(
    genus_rel: np.ndarray,
    age_to_idx: dict[str, np.ndarray],
    stacked_data: list[dict],
    top_genus_names: list[str],
    genus_to_col: dict[str, int],
) -> list[dict]:
    """Return top age-to-age transitions with MWU p-values and BH-FDR."""
    transitions: list[dict] = []

    for i in range(1, len(stacked_data)):
        prev = stacked_data[i - 1]
        curr = stacked_data[i]
        prev_age = str(prev["age_group"])
        curr_age = str(curr["age_group"])
        prev_idx = age_to_idx.get(prev_age, np.array([], dtype=int))
        curr_idx = age_to_idx.get(curr_age, np.array([], dtype=int))
        changes: list[dict] = []

        for genus in top_genus_names:
            col_idx = genus_to_col.get(genus)
            if col_idx is None:
                continue

            delta = float(curr.get(genus, 0.0)) - float(prev.get(genus, 0.0))
            if abs(delta) <= 0.3:
                continue

            entry = {
                "genus": genus,
                "change": round(abs(delta), 4),
                "direction": "increase" if delta > 0 else "decrease",
                "pvalue": None,
                "adjusted_p": None,
            }

            if len(prev_idx) >= 3 and len(curr_idx) >= 3:
                prev_vals = genus_rel[prev_idx, col_idx]
                curr_vals = genus_rel[curr_idx, col_idx]
                try:
                    test = stats.mannwhitneyu(prev_vals, curr_vals, alternative="two-sided")
                    entry["pvalue"] = round(float(test.pvalue), 6)
                except Exception:
                    entry["pvalue"] = None

            changes.append(entry)

        raw_pvals = [float(item["pvalue"]) for item in changes if item["pvalue"] is not None]
        if raw_pvals:
            adjusted = bh_correction(raw_pvals)
            p_idx = 0
            for item in changes:
                if item["pvalue"] is None:
                    continue
                item["adjusted_p"] = round(float(adjusted[p_idx]), 6)
                p_idx += 1

        changes.sort(key=lambda item: item["change"], reverse=True)
        top_changes = changes[:3]
        if top_changes:
            transitions.append({
                "from": prev_age,
                "to": curr_age,
                "top_changes": top_changes,
            })

    return transitions


def _lifecycle_kruskal_results(
    genus_rel: np.ndarray,
    age_groups_present: list[str],
    top_genus_names: list[str],
    genus_to_col: dict[str, int],
    age_to_idx: dict[str, np.ndarray],
) -> list[dict]:
    """Run genus-wise Kruskal-Wallis across age groups and BH-FDR."""
    raw_rows: list[dict] = []
    raw_pvals: list[float] = []

    for genus in top_genus_names:
        col_idx = genus_to_col.get(genus)
        if col_idx is None:
            continue

        groups = []
        for age_group in age_groups_present:
            age_idx = age_to_idx.get(age_group, np.array([], dtype=int))
            if len(age_idx) < 3:
                continue
            groups.append(genus_rel[age_idx, col_idx])

        if len(groups) < 3:
            continue

        try:
            h_stat, p_value = stats.kruskal(*groups)
        except Exception:
            continue

        k_groups = len(groups)
        n_total = sum(len(g) for g in groups)
        eta_sq = (float(h_stat) - k_groups + 1) / (n_total - k_groups) if n_total > k_groups else 0.0

        raw_rows.append({
            "genus": genus,
            "kruskal_h": round(float(h_stat), 4),
            "kruskal_p": round(float(p_value), 6),
            "adjusted_p": None,
            "significant": False,
            "eta_squared": round(eta_sq, 4),
        })
        raw_pvals.append(float(p_value))

    if raw_pvals:
        adjusted = bh_correction(raw_pvals)
        for idx, row in enumerate(raw_rows):
            row["adjusted_p"] = round(float(adjusted[idx]), 6)
            row["significant"] = bool(adjusted[idx] < 0.05)

    return raw_rows


def _lifecycle_internal(
    disease: str = "",
    country: str = "",
    top_genera: int = 15,
    fixed_genera: Optional[list[str]] = None,
    use_cache: bool = True,
) -> dict:
    """Compute lifecycle atlas data with optional fixed genera for compare mode."""
    fixed_part = ",".join(fixed_genera or [])
    cache_key = f"lifecycle:v6:{disease}:{country}:{top_genera}:{fixed_part}"
    if use_cache:
        cached = get_cached(cache_key)
        if cached:
            return cached
        disk_hit = get_disk_cached(cache_key)
        if disk_hit:
            set_cached(cache_key, disk_hit)
            return disk_hit

    meta = get_metadata()
    abund = get_abundance()

    if "age_group" not in meta.columns:
        raise HTTPException(400, "age_group column not found in metadata")

    filtered = _lifecycle_filter_meta(meta, disease=disease, country=country)
    if len(filtered) < 10:
        raise HTTPException(400, "Too few samples after filtering")

    valid_key_index = abund.index.intersection(filtered["sample_key"].unique())
    valid_keys = valid_key_index.tolist()
    if len(valid_keys) < 10:
        raise HTTPException(400, "Too few samples with abundance data")

    raw = abund.loc[valid_keys].to_numpy(dtype=np.float32, copy=True)
    rel = relative_abundance_matrix(raw).astype(np.float32, copy=False)
    col_names = abund.columns.tolist()
    genus_rel, genus_labels, genus_phylum_map = aggregate_by_level(rel, col_names, "genus")
    valid_genus_positions = [idx for idx, genus in enumerate(genus_labels) if is_valid_genus(genus)]
    if not valid_genus_positions:
        raise HTTPException(400, "No valid genera found for lifecycle atlas")

    genus_rel = genus_rel[:, valid_genus_positions]
    genus_names = [genus_labels[idx] for idx in valid_genus_positions]
    genus_to_col = {genus: idx for idx, genus in enumerate(genus_names)}
    genus_means = genus_rel.mean(axis=0)
    genus_order = np.argsort(genus_means)[::-1]
    ordered_genus_names = [genus_names[idx] for idx in genus_order]

    if fixed_genera:
        top_genus_names = [genus for genus in fixed_genera if genus in genus_to_col]
    else:
        top_genus_names = ordered_genus_names[:top_genera]

    filtered_age_lookup = (
        filtered[["sample_key", "age_group"]]
        .assign(age_group=lambda frame: frame["age_group"].fillna("Unknown").astype(str).str.strip())
        .drop_duplicates(subset="sample_key")
        .set_index("sample_key")["age_group"]
        .to_dict()
    )
    sample_ages = np.array([filtered_age_lookup.get(key, "Unknown") or "Unknown" for key in valid_keys], dtype=object)
    age_lookup_set = set(sample_ages.tolist())
    # Use only named age groups (exclude Unknown) for visualization and statistics
    age_groups_present = [ag for ag in AGE_NAMED_ORDER if ag in age_lookup_set]
    age_to_idx = {ag: np.flatnonzero(sample_ages == ag) for ag in age_groups_present}
    # Count known-age samples for display
    known_age_count = sum(len(age_to_idx[ag]) for ag in age_groups_present)
    unknown_count = int(np.sum(sample_ages == "Unknown"))

    stacked_data: list[dict] = []
    phylum_lookup = {
        genus: genus_phylum_map.get(genus, "Unknown")
        for genus in top_genus_names
    }

    for age_group in age_groups_present:
        age_idx = age_to_idx.get(age_group, np.array([], dtype=int))
        if len(age_idx) == 0:
            continue

        age_rel = genus_rel[age_idx]
        alpha_rel = age_rel / 100.0
        shannon_vals = np.array([shannon_diversity(alpha_rel[j]) for j in range(len(age_idx))], dtype=float)
        simpson_vals = np.array([simpson_diversity(alpha_rel[j]) for j in range(len(age_idx))], dtype=float)
        mean_vector = age_rel.mean(axis=0)

        row: dict = {
            "age_group": age_group,
            "sample_count": len(age_idx),
            "shannon_mean": round(float(shannon_vals.mean()), 4),
            "shannon_sd": round(float(shannon_vals.std()), 4),
            "simpson_mean": round(float(simpson_vals.mean()), 4),
            "simpson_sd": round(float(simpson_vals.std()), 4),
        }

        for genus in top_genus_names:
            col_idx = genus_to_col.get(genus)
            row[genus] = round(float(mean_vector[col_idx]), 4) if col_idx is not None else 0.0

        top_sum = sum(float(row[genus]) for genus in top_genus_names)
        row["Other"] = round(max(0.0, 100.0 - top_sum), 4)
        stacked_data.append(row)

    transitions = _lifecycle_top_transitions(genus_rel, age_to_idx, stacked_data, top_genus_names, genus_to_col)
    kruskal_results = _lifecycle_kruskal_results(genus_rel, age_groups_present, top_genus_names, genus_to_col, age_to_idx)

    # ── Spearman rank correlation (genus RA vs ordinal age) ──
    age_ordinal_map = {ag: i + 1 for i, ag in enumerate(AGE_NAMED_ORDER)}
    known_mask = np.isin(sample_ages, AGE_NAMED_ORDER)
    known_indices = np.flatnonzero(known_mask)
    spearman_results: list[dict] = []
    if len(known_indices) > 10 and len(age_groups_present) >= 3:
        ordinal_ages = np.array([age_ordinal_map[sample_ages[i]] for i in known_indices])
        sp_pvals: list[float] = []
        sp_rows: list[dict] = []
        for gname in top_genus_names:
            col_idx = genus_to_col.get(gname)
            if col_idx is None:
                continue
            vals = genus_rel[known_indices, col_idx]
            try:
                rho, pval = stats.spearmanr(ordinal_ages, vals)
            except Exception:
                continue
            sp_rows.append({"genus": gname, "rho": round(float(rho), 4), "pval": float(pval), "adjusted_p": None, "significant": False})
            sp_pvals.append(float(pval))
        if sp_pvals:
            adj = bh_correction(sp_pvals)
            for i, row in enumerate(sp_rows):
                row["adjusted_p"] = round(float(adj[i]), 6)
                row["significant"] = bool(adj[i] < 0.05)
        spearman_results = sp_rows

    # ── PERMANOVA (Bray-Curtis, subsampled, 999 permutations) ──
    # Uses top N genera only + row-normalization, matching gen_fig1f_lifecycle.py
    permanova_result: dict = {}
    if len(age_groups_present) >= 3 and known_age_count >= 30:
        try:
            max_per_group = 500
            rng = np.random.default_rng(42)
            sub_indices: list[int] = []
            sub_labels: list[str] = []
            for ag in age_groups_present:
                ag_idx = age_to_idx[ag]
                if len(ag_idx) > max_per_group:
                    chosen = rng.choice(ag_idx, max_per_group, replace=False)
                else:
                    chosen = np.asarray(ag_idx)
                sub_indices.extend(chosen.tolist())
                sub_labels.extend([ag] * len(chosen))
            # Use only top genera columns (matching the figure script)
            top_col_indices = [genus_to_col[g] for g in top_genus_names if g in genus_to_col]
            sub_matrix = genus_rel[sub_indices][:, top_col_indices].astype(np.float64)
            # Row-normalize to sum=1 before Bray-Curtis (matching script)
            row_sums = sub_matrix.sum(axis=1, keepdims=True)
            row_sums[row_sums == 0] = 1.0
            sub_matrix = sub_matrix / row_sums
            dist_mat = cdist(sub_matrix, sub_matrix, metric="braycurtis")
            dist_mat = np.nan_to_num(dist_mat, nan=0.0, posinf=1.0, neginf=0.0)
            np.fill_diagonal(dist_mat, 0.0)
            n_perm = len(sub_indices)
            k_perm = len(age_groups_present)
            labels_arr = np.array(sub_labels, dtype=object)
            D2 = dist_mat ** 2
            ss_total = float(np.sum(D2)) / (2.0 * n_perm)
            if ss_total <= 0.0 or np.isnan(ss_total):
                raise ValueError("SS_total is zero or NaN")
            ss_within = 0.0
            for ag in age_groups_present:
                mask_ag = labels_arr == ag
                n_g = mask_ag.sum()
                if n_g < 2:
                    continue
                sub_D2 = D2[np.ix_(mask_ag, mask_ag)]
                ss_within += float(np.sum(sub_D2)) / (2.0 * n_g)
            ss_between = ss_total - ss_within
            df_a = k_perm - 1
            df_w = n_perm - k_perm
            f_obs = (ss_between / df_a) / (ss_within / df_w) if ss_within > 0 and df_w > 0 else 0.0
            r_sq = ss_between / ss_total
            # Permutation test
            n_permutations = 999
            f_count = 0
            for _ in range(n_permutations):
                perm_labels = rng.permutation(labels_arr)
                ss_w_perm = 0.0
                for ag in age_groups_present:
                    m = perm_labels == ag
                    n_g = m.sum()
                    if n_g < 2:
                        continue
                    ss_w_perm += float(np.sum(D2[np.ix_(m, m)])) / (2.0 * n_g)
                ss_b_perm = ss_total - ss_w_perm
                f_perm = (ss_b_perm / df_a) / (ss_w_perm / df_w) if ss_w_perm > 0 else 0.0
                if f_perm >= f_obs:
                    f_count += 1
            p_perm = (f_count + 1) / (n_permutations + 1)
            permanova_result = {
                "r_squared": round(r_sq, 4),
                "pseudo_f": round(f_obs, 4),
                "p_value": round(p_perm, 4),
                "n_permutations": n_permutations,
                "n_samples_used": n_perm,
                "n_groups": k_perm,
                "df_between": df_a,
                "df_within": df_w,
            }
        except Exception:
            permanova_result = {}

    # ── Alpha diversity summary statistics ──
    alpha_stats: dict = {}
    if len(age_groups_present) >= 3 and known_age_count >= 30:
        try:
            k_alpha = len(age_groups_present)
            n_alpha = known_age_count
            sh_groups = []
            si_groups = []
            for ag in age_groups_present:
                ag_idx = age_to_idx[ag]
                ag_rel = genus_rel[ag_idx] / 100.0
                sh_vals = np.array([shannon_diversity(ag_rel[j]) for j in range(len(ag_idx))])
                si_vals = np.array([simpson_diversity(ag_rel[j]) for j in range(len(ag_idx))])
                sh_groups.append(sh_vals)
                si_groups.append(si_vals)
            h_sh, p_sh = stats.kruskal(*sh_groups)
            h_si, p_si = stats.kruskal(*si_groups)
            eta2_sh = (float(h_sh) - k_alpha + 1) / (n_alpha - k_alpha) if n_alpha > k_alpha else 0.0
            eta2_si = (float(h_si) - k_alpha + 1) / (n_alpha - k_alpha) if n_alpha > k_alpha else 0.0
            all_sh = np.concatenate(sh_groups)
            all_si = np.concatenate(si_groups)
            # Build ordinal ages in the same grouped order as all_sh/all_si
            # (all Infants first, then Children, etc.) to ensure alignment
            ordinal_all = np.concatenate([
                np.full(len(age_to_idx[ag]), age_ordinal_map[ag])
                for ag in age_groups_present
            ])
            rho_sh, p_rho_sh = stats.spearmanr(ordinal_all, all_sh)
            rho_si, p_rho_si = stats.spearmanr(ordinal_all, all_si)
            alpha_stats = {
                "shannon_kw_h": round(float(h_sh), 2),
                "shannon_kw_p": float(p_sh),
                "shannon_eta_squared": round(eta2_sh, 4),
                "shannon_spearman_rho": round(float(rho_sh), 4),
                "shannon_spearman_p": float(p_rho_sh),
                "simpson_kw_h": round(float(h_si), 2),
                "simpson_kw_p": float(p_si),
                "simpson_eta_squared": round(eta2_si, 4),
                "simpson_spearman_rho": round(float(rho_si), 4),
                "simpson_spearman_p": float(p_rho_si),
            }
        except Exception:
            alpha_stats = {}

    result = {
        "disease": disease or "All Samples (Global)",
        "country": country or "All",
        "total_samples": known_age_count,
        "total_samples_all": len(valid_keys),
        "unknown_count": unknown_count,
        "genera": top_genus_names + ["Other"],
        "phylum_map": {genus: phylum_lookup.get(genus, "Unknown") for genus in top_genus_names},
        "data": stacked_data,
        "transitions": transitions,
        "kruskal_results": kruskal_results,
        "spearman_results": spearman_results,
        "permanova": permanova_result,
        "alpha_diversity_stats": alpha_stats,
    }

    if use_cache:
        set_cached(cache_key, result)
        set_disk_cached(cache_key, result)
    return result


@app.get("/api/lifecycle",
         summary="Lifecycle microbiome atlas",
         description="Genus-level composition across 7 named life stages (Infant to Centenarian) plus Unknown.")
@limiter.limit("60/minute")
def lifecycle_atlas(
    request: Request,
    disease: str = "",
    country: str = "",
    top_genera: int = 15,
):
    """
    Return genus composition across 7 named life stages (Infant–Centenarian) plus Unknown.
    返回 7 个命名生命阶段（婴儿–百岁老人）加 Unknown 的属级组成
    """
    return _lifecycle_internal(disease=disease, country=country, top_genera=top_genera, use_cache=True)


@app.get("/api/lifecycle-compare",
         summary="Lifecycle comparison: disease vs NC",
         description="Side-by-side lifecycle trajectories for a disease cohort versus healthy controls.")
@limiter.limit("30/minute")
def lifecycle_compare(
    request: Request,
    disease: str,
    country: str = "",
    top_genera: int = 15,
):
    """Return lifecycle trajectories for disease and NC with aligned genera."""
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required for comparison mode")

    cache_key = f"lifecycle_compare:v3:{disease}:{country}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk_hit = get_disk_cached(cache_key)
    if disk_hit:
        set_cached(cache_key, disk_hit)
        return disk_hit

    disease_seed = _lifecycle_internal(disease=disease, country=country, top_genera=top_genera, use_cache=True)
    nc_seed = _lifecycle_internal(disease="NC", country=country, top_genera=top_genera, use_cache=True)

    union_genera: list[str] = []
    for genus in disease_seed["genera"] + nc_seed["genera"]:
        if genus == "Other" or genus in union_genera:
            continue
        union_genera.append(genus)

    shared_top_genera = max(top_genera, len(union_genera))
    disease_result = _lifecycle_internal(
        disease=disease,
        country=country,
        top_genera=shared_top_genera,
        fixed_genera=union_genera,
        use_cache=False,
    )
    nc_result = _lifecycle_internal(
        disease="NC",
        country=country,
        top_genera=shared_top_genera,
        fixed_genera=union_genera,
        use_cache=False,
    )

    result = {
        "disease_data": disease_result,
        "nc_data": nc_result,
    }
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


# ── 样本相似性搜索 API / Sample Similarity Search ─────────────────────────────


@app.get("/api/genus-names",
         summary="List all genera",
         description="Returns all valid genus names from abundance data.")
@limiter.limit("120/minute")
async def get_genus_names(request: Request):
    """返回丰度矩阵所有属名列表（供前端下载模板用）。
    Return list of genus names from abundance matrix columns.
    """
    abund = get_abundance()
    genera = sorted(set(extract_genus(c) for c in abund.columns))
    # 过滤无效属名
    genera = [g for g in genera if g and g not in ("", "NA", "unknown")]
    return {"genera": genera, "count": len(genera)}


@app.post("/api/similarity-search",
          summary="Sample similarity search",
          description="Find most similar samples using Bray-Curtis or Jaccard distance.")
@limiter.limit("20/minute")
async def similarity_search(request: Request, req: SimilarityRequest):
    """接收用户上传的丰度向量，返回 Top-K 最相似样本。
    Receive user abundance vector, return Top-K most similar samples.
    注意：不暴露批量原始数据，仅返回样本ID、距离、元数据摘要。
    """
    # ── 参数校验 ──
    if req.metric not in ("braycurtis", "jaccard"):
        raise HTTPException(400, "metric must be 'braycurtis' or 'jaccard'")
    if req.top_k < 1 or req.top_k > 50:
        raise HTTPException(400, "top_k must be between 1 and 50")
    if not req.abundances:
        raise HTTPException(400, "abundances dict must not be empty")

    abund = get_abundance()
    meta = get_metadata()
    inform_cols = [f"inform{i}" for i in range(12)]

    # ── 构建属名与列索引映射 ──
    genus_to_indices: dict[str, list[int]] = {}
    genus_display: dict[str, str] = {}
    for idx, column in enumerate(abund.columns):
        genus = extract_genus(column).strip()
        genus_key = genus.lower()
        if not genus or not is_valid_genus(genus):
            continue
        genus_to_indices.setdefault(genus_key, []).append(idx)
        genus_display.setdefault(genus_key, genus)

    # ── 样本过滤：先缩小搜索空间，再做相似性检索 ──
    filtered_meta = meta.copy()
    if req.filter_disease.strip():
        filtered_meta = filtered_meta[_inform_label_mask(filtered_meta, req.filter_disease)]
    if req.filter_country.strip():
        filtered_meta = filtered_meta[
            filtered_meta["country"].fillna("").astype(str).str.upper() == req.filter_country.strip().upper()
        ]
    if req.filter_age_group.strip() and "age_group" in filtered_meta.columns:
        filtered_meta = filtered_meta[
            filtered_meta["age_group"].fillna("").astype(str).str.strip() == req.filter_age_group.strip()
        ]

    filtered_keys = filtered_meta["sample_key"].dropna().astype(str).unique().tolist()
    has_filter = bool(req.filter_disease.strip() or req.filter_country.strip() or req.filter_age_group.strip())
    valid_idx = abund.index.intersection(filtered_keys) if has_filter else abund.index
    if len(valid_idx) < req.top_k:
        raise HTTPException(400, f"Too few samples after filtering: {len(valid_idx)}")

    abund_filtered = abund.loc[valid_idx]
    sample_keys = list(abund_filtered.index)

    # ── 将用户提交的属名->丰度值对齐到丰度矩阵的列顺序 ──
    query_vector = np.zeros(len(abund_filtered.columns), dtype=float)
    matched_genera = 0
    matched_genera_pairs: list[tuple[str, float]] = []
    unmatched_genera: list[str] = []
    for genus_name, value in req.abundances.items():
        genus_key = genus_name.lower().strip()
        col_indices = genus_to_indices.get(genus_key)
        if not col_indices:
            unmatched_genera.append(genus_name.strip())
            continue
        numeric_value = float(value)
        for idx in col_indices:
            query_vector[idx] = numeric_value
        matched_genera += 1
        matched_genera_pairs.append((genus_key, numeric_value))

    if matched_genera == 0:
        raise HTTPException(400, "No matching genera found in the abundance matrix")

    # ── 调用 analysis.py 中的相似性搜索函数 ──
    results = sample_similarity_search(
        query_vector=query_vector,
        abundance_matrix=abund_filtered.values,
        sample_keys=sample_keys,
        metric=req.metric,
        top_k=req.top_k,
    )

    meta_lookup = (
        filtered_meta.drop_duplicates(subset="sample_key")
        .set_index("sample_key", drop=False)
    )

    def _collect_diseases(row: pd.Series) -> list[str]:
        diseases: list[str] = []
        for col in inform_cols:
            if col not in row.index:
                continue
            value = _normalize_inform_label(row.get(col, ""))
            if not value:
                continue
            diseases.append(value)
        if not diseases:
            primary = _normalize_inform_label(row.get("inform-all", ""))
            if primary:
                return [primary]
        return diseases or ["Unknown"]

    # ── 补充元数据信息（疾病、国家、年龄组、项目） ──
    for item in results:
        key = item["sample_key"]
        if key in meta_lookup.index:
            row = meta_lookup.loc[key]
            disease_list = _collect_diseases(row)
            item["disease"] = disease_list[0]
            item["disease_list"] = disease_list
            item["country"] = str(row.get("country", "Unknown"))
            item["age_group"] = str(row.get("age_group", "Unknown")).strip() or "Unknown"
            item["project_id"] = str(row.get("project", "")).strip()
        else:
            item["disease"] = "Unknown"
            item["disease_list"] = ["Unknown"]
            item["country"] = "Unknown"
            item["age_group"] = "Unknown"
            item["project_id"] = ""

    # ── 轻量 preview 热图载荷：只返回 query + top matches 在少量属上的相对丰度 ──
    preview_taxa_keys = [genus_key for genus_key, _ in sorted(matched_genera_pairs, key=lambda item: item[1], reverse=True)[:12]]
    preview_taxa = [genus_display.get(genus_key, genus_key.title()) for genus_key in preview_taxa_keys]

    preview_matrix: list[list[float]] = []
    query_total = float(query_vector.sum()) or 1.0
    query_row: list[float] = []
    for genus_key in preview_taxa_keys:
        idxs = genus_to_indices.get(genus_key, [])
        query_row.append(round(float(query_vector[idxs].sum() / query_total * 100.0), 4) if idxs else 0.0)
    preview_matrix.append(query_row)

    for item in results:
        sample_key = item["sample_key"]
        if sample_key not in abund_filtered.index:
            preview_matrix.append([0.0 for _ in preview_taxa_keys])
            continue
        sample_values = abund_filtered.loc[sample_key].to_numpy(dtype=float)
        sample_total = float(sample_values.sum()) or 1.0
        row_values: list[float] = []
        for genus_key in preview_taxa_keys:
            idxs = genus_to_indices.get(genus_key, [])
            row_values.append(round(float(sample_values[idxs].sum() / sample_total * 100.0), 4) if idxs else 0.0)
        preview_matrix.append(row_values)

    return {
        "metric": req.metric,
        "top_k": req.top_k,
        "matched_genera": matched_genera,
        "total_genera": len(genus_to_indices),
        "unmatched_genera": unmatched_genera[:20],
        "preview_taxa": preview_taxa,
        "preview_matrix": preview_matrix,
        "results": results,
    }


# ── Cross-study meta-analysis / 跨研究元分析 ────────────────────────────────

@app.get("/api/project-list",
         summary="List available projects",
         description="Returns distinct project IDs with sample counts and disease coverage.",
         tags=["Analysis"])
@limiter.limit("60/minute")
async def project_list(request: Request):
    """Return project browser payload with summary cards and rich per-project fields."""
    cache_key = "project_list_v2"
    cached = get_cached(cache_key)
    if cached:
        return cached

    meta = get_metadata()
    project_col = get_project_column(meta)
    if project_col is None:
        raise HTTPException(500, "No project column in metadata")

    inform_cols = [f"inform{i}" for i in range(12)]
    projects: list[dict] = []
    all_countries: set[str] = set()
    all_diseases: set[str] = set()
    total_nc = 0
    total_disease = 0
    years_seen: list[int] = []

    for proj, grp in meta.groupby(project_col):
        overview = _project_overview(str(proj), grp, inform_cols)
        projects.append(overview)
        total_nc += overview["nc_count"]
        total_disease += overview["disease_count"]
        all_countries.update(overview["country_list"])
        all_diseases.update(overview["diseases"])
        if overview["year"] is not None:
            years_seen.append(int(overview["year"]))

    projects.sort(key=lambda item: item["sample_count"], reverse=True)
    summary = {
        "total_projects": len(projects),
        "total_samples": int(sum(item["sample_count"] for item in projects)),
        "total_nc": int(total_nc),
        "total_disease": int(total_disease),
        "n_countries": len({country for country in all_countries if country and country.lower() != "unknown"}),
        "n_diseases": len(all_diseases),
        "year_range": [min(years_seen), max(years_seen)] if years_seen else [],
    }
    result = {"projects": projects, "total": len(projects), "summary": summary}
    set_cached(cache_key, result)
    return result


@app.get("/api/project-detail",
         summary="Single project detailed profile",
         description="Detailed breakdown for a single BioProject: age/sex/disease/country distribution.",
         tags=["Analysis"])
@limiter.limit("30/minute")
async def project_detail(request: Request, project_id: str):
    """Return one project's aggregated breakdown without exposing raw samples."""
    normalized_project_id = str(project_id or "").strip()
    if not normalized_project_id:
        raise HTTPException(400, "project_id is required")

    cache_key = f"project_detail_v1:{normalized_project_id.lower()}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    meta = get_metadata()
    project_col = get_project_column(meta)
    if project_col is None:
        raise HTTPException(500, "No project column in metadata")

    proj_meta = meta[meta[project_col].astype(str).str.strip() == normalized_project_id].copy()
    if proj_meta.empty:
        raise HTTPException(404, f"Project not found: {normalized_project_id}")

    inform_cols = [f"inform{i}" for i in range(12)]
    overview = _project_overview(normalized_project_id, proj_meta, inform_cols)
    result = {
        **overview,
        "total_samples": overview["sample_count"],
        "ncbi_url": f"https://www.ncbi.nlm.nih.gov/bioproject/{normalized_project_id}",
        "by_disease": _disease_count_rows(proj_meta, inform_cols, limit=20),
        "by_country": _series_count_rows(proj_meta["country"], "country", limit=10) if "country" in proj_meta.columns else [],
        "by_age_group": _series_count_rows(proj_meta["age_group"], "age_group", limit=10) if "age_group" in proj_meta.columns else [],
        "by_sex": _series_count_rows(proj_meta["sex"], "sex", limit=10) if "sex" in proj_meta.columns else [],
    }
    set_cached(cache_key, result)
    return result


@app.post("/api/cross-study",
          summary="Cross-study meta-analysis",
          description="""
Cross-cohort consensus biomarker discovery with inverse-variance weighted meta-analysis.
跨队列一致性标志物发现（逆方差加权元分析）。

Steps:
1. For each selected project, split samples into disease vs. NC (normal control)
2. Run differential analysis independently per project
3. Combine effect sizes using inverse-variance weighted meta-analysis
4. Apply DerSimonian-Laird random effects model for heterogeneity
5. Report I² heterogeneity statistic and consensus markers
""",
          tags=["Analysis"])
@limiter.limit("10/minute")
async def cross_study_analysis(request: Request, req: CrossStudyRequest):
    """跨研究元分析：多队列一致性标志物发现"""
    if len(req.project_ids) < 2:
        raise HTTPException(400, "At least 2 projects required")
    cache_key = "cross_study:" + json.dumps(
        {
            "project_ids": sorted(str(project_id).strip() for project_id in req.project_ids),
            "disease": req.disease.strip(),
            "method": req.method,
            "taxonomy_level": req.taxonomy_level,
            "p_threshold": req.p_threshold,
            "min_studies": req.min_studies,
        },
        sort_keys=True,
    )
    cached = get_cached(cache_key)
    if cached:
        return cached
    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)
    col_names = abund.columns.tolist()
    project_col = get_project_column(meta)
    if project_col is None:
        raise HTTPException(500, "No project column in metadata")

    def _group_by_level(matrix: np.ndarray, cols: list[str], level: str):
        agg, labels, _ = aggregate_by_level(matrix, cols, level)
        return agg, labels

    INFORM_COLS = [f"inform{i}" for i in range(12)]
    disease_lower = req.disease.strip().lower()

    per_project_results = []
    taxa_global = None

    for proj_id in req.project_ids:
        proj_meta = meta[meta[project_col].astype(str).str.strip() == proj_id].copy()
        if len(proj_meta) == 0:
            per_project_results.append({
                "project_id": proj_id,
                "n_disease": 0, "n_control": 0,
                "error": "Project not found",
                "taxa_results": [],
            })
            continue

        # Split disease vs NC within this project
        disease_mask = pd.Series(False, index=proj_meta.index)
        for col in INFORM_COLS:
            if col in proj_meta.columns:
                disease_mask |= proj_meta[col].fillna("").astype(str).str.strip().str.lower() == disease_lower
        nc_mask = _strict_nc_mask(proj_meta, INFORM_COLS)

        disease_keys = [k for k in proj_meta.loc[disease_mask, "sample_key"].values if k in abund_idx]
        control_keys = [k for k in proj_meta.loc[nc_mask, "sample_key"].values if k in abund_idx]

        if len(disease_keys) < 3 or len(control_keys) < 3:
            per_project_results.append({
                "project_id": proj_id,
                "n_disease": len(disease_keys),
                "n_control": len(control_keys),
                "error": "Insufficient samples (need ≥3 per group)",
                "taxa_results": [],
            })
            continue

        # Extract and normalize abundance
        raw_d = abund.loc[disease_keys].values.astype(float)
        raw_c = abund.loc[control_keys].values.astype(float)
        mat_d = relative_abundance_matrix(raw_d)
        mat_c = relative_abundance_matrix(raw_c)

        agg_d, taxa = _group_by_level(mat_d, col_names, req.taxonomy_level)
        agg_c, _ = _group_by_level(mat_c, col_names, req.taxonomy_level)
        if taxa_global is None:
            taxa_global = taxa

        # Per-taxon differential test
        taxa_results = []
        pseudo = 1e-6
        for i, taxon in enumerate(taxa):
            vals_d = agg_d[:, i]
            vals_c = agg_c[:, i]
            mean_d = float(np.mean(vals_d))
            mean_c = float(np.mean(vals_c))
            # Effect size: difference in mean log2-transformed relative abundances
            # (pseudocount 1e-6), per paper Methods — matches log-scale SE below
            log2_d = np.log2(vals_d + pseudo)
            log2_c = np.log2(vals_c + pseudo)
            log2fc = float(np.mean(log2_d) - np.mean(log2_c))

            try:
                if req.method == "wilcoxon":
                    _, p = stats.mannwhitneyu(vals_d, vals_c, alternative="two-sided")
                else:
                    _, p = stats.ttest_ind(vals_d, vals_c)
            except Exception:
                p = 1.0

            # SE on log-transformed scale using sample variance (ddof=1)
            se = float(np.sqrt(
                np.var(log2_d, ddof=1) / len(log2_d) +
                np.var(log2_c, ddof=1) / len(log2_c)
            ))
            taxa_results.append({
                "taxon": taxon,
                "log2fc": round(log2fc, 4),
                "mean_disease": round(mean_d, 6),
                "mean_control": round(mean_c, 6),
                "p_value": round(float(p), 8),
                "se": round(se, 8),
            })

        per_project_results.append({
            "project_id": proj_id,
            "n_disease": len(disease_keys),
            "n_control": len(control_keys),
            "error": None,
            "taxa_results": taxa_results,
        })

    # ── Meta-analysis: inverse-variance weighted + DerSimonian-Laird ──
    if taxa_global is None:
        raise HTTPException(400, "No valid projects with sufficient data found")

    valid_projects = [p for p in per_project_results if p["error"] is None and len(p["taxa_results"]) > 0]
    if len(valid_projects) < 2:
        raise HTTPException(400, f"Need ≥2 valid projects, found {len(valid_projects)}")

    consensus_markers = []
    for tax_idx, taxon in enumerate(taxa_global):
        effects = []  # (log2fc, se) per project
        for proj in valid_projects:
            tr = proj["taxa_results"]
            if tax_idx < len(tr) and tr[tax_idx]["taxon"] == taxon:
                se = tr[tax_idx]["se"]
                if se > 0:
                    effects.append((tr[tax_idx]["log2fc"], se, tr[tax_idx]["p_value"], proj["project_id"]))

        if len(effects) < req.min_studies:
            continue

        # Fixed-effects: inverse-variance weighted
        weights = [1 / (se ** 2) for _, se, _, _ in effects]
        w_sum = sum(weights)
        beta_fe = sum(w * e for w, (e, _, _, _) in zip(weights, effects)) / w_sum
        se_fe = math.sqrt(1 / w_sum)

        # Cochran's Q (heterogeneity test)
        Q = sum(w * (e - beta_fe) ** 2 for w, (e, _, _, _) in zip(weights, effects))
        df = len(effects) - 1
        Q_p = float(1 - stats.chi2.cdf(Q, df)) if df > 0 else 1.0

        # I² heterogeneity
        I2 = max(0, (Q - df) / Q * 100) if Q > 0 else 0.0

        # DerSimonian-Laird random effects tau²
        c = w_sum - sum(w ** 2 for w in weights) / w_sum
        tau2 = max(0, (Q - df) / c) if c > 0 else 0.0

        # Random-effects estimate
        re_weights = [1 / (se ** 2 + tau2) for _, se, _, _ in effects]
        re_w_sum = sum(re_weights)
        beta_re = sum(w * e for w, (e, _, _, _) in zip(re_weights, effects)) / re_w_sum
        se_re = math.sqrt(1 / re_w_sum)

        # Z-test for meta effect
        z = beta_re / se_re if se_re > 0 else 0
        meta_p = float(2 * (1 - stats.norm.cdf(abs(z))))

        # Count how many projects show significance
        n_sig = sum(1 for _, _, p, _ in effects if p < req.p_threshold)

        # Direction consistency
        directions = [1 if e > 0 else -1 for e, _, _, _ in effects]
        direction = "disease" if sum(directions) > 0 else "control"
        if len(set(directions)) > 1:
            direction = "mixed"

        per_project_detail = {}
        for e, se, p, pid in effects:
            per_project_detail[pid] = {
                "log2fc": round(e, 4),
                "se": round(se, 6),
                "p_value": round(p, 8),
            }

        consensus_markers.append({
            "taxon": taxon,
            "meta_log2fc": round(beta_re, 4),
            "meta_se": round(se_re, 6),
            "meta_p": round(meta_p, 8),
            "ci_low": round(beta_re - 1.96 * se_re, 4),
            "ci_high": round(beta_re + 1.96 * se_re, 4),
            "n_studies": len(effects),
            "n_significant": n_sig,
            "I2": round(I2, 1),
            "Q_p": round(Q_p, 6),
            "direction": direction,
            "per_project": per_project_detail,
        })

    adjusted_meta_p = bh_correction([float(marker["meta_p"]) for marker in consensus_markers])
    for marker, adjusted_p in zip(consensus_markers, adjusted_meta_p):
        marker["adjusted_meta_p"] = round(float(adjusted_p), 8)

    # Sort by BH-adjusted meta p-value, then raw meta p-value
    consensus_markers.sort(key=lambda x: (x["adjusted_meta_p"], x["meta_p"]))
    significant_markers = [m for m in consensus_markers if m["adjusted_meta_p"] < req.p_threshold]

    result = {
        "disease": req.disease,
        "method": req.method,
        "taxonomy_level": req.taxonomy_level,
        "n_projects": len(valid_projects),
        "project_summaries": [
            {
                "project_id": p["project_id"],
                "n_disease": p["n_disease"],
                "n_control": p["n_control"],
                "error": p["error"],
            }
            for p in per_project_results
        ],
        "consensus_markers": significant_markers[:300],
        "total_significant": len(significant_markers),
        "all_markers": consensus_markers[:500],
    }
    set_cached(cache_key, result)
    return result


# ── Health Index / 微生物组健康指数 ──────────────────────────────────────────

@lru_cache(maxsize=1)
def _compute_health_disease_genera() -> dict:
    """
    预计算健康关联属和疾病关联属。
    Precompute health-associated and disease-associated genera from NC vs all-disease.
    """
    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)
    col_names = abund.columns.tolist()

    INFORM_COLS = [f"inform{i}" for i in range(12)]

    # Strict NC: inform0=NC and inform1-11 empty
    nc_mask = _strict_nc_mask(meta, INFORM_COLS)
    nc_keys = [k for k in meta.loc[nc_mask, "sample_key"].values if k in abund_idx]

    # Disease samples: strict non-NC with at least one disease label in inform0-11
    disease_mask = _non_nc_disease_mask(meta, INFORM_COLS)
    disease_keys = [k for k in meta.loc[disease_mask, "sample_key"].values if k in abund_idx]

    # Subsample for performance
    np.random.seed(42)
    if len(nc_keys) > 5000:
        nc_keys = list(np.random.choice(nc_keys, 5000, replace=False))
    if len(disease_keys) > 5000:
        disease_keys = list(np.random.choice(disease_keys, 5000, replace=False))

    if len(nc_keys) < 10 or len(disease_keys) < 10:
        return {"health_genera": [], "disease_genera": [], "nc_stats": {}}

    raw_nc = abund.loc[nc_keys].values.astype(float)
    raw_dis = abund.loc[disease_keys].values.astype(float)
    tot_nc = raw_nc.sum(axis=1, keepdims=True); tot_nc[tot_nc == 0] = 1
    tot_dis = raw_dis.sum(axis=1, keepdims=True); tot_dis[tot_dis == 0] = 1
    mat_nc = raw_nc / tot_nc * 100
    mat_dis = raw_dis / tot_dis * 100

    # Aggregate to genus level
    genus_labels = [extract_genus(c) for c in col_names]
    unique_genera = list(dict.fromkeys(genus_labels))
    agg_nc = np.zeros((len(nc_keys), len(unique_genera)))
    agg_dis = np.zeros((len(disease_keys), len(unique_genera)))
    for i, g in enumerate(unique_genera):
        idxs = [j for j, l in enumerate(genus_labels) if l == g]
        agg_nc[:, i] = mat_nc[:, idxs].sum(axis=1)
        agg_dis[:, i] = mat_dis[:, idxs].sum(axis=1)

    # Test each genus: enriched in NC (health) vs enriched in disease
    all_results = []
    nc_stats = {}

    for i, genus in enumerate(unique_genera):
        if not is_valid_genus(genus):
            continue
        vals_nc = agg_nc[:, i]
        vals_dis = agg_dis[:, i]
        mean_nc = float(np.mean(vals_nc))
        mean_dis = float(np.mean(vals_dis))

        if np.std(vals_nc) == 0 and np.std(vals_dis) == 0:
            continue
        try:
            _, p = stats.mannwhitneyu(vals_nc, vals_dis, alternative="two-sided")
        except Exception:
            continue

        nc_stats[genus] = {
            "mean": round(mean_nc, 6),
            "median": round(float(np.median(vals_nc)), 6),
            "std": round(float(np.std(vals_nc)), 6),
            "p10": round(float(np.percentile(vals_nc, 10)), 6),
            "p25": round(float(np.percentile(vals_nc, 25)), 6),
            "p75": round(float(np.percentile(vals_nc, 75)), 6),
            "p90": round(float(np.percentile(vals_nc, 90)), 6),
        }

        pseudo = 1e-6
        log2fc = math.log2((mean_nc + pseudo) / (mean_dis + pseudo))
        all_results.append({
            "genus": genus,
            "log2fc": round(log2fc, 4),
            "p_value": round(float(p), 8),
            "mean_nc": round(mean_nc, 6),
            "mean_disease": round(mean_dis, 6),
        })

    adjusted_pvals = bh_correction([float(row["p_value"]) for row in all_results])
    health_genera = []
    disease_genera = []
    for row, adjusted_p in zip(all_results, adjusted_pvals):
        row["adjusted_p"] = round(float(adjusted_p), 8)
        row["weight"] = round(abs(float(row["log2fc"])), 4)
        if adjusted_p >= 0.05 or abs(float(row["log2fc"])) < 0.5:
            continue
        entry = {
            "genus": row["genus"],
            "log2fc": row["log2fc"],
            "p_value": row["p_value"],
            "adjusted_p": row["adjusted_p"],
            "mean_nc": row["mean_nc"],
            "mean_disease": row["mean_disease"],
            "weight": row["weight"],
        }
        if row["mean_nc"] > row["mean_disease"]:
            health_genera.append(entry)
        else:
            disease_genera.append(entry)

    # Sort by absolute log2fc, take top 50
    health_genera.sort(key=lambda x: abs(float(x["log2fc"])), reverse=True)
    disease_genera.sort(key=lambda x: abs(float(x["log2fc"])), reverse=True)

    return {
        "health_genera": health_genera[:50],
        "disease_genera": disease_genera[:50],
        "nc_stats": nc_stats,
        "n_nc_samples": len(nc_keys),
        "n_disease_samples": len(disease_keys),
    }


@app.post("/api/health-index",
          summary="Gut Microbiome Health Index (GMHI)",
          description="""
Calculate a gut microbiome health score (0-100) based on user-provided genus abundances.
基于用户提供的属级丰度计算肠道微生物组健康评分。

Algorithm:
1. Compare user genera against precomputed health-associated and disease-associated genera
2. H_score = log10(sum_health_abundances / sum_disease_abundances)
3. Normalize to 0-100 scale based on NC population distribution
4. Provide per-genus deviation from healthy reference
""",
          tags=["Similarity"])
@limiter.limit("20/minute")
async def health_index(request: Request, req: HealthIndexRequest):
    """计算微生物组健康指数"""
    if not req.abundances:
        raise HTTPException(400, "abundances dict must not be empty")

    ref = _compute_health_disease_genera()
    if not ref["health_genera"] and not ref["disease_genera"]:
        raise HTTPException(500, "Health index reference data not available")

    health_set = {g["genus"].lower(): g for g in ref["health_genera"]}
    disease_set = {g["genus"].lower(): g for g in ref["disease_genera"]}
    nc_stats = ref["nc_stats"]

    # Match user genera
    h_sum = 0.0
    d_sum = 0.0
    h_weighted = 0.0
    d_weighted = 0.0
    health_matched = []
    disease_matched = []
    per_genus = []

    for genus, value in req.abundances.items():
        g_lower = genus.strip().lower()
        g_title = genus.strip().title()
        numeric_value = float(value)

        # Check if this is a health or disease genus
        if g_lower in health_set:
            weight = float(health_set[g_lower].get("weight", 1.0))
            h_sum += numeric_value
            h_weighted += numeric_value * weight
            health_matched.append({
                "genus": g_title,
                "abundance": round(numeric_value, 6),
                "weight": round(weight, 4),
                "contribution": 0.0,
            })
        if g_lower in disease_set:
            weight = float(disease_set[g_lower].get("weight", 1.0))
            d_sum += numeric_value
            d_weighted += numeric_value * weight
            disease_matched.append({
                "genus": g_title,
                "abundance": round(numeric_value, 6),
                "weight": round(weight, 4),
                "contribution": 0.0,
            })

        # Per-genus deviation from NC reference
        if g_title in nc_stats:
            ref_stat = nc_stats[g_title]
            status = "normal"
            if numeric_value > ref_stat["p75"] * 1.5:
                status = "high"
            elif numeric_value < ref_stat["p25"] * 0.5:
                status = "low"
            per_genus.append({
                "genus": g_title,
                "user_abundance": round(numeric_value, 6),
                "nc_mean": ref_stat["mean"],
                "nc_median": ref_stat["median"],
                "nc_p10": ref_stat["p10"],
                "nc_p25": ref_stat["p25"],
                "nc_p75": ref_stat["p75"],
                "nc_p90": ref_stat["p90"],
                "status": status,
            })

    total_h_weighted = sum(float(item["abundance"]) * float(item["weight"]) for item in health_matched) or 1e-6
    total_d_weighted = sum(float(item["abundance"]) * float(item["weight"]) for item in disease_matched) or 1e-6
    for item in health_matched:
        item["contribution"] = round(float(item["abundance"]) * float(item["weight"]) / total_h_weighted, 4)
    for item in disease_matched:
        item["contribution"] = round(float(item["abundance"]) * float(item["weight"]) / total_d_weighted, 4)

    # Calculate raw score
    pseudo = 1e-6
    raw_score = math.log10((h_sum + pseudo) / (d_sum + pseudo))
    raw_score_weighted = math.log10((h_weighted + pseudo) / (d_weighted + pseudo))

    # Use empirical percentile calibration from population data
    # 使用群体数据的经验百分位数校准
    pop = _compute_population_gmhi()
    cal = pop.get("calibration", {})
    p5 = cal.get("p5", -2.0)
    p95 = cal.get("p95", 2.0)
    score_range = p95 - p5 if p95 != p5 else 1.0
    normalized = max(0, min(100, (raw_score_weighted - p5) / score_range * 100))

    # Determine category
    nc_pop = pop.get("nc_stats", {})
    good_threshold = float(nc_pop.get("p25", 70))
    moderate_threshold = float(nc_pop.get("p10", 40))
    if normalized >= good_threshold:
        category = "good"
    elif normalized >= moderate_threshold:
        category = "moderate"
    else:
        category = "attention"

    nc_scores_sorted = np.array(pop.get("nc_scores_sorted", []), dtype=float)
    if len(nc_scores_sorted) > 0:
        population_percentile = float(np.searchsorted(nc_scores_sorted, normalized, side="right") / len(nc_scores_sorted) * 100)
    else:
        population_percentile = 50.0

    # Sort per_genus by absolute deviation
    per_genus.sort(key=lambda x: abs(x["user_abundance"] - x["nc_mean"]), reverse=True)

    return {
        "score": round(normalized, 1),
        "raw_score": round(raw_score, 4),
        "raw_score_weighted": round(raw_score_weighted, 4),
        "category": category,
        "population_percentile": round(population_percentile, 1),
        "health_genera_matched": len(health_matched),
        "disease_genera_matched": len(disease_matched),
        "health_genera_sum": round(h_sum, 4),
        "disease_genera_sum": round(d_sum, 4),
        "health_genera_detail": health_matched,
        "disease_genera_detail": disease_matched,
        "per_genus_deviation": per_genus[:50],
        "reference": {
            "n_nc_samples": ref.get("n_nc_samples", 0),
            "n_disease_samples": ref.get("n_disease_samples", 0),
            "health_genera_total": len(ref["health_genera"]),
            "disease_genera_total": len(ref["disease_genera"]),
        },
    }


@lru_cache(maxsize=1)
def _compute_population_gmhi() -> dict:
    """预计算 NC 和疾病样本的群体 GMHI 分布，用于前端展示"""
    ref = _compute_health_disease_genera()
    if not ref["health_genera"] and not ref["disease_genera"]:
        return {"nc_scores": [], "disease_scores": [], "histogram": []}

    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)
    col_names = abund.columns.tolist()

    INFORM_COLS = [f"inform{i}" for i in range(12)]
    nc_mask = _strict_nc_mask(meta, INFORM_COLS)
    nc_keys = [k for k in meta.loc[nc_mask, "sample_key"].values if k in abund_idx]
    disease_mask = _non_nc_disease_mask(meta, INFORM_COLS)
    disease_keys = [k for k in meta.loc[disease_mask, "sample_key"].values if k in abund_idx]

    np.random.seed(42)
    nc_sample = list(np.random.choice(nc_keys, min(2000, len(nc_keys)), replace=False)) if len(nc_keys) > 0 else []
    dis_sample = list(np.random.choice(disease_keys, min(2000, len(disease_keys)), replace=False)) if len(disease_keys) > 0 else []

    health_weights = {g["genus"].lower(): float(g.get("weight", 1.0)) for g in ref["health_genera"]}
    disease_weights = {g["genus"].lower(): float(g.get("weight", 1.0)) for g in ref["disease_genera"]}

    genus_labels = [extract_genus(c).lower() for c in col_names]
    h_col_weights = [(i, health_weights[g]) for i, g in enumerate(genus_labels) if g in health_weights]
    d_col_weights = [(i, disease_weights[g]) for i, g in enumerate(genus_labels) if g in disease_weights]

    def compute_raw_scores(keys):
        if not keys:
            return np.array([])
        raw = abund.loc[keys].values.astype(float)
        totals = raw.sum(axis=1, keepdims=True)
        totals[totals == 0] = 1
        rel = raw / totals * 100
        h_sums = np.zeros(len(keys))
        d_sums = np.zeros(len(keys))
        for idx, weight in h_col_weights:
            h_sums += rel[:, idx] * weight
        for idx, weight in d_col_weights:
            d_sums += rel[:, idx] * weight
        pseudo = 1e-6
        return np.log10((h_sums + pseudo) / (d_sums + pseudo))

    nc_raw = compute_raw_scores(nc_sample)
    dis_raw = compute_raw_scores(dis_sample)

    # Use empirical percentile-based normalization from ALL raw scores
    # 基于所有样本的原始分数经验分布做百分位数标准化
    all_raw = np.concatenate([nc_raw, dis_raw]) if len(nc_raw) > 0 and len(dis_raw) > 0 else nc_raw
    if len(all_raw) == 0:
        all_raw = np.array([0])
    p5 = float(np.percentile(all_raw, 5))
    p95 = float(np.percentile(all_raw, 95))
    score_range = p95 - p5 if p95 != p5 else 1.0

    def normalize_scores(raw_arr):
        if len(raw_arr) == 0:
            return []
        return np.clip((raw_arr - p5) / score_range * 100, 0, 100).tolist()

    nc_scores = normalize_scores(nc_raw)
    dis_scores = normalize_scores(dis_raw)

    # Build histogram bins (0-100, step 5)
    bins = list(range(0, 105, 5))
    nc_hist, _ = np.histogram(nc_scores, bins=bins) if nc_scores else (np.zeros(len(bins) - 1), None)
    dis_hist, _ = np.histogram(dis_scores, bins=bins) if dis_scores else (np.zeros(len(bins) - 1), None)
    histogram = []
    for i in range(len(bins) - 1):
        histogram.append({
            "bin_start": bins[i], "bin_end": bins[i + 1],
            "nc_count": int(nc_hist[i]), "disease_count": int(dis_hist[i]),
        })

    nc_arr = np.array(nc_scores) if nc_scores else np.array([0])
    dis_arr = np.array(dis_scores) if dis_scores else np.array([0])

    return {
        "histogram": histogram,
        "nc_stats": {
            "n": len(nc_keys),  # actual total, not subsample
            "mean": round(float(np.mean(nc_arr)), 1),
            "median": round(float(np.median(nc_arr)), 1),
            "std": round(float(np.std(nc_arr)), 1),
            "p10": round(float(np.percentile(nc_arr, 10)), 1),
            "p25": round(float(np.percentile(nc_arr, 25)), 1),
            "p75": round(float(np.percentile(nc_arr, 75)), 1),
            "p90": round(float(np.percentile(nc_arr, 90)), 1),
        },
        "disease_stats": {
            "n": len(disease_keys),  # actual total, not subsample
            "mean": round(float(np.mean(dis_arr)), 1),
            "median": round(float(np.median(dis_arr)), 1),
            "std": round(float(np.std(dis_arr)), 1),
            "p10": round(float(np.percentile(dis_arr, 10)), 1),
            "p25": round(float(np.percentile(dis_arr, 25)), 1),
            "p75": round(float(np.percentile(dis_arr, 75)), 1),
            "p90": round(float(np.percentile(dis_arr, 90)), 1),
        },
        "calibration": {
            "p5": round(p5, 4),
            "p95": round(p95, 4),
        },
        "nc_scores_sorted": sorted(float(score) for score in nc_scores),
    }


@app.get("/api/health-index/reference",
         summary="Health index reference data",
         description="Returns precomputed health/disease genera lists, NC reference statistics, and population GMHI distribution.",
         tags=["Similarity"])
@limiter.limit("60/minute")
async def health_index_reference(request: Request):
    """返回健康指数参考数据（健康属/疾病属列表 + 群体分布）"""
    ref = _compute_health_disease_genera()
    pop = _compute_population_gmhi()
    return {
        "health_genera": ref["health_genera"],
        "disease_genera": ref["disease_genera"],
        "n_nc_samples": pop.get("nc_stats", {}).get("n", ref.get("n_nc_samples", 0)),
        "n_disease_samples": pop.get("disease_stats", {}).get("n", ref.get("n_disease_samples", 0)),
        "population": pop,
    }


# ── Usage tracking / 使用统计追踪 ────────────────────────────────────────────

class TrackEvent(BaseModel):
    """使用统计事件"""
    event: str       # page_view / analysis_run / export / search
    page: str = ""
    detail: str = ""

_ANALYTICS_FILE = Path(__file__).parent / "analytics.jsonl"

@app.post("/api/track", tags=["Admin"],
          summary="Track usage event",
          description="Log usage events for publication metrics.")
@limiter.limit("120/minute")
async def track_event(request: Request, evt: TrackEvent):
    """记录使用统计事件（用于论文指标）"""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "event": evt.event,
        "page": evt.page,
        "detail": evt.detail,
    }
    try:
        with open(_ANALYTICS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass
    return {"status": "ok"}


@app.get("/api/admin/analytics", tags=["Admin"],
         summary="View analytics summary",
         description="Returns aggregated usage statistics (admin only).")
@limiter.limit("30/minute")
async def analytics_summary(request: Request, token: str = ""):
    """管理员查看使用统计汇总"""
    if token != ADMIN_TOKEN:
        raise HTTPException(403, "Invalid admin token")

    if not _ANALYTICS_FILE.exists():
        return {"events": [], "summary": {}}

    events = []
    try:
        with open(_ANALYTICS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
    except Exception:
        pass

    # Aggregate
    from collections import Counter
    event_counts = Counter(e.get("event", "") for e in events)
    page_counts = Counter(e.get("page", "") for e in events)

    return {
        "total_events": len(events),
        "by_event_type": dict(event_counts.most_common(20)),
        "by_page": dict(page_counts.most_common(20)),
        "recent": events[-20:] if events else [],
    }


# ── API v1 version aliases / API v1 版本别名 ─────────────────────────────────
@app.get("/api/v1/{path:path}")
@limiter.limit("120/minute")
async def v1_redirect_get(path: str, request: Request):
    """Redirect GET /api/v1/* to /api/* for versioned access"""
    query = str(request.query_params)
    url = f"/api/{path}" + (f"?{query}" if query else "")
    return RedirectResponse(url=url, status_code=307)

@app.post("/api/v1/{path:path}")
@limiter.limit("120/minute")
async def v1_redirect_post(path: str, request: Request):
    """Redirect POST /api/v1/* to /api/* for versioned access"""
    query = str(request.query_params)
    url = f"/api/{path}" + (f"?{query}" if query else "")
    return RedirectResponse(url=url, status_code=307)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
