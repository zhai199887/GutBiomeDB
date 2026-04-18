"""
main.py – GutBiomeDB FastAPI backend
"""

import hashlib
import logging
import os
import json
import math
import re
import tempfile
import threading
from datetime import datetime, timedelta
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

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Load environment variables ─────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env.local", override=True)
load_dotenv(Path(__file__).parent.parent / ".env", override=False)

METADATA_PATH = os.getenv("METADATA_PATH", "")  # set via .env.local
ABUNDANCE_PATH = os.getenv("ABUNDANCE_PATH", "")  # set via .env.local
ADMIN_TOKEN    = os.getenv("ADMIN_TOKEN", "")

# Validate at startup — use logging so warnings are never silently swallowed
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

A comprehensive analysis platform for the human gut microbiome, integrating **168,464 samples** across **4,680 taxonomic features** (resolving to ~3,142 unique genera after taxonomy-level aggregation), **72 countries**, and **224 condition categories** (223 non-NC condition labels plus one NC category). Differential analysis is performed at the genus level by default.

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

# ── Rate limiting ─────────────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS: allow all origins in dev, restrict to frontend URL in production
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


# ── Security headers ────────────────────────────────────────────────────────
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


# ── API access log (for spotting direct-API scraping) ──────────────
_API_ACCESS_FILE = Path(__file__).parent / "api_access.jsonl"
_API_ACCESS_SKIP_PREFIXES = ("/api/track", "/api/health", "/api/admin/", "/api/cache-audit")


class ApiAccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        import time as _time
        path = request.url.path
        is_api = path.startswith("/api/")
        skip = is_api and any(path.startswith(p) for p in _API_ACCESS_SKIP_PREFIXES)

        t0 = _time.perf_counter()
        response = await call_next(request)
        if not is_api or skip:
            return response

        dt_ms = int((_time.perf_counter() - t0) * 1000)
        forwarded = request.headers.get("x-forwarded-for", "")
        ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "")
        try:
            with open(_API_ACCESS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": datetime.now().isoformat(timespec="seconds"),
                    "ip": ip,
                    "path": path,
                    "method": request.method,
                    "status": response.status_code,
                    "dt_ms": dt_ms,
                    "ua": (request.headers.get("user-agent", "") or "")[:200],
                    "referer": (request.headers.get("referer", "") or "")[:200],
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        return response


app.add_middleware(ApiAccessLogMiddleware)


def no_cache_tracking(fn):
    """Mark an endpoint as intentionally uncached. cache_audit skips it."""
    fn._no_cache_tracking = True
    return fn


# ── Startup warmup ─────────────────────────────────────────────────────────────

@app.on_event("startup")
def warmup_data():
    """Pre-load data into memory at startup to avoid cold-start latency."""
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
            {"url": "http://127.0.0.1:8000/api/project-timeline"},
            {"url": "http://127.0.0.1:8000/api/disease-names-zh"},
            {"url": "http://127.0.0.1:8000/api/disease-display-names"},
            {"url": "http://127.0.0.1:8000/api/disease-ontology"},
            {"url": "http://127.0.0.1:8000/api/phenotype-groups?dim_type=disease"},
            {"url": "http://127.0.0.1:8000/api/phenotype-groups?dim_type=age"},
            {"url": "http://127.0.0.1:8000/api/phenotype-groups?dim_type=sex"},
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
            ("chord default genus 12", lambda: getattr(chord_data, "__wrapped__", chord_data)(None, 10, 12)),
            ("cooccurrence IBD", lambda: getattr(cooccurrence_network, "__wrapped__", cooccurrence_network)(None, "IBD", 0.3, 50, 3000, "sparcc", 0.05)),
            ("cooccurrence CRC", lambda: getattr(cooccurrence_network, "__wrapped__", cooccurrence_network)(None, "colorectal_cancer", 0.3, 50, 3000, "sparcc", 0.05)),
            ("network-compare IBD", lambda: getattr(network_compare, "__wrapped__", network_compare)(None, "IBD", 0.3, 50, 3000, "sparcc", 0.05)),
            ("phenotype-taxa Faecalibacterium sex", lambda: getattr(phenotype_taxa_profile, "__wrapped__", phenotype_taxa_profile)(None, "Faecalibacterium", "sex")),
        ]
        # Parallel warmup: non-phenotype heavy endpoints, max_workers=4
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

        # Phenotype warmup: run sequentially to avoid stacking memory peaks (~5 GB each)
        phenotype_warmups = [
            ("phenotype sex F vs M genus", lambda: getattr(phenotype_association, "__wrapped__", phenotype_association)(None, "sex", "female", "male", "genus", 0.10, 100)),
            ("phenotype sex F vs M phylum", lambda: getattr(phenotype_association, "__wrapped__", phenotype_association)(None, "sex", "female", "male", "phylum", 0.10, 100)),
            ("phenotype age Adult vs Older_Adult", lambda: getattr(phenotype_association, "__wrapped__", phenotype_association)(None, "age", "Adult", "Older_Adult", "genus", 0.10, 100)),
        ]
        for item in phenotype_warmups:
            _run_warmup(item)
        logging.info("Phenotype warmups completed (sequential)")

        # Disease browser warmup: sequentially pre-compute profiles + studies for top diseases
        _top_diseases = ["IBD", "CD", "UC", "obesity", "colorectal_cancer",
                         "Type 2 diabetes", "compensated cirrhosis", "adenoma", "IBS"]
        disease_warmups = []
        for _d in _top_diseases:
            disease_warmups.append(
                (f"disease-profile {_d}", lambda d=_d: getattr(disease_profile, "__wrapped__", disease_profile)(None, d, 40))
            )
            disease_warmups.append(
                (f"disease-studies {_d}", lambda d=_d: getattr(disease_studies, "__wrapped__", disease_studies)(None, d))
            )
        for item in disease_warmups:
            _run_warmup(item)
        logging.info("Disease browser warmups completed (sequential)")

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


# ── Cache-version observability ────────────────────────────────────────────────

import cache_audit

CACHE_AUDIT_HASH_FILE = Path(os.getenv(
    "CACHE_AUDIT_HASH_FILE",
    str(Path(__file__).parent / "_disk_cache" / ".endpoint_source_hashes.json"),
))


@app.on_event("startup")
def _run_cache_audit():
    try:
        report = cache_audit.run(app, CACHE_AUDIT_HASH_FILE)
    except cache_audit.DuplicateCacheKeyError as e:
        logging.error("[cache-audit] FATAL duplicate cache_key: %s", e)
        raise
    except Exception as e:
        logging.exception("[cache-audit] audit crashed, backend continues: %s", e)
        app.state.cache_audit_report = None
        return
    app.state.cache_audit_report = report
    stale_names = ", ".join(w["name"] for w in report.stale[:5])
    logging.info(
        "[cache-audit] scanned %d routes, %d tracked, %d stale (%s), %d seeded, %d unknown in %.1fms",
        report.total, report.tracked, len(report.stale), stale_names,
        len(report.seeded), len(report.unknown), report.elapsed_ms,
    )


# ── Response cache for compute-heavy endpoints ─────────────────────────────────

_RESULT_CACHE: dict[str, tuple[float, float, dict]] = {}
_CACHE_TTL = 3600  # 1 hour

# Persistent disk cache directory (survives restarts)
_DISK_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_disk_cache")
os.makedirs(_DISK_CACHE_DIR, exist_ok=True)
_DISK_CACHE_TTL = 86400 * 7  # 7 days

def _disk_cache_path(key: str) -> str:
    # Filename is derived only from the endpoint-provided cache_key.
    # Freshness is enforced by get_disk_cached_by_data (file mtime vs source-data mtime).
    # Per-endpoint output-format versions belong inside the cache_key itself
    # (e.g. "phenotype_assoc_v2:..."), so unrelated endpoints are not
    # invalidated when one endpoint's schema changes.
    safe = key.replace(":", "_").replace("/", "_")
    return os.path.join(_DISK_CACHE_DIR, f"{safe}.json")

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

def _data_mtime() -> float:
    """Return max mtime of metadata + abundance source files."""
    mt = 0.0
    for p in (METADATA_PATH, ABUNDANCE_PATH):
        if p and os.path.exists(p):
            mt = max(mt, os.path.getmtime(p))
    return mt

def get_disk_cached_by_data(key: str):
    """Load from disk cache if source data hasn't changed (no TTL expiry)."""
    path = _disk_cache_path(key)
    try:
        if not os.path.exists(path):
            return None
        # Cache file must be newer than source data; otherwise treat as expired
        if os.path.getmtime(path) < _data_mtime():
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

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


# ── Disease ontology ───────────────────────────────────────────────────────────
ONTOLOGY_PATH = os.path.join(os.path.dirname(__file__), "disease_ontology.json")
with open(ONTOLOGY_PATH, "r", encoding="utf-8") as f:
    DISEASE_ONTOLOGY: dict = json.load(f)
logging.info(f"Disease ontology loaded: {len(DISEASE_ONTOLOGY)} entries")

# ── Data loading (cached) ──────────────────────────────────────────────────────

_METADATA_LOCK = threading.Lock()
_ABUNDANCE_LOCK = threading.Lock()


@lru_cache(maxsize=1)
def _load_metadata_cached() -> pd.DataFrame:
    """Load and clean metadata CSV."""
    logging.info(f"Loading metadata from {METADATA_PATH}...")
    df = _read_csv_with_fallbacks(METADATA_PATH, on_bad_lines="skip", low_memory=False)

    # Normalize column names
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

    if "inform-all" in df.columns:
        df["disease"] = df["inform-all"].fillna("").astype(str).str.strip()
        df.loc[df["disease"] == "", "disease"] = "unknown"
    else:
        df["disease"] = "unknown"

    # Build composite sample key matching abundance matrix rownames
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
    """Load abundance CSV (large ~1.5 GB)."""
    logging.info(f"Loading abundance from {ABUNDANCE_PATH}...")
    # First column is sample_id (rownames from R)
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


# ISO 3166-1 alpha-2 → country name mapping
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
    """Read CSV with UTF-8 preference and GBK/latin1 fallbacks."""
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
    """Convert ISO code to human-readable name."""
    return COUNTRY_NAMES.get(code, code)


# ── Disease name i18n ────────────────────────────────────────────────────────
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
    """Return Chinese name if available, else original."""
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
    e.g. "Bacteria.Bacillota.Clostridia.Lachnospirales.Lachnospiraceae.Blautia" → "Blautia"
    """
    parts = col_name.split(".")
    return parts[-1] if parts else col_name


def extract_phylum(col_name: str) -> str:
    """Extract phylum from full taxonomy."""
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


def _abundance_data_columns() -> list[str]:
    if not ABUNDANCE_PATH or not os.path.exists(ABUNDANCE_PATH):
        return []
    columns = pd.read_csv(ABUNDANCE_PATH, nrows=0).columns.tolist()
    if not columns:
        return []
    return columns[1:] if columns[0].strip().lower() in {"sample_id", "sampleid", "sample", "rownames"} else columns


def count_total_taxa_from_abundance() -> int:
    """Total number of taxonomic features (columns) in the abundance matrix.

    Each column is a full taxonomic lineage string, not a genus — many columns may
    collapse to the same genus once `extract_genus` is applied. Use
    `count_unique_genera_resolved()` for the true unique-genus count.
    """
    return len(_abundance_data_columns())


def count_unique_genera_resolved() -> int:
    """Unique genus count after resolving each column to its genus name."""
    cols = _abundance_data_columns()
    if not cols:
        return 0
    return len({extract_genus(c).strip() for c in cols})


def count_unique_genera_from_abundance() -> int:
    """Alias for count_total_taxa_from_abundance (backward compatibility)."""
    return count_total_taxa_from_abundance()


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


# ── Pydantic models ────────────────────────────────────────────────────────────

class GroupFilter(BaseModel):
    """Filter conditions for a sample group."""
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
    """Sample similarity search request model."""
    abundances: dict[str, float]  # genus_name -> abundance_value
    metric: str = "braycurtis"    # braycurtis or jaccard
    top_k: int = 10               # number of most similar samples to return
    filter_disease: str = ""
    filter_country: str = ""
    filter_age_group: str = ""


class CrossStudyRequest(BaseModel):
    """Cross-study meta-analysis request model."""
    project_ids: list[str]        # list of project IDs
    disease: str                  # target disease
    method: str = "wilcoxon"      # wilcoxon / t-test
    taxonomy_level: str = "genus" # genus / family / phylum
    p_threshold: float = 0.05     # significance threshold
    min_studies: int = 2          # minimum number of concordant cohorts


class SampleCountRequest(BaseModel):
    group_a_filter: GroupFilter
    group_b_filter: GroupFilter


class SpearmanAnalysisRequest(BaseModel):
    group_a_filter: GroupFilter
    group_b_filter: GroupFilter
    taxonomy_level: str = "genus"
    max_taxa: int = 18


class HealthIndexRequest(BaseModel):
    """Microbiome health index request model."""
    abundances: dict[str, float]  # genus_name -> abundance_value
    age_group: str = ""           # optional age group for reference range
    amplicon: str = ""            # e.g. "v3-v4", "v4" (falls back to OTHER)
    iso: str = ""                 # ISO-2 country code (falls back to OTHER)
    sex: str = "unknown"          # female / male / unknown
    length: float | None = None   # sequencing read length (bp); falls back to training median


# ── Helper functions ────────────────────────────────────────────────────────────

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
    """Calculate Shannon diversity index."""
    row = row[row > 0]
    if len(row) == 0:
        return 0.0
    p = row / row.sum()
    return float(-np.sum(p * np.log(p)))


def simpson_diversity(row: np.ndarray) -> float:
    """Calculate Simpson diversity index (1-D)."""
    row = row[row > 0]
    if len(row) == 0:
        return 0.0
    p = row / row.sum()
    return float(1 - np.sum(p ** 2))


def bray_curtis_pcoa(matrix_a: np.ndarray, matrix_b: np.ndarray,
                     max_samples: int = 200):
    """
    Compute Bray-Curtis distance matrix and PCoA for two groups.
    Subsamples to max_samples per group for performance.
    """
    # Set random seed for reproducible subsampling
    np.random.seed(42)

    # Subsample for performance
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
    bc = cdist(combined, combined, metric="braycurtis")
    bc = np.nan_to_num(bc)  # replace any NaN from all-zero rows

    # Classical MDS (PCoA)
    # Double centering
    n_pts = bc.shape[0]
    H = np.eye(n_pts) - np.ones((n_pts, n_pts)) / n_pts
    B = -0.5 * H @ (bc ** 2) @ H

    eigenvalues, eigenvectors = np.linalg.eigh(B)
    # Sort descending
    idx_sort = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[idx_sort]
    eigenvectors = eigenvectors[:, idx_sort]

    # Take first 2 PCoA axes
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

    Steps:
    1. Kruskal-Wallis test per taxon (non-parametric ANOVA)
    2. Estimate LDA score from between/within group variance ratio
    3. Filter by p-value and LDA threshold
    """
    results = []
    for i, taxon in enumerate(taxa):
        vals_a = agg_a[:, i]
        vals_b = agg_b[:, i]

        # Skip taxa with no variation
        if np.std(vals_a) == 0 and np.std(vals_b) == 0:
            continue

        # Kruskal-Wallis test (non-parametric one-way ANOVA)
        try:
            kw_stat, kw_p = stats.kruskal(vals_a, vals_b)
        except Exception:
            continue

        if kw_p >= p_threshold:
            continue

        # LDA effect size estimation
        # Approximation: log10(1 + abs(mean_diff) * scaling_factor)
        mean_a = float(np.mean(vals_a))
        mean_b = float(np.mean(vals_b))
        grand_mean = float(np.mean(np.concatenate([vals_a, vals_b])))

        # Between-class variance
        n_a, n_b = len(vals_a), len(vals_b)
        between_var = (n_a * (mean_a - grand_mean) ** 2 +
                       n_b * (mean_b - grand_mean) ** 2) / (n_a + n_b)

        # Within-class variance
        within_var = (n_a * float(np.var(vals_a)) +
                      n_b * float(np.var(vals_b))) / (n_a + n_b)

        # LDA score approximation
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

    # Sort by LDA score descending
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

    Returns F-statistic, p-value, R² (effect size).
    """
    np.random.seed(42)

    # Subsample for performance
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

    # Compute Bray-Curtis distance matrix
    bc_dist = cdist(combined, combined, metric="braycurtis")
    bc_dist = np.nan_to_num(bc_dist)

    # Calculate pseudo-F statistic
    def calc_pseudo_f(dist_matrix: np.ndarray, group_labels: np.ndarray) -> tuple[float, float]:
        """Calculate pseudo-F and R² from distance matrix and group labels."""
        n_total = len(group_labels)
        groups = np.unique(group_labels)
        k = len(groups)

        # Total sum of squared distances
        ss_total = np.sum(dist_matrix ** 2) / (2 * n_total)

        # Within-group sum of squares
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

    # Permutation test
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


# ── API endpoints ───────────────────────────────────────────────────────────────

@app.get("/api/health", summary="Health check",
         description="Returns API status and current timestamp.")
@no_cache_tracking
@limiter.limit("120/minute")
def health(request: Request):
    """Health check."""
    body = {"status": "ok", "timestamp": datetime.now().isoformat()}
    report = getattr(app.state, "cache_audit_report", None)
    if report is not None:
        if report.stale:
            body["stale_cache_warnings"] = report.stale
        if report.seeded:
            body["seeded_count"] = len(report.seeded)
        if report.unknown:
            body["unknown_count"] = len(report.unknown)
    return body


@app.get("/api/filter-options",
         summary="Get filter options",
         description="Returns available filter values for countries, diseases, age groups, and sexes.")
@limiter.limit("120/minute")
def filter_options(request: Request):
    """
    Return available filter option values from metadata.
    """
    cache_key = "filter_options_v1"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
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
        "diseases": diseases[:500],       # limit for payload size
        "age_groups": age_groups,
        "sexes": sexes,
    }
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


@app.get("/api/data-stats",
         summary="Dataset statistics",
         description="Returns total sample count, country count, condition-label counts, and data version.")
@limiter.limit("120/minute")
def data_stats(request: Request):
    """
    Return current dataset statistics for homepage display.
    """
    cache_key = "data_stats_v1"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
    meta = get_metadata()

    # Read version info if exists
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
        "total_diseases": len(non_nc_condition_labels),
        "total_non_nc_condition_labels": len(non_nc_condition_labels),
        "total_condition_categories": len(non_nc_condition_labels) + int(has_nc_category),
        "total_projects": count_unique_projects(meta),
        "total_taxa": count_total_taxa_from_abundance(),
        "total_unique_genera": count_unique_genera_resolved(),
        "total_genera": count_total_taxa_from_abundance(),
        "country_project_counts": country_project_counts,
        "last_updated": version_info.get("last_updated", datetime.now().strftime("%Y-%m-%d")),
        "version": version_info.get("version", f"v1.0_{datetime.now().strftime('%Y%m%d')}"),
    }
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


@app.get("/api/project-timeline",
         summary="Project growth timeline",
         description="Returns yearly sample and project counts for homepage trend views.")
@limiter.limit("120/minute")
def project_timeline(request: Request):
    """Return yearly sample/project counts derived from metadata pubdate."""
    cache_key = "project_timeline_v1"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

    meta = get_metadata().copy()
    project_col = get_project_column(meta)

    if "year" not in meta.columns and "pubdate" in meta.columns:
        meta["year"] = pd.to_datetime(meta["pubdate"], errors="coerce").dt.year

    if "year" not in meta.columns:
        result = {"timeline": []}
        set_cached(cache_key, result)
        set_disk_cached(cache_key, result)
        return result

    year_meta = meta.dropna(subset=["year"]).copy()
    if year_meta.empty:
        result = {"timeline": []}
        set_cached(cache_key, result)
        set_disk_cached(cache_key, result)
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
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


@app.get("/api/disease-names-zh",
         summary="Disease name translations",
         description="Returns Chinese translations for disease names.")
@limiter.limit("120/minute")
def get_disease_names_zh(request: Request):
    """Return disease name Chinese translations.
    Merges disease_names_zh.json + disease_ontology standard_name_zh"""
    cache_key = "disease_names_zh_v1"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
    merged = dict(DISEASE_NAMES_ZH)
    # Add translations from ontology that aren't in the manual file
    for key, info in DISEASE_ONTOLOGY.items():
        if key not in merged:
            zh = info.get("standard_name_zh", "")
            if zh:
                merged[key] = zh
    set_cached(cache_key, merged)
    set_disk_cached(cache_key, merged)
    return merged


@app.get("/api/disease-display-names",
         summary="Standardized display names for diseases",
         description="Returns a mapping from raw disease keys to standardized full display names (no abbreviation suffix).")
@limiter.limit("120/minute")
def get_disease_display_names(request: Request):
    """Return standardized display names (full name only, no abbreviation suffix)."""
    cache_key = "disease_display_names_v1"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
    keys = set(DISEASE_ONTOLOGY.keys()) | set(DISEASE_NAMES_EN_FALLBACK.keys())
    keys.update(item["name"] for item in get_disease_list_cached())
    result = {key: disease_to_en(key) for key in sorted(keys, key=_disease_sort_key)}
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


@app.post("/api/estimate-sample-count",
          summary="Estimate sample count",
          description="Return matched sample counts for Group A and Group B before running differential analysis.")
@no_cache_tracking
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
@no_cache_tracking
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
@no_cache_tracking
@limiter.limit("20/minute")
def diff_analysis(request: Request, req: DiffAnalysisRequest):
    """
    Perform differential abundance analysis between two sample groups.

    Steps:
    1. Filter metadata to get sample IDs for each group
    2. Extract abundance data for those samples
    3. Run Wilcoxon/t-test per taxon
    4. Apply BH correction
    5. Calculate alpha and beta diversity
    """
    meta = get_metadata()
    abund = get_abundance()

    # Get sample subsets
    group_a_meta = apply_filter(meta, req.group_a_filter)
    group_b_meta = apply_filter(meta, req.group_b_filter)

    if len(group_a_meta) == 0:
        raise HTTPException(400, "Group A: no samples match the given filters")
    if len(group_b_meta) == 0:
        raise HTTPException(400, "Group B: no samples match the given filters")

    # Match sample keys to abundance index
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

@app.get("/api/phenotype-groups",
         summary="List phenotype groups",
         description="Return all available groups for a dimension type with sample counts.")
@limiter.limit("60/minute")
def phenotype_groups(request: Request, dim_type: str = "disease"):
    """
    Return groups and sample counts for a given dimension type.
    """
    cache_key = f"phenotype_groups_v1:{dim_type}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

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
        result = {"dim_type": dim_type, "groups": groups}
    elif dim_type == "age":
        if "age_group" not in meta.columns:
            result = {"dim_type": dim_type, "groups": []}
        else:
            vc = meta["age_group"].dropna().astype(str).value_counts()
            groups = [{"group": k, "sample_count": int(v)} for k, v in vc.items() if k.strip()]
            result = {"dim_type": dim_type, "groups": groups}
    elif dim_type == "sex":
        if "sex" not in meta.columns:
            result = {"dim_type": dim_type, "groups": []}
        else:
            vc = meta["sex"].dropna().astype(str).value_counts()
            groups = [{"group": k, "sample_count": int(v)} for k, v in vc.items() if k.strip() and k in {"male", "female"}]
            result = {"dim_type": dim_type, "groups": groups}
    else:
        result = {"dim_type": dim_type, "groups": []}

    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


def _get_samples_by_pheno(meta: pd.DataFrame, dim_type: str, group: str) -> pd.Series:
    """Return a boolean mask over meta rows matching the given phenotype group.

    dim_type:
        - "sex":     exact match on meta["sex"].
        - "age":     exact match on meta["age_group"].
        - "disease": match on any of inform0..inform11 equal to the group label
                     (covers both disease labels such as "UC" and the control
                     label "NC").
    """
    if dim_type == "sex":
        if "sex" not in meta.columns:
            return pd.Series(False, index=meta.index)
        return meta["sex"].fillna("").astype(str).str.strip() == group
    if dim_type == "age":
        if "age_group" not in meta.columns:
            return pd.Series(False, index=meta.index)
        return meta["age_group"].fillna("").astype(str).str.strip() == group
    if dim_type == "disease":
        mask = pd.Series(False, index=meta.index)
        for col in (f"inform{i}" for i in range(12)):
            if col in meta.columns:
                mask |= meta[col].fillna("").astype(str).str.strip() == group
        return mask
    return pd.Series(False, index=meta.index)


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
    Core phenotype association analysis: Mann-Whitney U + BH-FDR + effect size + prevalence + phylum annotation.
    """
    # Cache schema version: bump _v<n> when this endpoint's output shape changes.
    cache_key = f"phenotype_assoc_v1:{dim_type}:{group_a}:{group_b}:{tax_level}:{min_prevalence}:{top_n}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached
    disk_cached = get_disk_cached_by_data(cache_key)
    if disk_cached is not None:
        set_cached(cache_key, disk_cached)
        return disk_cached

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

    result = {
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
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


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
    Return abundance distribution (Q1/Q3/median/whiskers) per phenotype group (for box plots).
    """
    cache_key = f"phenotype_taxa_profile_v1:{taxon}:{dim_type}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached
    disk_cached = get_disk_cached_by_data(cache_key)
    if disk_cached is not None:
        set_cached(cache_key, disk_cached)
        return disk_cached

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
    result = {"taxon": taxon, "dim_type": dim_type, "groups": profile_data}
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


# ── Species search & profile endpoints ──────────────────

# Invalid genus names to filter out (taxonomy parsing artifacts)
_INVALID_GENERA = {"NA", "group", "Sedis", "Incertae", "unclassified",
                   "uncultured", "Unknown", "unknown", "noname", "002", "sp"}


def is_valid_genus(name: str) -> bool:
    """Check if genus name is a real biological name."""
    if not name or len(name) < 3:
        return False
    if name in _INVALID_GENERA:
        return False
    if name[0].islower():  # real genus names are capitalized
        return False
    if name.isdigit():
        return False
    return True


@lru_cache(maxsize=1)
def get_genus_list() -> list[str]:
    """Return sorted list of all valid genus names from abundance data."""
    abund = get_abundance()
    genera = sorted(set(g for g in (extract_genus(c) for c in abund.columns) if is_valid_genus(g)))
    return genera


@app.get("/api/species-search",
         summary="Search genera",
         description="Full-text search across genus names, returns up to 20 matches.")
@no_cache_tracking
@limiter.limit("120/minute")
def species_search(request: Request, q: str = ""):
    """
    Autocomplete genus search. Returns up to 20 matching genus names.
    """
    if not q or len(q.strip()) < 2:
        return {"results": []}
    q_lower = q.strip().lower()
    genera = get_genus_list()
    # Prefix matches first, then contains matches
    prefix = [g for g in genera if g.lower().startswith(q_lower)]
    contains = [g for g in genera if q_lower in g.lower() and g not in prefix]
    return {"results": (prefix + contains)[:20]}


@app.get("/api/species-profile",
         summary="Genus profile",
         description="Detailed abundance profile for a genus across diseases, countries, age groups, and sex.")
@limiter.limit("60/minute")
def species_profile(request: Request, genus: str):
    """
    Return a comprehensive profile for a given genus: abundance, disease, country, age, and sex distributions.
    """
    cache_key = f"species_profile_v1:{genus.strip().lower()}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

    meta = get_metadata()
    abund = get_abundance()

    if not genus or not genus.strip():
        raise HTTPException(400, "genus parameter is required")

    genus = genus.strip()

    # Find matching columns in abundance matrix
    matching_cols = [c for c in abund.columns if extract_genus(c).lower() == genus.lower()]
    if not matching_cols:
        raise HTTPException(404, f"Genus '{genus}' not found in abundance data")

    # Resolve the canonical genus name
    canonical_name = extract_genus(matching_cols[0])

    # Sum abundance across all matching columns for the genus
    genus_raw = abund[matching_cols].sum(axis=1)

    # Convert to relative abundance (%) per sample
    sample_totals = abund.sum(axis=1).replace(0, 1)
    genus_abundance = genus_raw / sample_totals * 100

    # Match to metadata
    meta_with_key = meta.set_index("sample_key")
    common_keys = genus_abundance.index.intersection(meta_with_key.index)

    if len(common_keys) == 0:
        raise HTTPException(404, "No matching samples between abundance and metadata")

    ga = genus_abundance.loc[common_keys]
    mm = meta_with_key.loc[common_keys]

    # Overall stats
    present_count = int((ga > 0).sum())
    total_count = len(ga)
    mean_abundance = float(ga.mean())
    median_abundance = float(np.median(ga.values.astype(float))) if total_count > 0 else 0.0
    prevalence = present_count / total_count if total_count > 0 else 0
    phylum = extract_phylum(matching_cols[0]) or "Other"

    # Strict NC baseline
    inform_cols = [f"inform{i}" for i in range(12)]
    nc_mask = _strict_nc_mask(mm, inform_cols)
    nc_vals = ga.loc[nc_mask].values.astype(float)
    nc_mean_val = float(np.mean(nc_vals)) if len(nc_vals) > 0 else 0.0
    nc_prevalence_val = float((nc_vals > 0).sum() / len(nc_vals)) if len(nc_vals) > 0 else 0.0

    AGE_GROUP_ORDER = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian", "Unknown"]

    # Helper: group mean abundance
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

    # Disease distribution (from inform0-11)
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
    set_disk_cached(cache_key, result)
    return result


# ── Biomarker profile endpoint ─────────────────────────

@app.get("/api/biomarker-profile",
         summary="Cross-disease biomarker profile",
         description="For a given genus, compute log2 fold change vs healthy controls across all diseases.",
         tags=["Species"])
@limiter.limit("30/minute")
def biomarker_profile(request: Request, genus: str, min_samples: int = 10):
    """
    Cross-disease biomarker profile: for a given genus, compute its differential
    abundance (log2FC + Wilcoxon p) against healthy controls in every disease.
    """
    if not genus or not genus.strip():
        raise HTTPException(400, "genus parameter is required")

    cache_key = f"biomarker_profile_v1:{genus.strip().lower()}:{min_samples}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
    genus = genus.strip()

    meta = get_metadata()
    abund = get_abundance()

    # Find genus columns
    matching_cols = [c for c in abund.columns if extract_genus(c).lower() == genus.lower()]
    if not matching_cols:
        raise HTTPException(404, f"Genus '{genus}' not found in abundance data")

    canonical_name = extract_genus(matching_cols[0])

    # Compute genus relative abundance
    genus_raw = abund[matching_cols].sum(axis=1)
    sample_totals = abund.sum(axis=1).replace(0, 1)
    genus_rel = (genus_raw / sample_totals * 100).rename("rel_abund")

    # Link metadata
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

    # Collect disease groups
    disease_samples: dict[str, list[str]] = {}
    for col in INFORM_COLS:
        if col not in mm.columns:
            continue
        for idx, val in mm[col].fillna("").astype(str).str.strip().items():
            normalized = _normalize_inform_label(val)
            if not normalized or normalized.lower() == "nc":
                continue
            disease_samples.setdefault(normalized, []).append(str(idx))

    # Compute log2FC and Wilcoxon p for each disease
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

    # BH FDR correction
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
    set_disk_cached(cache_key, result)
    return result


# ── Disease ontology endpoint ──────────────────────────────────

@app.get("/api/disease-ontology", tags=["Disease"],
         summary="Disease ontology mapping",
         description="Returns standardized disease names, MeSH IDs, ICD-10 codes, and categories for all diseases")
@limiter.limit("120/minute")
def disease_ontology(request: Request):
    cache_key = "disease_ontology_v1"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
    result = DISEASE_ONTOLOGY
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


# ── Disease browser endpoints ─────────────────────────────────

@lru_cache(maxsize=1)
def get_disease_list_cached() -> list[dict]:
    """
    Build a list of all unique diseases with sample counts.
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
    """
    cache_key = f"disease_list_v1:{q.strip().lower()}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

    diseases = get_disease_list_cached()
    if q and q.strip():
        q_lower = q.strip().lower()
        diseases = [d for d in diseases if q_lower in d["name"].lower()]
    # Enrich each disease with ontology info
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
    result = {"diseases": enriched}
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


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

    cache_key = f"metabolism_profile_v1:{category_id}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

    context = _prepare_metabolism_context(get_metadata(), get_abundance())
    result = _metabolism_category_result(category, context, METABOLISM_PROFILE_DISEASE_LIMIT)
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
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
    cache_key = "metabolism_overview_v1"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk, ttl=METABOLISM_OVERVIEW_TTL)
        return disk

    mapping = load_metabolism_mapping()
    context = _prepare_metabolism_context(get_metadata(), get_abundance())
    result = _build_metabolism_overview(mapping, context)
    set_cached(cache_key, result, ttl=METABOLISM_OVERVIEW_TTL)
    set_disk_cached(cache_key, result)
    return result


@app.get("/api/disease-profile",
         summary="Disease microbiome profile",
         description="Top genera for a disease vs healthy controls with fold-change and prevalence.")
@limiter.limit("60/minute")
def disease_profile(request: Request, disease: str, top_n: int = 40):
    """
    Return a profile for a given disease:
    - Top N genera by mean abundance in disease samples
    - Comparison with healthy control (control = samples with no disease annotation)
    - Age group distribution
    - Sex distribution
    - Country distribution
    """
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")
    disease = disease.strip()

    cache_key = f"disease_profile_v1:{disease}:{top_n}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

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
    set_disk_cached(cache_key, result)
    return result


# ── Microbe-disease network endpoint ─────────────────────

@app.get("/api/disease-studies",
         summary="Disease study breakdown",
         description="Returns per-project disease/control counts, dominant country, top marker, and consistency score.")
@limiter.limit("60/minute")
def disease_studies(request: Request, disease: str):
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")
    disease = disease.strip()

    cache_key = f"disease_studies_v1:{disease}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

    try:
        result = build_disease_studies(get_metadata(), get_abundance(), disease)
    except ValueError as exc:
        message = str(exc)
        if "not found" in message.lower():
            raise HTTPException(404, message)
        raise HTTPException(400, message)

    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
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
    Edge weight = mean abundance of genus in disease samples.
    """
    cache_key = f"network_v1:{top_diseases}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    # Disk-persistent cache: no recomputation needed after restart (7-day TTL)
    disk_cached = get_disk_cached_by_data(cache_key)
    if disk_cached:
        set_cached(cache_key, disk_cached)   # sync to in-memory cache
        return disk_cached
    meta = get_metadata()
    abund = get_abundance()

    INFORM_COLS = [f"inform{i}" for i in range(12)]

    # Get top diseases by sample count
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

    # Build genus map (keep only valid genera via is_valid_genus)
    genus_map: dict[str, list[str]] = {}
    for col in abund.columns:
        g = extract_genus(col)
        if is_valid_genus(g):
            genus_map.setdefault(g, []).append(col)

    # Pre-aggregate columns by genus into genus_abund (rows=samples, cols=genera)
    # Avoids per-genus slicing inside the per-disease inner loop for speed
    genus_cols_ordered = list(genus_map.keys())
    genus_abund_full = pd.DataFrame(
        {g: abund[cols].sum(axis=1) for g, cols in genus_map.items()},
        index=abund.index,
    )

    # For each disease, compute mean abundance of each genus
    edges: list[dict] = []
    genus_set: set[str] = set()

    for disease_name in disease_names:
        sample_indices = disease_counts[disease_name]
        sample_keys = meta.loc[list(sample_indices), "sample_key"].dropna().unique()
        common = genus_abund_full.index.intersection(sample_keys)
        if len(common) == 0:
            continue
        disease_slice = genus_abund_full.loc[common]
        # Row-normalise to relative abundance (%)
        row_totals = disease_slice.sum(axis=1).replace(0, 1)
        disease_rel = disease_slice.div(row_totals, axis=0) * 100

        # Rank genera by mean abundance, keep top_genera
        genus_means_s = disease_rel.mean()
        genus_means_s = genus_means_s[genus_means_s > 0].nlargest(top_genera)

        for genus, mean_val in genus_means_s.items():
            edges.append({
                "source": disease_name,
                "target": genus,
                "weight": round(float(mean_val), 6),
            })
            genus_set.add(genus)

    # Build nodes
    nodes = []
    for d in disease_names:
        nodes.append({"id": d, "type": "disease", "size": len(disease_counts[d])})
    for g in genus_set:
        nodes.append({"id": g, "type": "genus", "size": 1})

    result = {"nodes": nodes, "edges": edges}
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)   # persist to disk for instant load after restart
    return result


# ── Data management endpoints ──────────────────────────────────

def _check_admin(token: str | None):
    """Reject if ADMIN_TOKEN unset or token doesn't match."""
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        raise HTTPException(401, "Invalid admin token")


@app.get("/api/admin/check",
         summary="Admin authentication check",
         description="Verify admin token validity.")
@no_cache_tracking
@limiter.limit("10/minute")
def admin_check(request: Request, x_admin_token: str | None = Header(None)):
    """Verify admin token."""
    _check_admin(x_admin_token)
    return {"status": "authorized"}


@app.delete("/api/admin/clear-disk-cache",
            summary="Clear disk cache",
            description="Delete all disk-persisted cache files. Call after changing computation logic or uploading new data.",
            tags=["Admin"])
@no_cache_tracking
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


@app.delete("/api/admin/purge-endpoint-cache",
            summary="Purge cache for a single endpoint",
            description="Delete disk cache files matching the endpoint's cache_key prefix "
                        "AND clear the same prefix from memory cache. Use when a code "
                        "change alters an endpoint's output so stale cached results must be discarded.",
            tags=["Admin"])
@no_cache_tracking
@limiter.limit("20/minute")
def purge_endpoint_cache(
    request: Request,
    endpoint: str,
    x_admin_token: str | None = Header(None),
):
    """Purge disk+memory cache entries for a single endpoint by cache_key prefix.

    `endpoint` is the endpoint name from /api/cache-audit (e.g. 'species_profile'),
    which maps to cache_key prefix `species_profile_v1:`.
    """
    _check_admin(x_admin_token)
    audits = cache_audit.scan_endpoints(app)
    audit = next((a for a in audits if a.fn_name == endpoint), None)
    if audit is None:
        raise HTTPException(404, f"endpoint '{endpoint}' not found in audit scan")
    if not audit.cache_key_name:
        raise HTTPException(400, f"endpoint '{endpoint}' has no cache_key (not tracked)")
    # cache_key_name is the bare identifier (e.g. 'species_profile_v1'); disk
    # filenames derive from full cache keys (e.g. 'species_profile_v1_<slug>.json').
    fname_prefix = f"{audit.cache_key_name}_"
    mem_prefix = f"{audit.cache_key_name}:"

    deleted_files: list[str] = []
    try:
        for fname in os.listdir(_DISK_CACHE_DIR):
            if not fname.endswith(".json") or fname.startswith("."):
                continue
            if fname.startswith(fname_prefix):
                os.remove(os.path.join(_DISK_CACHE_DIR, fname))
                deleted_files.append(fname)
    except Exception as e:
        raise HTTPException(500, f"Failed to purge disk cache: {e}") from e

    mem_keys = [k for k in list(_RESULT_CACHE.keys()) if k.startswith(mem_prefix)]
    for k in mem_keys:
        _RESULT_CACHE.pop(k, None)

    return {
        "endpoint": endpoint,
        "cache_key_name": audit.cache_key_name,
        "disk_files_deleted": len(deleted_files),
        "memory_keys_cleared": len(mem_keys),
        "sample_files": deleted_files[:5],
    }


@app.get("/api/admin/genus-prevalence",
         summary="Top-N genera ranked by sample prevalence",
         description="Return the top-N genus names sorted by number of samples "
                     "where the genus has non-zero abundance. Used by warmup scripts "
                     "to prioritize high-coverage genera over rare ones.",
         tags=["Admin"])
@no_cache_tracking
@limiter.limit("6/minute")
def genus_prevalence(
    request: Request,
    top: int = 500,
    x_admin_token: str | None = Header(None),
):
    _check_admin(x_admin_token)
    if top < 1 or top > 5000:
        raise HTTPException(400, "top must be in [1, 5000]")
    abund = get_abundance()

    # Species columns → genus mapping (drop invalid genera).
    col_to_genus: dict[str, str] = {}
    for c in abund.columns:
        g = extract_genus(c)
        if is_valid_genus(g):
            col_to_genus[c] = g

    # Build genus → column list.
    genus_cols: dict[str, list[str]] = {}
    for c, g in col_to_genus.items():
        genus_cols.setdefault(g, []).append(c)

    # Prevalence = count of samples where summed genus abundance > 0.
    prev: dict[str, int] = {}
    for g, cols in genus_cols.items():
        if len(cols) == 1:
            series = abund[cols[0]]
        else:
            series = abund[cols].sum(axis=1)
        prev[g] = int((series > 0).sum())

    ranked = sorted(prev.items(), key=lambda kv: kv[1], reverse=True)[:top]
    return {
        "total_genera": len(prev),
        "total_samples": int(abund.shape[0]),
        "top_n": len(ranked),
        "genera": [{"genus": g, "prevalence": p,
                    "prevalence_pct": round(100.0 * p / max(abund.shape[0], 1), 2)}
                   for g, p in ranked],
    }


@app.post("/api/admin/rehash-seed",
          summary="Reset cache-audit baseline hashes",
          description="Rewrite .endpoint_source_hashes.json using current source. "
                      "Use after a legitimate refactor to silence spurious stale warnings. "
                      "Pass ?endpoint=NAME to reset one entry only.",
          tags=["Admin"])
@no_cache_tracking
@limiter.limit("10/minute")
def rehash_seed(
    request: Request,
    endpoint: Optional[str] = None,
    x_admin_token: str | None = Header(None),
):
    _check_admin(x_admin_token)
    if endpoint is None:
        try:
            CACHE_AUDIT_HASH_FILE.unlink(missing_ok=True)
        except OSError as e:
            raise HTTPException(500, f"Failed to delete hash file: {e}") from e
        report = cache_audit.run(app, CACHE_AUDIT_HASH_FILE)
        app.state.cache_audit_report = report
        return {"reset": "all", "seeded": len(report.seeded)}
    audits = cache_audit.scan_endpoints(app)
    try:
        cache_audit.reset_endpoint(endpoint, CACHE_AUDIT_HASH_FILE, audits)
    except KeyError:
        raise HTTPException(404, f"endpoint '{endpoint}' not found in audit scan")
    app.state.cache_audit_report = cache_audit.run(app, CACHE_AUDIT_HASH_FILE)
    return {"reset": endpoint}


@app.get("/api/cache-audit",
         summary="Per-endpoint cache status snapshot",
         description="Read-only snapshot: per-endpoint tracking state, disk file "
                     "size+age, orphan files. Zero background cost - re-scans "
                     "on each call (~100ms), no common polling expected.",
         tags=["Admin"])
@no_cache_tracking
@limiter.limit("30/minute")
def cache_audit_snapshot(request: Request):
    """Return full cache status for all endpoints (for admin panel)."""
    audits = cache_audit.scan_endpoints(app)
    prior = cache_audit.load_prior(CACHE_AUDIT_HASH_FILE)
    now_ts = datetime.now().timestamp()

    disk_files: dict[str, dict] = {}
    try:
        for fname in os.listdir(_DISK_CACHE_DIR):
            if not fname.endswith(".json") or fname.startswith("."):
                continue
            fpath = os.path.join(_DISK_CACHE_DIR, fname)
            try:
                stat = os.stat(fpath)
                disk_files[fname] = {
                    "size_kb": round(stat.st_size / 1024, 2),
                    "age_hours": round((now_ts - stat.st_mtime) / 3600, 2),
                }
            except OSError:
                pass
    except OSError:
        pass

    def _classify_uncached(path: str) -> str:
        if path.startswith("/api/download/"):
            return "download_wrapper"
        if path.startswith("/api/admin/"):
            return "admin_endpoint"
        if path == "/api/health":
            return "trivial_static"
        if path.startswith("/api/v1/"):
            return "compatibility_redirect"
        return "intentional_no_cache"

    tracked: list[dict] = []
    uncached: list[dict] = []
    unknown_list: list[dict] = []
    used_files: set[str] = set()

    for a in audits:
        entry = {"path": a.path, "method": a.method, "fn_name": a.fn_name}
        if a.status == "tracked":
            assert a.cache_key_name and a.version
            prefix = f"{a.cache_key_name}_{a.version}"
            matching = [
                f for f in disk_files
                if f == f"{prefix}.json" or f.startswith(f"{prefix}_")
            ]
            used_files.update(matching)
            total_kb = round(sum(disk_files[f]["size_kb"] for f in matching), 2)
            newest_age = min(
                (disk_files[f]["age_hours"] for f in matching),
                default=None,
            )
            baseline = prior.get(a.cache_key_name, {})
            baseline_hash = baseline.get("hash", "")
            entry.update({
                "cache_key": a.cache_key_name,
                "version": a.version,
                "current_hash": a.current_hash,
                "baseline_hash": baseline_hash,
                "stale": bool(baseline_hash) and baseline_hash != a.current_hash,
                "disk_file_count": len(matching),
                "disk_total_kb": total_kb,
                "disk_newest_age_hours": newest_age,
            })
            tracked.append(entry)
        elif a.status == "no_cache_by_design":
            entry["reason"] = _classify_uncached(a.path)
            uncached.append(entry)
        else:
            entry["status"] = a.status
            if a.cache_key_name:
                entry["cache_key"] = a.cache_key_name
            unknown_list.append(entry)

    orphan_files = sorted(set(disk_files.keys()) - used_files)

    return {
        "summary": {
            "total_endpoints": len(audits),
            "tracked": len(tracked),
            "uncached_by_design": len(uncached),
            "unknown": len(unknown_list),
            "stale_count": sum(1 for t in tracked if t.get("stale")),
            "disk_file_count": len(disk_files),
            "disk_orphan_count": len(orphan_files),
            "baseline_seeded_at": prior.get("_meta", {}).get("seeded_at"),
        },
        "tracked": sorted(tracked, key=lambda x: x["path"]),
        "uncached_by_design": sorted(uncached, key=lambda x: x["path"]),
        "unknown": sorted(unknown_list, key=lambda x: x["path"]),
        "disk_orphan_files": orphan_files,
    }


@app.post("/api/admin/upload-metadata",
          summary="Upload metadata",
          description="Upload and merge new metadata CSV file (requires admin token).")
@no_cache_tracking
@limiter.limit("10/minute")
async def upload_metadata(
    request: Request,
    file: UploadFile = File(...),
    x_admin_token: str | None = Header(None),
):
    """
    Upload new metadata CSV to merge into dataset. Requires admin token.
    """
    _check_admin(x_admin_token)

    # Save to temp file
    suffix = ".csv"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        from data_manager import update_metadata
        result = update_metadata(tmp_path)
        # Clear metadata cache so next request loads fresh data
        get_metadata.cache_clear()
        return result
    finally:
        os.unlink(tmp_path)


@app.post("/api/admin/validate-metadata",
          summary="Validate metadata",
          description="Validate metadata CSV file format and columns (requires admin token).")
@no_cache_tracking
@limiter.limit("10/minute")
async def validate_metadata_endpoint(
    request: Request,
    file: UploadFile = File(...),
    x_admin_token: str | None = Header(None),
):
    """Validate metadata CSV format without merging."""
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


# ── Data download endpoints ────────────────────────────────────

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
@no_cache_tracking
@limiter.limit("30/minute")
def download_summary_stats(request: Request, format: str = "csv"):
    """
    Download aggregated summary statistics (NOT raw sample data).

    Returns sample counts by country, disease, age group, and sex.
    """
    meta = get_metadata()
    INFORM_COLS = [f"inform{i}" for i in range(12)]

    # Country stats
    country_counts = meta["country"].value_counts().to_dict()
    # Disease stats
    disease_counts = _inform_label_counts(meta, include_nc=False)
    # Age group stats
    age_counts = meta["age_group"].value_counts().to_dict() if "age_group" in meta.columns else {}
    # Sex stats
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
@no_cache_tracking
@limiter.limit("30/minute")
def download_disease_profile_data(request: Request, disease: str, format: str = "csv"):
    """Download disease profile data."""
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
@no_cache_tracking
@limiter.limit("30/minute")
def download_species_profile_data(request: Request, genus: str, format: str = "csv"):
    """Download species profile data."""
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
@no_cache_tracking
@limiter.limit("30/minute")
def download_genus_list(request: Request, format: str = "csv"):
    """Download list of all genera."""
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
@no_cache_tracking
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
@no_cache_tracking
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
@no_cache_tracking
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
@no_cache_tracking
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
    """
    cache_key = f"biomarker_discovery_v1:{disease.strip() if disease else ''}:{lda_threshold}:{p_threshold}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk_hit = get_disk_cached_by_data(cache_key)
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
    """
    if not disease or not disease.strip():
        raise HTTPException(400, "disease parameter is required")
    disease = disease.strip()

    cache_key = f"lollipop_v1:{disease}:{top_n}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk_hit = get_disk_cached_by_data(cache_key)
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
    """
    cache_key = f"chord_v1:{top_diseases}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
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
    set_disk_cached(cache_key, result)
    return result


@app.get("/api/species-cooccurrence",
         summary="Species co-occurrence partners",
         description="Top co-occurring genera for a given genus, based on Spearman correlation across healthy controls or a selected disease context.",
         tags=["Species"])
@limiter.limit("30/minute")
def species_cooccurrence(request: Request, genus: str, top_k: int = 10, disease: str = ""):
    """
    Return top co-occurring genera for a given genus.
    """
    if not genus or not genus.strip():
        raise HTTPException(400, "genus parameter is required")

    disease_name = disease.strip()
    cache_key = f"species_cooccurrence_v1:{genus.strip().lower()}:{top_k}:{disease_name.lower() or '__nc__'}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

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
    set_disk_cached(cache_key, result)
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
    """
    cache_key = f"cooccurrence_v1:{disease}:{min_r}:{top_genera}:{max_samples}:{method}:{fdr_threshold}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
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
    set_disk_cached(cache_key, result)
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
    cache_key = f"network_compare_v1:{disease}:{min_r}:{top_genera}:{max_samples}:{method}:{fdr_threshold}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

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
    set_disk_cached(cache_key, result)
    return result


AGE_GROUP_ORDER = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian", "Unknown"]
AGE_NAMED_ORDER = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian"]


def _json_safe(obj):
    # Starlette's JSON encoder rejects NaN/inf (strict JSON). scipy.stats
    # routines (spearmanr, kruskal, mannwhitneyu) legitimately return NaN on
    # degenerate inputs, and eta-squared divisions can too. Replace every
    # non-finite float with None so the response round-trips cleanly.
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, (np.floating,)):
        f = float(obj)
        return f if math.isfinite(f) else None
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_json_safe(v) for v in obj)
    return obj


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
    cache_key = f"lifecycle_v9:{disease}:{country}:{top_genera}:{fixed_part}"
    if use_cache:
        cached = get_cached(cache_key)
        if cached:
            return cached
        disk_hit = get_disk_cached_by_data(cache_key)
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

    # PERMANOVA computed offline (too expensive for real-time API)
    permanova_result: dict = {}

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

    result = _json_safe(result)
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
    Return genus composition across 7 named life stages (Infant-Centenarian) plus Unknown.
    """
    cache_key = "lifecycle_v9"  # cache_audit tracking marker; real key built in _lifecycle_internal
    _ = cache_key
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

    cache_key = f"lifecycle_compare_v3:{disease}:{country}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk_hit = get_disk_cached_by_data(cache_key)
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
    result = _json_safe(result)
    set_cached(cache_key, result)
    set_disk_cached(cache_key, result)
    return result


# ── Sample Similarity Search ─────────────────────────────


@app.get("/api/genus-names",
         summary="List all genera",
         description="Returns all valid genus names from abundance data.")
@no_cache_tracking
@limiter.limit("120/minute")
async def get_genus_names(request: Request):
    """Return list of genus names from abundance matrix columns (for frontend template download)."""
    abund = get_abundance()
    genera = sorted(set(extract_genus(c) for c in abund.columns))
    # Filter out invalid genus names
    genera = [g for g in genera if g and g not in ("", "NA", "unknown")]
    return {"genera": genera, "count": len(genera)}


@app.post("/api/similarity-search",
          summary="Sample similarity search",
          description="Find most similar samples using Bray-Curtis or Jaccard distance.")
@no_cache_tracking
@limiter.limit("20/minute")
async def similarity_search(request: Request, req: SimilarityRequest):
    """Receive user abundance vector, return Top-K most similar samples.
    Note: does not expose bulk raw data; only returns sample IDs, distances, and metadata summaries.
    """
    # ── Parameter validation ──
    if req.metric not in ("braycurtis", "jaccard"):
        raise HTTPException(400, "metric must be 'braycurtis' or 'jaccard'")
    if req.top_k < 1 or req.top_k > 50:
        raise HTTPException(400, "top_k must be between 1 and 50")
    if not req.abundances:
        raise HTTPException(400, "abundances dict must not be empty")

    abund = get_abundance()
    meta = get_metadata()
    inform_cols = [f"inform{i}" for i in range(12)]

    # ── Build genus name to column index mapping ──
    genus_to_indices: dict[str, list[int]] = {}
    genus_display: dict[str, str] = {}
    for idx, column in enumerate(abund.columns):
        genus = extract_genus(column).strip()
        genus_key = genus.lower()
        if not genus or not is_valid_genus(genus):
            continue
        genus_to_indices.setdefault(genus_key, []).append(idx)
        genus_display.setdefault(genus_key, genus)

    # ── Sample filtering: narrow search space before similarity retrieval ──
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

    # ── Align user-submitted genus->abundance values to matrix column order ──
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

    # ── Call similarity search function from analysis.py ──
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

    # ── Enrich with metadata (disease, country, age group, project) ──
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

    # ── Lightweight preview heatmap payload: return relative abundances for query + top matches on a few genera ──
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


# ── Cross-study meta-analysis ────────────────────────────────

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
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

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
    set_disk_cached(cache_key, result)
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
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk

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
    set_disk_cached(cache_key, result)
    return result


@app.post("/api/cross-study",
          summary="Cross-study meta-analysis",
          description="""
Cross-cohort consensus biomarker discovery with inverse-variance weighted meta-analysis.

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
    """Cross-study meta-analysis: multi-cohort consensus biomarker discovery."""
    if len(req.project_ids) < 2:
        raise HTTPException(400, "At least 2 projects required")
    _cross_study_payload = {
        "project_ids": sorted(str(project_id).strip() for project_id in req.project_ids),
        "disease": req.disease.strip(),
        "method": req.method,
        "taxonomy_level": req.taxonomy_level,
        "p_threshold": req.p_threshold,
        "min_studies": req.min_studies,
    }
    _cross_study_body_hash = hashlib.sha256(
        json.dumps(_cross_study_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    cache_key = f"cross_study_v2:{_cross_study_body_hash}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    disk = get_disk_cached_by_data(cache_key)
    if disk is not None:
        set_cached(cache_key, disk)
        return disk
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
            # (pseudocount 1e-6) — log-scale SE for meta-analysis
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
    set_disk_cached(cache_key, result)
    return result


# ── Health Index ──────────────────────────────────────────

def _psi_score(values_pct: np.ndarray, R_prime: float, detection_pct: float = 1e-3) -> float:
    """
    Gupta 2020 (Nat Commun) marker-set ψ score:
        ψ_M = (R_M / R_M_prime) × H_M
    where R_M is the count of markers with relative abundance > detection,
    R_M_prime is the median observed marker count in healthy training,
    H_M is Shannon entropy over the marker abundances (renormalised to sum 1).

    Args:
        values_pct: 1-D array of relative abundances (%) for the marker genera
                    found in this sample. Length = number of markers in set M.
        R_prime: median NC marker richness for set M.
        detection_pct: relative-abundance detection threshold in % units
                       (Gupta uses 1e-5 fraction = 1e-3 %).
    Returns:
        ψ scalar (≥ 0).
    """
    if values_pct.size == 0 or R_prime <= 0:
        return 0.0
    present = values_pct[values_pct > detection_pct]
    R = float(present.size)
    if R == 0:
        return 0.0
    p = present / present.sum()
    # Shannon entropy in nats — Gupta uses natural log
    H = float(-(p * np.log(p + 1e-12)).sum())
    return (R / R_prime) * H


@lru_cache(maxsize=1)
def _compute_health_disease_genera() -> dict:
    """
    Per-study random-effects meta-analysis for health/disease genus selection.
    Computes NC vs disease log2FC and variance independently within each BioProject,
    then pools cross-study effect sizes via DerSimonian-Laird random-effects model.

    Disk cache: result pickled next to main.py. Auto-invalidated when either
    METADATA_PATH or ABUNDANCE_PATH source file mtime is newer than the pickle.
    """
    import pickle
    _cache_path = Path(__file__).parent / "health_disease_genera.pkl"
    try:
        _src_mtime = max(
            os.path.getmtime(METADATA_PATH) if METADATA_PATH and os.path.exists(METADATA_PATH) else 0,
            os.path.getmtime(ABUNDANCE_PATH) if ABUNDANCE_PATH and os.path.exists(ABUNDANCE_PATH) else 0,
        )
        if _cache_path.exists() and _cache_path.stat().st_mtime >= _src_mtime > 0:
            with open(_cache_path, "rb") as _f:
                _cached = pickle.load(_f)
            logging.info(f"health_disease_genera: loaded from disk cache ({_cache_path.name})")
            return _cached
    except Exception as _e:
        logging.warning(f"health_disease_genera: disk cache read failed ({_e}), recomputing")

    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)
    col_names = abund.columns.tolist()

    INFORM_COLS = [f"inform{i}" for i in range(12)]

    project_col = get_project_column(meta)
    if project_col is None:
        return {"health_genera": [], "disease_genera": [], "nc_stats": {}}

    nc_mask = _strict_nc_mask(meta, INFORM_COLS)
    disease_mask = _non_nc_disease_mask(meta, INFORM_COLS)

    meta_local = meta.copy()
    meta_local["_is_nc"] = nc_mask
    meta_local["_is_dis"] = disease_mask
    meta_local["_proj"] = meta_local[project_col].astype(str).str.strip()
    meta_local = meta_local[(meta_local["_is_nc"] | meta_local["_is_dis"])]
    meta_local = meta_local[meta_local["sample_key"].isin(abund_idx)]

    if len(meta_local) < 20:
        return {"health_genera": [], "disease_genera": [], "nc_stats": {}}

    # Genus-level aggregation (done once upfront)
    genus_labels = [extract_genus(c) for c in col_names]
    unique_genera_all = list(dict.fromkeys(genus_labels))
    valid_idx = [i for i, g in enumerate(unique_genera_all) if is_valid_genus(g)]
    unique_genera = [unique_genera_all[i] for i in valid_idx]
    genus_to_cols: dict[str, list[int]] = {g: [] for g in unique_genera}
    for j, lbl in enumerate(genus_labels):
        if lbl in genus_to_cols:
            genus_to_cols[lbl].append(j)

    # ── Per-study effect size (Hedges' g on log-transformed abundance) ──
    # log(abundance + 1e-6) → mean / sd by group → standardised mean difference
    per_study_effects: dict[str, list[tuple[float, float, int]]] = {g: [] for g in unique_genera}
    # values: list of (g_hedges, var_g, n_total) one per study

    nc_pool_keys: list[str] = []  # for nc_stats reference (raw, all NC)

    pseudo = 1e-6
    studies_used = 0
    for proj_id, sub in meta_local.groupby("_proj"):
        nc_keys = sub.loc[sub["_is_nc"], "sample_key"].tolist()
        dis_keys = sub.loc[sub["_is_dis"], "sample_key"].tolist()
        # Need both arms with ≥5 samples to compute a stable effect
        if len(nc_keys) < 5 or len(dis_keys) < 5:
            continue
        studies_used += 1

        raw_nc = abund.loc[nc_keys].values.astype(float)
        raw_dis = abund.loc[dis_keys].values.astype(float)
        tot_nc = raw_nc.sum(axis=1, keepdims=True); tot_nc[tot_nc == 0] = 1
        tot_dis = raw_dis.sum(axis=1, keepdims=True); tot_dis[tot_dis == 0] = 1
        mat_nc = raw_nc / tot_nc * 100
        mat_dis = raw_dis / tot_dis * 100
        n1, n2 = len(nc_keys), len(dis_keys)
        nc_pool_keys.extend(nc_keys)

        for gi, genus in enumerate(unique_genera):
            cidx = genus_to_cols[genus]
            if not cidx:
                continue
            v_nc = mat_nc[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_nc[:, cidx[0]]
            v_dis = mat_dis[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_dis[:, cidx[0]]
            m1, m2 = float(v_nc.mean()), float(v_dis.mean())
            s1, s2 = float(v_nc.std(ddof=1)), float(v_dis.std(ddof=1))
            if s1 + s2 < 1e-9:
                continue
            sp2 = ((n1 - 1) * s1 * s1 + (n2 - 1) * s2 * s2) / (n1 + n2 - 2)
            if sp2 <= 0:
                continue
            sp = math.sqrt(sp2)
            d = (m1 - m2) / sp                           # Cohen's d (NC − Disease)
            # Hedges' small-sample correction
            J = 1.0 - 3.0 / (4 * (n1 + n2) - 9)
            g_h = J * d
            var_g = (n1 + n2) / (n1 * n2) + (g_h * g_h) / (2 * (n1 + n2))
            if not (math.isfinite(g_h) and math.isfinite(var_g)) or var_g <= 0:
                continue
            per_study_effects[genus].append((g_h, var_g, n1 + n2))

    if studies_used < 2:
        # Fallback: insufficient stratification — return empty so caller knows
        return {
            "health_genera": [], "disease_genera": [], "nc_stats": {},
            "n_studies": studies_used,
        }

    # ── DerSimonian-Laird random-effects pooling ──
    def pool_re(effects: list[tuple[float, float, int]]) -> tuple[float, float, float, int] | None:
        if len(effects) < 2:
            return None
        gs = np.array([e[0] for e in effects], dtype=float)
        vs = np.array([e[1] for e in effects], dtype=float)
        w_fe = 1.0 / vs
        g_fe = float((w_fe * gs).sum() / w_fe.sum())
        Q = float((w_fe * (gs - g_fe) ** 2).sum())
        k = len(effects)
        c = float(w_fe.sum() - (w_fe * w_fe).sum() / w_fe.sum())
        tau2 = max(0.0, (Q - (k - 1)) / c) if c > 0 else 0.0
        w_re = 1.0 / (vs + tau2)
        g_re = float((w_re * gs).sum() / w_re.sum())
        se_re = math.sqrt(1.0 / w_re.sum())
        # two-sided z test
        z = g_re / se_re if se_re > 0 else 0.0
        p = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))
        return g_re, se_re, p, k

    all_results = []
    for genus in unique_genera:
        eff = per_study_effects[genus]
        pooled = pool_re(eff)
        if pooled is None:
            continue
        g_re, se_re, p, k = pooled
        all_results.append({
            "genus": genus,
            "hedges_g": round(g_re, 4),
            "se": round(se_re, 4),
            "p_value": round(float(p), 10),
            "k_studies": k,
        })

    if not all_results:
        return {
            "health_genera": [], "disease_genera": [], "nc_stats": {},
            "n_studies": studies_used,
        }

    adjusted = bh_correction([row["p_value"] for row in all_results])
    for row, q in zip(all_results, adjusted):
        row["adjusted_p"] = round(float(q), 10)

    # ── nc_stats and per-genus mean abundances on the global NC pool ──
    nc_pool_keys_unique = list(dict.fromkeys(nc_pool_keys))
    nc_stats: dict[str, dict] = {}
    mean_nc_map: dict[str, float] = {}
    mean_dis_map: dict[str, float] = {}
    if nc_pool_keys_unique:
        raw_nc_all = abund.loc[nc_pool_keys_unique].values.astype(float)
        tot = raw_nc_all.sum(axis=1, keepdims=True); tot[tot == 0] = 1
        mat_nc_all = raw_nc_all / tot * 100
        # global disease pool for mean_disease reference
        dis_keys_all = meta_local.loc[meta_local["_is_dis"], "sample_key"].tolist()
        raw_dis_all = abund.loc[dis_keys_all].values.astype(float)
        tot_d = raw_dis_all.sum(axis=1, keepdims=True); tot_d[tot_d == 0] = 1
        mat_dis_all = raw_dis_all / tot_d * 100

        for genus in unique_genera:
            cidx = genus_to_cols[genus]
            if not cidx:
                continue
            v_nc = mat_nc_all[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_nc_all[:, cidx[0]]
            v_dis = mat_dis_all[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_dis_all[:, cidx[0]]
            mean_nc_map[genus] = float(v_nc.mean())
            mean_dis_map[genus] = float(v_dis.mean())
            nc_stats[genus] = {
                "mean": round(float(v_nc.mean()), 6),
                "median": round(float(np.median(v_nc)), 6),
                "std": round(float(v_nc.std()), 6),
                "p10": round(float(np.percentile(v_nc, 10)), 6),
                "p25": round(float(np.percentile(v_nc, 25)), 6),
                "p75": round(float(np.percentile(v_nc, 75)), 6),
                "p90": round(float(np.percentile(v_nc, 90)), 6),
            }
        n_dis_total = len(dis_keys_all)
    else:
        n_dis_total = 0

    # Hybrid marker selection: pooled Wilcoxon for breadth, RE Hedges' g for weight
    re_lookup = {row["genus"]: row for row in all_results}

    nc_pool_keys_unique2 = list(dict.fromkeys(nc_pool_keys))
    dis_keys_pool = meta_local.loc[meta_local["_is_dis"], "sample_key"].tolist()
    if not nc_pool_keys_unique2 or not dis_keys_pool:
        return {
            "health_genera": [], "disease_genera": [], "nc_stats": {},
            "n_studies": studies_used,
        }

    raw_nc_p = abund.loc[nc_pool_keys_unique2].values.astype(float)
    raw_dis_p = abund.loc[dis_keys_pool].values.astype(float)
    tot_n = raw_nc_p.sum(axis=1, keepdims=True); tot_n[tot_n == 0] = 1
    tot_d = raw_dis_p.sum(axis=1, keepdims=True); tot_d[tot_d == 0] = 1
    mat_nc_p = raw_nc_p / tot_n * 100
    mat_dis_p = raw_dis_p / tot_d * 100

    pooled_results = []
    for genus in unique_genera:
        cidx = genus_to_cols[genus]
        if not cidx:
            continue
        v_nc = mat_nc_p[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_nc_p[:, cidx[0]]
        v_dis = mat_dis_p[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_dis_p[:, cidx[0]]
        if v_nc.std() + v_dis.std() < 1e-12:
            continue
        try:
            _, p = stats.mannwhitneyu(v_nc, v_dis, alternative="two-sided")
        except Exception:
            continue
        m_nc = float(v_nc.mean())
        m_dis = float(v_dis.mean())
        pooled_results.append({
            "genus": genus,
            "p_value": float(p),
            "mean_nc": m_nc,
            "mean_disease": m_dis,
            "log2fc": math.log2((m_nc + pseudo) / (m_dis + pseudo)),
        })

    if not pooled_results:
        return {
            "health_genera": [], "disease_genera": [], "nc_stats": {},
            "n_studies": studies_used,
        }
    q_pooled = bh_correction([r["p_value"] for r in pooled_results])
    for r, q in zip(pooled_results, q_pooled):
        r["adjusted_p"] = float(q)

    # Marker selection: Wilcoxon + log2FC + abundance floor
    detection_pct = 1e-3  # 0.001 % — matches Gupta detection threshold 1e-5 fraction
    mask_nc_bin = mat_nc_p > detection_pct
    mask_dis_bin = mat_dis_p > detection_pct
    n_nc_samples = mat_nc_p.shape[0]
    n_dis_samples = mat_dis_p.shape[0]

    prevalence_map: dict[str, tuple[float, float]] = {}
    for genus in unique_genera:
        cidx = genus_to_cols[genus]
        if not cidx:
            continue
        if len(cidx) > 1:
            pres_nc = mask_nc_bin[:, cidx].any(axis=1)
            pres_dis = mask_dis_bin[:, cidx].any(axis=1)
        else:
            pres_nc = mask_nc_bin[:, cidx[0]]
            pres_dis = mask_dis_bin[:, cidx[0]]
        prevalence_map[genus] = (
            float(pres_nc.sum()) / n_nc_samples * 100.0,
            float(pres_dis.sum()) / n_dis_samples * 100.0,
        )

    re_lookup_weights = {row["genus"]: row for row in all_results}
    health_genera = []
    disease_genera = []

    for r in pooled_results:
        if r["adjusted_p"] >= 0.05 or abs(r["log2fc"]) < 0.3:
            continue
        # Require biologically meaningful abundance + prevalence (drops rare/environmental taxa)
        if max(r["mean_nc"], r["mean_disease"]) < 3e-2:
            continue
        _ph_tmp, _pnh_tmp = prevalence_map.get(r["genus"], (0.0, 0.0))
        if max(_ph_tmp, _pnh_tmp) < 15.0:
            continue
        re_row = re_lookup_weights.get(r["genus"])
        hg = float(re_row["hedges_g"]) if re_row is not None else None
        se = float(re_row["se"]) if re_row is not None else None
        k_studies = int(re_row["k_studies"]) if re_row is not None else 0
        re_q = float(re_row["adjusted_p"]) if re_row is not None else None
        ph, pnh = prevalence_map.get(r["genus"], (0.0, 0.0))
        entry = {
            "genus": r["genus"],
            "log2fc": round(r["log2fc"], 4),
            "p_value": round(r["p_value"], 10),
            "adjusted_p": round(r["adjusted_p"], 10),
            "mean_nc": round(r["mean_nc"], 6),
            "mean_disease": round(r["mean_disease"], 6),
            "PH": round(ph, 2),
            "PNH": round(pnh, 2),
            "PH_diff": round(ph - pnh, 2),
            "hedges_g": round(hg, 4) if hg is not None else None,
            "se": round(se, 4) if se is not None else None,
            "k_studies": k_studies,
            "re_adjusted_p": round(re_q, 10) if re_q is not None else None,
            "weight": round(abs(float(r["log2fc"])), 4),
        }
        if r["mean_nc"] > r["mean_disease"]:
            health_genera.append(entry)
        else:
            disease_genera.append(entry)

    health_genera.sort(key=lambda x: x["weight"], reverse=True)
    disease_genera.sort(key=lambda x: x["weight"], reverse=True)

    # Cap marker counts to 50 per direction
    health_genera = health_genera[:50]
    disease_genera = disease_genera[:50]

    # ── Compute R_MH_prime / R_MN_prime (median marker richness in NC pool) ──
    # following Gupta 2020 eq. 1: ψ_M = (R_M / R_M_prime) × H_M
    # where R_M_prime is the median observed marker count in healthy training.
    detection_thresh = 1e-5  # Gupta 2020: relative abundance > 0.00001 (units = fraction)
    # mat_nc_p is in % (0-100), so threshold in % = 1e-5 * 100 = 1e-3
    detection_thresh_pct = detection_thresh * 100.0

    def _marker_matrix_pct(genera_list):
        """Build (n_samples × n_markers) matrix of NC %-abundance for given marker genera."""
        cols = []
        for g in genera_list:
            cidx = genus_to_cols.get(g["genus"], [])
            if not cidx:
                cols.append(np.zeros(mat_nc_p.shape[0]))
                continue
            v = mat_nc_p[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_nc_p[:, cidx[0]]
            cols.append(v)
        return np.column_stack(cols) if cols else np.zeros((mat_nc_p.shape[0], 0))

    def _marker_matrix_pct_dis(genera_list):
        """Same but on disease pool."""
        cols = []
        for g in genera_list:
            cidx = genus_to_cols.get(g["genus"], [])
            if not cidx:
                cols.append(np.zeros(mat_dis_p.shape[0]))
                continue
            v = mat_dis_p[:, cidx].sum(axis=1) if len(cidx) > 1 else mat_dis_p[:, cidx[0]]
            cols.append(v)
        return np.column_stack(cols) if cols else np.zeros((mat_dis_p.shape[0], 0))

    h_mat = _marker_matrix_pct(health_genera)
    d_mat = _marker_matrix_pct(disease_genera)
    if h_mat.shape[1] > 0:
        h_richness = (h_mat > detection_thresh_pct).sum(axis=1)
        R_MH_prime = float(np.median(h_richness)) if len(h_richness) > 0 else 1.0
    else:
        R_MH_prime = 1.0
    if d_mat.shape[1] > 0:
        d_richness = (d_mat > detection_thresh_pct).sum(axis=1)
        R_MN_prime = float(np.median(d_richness)) if len(d_richness) > 0 else 1.0
    else:
        R_MN_prime = 1.0
    R_MH_prime = max(R_MH_prime, 1.0)
    R_MN_prime = max(R_MN_prime, 1.0)

    _result = {
        "health_genera": health_genera,
        "disease_genera": disease_genera,
        "nc_stats": nc_stats,
        "n_nc_samples": len(nc_pool_keys_unique),
        "n_disease_samples": n_dis_total,
        "n_studies": studies_used,
        "R_MH_prime": R_MH_prime,
        "R_MN_prime": R_MN_prime,
        "detection_threshold": detection_thresh,
        "method": "Wilcoxon(FDR<0.05) + |log2FC|≥0.5 + mean≥0.01% + Gupta 2020 ψ-formula",
    }
    try:
        _tmp = _cache_path.with_suffix(".pkl.tmp")
        with open(_tmp, "wb") as _f:
            pickle.dump(_result, _f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(_tmp, _cache_path)
        logging.info(f"health_disease_genera: saved disk cache -> {_cache_path.name}")
    except Exception as _e:
        logging.warning(f"health_disease_genera: disk cache save failed ({_e})")
    return _result


@app.post("/api/health-index",
          summary="Gut Microbiome Health Index (GMHI)",
          description="""
Gut Microbiome Health Index (GMHI) — Gupta et al. 2020 Nat Commun replication.
Gut microbiome health index computed using the Gupta 2020 psi formula.

Formula (Gupta 2020, Eq. 1):
    GMHI = log10((ψ_MH + ε) / (ψ_MN + ε))
    ψ_M  = (R_M / R_M_prime) × H_M
where R_M  = observed marker richness (rel. abund. > 1e-5),
      R_M_prime = median NC marker richness (training cohort),
      H_M  = Shannon entropy over marker abundances,
      ε    = 1e-5.

Markers (MH/MN genera) are selected via random-effects meta-analysis
(DerSimonian-Laird) of per-study Hedges' g across 156 BioProjects, retaining
genera with FDR-adjusted Wilcoxon p<0.05, |log2FC|≥0.5, and |g|≥0.2.
""",
          tags=["Similarity"])
@no_cache_tracking
@limiter.limit("20/minute")
async def health_index(request: Request, req: HealthIndexRequest):
    """Compute microbiome health index."""
    if not req.abundances:
        raise HTTPException(400, "abundances dict must not be empty")

    ref = _compute_health_disease_genera()
    if not ref["health_genera"] and not ref["disease_genera"]:
        raise HTTPException(500, "Health index reference data not available")

    health_set = {g["genus"].lower(): g for g in ref["health_genera"]}
    disease_set = {g["genus"].lower(): g for g in ref["disease_genera"]}
    nc_stats = ref["nc_stats"]
    R_MH_prime = float(ref.get("R_MH_prime", 1.0))
    R_MN_prime = float(ref.get("R_MN_prime", 1.0))
    detection_pct = float(ref.get("detection_threshold", 1e-5)) * 100.0  # → % units

    # Renormalise user abundances to % (in case they sum ≠ 100)
    user_total = sum(float(v) for v in req.abundances.values()) or 1.0
    user_norm = {k.strip(): float(v) / user_total * 100.0 for k, v in req.abundances.items()}

    h_marker_vals = []  # %-abundance for matched MH markers
    d_marker_vals = []  # %-abundance for matched MN markers
    health_matched = []
    disease_matched = []
    per_genus = []

    for genus, value in req.abundances.items():
        g_lower = genus.strip().lower()
        g_title = genus.strip().title()
        numeric_value = float(user_norm.get(genus.strip(), 0.0))

        if g_lower in health_set:
            weight = float(health_set[g_lower].get("weight", 1.0))
            h_marker_vals.append(numeric_value)
            health_matched.append({
                "genus": g_title,
                "abundance": round(numeric_value, 6),
                "weight": round(weight, 4),
                "contribution": 0.0,
            })
        if g_lower in disease_set:
            weight = float(disease_set[g_lower].get("weight", 1.0))
            d_marker_vals.append(numeric_value)
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

    # Per-marker contribution = abundance / total marker abundance in that set
    total_h = sum(float(item["abundance"]) for item in health_matched) or 1e-9
    total_d = sum(float(item["abundance"]) for item in disease_matched) or 1e-9
    for item in health_matched:
        item["contribution"] = round(float(item["abundance"]) / total_h, 4)
    for item in disease_matched:
        item["contribution"] = round(float(item["abundance"]) / total_d, 4)

    # ── Gupta 2020 ψ formula ──
    psi_MH = _psi_score(np.asarray(h_marker_vals, dtype=float), R_MH_prime, detection_pct)
    psi_MN = _psi_score(np.asarray(d_marker_vals, dtype=float), R_MN_prime, detection_pct)
    pseudo = 1e-5  # Gupta's ε
    raw_score_weighted = math.log10((psi_MH + pseudo) / (psi_MN + pseudo))
    raw_score = raw_score_weighted

    # ── Universal softmax health score (frozen 10-class classifier) ──
    # Single-sample scoring via frozen universal softmax P(NC)*100; percentile
    # looked up from Supp Table 6 NC distribution.
    try:
        p_nc, _n_matched_univ, _n_total_univ = _score_universal_pnc(
            req.abundances,
            amplicon=req.amplicon or "",
            iso=req.iso or "",
            age_group=req.age_group or "Unknown",
            sex=req.sex or "unknown",
            length=req.length,
        )
        normalized = float(round(p_nc * 100.0, 1))
    except Exception as e:
        logging.warning(f"[health-index] universal softmax fallback: {e}")
        # Fallback: Gupta 2020 normalisation
        normalized = max(0.0, min(100.0, (raw_score_weighted + 2.0) / 4.0 * 100.0))

    pop = _compute_population_gmhi()
    nc_pop = pop.get("nc_stats", {})
    # Tier thresholds: high≥70, moderate 40-70, low<40
    if normalized >= 70.0:
        category = "good"
    elif normalized >= 40.0:
        category = "moderate"
    else:
        category = "attention"

    nc_scores_sorted = np.array(pop.get("nc_scores_sorted", []), dtype=float)
    if len(nc_scores_sorted) > 0:
        population_percentile = float(
            np.searchsorted(nc_scores_sorted, normalized, side="right") / len(nc_scores_sorted) * 100
        )
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
        "psi_MH": round(psi_MH, 6),
        "psi_MN": round(psi_MN, 6),
        "R_MH_prime": round(R_MH_prime, 2),
        "R_MN_prime": round(R_MN_prime, 2),
        "health_genera_detail": health_matched,
        "disease_genera_detail": disease_matched,
        "per_genus_deviation": per_genus[:50],
        "reference": {
            "n_nc_samples": pop.get("nc_stats", {}).get("n", ref.get("n_nc_samples", 0)),
            "n_disease_samples": pop.get("disease_stats", {}).get("n", ref.get("n_disease_samples", 0)),
            "health_genera_total": len(ref["health_genera"]),
            "disease_genera_total": len(ref["disease_genera"]),
        },
    }


@lru_cache(maxsize=1)
def _compute_population_gmhi() -> dict:
    """Population distribution over the 168k compendium using the frozen
    universal softmax health score (Supp Table 6). NC / Disease strata use
    the inform-all label from the table."""
    pop = _load_universal_population_scores()
    if not pop:
        return {"histogram": [], "nc_stats": {}, "disease_stats": {}, "nc_scores_sorted": []}

    nc_arr = pop["nc_scores_sorted"]
    dis_arr = pop["disease_scores_sorted"]

    bins = list(range(0, 105, 5))
    nc_hist, _ = np.histogram(nc_arr, bins=bins) if len(nc_arr) else (np.zeros(len(bins) - 1), None)
    dis_hist, _ = np.histogram(dis_arr, bins=bins) if len(dis_arr) else (np.zeros(len(bins) - 1), None)
    histogram = [
        {
            "bin_start": bins[i],
            "bin_end": bins[i + 1],
            "nc_count": int(nc_hist[i]),
            "disease_count": int(dis_hist[i]),
        }
        for i in range(len(bins) - 1)
    ]

    def _stats(arr: np.ndarray, n_total: int) -> dict:
        if len(arr) == 0:
            return {"n": n_total, "mean": 0.0, "median": 0.0, "std": 0.0,
                    "p10": 0.0, "p25": 0.0, "p75": 0.0, "p90": 0.0}
        return {
            "n": n_total,
            "mean": round(float(np.mean(arr)), 1),
            "median": round(float(np.median(arr)), 1),
            "std": round(float(np.std(arr)), 1),
            "p10": round(float(np.percentile(arr, 10)), 1),
            "p25": round(float(np.percentile(arr, 25)), 1),
            "p75": round(float(np.percentile(arr, 75)), 1),
            "p90": round(float(np.percentile(arr, 90)), 1),
        }

    return {
        "histogram": histogram,
        "nc_stats": _stats(nc_arr, pop["n_nc"]),
        "disease_stats": _stats(dis_arr, pop["n_dis"]),
        "calibration": {"p5": 0.0, "p95": 100.0},
        "nc_scores_sorted": nc_arr.tolist(),
    }


@app.get("/api/health-index/reference",
         summary="Health index reference data",
         description="Returns precomputed health/disease genera lists, NC reference statistics, and population GMHI distribution.",
         tags=["Similarity"])
@no_cache_tracking
@limiter.limit("60/minute")
async def health_index_reference(request: Request):
    """Return health index reference data (health/disease genera lists + population distribution)."""
    ref = _compute_health_disease_genera()
    pop = _compute_population_gmhi()
    return {
        "health_genera": ref["health_genera"],
        "disease_genera": ref["disease_genera"],
        "n_nc_samples": pop.get("nc_stats", {}).get("n", ref.get("n_nc_samples", 0)),
        "n_disease_samples": pop.get("disease_stats", {}).get("n", ref.get("n_disease_samples", 0)),
        "population": pop,
    }


# ── Usage tracking ────────────────────────────────────────────

class TrackEvent(BaseModel):
    """Usage tracking event."""
    event: str       # page_view / analysis_run / export / search
    page: str = ""
    detail: str = ""

_ANALYTICS_FILE = Path(__file__).parent / "analytics.jsonl"

_OWNER_IPS = {ip.strip() for ip in os.getenv("ANALYTICS_EXCLUDE_IPS", "23.172.200.73").split(",") if ip.strip()}
_BOT_UA_KEYWORDS = ("bot", "spider", "crawl", "headless", "monitor", "scraper", "preview", "fetch", "facebookexternalhit", "lighthouse")
# Cloud / probe IP prefixes — narrow enough to avoid hitting real GCP/AWS users.
# Verified sources:
#   66.249.   Googlebot
#   34.72.    Google Cloud us-central1 (Vercel infra)
#   104.196-198. Google Cloud (Vercel infra)
#   35.224/232/235/236.  Google Cloud (Vercel infra)
#   13.57.    AWS us-west-1 (Vercel Speed Insights)
#   54.215.   AWS us-west-1 (Vercel Speed Insights)
#   205.169.  Palo Alto Networks (Cortex Xpanse asset scanner)
_CLOUD_IP_PREFIXES = (
    "66.249.", "34.72.",
    "104.196.", "104.197.", "104.198.",
    "35.224.", "35.232.", "35.235.", "35.236.",
    "13.57.", "54.215.",
    "205.169.",
)


def _classify_visitor(ip: str, ua: str, referer: str = "") -> tuple[str, str, str]:
    """Returns (label, browser_short, device_short)."""
    ua_l = (ua or "").lower()
    ref_l = (referer or "").lower()
    if ip in _OWNER_IPS:
        label = "owner"
    elif "headlesschrome" in ua_l and ".vercel.app" in ref_l:
        # Vercel deploy preview / Speed Insights probe — counts as us
        label = "owner"
    elif any(k in ua_l for k in _BOT_UA_KEYWORDS):
        if "googlebot" in ua_l:
            label = "googlebot"
        elif "bingbot" in ua_l:
            label = "bingbot"
        else:
            label = "bot"
    elif ip.startswith(_CLOUD_IP_PREFIXES):
        label = "cloud_probe"
    else:
        label = "human"

    if "edg/" in ua_l:
        m = re.search(r"edg/([\d.]+)", ua_l)
        browser = f"Edge {m.group(1).split('.')[0]}" if m else "Edge"
    elif "crios/" in ua_l:
        m = re.search(r"crios/([\d.]+)", ua_l)
        browser = f"Chrome iOS {m.group(1).split('.')[0]}" if m else "Chrome iOS"
    elif "firefox/" in ua_l:
        m = re.search(r"firefox/([\d.]+)", ua_l)
        browser = f"Firefox {m.group(1).split('.')[0]}" if m else "Firefox"
    elif "chrome/" in ua_l:
        m = re.search(r"chrome/([\d.]+)", ua_l)
        browser = f"Chrome {m.group(1).split('.')[0]}" if m else "Chrome"
    elif "safari/" in ua_l and "version/" in ua_l:
        m = re.search(r"version/([\d.]+)", ua_l)
        browser = f"Safari {m.group(1).split('.')[0]}" if m else "Safari"
    elif "googlebot" in ua_l:
        browser = "Googlebot"
    elif "bingbot" in ua_l:
        browser = "Bingbot"
    else:
        browser = "Unknown"

    if "iphone" in ua_l:
        device = "iPhone"
    elif "ipad" in ua_l:
        device = "iPad"
    elif "android" in ua_l:
        device = "Android"
    elif "windows" in ua_l:
        device = "Windows"
    elif "mac os x" in ua_l or "macintosh" in ua_l:
        device = "macOS"
    elif "linux" in ua_l:
        device = "Linux"
    else:
        device = "Unknown"

    return label, browser, device


@app.post("/api/track", tags=["Admin"],
          summary="Track usage event",
          description="Log usage events for publication metrics.")
@no_cache_tracking
@limiter.limit("120/minute")
async def track_event(request: Request, evt: TrackEvent):
    """Log usage event (for publication metrics)."""
    # Extract visitor info from request headers
    forwarded = request.headers.get("x-forwarded-for", "")
    ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "")
    entry = {
        "timestamp": datetime.now().isoformat(),
        "event": evt.event,
        "page": evt.page,
        "detail": evt.detail,
        "ip": ip,
        "ua": request.headers.get("user-agent", ""),
        "referer": request.headers.get("referer", ""),
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
@no_cache_tracking
@limiter.limit("30/minute")
async def analytics_summary(request: Request, token: str = ""):
    """Admin view of aggregated usage statistics."""
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
    from collections import Counter, defaultdict
    event_counts = Counter(e.get("event", "") for e in events)
    page_counts = Counter(e.get("page", "") for e in events)

    # Daily breakdown — daily.unique_visitors EXCLUDES owner & bots
    daily: dict = {}
    for e in events:
        day = e.get("timestamp", "")[:10]
        if not day:
            continue
        if day not in daily:
            daily[day] = {
                "views": 0, "ips_all": set(), "ips_human": set(),
                "pages": Counter(),
            }
        daily[day]["views"] += 1
        ip = e.get("ip", "")
        if ip:
            daily[day]["ips_all"].add(ip)
            label, _, _ = _classify_visitor(ip, e.get("ua", ""), e.get("referer", ""))
            if label == "human":
                daily[day]["ips_human"].add(ip)
        daily[day]["pages"][e.get("page", "")] += 1

    # Per-IP visitor profiles (overall)
    visitor_data: dict = defaultdict(lambda: {
        "events": 0, "pages": Counter(), "ua": "", "referer": "",
        "first_seen": "", "last_seen": "",
    })
    for e in events:
        ip = e.get("ip", "")
        if not ip:
            continue
        v = visitor_data[ip]
        v["events"] += 1
        v["pages"][e.get("page", "")] += 1
        ts = e.get("timestamp", "")
        if not v["first_seen"] or ts < v["first_seen"]:
            v["first_seen"] = ts
        if ts > v["last_seen"]:
            v["last_seen"] = ts
            v["ua"] = e.get("ua", "")
            v["referer"] = e.get("referer", "")

    visitors = []
    for ip, v in visitor_data.items():
        label, browser, device = _classify_visitor(ip, v["ua"], v["referer"])
        visitors.append({
            "ip": ip,
            "label": label,
            "browser": browser,
            "device": device,
            "events": v["events"],
            "top_pages": dict(v["pages"].most_common(5)),
            "first_seen": v["first_seen"],
            "last_seen": v["last_seen"],
            "ua": v["ua"],
            "referer": v["referer"],
        })
    visitors.sort(key=lambda x: -x["events"])

    # Per-day visitor list (detail)
    daily_visitors: dict = defaultdict(lambda: defaultdict(lambda: {
        "events": 0, "pages": Counter(), "ua": "", "referer": "",
        "first_seen": "", "last_seen": "", "timeline": [],
    }))
    for e in events:
        day = e.get("timestamp", "")[:10]
        ip = e.get("ip", "")
        ts = e.get("timestamp", "")
        if not day or not ip:
            continue
        dv = daily_visitors[day][ip]
        dv["events"] += 1
        dv["pages"][e.get("page", "")] += 1
        if e.get("ua"):
            dv["ua"] = e.get("ua", "")
        if e.get("referer"):
            dv["referer"] = e.get("referer", "")
        if ts:
            if not dv["first_seen"] or ts < dv["first_seen"]:
                dv["first_seen"] = ts
            if ts > dv["last_seen"]:
                dv["last_seen"] = ts
            dv["timeline"].append({
                "ts": ts,
                "page": e.get("page", ""),
                "event": e.get("event", ""),
                "referer": e.get("referer", ""),
            })

    def _duration_sec(first: str, last: str) -> int:
        try:
            t0 = datetime.fromisoformat(first)
            t1 = datetime.fromisoformat(last)
            return max(0, int((t1 - t0).total_seconds()))
        except Exception:
            return 0

    daily_visitors_out: dict = {}
    for day, ips in daily_visitors.items():
        items = []
        for ip, v in ips.items():
            label, browser, device = _classify_visitor(ip, v["ua"], v["referer"])
            v["timeline"].sort(key=lambda x: x["ts"])
            items.append({
                "ip": ip, "label": label, "browser": browser, "device": device,
                "events": v["events"],
                "top_pages": dict(v["pages"].most_common(5)),
                "ua": v["ua"], "referer": v["referer"],
                "first_seen": v["first_seen"],
                "last_seen": v["last_seen"],
                "duration_sec": _duration_sec(v["first_seen"], v["last_seen"]),
                "timeline": v["timeline"],
            })
        items.sort(key=lambda x: -x["events"])
        daily_visitors_out[day] = items

    daily_summary = {}
    for day in sorted(daily):
        d = daily[day]
        daily_summary[day] = {
            "views": d["views"],
            "unique_visitors": len(d["ips_human"]),
            "unique_visitors_raw": len(d["ips_all"]),
            "top_pages": dict(d["pages"].most_common(10)),
            "visitors": daily_visitors_out.get(day, []),
        }

    # Unique visitors (all time) — human only for headline
    all_ips = set(e.get("ip", "") for e in events if e.get("ip"))
    human_ips = {v["ip"] for v in visitors if v["label"] == "human"}

    return {
        "total_events": len(events),
        "unique_visitors": len(human_ips),
        "unique_visitors_raw": len(all_ips),
        "owner_ips": sorted(_OWNER_IPS),
        "by_event_type": dict(event_counts.most_common(20)),
        "by_page": dict(page_counts.most_common(20)),
        "daily": daily_summary,
        "visitors": visitors,
        "recent": events[-20:] if events else [],
    }


# ── API access traffic summary ────────────────────────────────────

@app.get("/api/admin/api-traffic", tags=["Admin"],
         summary="Direct-API traffic summary",
         description="Aggregates /api/* access log to spot direct scraping (cross-references analytics.jsonl).")
@no_cache_tracking
@limiter.limit("30/minute")
async def api_traffic_summary(request: Request, token: str = "", days: int = 14):
    """Surface API consumers; mark IPs that never browsed pages as 'direct API only'."""
    if token != ADMIN_TOKEN:
        raise HTTPException(403, "Invalid admin token")

    if not _API_ACCESS_FILE.exists():
        return {"total_requests": 0, "daily": {}, "top_ips": [], "top_paths": []}

    # Page-view IP set — IPs that we know browsed the site (from analytics.jsonl)
    browser_ips: set[str] = set()
    if _ANALYTICS_FILE.exists():
        try:
            with open(_ANALYTICS_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        e = json.loads(line)
                    except Exception:
                        continue
                    ip = e.get("ip", "")
                    if ip:
                        browser_ips.add(ip)
                    if e.get("ip", "") and e.get("event") == "page_view":
                        browser_ips.add(e["ip"])
        except Exception:
            pass

    from collections import Counter, defaultdict
    rows = []
    try:
        with open(_API_ACCESS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        pass

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    rows = [r for r in rows if r.get("ts", "") >= cutoff]

    daily: dict = defaultdict(lambda: {
        "requests": 0,
        "ips": Counter(),
        "paths": Counter(),
        "ip_meta": {},   # ip -> {ua, referer, paths}
        "errors_5xx": 0,
        "errors_4xx": 0,
    })
    for r in rows:
        day = r.get("ts", "")[:10]
        if not day:
            continue
        d = daily[day]
        d["requests"] += 1
        ip = r.get("ip", "")
        path = r.get("path", "")
        status = r.get("status", 0)
        if ip:
            d["ips"][ip] += 1
            meta = d["ip_meta"].setdefault(ip, {"ua": "", "referer": "", "paths": Counter()})
            meta["ua"] = r.get("ua", meta["ua"])
            meta["referer"] = r.get("referer", meta["referer"])
            meta["paths"][path] += 1
        d["paths"][path] += 1
        if 500 <= status < 600:
            d["errors_5xx"] += 1
        elif 400 <= status < 500:
            d["errors_4xx"] += 1

    daily_out: dict = {}
    for day in sorted(daily):
        d = daily[day]
        ips_detail = []
        for ip, cnt in d["ips"].most_common(20):
            meta = d["ip_meta"].get(ip, {})
            label, browser, device = _classify_visitor(ip, meta.get("ua", ""), meta.get("referer", ""))
            # Promote: API hits with no page_view → direct_api (suspicious)
            if ip not in browser_ips and label == "human":
                source = "direct_api"
            else:
                source = label
            ips_detail.append({
                "ip": ip,
                "requests": cnt,
                "source": source,
                "label": label,
                "browser": browser, "device": device,
                "top_paths": dict(meta.get("paths", Counter()).most_common(5)),
                "ua": meta.get("ua", "")[:120],
            })
        daily_out[day] = {
            "requests": d["requests"],
            "unique_ips": len(d["ips"]),
            "errors_4xx": d["errors_4xx"],
            "errors_5xx": d["errors_5xx"],
            "top_paths": dict(d["paths"].most_common(15)),
            "top_ips": ips_detail,
        }

    # Overall top IPs / paths across the window
    all_ips = Counter()
    all_paths = Counter()
    for d in daily.values():
        all_ips.update(d["ips"])
        all_paths.update(d["paths"])

    top_ips_overall = []
    for ip, cnt in all_ips.most_common(20):
        # find most recent meta
        ua = referer = ""
        for r in reversed(rows):
            if r.get("ip") == ip:
                ua = r.get("ua", "")
                referer = r.get("referer", "")
                break
        label, browser, device = _classify_visitor(ip, ua, referer)
        source = "direct_api" if (ip not in browser_ips and label == "human") else label
        top_ips_overall.append({
            "ip": ip, "requests": cnt, "source": source, "label": label,
            "browser": browser, "device": device, "ua": ua[:120],
        })

    return {
        "window_days": days,
        "total_requests": sum(d["requests"] for d in daily.values()),
        "unique_ips": len(all_ips),
        "browser_ips_seen": len(browser_ips),
        "daily": daily_out,
        "top_ips": top_ips_overall,
        "top_paths": dict(all_paths.most_common(20)),
    }


# ── Email daily report ────────────────────────────────────────────

class EmailReportRequest(BaseModel):
    token: str
    to: str = "zhaijinxia07@gmail.com"

@app.post("/api/admin/email-report", tags=["Admin"],
          summary="Generate and email daily analytics report")
@no_cache_tracking
@limiter.limit("10/hour")
async def email_daily_report(request: Request, req: EmailReportRequest):
    """Generate yesterday's analytics report and send via email."""
    if req.token != ADMIN_TOKEN:
        raise HTTPException(403, "Invalid admin token")

    import smtplib
    from email.mime.text import MIMEText
    from collections import Counter

    # Read analytics
    events = []
    if _ANALYTICS_FILE.exists():
        with open(_ANALYTICS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except Exception:
                        pass

    if not events:
        raise HTTPException(404, "No analytics data")

    # Find yesterday (Beijing time UTC+8)
    from datetime import timedelta, timezone
    bj_tz = timezone(timedelta(hours=8))
    now_bj = datetime.now(bj_tz)
    yesterday = (now_bj - timedelta(days=1)).strftime("%Y-%m-%d")
    day_before = (now_bj - timedelta(days=2)).strftime("%Y-%m-%d")

    # Build daily stats
    daily_data: dict = {}
    for e in events:
        day = e.get("timestamp", "")[:10]
        if not day:
            continue
        if day not in daily_data:
            daily_data[day] = {"views": 0, "ips": set(), "pages": Counter(),
                               "uas": [], "referers": []}
        daily_data[day]["views"] += 1
        ip = e.get("ip", "")
        if ip:
            daily_data[day]["ips"].add(ip)
        daily_data[day]["pages"][e.get("page", "")] += 1
        daily_data[day]["uas"].append(e.get("ua", ""))
        daily_data[day]["referers"].append(e.get("referer", ""))

    # Pick report date
    report_date = yesterday if yesterday in daily_data else (
        sorted(daily_data.keys())[-1] if daily_data else yesterday)
    d = daily_data.get(report_date, {"views": 0, "ips": set(), "pages": Counter(),
                                      "uas": [], "referers": []})

    # Trend
    prev = daily_data.get(day_before, {"views": 0})
    trend_pct = ((d["views"] - prev["views"]) / prev["views"] * 100
                 if prev["views"] > 0 else 0)
    trend_str = f"↑ {trend_pct:.0f}%" if trend_pct > 0 else (
        f"↓ {abs(trend_pct):.0f}%" if trend_pct < 0 else "→ 持平")

    # Device breakdown
    mobile_cnt = sum(1 for ua in d["uas"] if any(
        k in ua.lower() for k in ("mobile", "android", "iphone", "ipad")))
    desktop_cnt = len(d["uas"]) - mobile_cnt
    total_ua = max(len(d["uas"]), 1)

    # Top pages
    top_pages_lines = []
    for i, (page, cnt) in enumerate(d["pages"].most_common(8), 1):
        label = {"": "未知", "/": "首页"}.get(page, page)
        top_pages_lines.append(f"  {i}. {label} — {cnt}次")

    # Referer sources
    ref_counter = Counter()
    for r in d["referers"]:
        if not r:
            ref_counter["直接访问"] += 1
        elif "google" in r.lower():
            ref_counter["Google"] += 1
        elif "gutbiomedb" in r.lower():
            ref_counter["站内跳转"] += 1
        else:
            ref_counter[r[:40]] += 1
    ref_lines = [f"  - {k}：{v}次" for k, v in ref_counter.most_common(5)]

    all_ips = set()
    for dd in daily_data.values():
        all_ips |= dd["ips"]
    total_views = sum(dd["views"] for dd in daily_data.values())

    body = f"""📊 GutBiomeDB 访问日报 — {report_date}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 总浏览量：{d['views']} 次
👥 独立访客：{len(d['ips'])} 个IP

🔥 热门页面：
{chr(10).join(top_pages_lines) if top_pages_lines else '  (无数据)'}

🌐 访客来源：
{chr(10).join(ref_lines) if ref_lines else '  (无来源数据)'}

📱 设备分布：
  - 桌面端：{desktop_cnt}/{total_ua} ({desktop_cnt/total_ua*100:.0f}%)
  - 移动端：{mobile_cnt}/{total_ua} ({mobile_cnt/total_ua*100:.0f}%)

📊 趋势：较前日 {trend_str}

累计总浏览量：{total_views} 次 | 累计独立访客：{len(all_ips)} 个
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
平台：https://gutbiomedb.online
Vercel Analytics: https://vercel.com → compendium_website → Analytics

此邮件由 GutBiomeDB 自动报告系统发送。
"""
    subject = f"📊 GutBiomeDB 访问日报 — {report_date}"

    # Send via SMTP
    smtp_user = os.environ.get("GMAIL_USER", "zhaijinxia07@gmail.com")
    smtp_pass = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not smtp_pass:
        raise HTTPException(500, "GMAIL_APP_PASSWORD not configured in .env.local")

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = req.to

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
    except Exception as exc:
        raise HTTPException(500, f"Email send failed: {exc}")

    return {"status": "sent", "to": req.to, "subject": subject, "date": report_date}


# ── Universal GBHI (/api/health_score) ──────────────────────────────────────
# Frozen multinomial softmax classifier from gbhi_universal.pkl. Single
# LogisticRegression (lbfgs, C=1.0, class_weight=balanced) over 10 classes,
# fit on 98,847 labeled samples. Feature vector z(s) ∈ R^352 =
#   [ R_union (289 marker residuals) | 61 cov dummies + log10(length) | psi_universal ]
# Covariate residualization β̂ fitted once on full 168k compendium (Wirbel 2019
# batch-norm style; label-agnostic). Health(s) = P(NC|s) * 100.

GBHI_UNIVERSAL_PKL = r"E:\tasks\screenshots\fig1g\gbhi_models\gbhi_universal.pkl"
GBHI_CACHE_NPZ     = r"E:\tasks\screenshots\fig1g\v6_cache.npz"

_GBHI_UNIVERSAL_BLOB: dict | None = None
_GBHI_GENERA: list[str] | None = None
_GBHI_GENUS_INDEX: dict[str, int] | None = None


def _load_gbhi_universal() -> tuple[dict, list[str], dict[str, int]]:
    """Lazy-load frozen softmax + genus ordering. Cached process-wide."""
    global _GBHI_UNIVERSAL_BLOB, _GBHI_GENERA, _GBHI_GENUS_INDEX
    if _GBHI_UNIVERSAL_BLOB is None:
        import pickle
        with open(GBHI_UNIVERSAL_PKL, "rb") as f:
            _GBHI_UNIVERSAL_BLOB = pickle.load(f)
        c = np.load(GBHI_CACHE_NPZ, allow_pickle=True)
        _key = "genus_columns" if "genus_columns" in c else "genera"
        _GBHI_GENERA = [str(x) for x in c[_key]]
        _GBHI_GENUS_INDEX = {g.lower(): i for i, g in enumerate(_GBHI_GENERA)}
        # Also add short genus name (last segment after last dot) for user input matching
        for i, g in enumerate(_GBHI_GENERA):
            short = g.rsplit(".", 1)[-1].lower()
            if short and short not in _GBHI_GENUS_INDEX:
                _GBHI_GENUS_INDEX[short] = i
        blob = _GBHI_UNIVERSAL_BLOB
        if blob.get("n_genus") != len(_GBHI_GENERA):
            logging.warning(
                "GBHI universal: pkl.n_genus=%s but cache has %d genera",
                blob.get("n_genus"), len(_GBHI_GENERA),
            )
    return _GBHI_UNIVERSAL_BLOB, _GBHI_GENERA, _GBHI_GENUS_INDEX


def _gbhi_gupta_psi(G_pct: np.ndarray, mh: list[int], mn: list[int]) -> float:
    """Shared universal psi (log10 ratio) — matches fit_gbhi_universal.gupta_psi."""
    EPS = 1e-5
    gp = np.clip(G_pct / 100.0, 1e-12, 1.0)
    H = float(-(gp * np.log(gp)).sum())

    def rp(idx: list[int]) -> float:
        if not idx:
            return 1.0
        pres = int((G_pct[idx] > 0).sum())
        return float(pres) if pres > 0 else 1.0

    Rh, Rn = rp(mh), rp(mn)
    psi_h = (float((G_pct[mh] > 0).sum()) / Rh) * H if mh else EPS
    psi_n = (float((G_pct[mn] > 0).sum()) / Rn) * H if mn else EPS
    val = math.log10((psi_h + EPS) / (psi_n + EPS))
    if not math.isfinite(val):
        return 0.0
    return val


SUPP_TABLE6_XLSX = os.path.join(os.path.dirname(__file__), "..", "docs", "NatureMicrobiology_LaTeX", "table7_gbhi_scores.xlsx")


@lru_cache(maxsize=1)
def _load_universal_population_scores() -> dict:
    """Load Supp Table 6 (per-sample universal-softmax health scores for all
    168k compendium samples) and return stratified distributions for NC /
    Disease / Unknown based on inform-all."""
    try:
        df = pd.read_excel(SUPP_TABLE6_XLSX, engine="openpyxl")
    except Exception as e:
        logging.warning(f"[universal pop] Supp Table 6 load failed: {e}")
        return {}
    ia = df["inform-all"].fillna("").astype(str).str.strip()
    nc_mask = ia.str.upper().eq("NC").values
    unk_mask = (ia.eq("") | ia.str.lower().isin(["nan", "unknown", "none", "na"])).values
    dis_mask = (~nc_mask) & (~unk_mask)
    h = df["health_score"].astype(float).values
    return {
        "nc_scores_sorted": np.sort(h[nc_mask]),
        "disease_scores_sorted": np.sort(h[dis_mask]),
        "unknown_scores_sorted": np.sort(h[unk_mask]),
        "all_scores": h,
        "n_nc": int(nc_mask.sum()),
        "n_dis": int(dis_mask.sum()),
        "n_unk": int(unk_mask.sum()),
    }


def _score_universal_pnc(
    abundances: dict[str, float],
    *,
    amplicon: str = "",
    iso: str = "",
    age_group: str = "Unknown",
    sex: str = "unknown",
    length: float | None = None,
) -> tuple[float, int, int]:
    """Frozen universal softmax pipeline on a user abundance dict.
    Returns (p_nc, n_matched, n_total_genera). Mirrors /api/health_score."""
    blob, genera, genus_index = _load_gbhi_universal()
    n_genus = len(genera)

    total = sum(float(v) for v in abundances.values()) or 1.0
    G_pct = np.zeros(n_genus, dtype=np.float32)
    matched = 0
    for name, v in abundances.items():
        j = genus_index.get(str(name).strip().lower())
        if j is not None:
            G_pct[j] = float(v) / total * 100.0
            matched += 1

    freeze = blob["freeze"]
    dummy_pairs = freeze["dummy_pairs"]
    amp_keep = set(freeze["amp_keep"]); iso_keep = set(freeze["iso_keep"])
    age_keep = set(freeze["age_keep"]); sex_keep = set(freeze["sex_keep"])

    amp = (amplicon or "na").strip().lower() or "na"
    if amp not in amp_keep:
        amp = "OTHER"
    iso_v = (iso or "NA").strip().upper() or "NA"
    if iso_v not in iso_keep:
        iso_v = "OTHER"
    age = (age_group or "Unknown").strip() or "Unknown"
    if age not in age_keep:
        age = "Unknown"
    sx = (sex or "unknown").strip().lower() or "unknown"
    if sx not in sex_keep:
        sx = "unknown"
    cat_map = {"_amp": amp, "_iso": iso_v, "_age": age, "_sex": sx}

    dummies = np.zeros(len(dummy_pairs), dtype=np.float32)
    for i, (pre, val) in enumerate(dummy_pairs):
        if cat_map.get(pre) == val:
            dummies[i] = 1.0
    length_v = float(length) if length and length > 0 else 300.0
    log_len = np.float32(math.log10(length_v + 1.0))
    cov_feat = np.concatenate([dummies, np.array([log_len], dtype=np.float32)])
    x_row = np.concatenate([np.array([1.0], dtype=np.float32), cov_feat])

    beta = np.asarray(blob["beta"], dtype=np.float32)
    AB_EPS = 1e-3
    log_g = np.log10(G_pct + AB_EPS).astype(np.float32)
    pred = x_row @ beta
    R_all = log_g - pred
    union_idx = np.asarray(blob["union_idx"], dtype=int)
    R_union = R_all[union_idx]

    psi = np.float32(_gbhi_gupta_psi(G_pct, list(blob["mh_union"]), list(blob["mn_union"])))
    z = np.concatenate([R_union, cov_feat, np.array([psi], dtype=np.float32)])

    sc_mean = np.asarray(blob["sc_mean"], dtype=np.float32)
    sc_scale = np.asarray(blob["sc_scale"], dtype=np.float32)
    z_std = (z - sc_mean) / np.where(sc_scale == 0, 1.0, sc_scale)

    W = np.asarray(blob["W"], dtype=np.float32)
    b = np.asarray(blob["b"], dtype=np.float32)
    logits = W @ z_std + b
    logits = logits - float(logits.max())
    exp_l = np.exp(logits)
    probs = exp_l / exp_l.sum()
    nc_ci = list(blob["class_names"]).index("NC")
    return float(probs[nc_ci]), matched, n_genus


class HealthScoreRequest(BaseModel):
    """Input payload for /api/health_score.

    abundances   : dict genus_name -> relative abundance (% or fraction;
                   will be renormalized to sum to 100 %).
    amplicon     : e.g. "v3-v4", "v4", "v1-v2" (falls back to OTHER).
    iso          : ISO-2 country code (e.g. "CN", "US"; falls back to OTHER).
    age_group    : one of Adolescent/Adult/Centenarian/Child/Infant/NC/
                   Older_Adult/Oldest_Old/Unknown/Necrotizing enterocolitis.
    sex          : female / male / unknown.
    length       : sequencing length (bp); if missing, uses training median.
    """
    abundances: dict[str, float]
    amplicon: str = ""
    iso: str = ""
    age_group: str = "Unknown"
    sex: str = "unknown"
    length: float | None = None


def _gbhi_tier(p_nc: float) -> str:
    """Tier from P(NC).

    high      : P(NC) * 100 >= 70  (NC training median ≈ 0.75)
    moderate  : 40 <= P(NC)*100 < 70
    low       : P(NC) * 100 < 40
    """
    s = p_nc * 100.0
    if s >= 70.0:
        return "high"
    if s >= 40.0:
        return "moderate"
    return "low"


_CLASS_KEY_MAP = {
    "NC": "p_nc",
    "c_difficile_infection": "p_cdi",
    "CD": "p_cd",
    "UC": "p_uc",
    "rheumatoid arthritis": "p_ra",
    "HIV": "p_hiv",
    "adenoma": "p_adenoma",
    "obesity": "p_obesity",
    "IBS": "p_ibs",
    "colorectal_cancer": "p_crc",
}


@app.post("/api/health_score",
          summary="Universal GBHI health score (multinomial softmax)",
          description="""
Universal GBHI — frozen multinomial softmax layer.

Single scikit-learn LogisticRegression (solver=lbfgs, C=1.0, max_iter=2000,
class_weight='balanced') over 10 classes:
NC, c_difficile_infection, CD, UC, rheumatoid arthritis, HIV, adenoma,
obesity, IBS, colorectal_cancer. Fitted on 98,847 labeled samples
(82,106 NC + 16,741 disease).

Feature pipeline (identical to fit_gbhi_universal.py):
  1. 4680-dim genus %-abundance vector aligned to frozen compendium order.
  2. log10(G+1e-3) residualized against frozen β̂ (intercept + 61 covariate
     dummies + log10_length) fitted once on the full 168k compendium.
  3. Take residuals at 289 union marker indices  →  R_union.
  4. Concatenate [R_union | cov_dummies(61) | log10_length | psi_universal]
     = R^352 feature vector z.
  5. Standardize with frozen scaler (sc_mean / sc_scale).
  6. logits = W @ z_std + b ; probs = softmax(logits).
Health(s) = P(NC|s) * 100.
""",
          tags=["Similarity"])
@no_cache_tracking
@limiter.limit("20/minute")
async def health_score(request: Request, req: HealthScoreRequest):
    if not req.abundances:
        raise HTTPException(400, "abundances dict must not be empty")

    try:
        blob, genera, genus_index = _load_gbhi_universal()
    except FileNotFoundError as e:
        raise HTTPException(500, f"GBHI universal model not found: {e}")

    n_genus = len(genera)

    # ── 1. align user abundances to frozen 4680-genus order (percent) ──
    total = sum(float(v) for v in req.abundances.values()) or 1.0
    G_pct = np.zeros(n_genus, dtype=np.float32)
    matched = 0
    for name, v in req.abundances.items():
        k = str(name).strip().lower()
        j = genus_index.get(k)
        if j is not None:
            G_pct[j] = float(v) / total * 100.0
            matched += 1

    # ── 2. covariate design row in frozen schema ──
    freeze = blob["freeze"]
    dummy_pairs = freeze["dummy_pairs"]       # list of (prefix, value)
    amp_keep = set(freeze["amp_keep"])
    iso_keep = set(freeze["iso_keep"])
    age_keep = set(freeze["age_keep"])
    sex_keep = set(freeze["sex_keep"])

    amp = (req.amplicon or "na").strip().lower() or "na"
    if amp not in amp_keep:
        amp = "OTHER"
    iso = (req.iso or "NA").strip().upper() or "NA"
    if iso not in iso_keep:
        iso = "OTHER"
    age = (req.age_group or "Unknown").strip() or "Unknown"
    if age not in age_keep:
        age = "Unknown"
    sex = (req.sex or "unknown").strip().lower() or "unknown"
    if sex not in sex_keep:
        sex = "unknown"
    cat_map = {"_amp": amp, "_iso": iso, "_age": age, "_sex": sex}

    dummies = np.zeros(len(dummy_pairs), dtype=np.float32)
    for i, (pre, val) in enumerate(dummy_pairs):
        if cat_map.get(pre) == val:
            dummies[i] = 1.0

    # Training used log10(length+1) with median fill (300 bp default).
    length = float(req.length) if req.length and req.length > 0 else 300.0
    log_len = np.float32(math.log10(length + 1.0))

    # cov_feat row = [dummies..., log10_length]  (shape 62,)
    cov_feat = np.concatenate([dummies, np.array([log_len], dtype=np.float32)])

    # X row = [1, cov_feat] for residualization — (1 + 61 + 1) = 63, matches β̂
    x_row = np.concatenate([np.array([1.0], dtype=np.float32), cov_feat])
    beta = np.asarray(blob["beta"], dtype=np.float32)   # (63, 4680)
    if x_row.shape[0] != beta.shape[0]:
        raise HTTPException(
            500,
            f"GBHI beta shape mismatch: x_row={x_row.shape} beta={beta.shape}",
        )

    # ── 3. residualize log10(G+eps) against β̂ ──
    AB_EPS = 1e-3
    log_g = np.log10(G_pct + AB_EPS).astype(np.float32)    # (4680,)
    pred = x_row @ beta                                    # (4680,)
    R_all = log_g - pred                                   # (4680,)
    union_idx = np.asarray(blob["union_idx"], dtype=int)
    R_union = R_all[union_idx]                             # (289,)

    # ── 4. psi_universal using union markers ──
    psi = np.float32(_gbhi_gupta_psi(G_pct, list(blob["mh_union"]), list(blob["mn_union"])))

    z = np.concatenate([R_union, cov_feat, np.array([psi], dtype=np.float32)])
    expected = len(union_idx) + len(dummy_pairs) + 1 + 1
    if z.shape[0] != expected:
        raise HTTPException(
            500,
            f"GBHI feature dim mismatch: got {z.shape[0]} expected {expected}",
        )

    # ── 5. standardize + softmax ──
    sc_mean = np.asarray(blob["sc_mean"], dtype=np.float32)
    sc_scale = np.asarray(blob["sc_scale"], dtype=np.float32)
    z_std = (z - sc_mean) / np.where(sc_scale == 0, 1.0, sc_scale)

    W = np.asarray(blob["W"], dtype=np.float32)            # (10, 352)
    b = np.asarray(blob["b"], dtype=np.float32)            # (10,)
    logits = W @ z_std + b
    logits = logits - float(logits.max())                  # numerical stability
    exp_l = np.exp(logits)
    probs = exp_l / exp_l.sum()

    class_names = blob["class_names"]
    probs_out: dict[str, float] = {}
    for cname, p in zip(class_names, probs):
        key = _CLASS_KEY_MAP.get(cname)
        if key is not None:
            probs_out[key] = round(float(p), 6)

    p_nc = float(probs_out.get("p_nc", 0.0))
    health = round(p_nc * 100.0, 2)

    return {
        **probs_out,
        "health_score": health,
        "tier": _gbhi_tier(p_nc),
        "n_matched_genera": matched,
        "n_total_genera": n_genus,
        "model_version": blob.get("version", "gbhi_universal_v1"),
        "train_date": blob.get("train_date", ""),
        "n_train": blob.get("n_train", 0),
        "class_names": class_names,
    }


# ── API v1 version aliases ─────────────────────────────────
@app.get("/api/v1/{path:path}")
@no_cache_tracking
@limiter.limit("120/minute")
async def v1_redirect_get(path: str, request: Request):
    """Redirect GET /api/v1/* to /api/* for versioned access"""
    query = str(request.query_params)
    url = f"/api/{path}" + (f"?{query}" if query else "")
    return RedirectResponse(url=url, status_code=307)

@app.post("/api/v1/{path:path}")
@no_cache_tracking
@limiter.limit("120/minute")
async def v1_redirect_post(path: str, request: Request):
    """Redirect POST /api/v1/* to /api/* for versioned access"""
    query = str(request.query_params)
    url = f"/api/{path}" + (f"?{query}" if query else "")
    return RedirectResponse(url=url, status_code=307)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
