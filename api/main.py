"""
main.py – Gut Microbiome Atlas FastAPI backend
主后端：差异分析、筛选选项、数据统计 API
"""

import logging
import os
import json
import math
import tempfile
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
from analysis import wilcoxon_marker_test, spearman_cooccurrence, sample_similarity_search

# Configure logging / 配置日志
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Load environment variables / 加载环境变量 ─────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env.local", override=True)
load_dotenv(Path(__file__).parent.parent / ".env", override=False)

METADATA_PATH = os.getenv("METADATA_PATH", "")  # set via .env.local
ABUNDANCE_PATH = os.getenv("ABUNDANCE_PATH", "")  # set via .env.local
ADMIN_TOKEN    = os.getenv("ADMIN_TOKEN", "")

# Validate at startup — use logging so warnings are never silently swallowed
# 启动时校验：使用 logging 确保警告不会被静默丢弃
if not METADATA_PATH:
    logging.warning("METADATA_PATH not set — data endpoints will fail. Set it in .env.local")
if not ABUNDANCE_PATH:
    logging.warning("ABUNDANCE_PATH not set — diff-analysis endpoints will fail. Set it in .env.local")
if not ADMIN_TOKEN:
    logging.warning("ADMIN_TOKEN not set — admin endpoints will reject all requests")

app = FastAPI(
    title="Gut Microbiome Atlas API",
    version="2.0.0",
    description="""
# Gut Microbiome Atlas — RESTful API

A comprehensive analysis platform for the human gut microbiome, integrating **168,464 samples** across **4,680 genera**, **66 countries**, and **218+ diseases**.

## Features
- **Differential Analysis**: Wilcoxon rank-sum test, t-test, LEfSe (LDA effect size), PERMANOVA
- **Species Profiling**: Genus-level abundance across diseases, countries, age groups, and sex
- **Disease Biomarker Discovery**: Wilcoxon + BH FDR correction + LDA effect size estimation
- **Co-occurrence Network**: Spearman correlation-based microbial interaction networks
- **Sample Similarity Search**: Bray-Curtis / Jaccard distance-based sample matching
- **Lifecycle Atlas**: Age-stratified microbiome composition across 8 life stages
- **Data Export**: CSV/JSON/TSV download for all analysis results

## Citation
If you use this API in your research, please cite:
> Zhai J, Li Y, Liu J, Su X, Cui R, Zheng D, Sun Y, Yu J, Dai C. Gut Microbiome Atlas: a comprehensive platform for exploring human gut microbiome across diseases, geography, and lifespan.

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
    启动时预加载数据到内存，避免首次请求延迟"""
    try:
        if METADATA_PATH:
            get_metadata()
            logging.info("Metadata pre-loaded into cache")
        if ABUNDANCE_PATH:
            get_abundance()
            logging.info("Abundance pre-loaded into cache")
    except Exception as e:
        logging.warning(f"Warmup failed (non-fatal): {e}")

    # Pre-warm fixed endpoints in background thread
    # 后台线程预热固定端点（filter-options, data-stats, disease-list, network）
    import threading

    def _warmup_endpoints():
        import time
        time.sleep(2)  # wait for server to be ready
        import urllib.request
        endpoints = [
            "http://127.0.0.1:8000/api/filter-options",
            "http://127.0.0.1:8000/api/data-stats",
            "http://127.0.0.1:8000/api/disease-list",
            "http://127.0.0.1:8000/api/network?top_diseases=12&top_genera=15",
            "http://127.0.0.1:8000/api/disease-profile?disease=NC",
        ]
        for url in endpoints:
            try:
                urllib.request.urlopen(url, timeout=120)
                logging.info(f"Warmup OK: {url.split('/')[-1].split('?')[0]}")
            except Exception as e:
                logging.warning(f"Warmup failed: {url} -> {e}")

    threading.Thread(target=_warmup_endpoints, daemon=True).start()
    logging.info("Background endpoint warmup started")


# ── Response cache for compute-heavy endpoints / 计算密集端点结果缓存 ─────────

_RESULT_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 3600  # 1 hour

def get_cached(key: str):
    """Return cached result if exists and not expired."""
    if key in _RESULT_CACHE:
        ts, val = _RESULT_CACHE[key]
        if (datetime.now().timestamp() - ts) < _CACHE_TTL:
            return val
        del _RESULT_CACHE[key]
    return None

def set_cached(key: str, val: dict):
    """Store result in cache."""
    _RESULT_CACHE[key] = (datetime.now().timestamp(), val)
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

@lru_cache(maxsize=1)
def get_metadata() -> pd.DataFrame:
    """Load and clean metadata CSV. / 加载并清理元数据CSV"""
    logging.info(f"Loading metadata from {METADATA_PATH}...")
    df = pd.read_csv(METADATA_PATH, encoding="gbk", on_bad_lines="skip", low_memory=False)

    # Normalize column names / 规范化列名
    df.columns = [c.strip() for c in df.columns]

    # Use iso column for country (matches frontend data model)
    # 使用 iso 列作为国家代码（与前端一致，如 US/CN/JP）
    if "iso" in df.columns:
        df["country"] = df["iso"].fillna("unknown").astype(str).str.strip()
        # Merge TW (Taiwan) and HK (Hong Kong) into CN (China)
        # 将台湾和香港的样本归入中国
        df.loc[df["country"].isin(["TW", "HK", "MO"]), "country"] = "CN"
    elif "geo_loc_name" in df.columns:
        df["country"] = df["geo_loc_name"].str.split(":").str[0].str.strip().str.lower()
    else:
        df["country"] = "unknown"

    # Disease columns: keep inform0-11 for per-disease matching
    # 疾病列：保留 inform0-11 用于单病种匹配
    # inform-all contains combined diseases (e.g. "IBS;chickenpox;IBD")
    # inform0-11 are individual diseases split from inform-all
    # A sample with inform-all="IBS;IBD" has inform0="IBS", inform1="IBD"
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    for col in INFORM_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # Legacy: keep "disease" as inform-all for backward compatibility
    if "inform-all" in df.columns:
        df["disease"] = df["inform-all"].fillna("unknown").astype(str).str.strip()
    else:
        df["disease"] = "unknown"

    # Build composite sample key matching abundance matrix rownames
    # 构建与丰度矩阵行名匹配的复合样本键
    # Abundance rownames: "PROJECT_SRR"
    if "srr" in df.columns and "project" in df.columns:
        df["sample_key"] = df["project"].astype(str) + "_" + df["srr"].astype(str)
    else:
        df["sample_key"] = df.index.astype(str)

    logging.info(f"Metadata loaded: {len(df)} rows")
    return df


@lru_cache(maxsize=1)
def get_abundance() -> pd.DataFrame:
    """Load abundance CSV (large ~1.5 GB). / 加载丰度CSV（约1.5GB大文件）"""
    logging.info(f"Loading abundance from {ABUNDANCE_PATH}...")
    # First column is sample_id (rownames from R)
    # 第一列是样本ID（来自R的行名）
    df = pd.read_csv(ABUNDANCE_PATH, index_col=0, low_memory=False)
    logging.info(f"Abundance loaded: {df.shape}")
    return df


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
    "LK": "Sri Lanka", "LT": "Lithuania", "LV": "Latvia", "MA": "Morocco", "MD": "Moldova",
    "MG": "Madagascar", "ML": "Mali", "MM": "Myanmar", "MN": "Mongolia", "MW": "Malawi",
    "MX": "Mexico", "MY": "Malaysia", "MZ": "Mozambique", "NG": "Nigeria", "NL": "Netherlands",
    "NO": "Norway", "NP": "Nepal", "NZ": "New Zealand", "PE": "Peru", "PG": "Papua New Guinea",
    "PH": "Philippines", "PK": "Pakistan", "PL": "Poland", "PT": "Portugal", "RO": "Romania",
    "RS": "Serbia", "RU": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia", "SE": "Sweden",
    "SG": "Singapore", "SI": "Slovenia", "SK": "Slovakia", "SN": "Senegal", "SV": "El Salvador",
    "TH": "Thailand", "TN": "Tunisia", "TR": "Turkey", "TW": "Taiwan", "TZ": "Tanzania",
    "UA": "Ukraine", "UG": "Uganda", "US": "United States", "UZ": "Uzbekistan",
    "VE": "Venezuela", "VN": "Vietnam", "ZA": "South Africa", "ZM": "Zambia", "ZW": "Zimbabwe",
}


def iso_to_name(code: str) -> str:
    """Convert ISO code to human-readable name. / ISO代码转可读国名"""
    return COUNTRY_NAMES.get(code, code)


# ── Disease name i18n / 疾病名称中英文映射 ────────────────────────────────────
_DISEASE_ZH_PATH = Path(__file__).parent / "disease_names_zh.json"
DISEASE_NAMES_ZH: dict[str, str] = {}
if _DISEASE_ZH_PATH.exists():
    with open(_DISEASE_ZH_PATH, encoding="utf-8") as f:
        DISEASE_NAMES_ZH = json.load(f)


def disease_to_zh(name: str) -> str:
    """Return Chinese name if available, else original. / 返回中文疾病名（如有）"""
    return DISEASE_NAMES_ZH.get(name, name)


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
    taxonomy_level: str = "genus"   # phylum / genus
    method: str = "wilcoxon"        # wilcoxon / t-test / lefse / permanova


class SimilarityRequest(BaseModel):
    """样本相似性搜索请求模型"""
    abundances: dict[str, float]  # genus_name -> abundance_value / 属名 -> 丰度值
    metric: str = "braycurtis"    # braycurtis or jaccard / 距离度量
    top_k: int = 10               # 返回最相似样本数量


class CrossStudyRequest(BaseModel):
    """跨研究元分析请求模型"""
    project_ids: list[str]        # 项目ID列表
    disease: str                  # 目标疾病
    method: str = "wilcoxon"      # wilcoxon / t-test
    taxonomy_level: str = "genus" # genus / phylum
    p_threshold: float = 0.05     # 显著性阈值
    min_studies: int = 2          # 最少一致队列数


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

    countries = sorted(meta["country"].dropna().unique().tolist())

    # Extract individual diseases from inform0-11 columns
    # 从 inform0-11 列提取独立疾病（非组合值）
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    all_diseases: set[str] = set()
    for col in INFORM_COLS:
        if col in meta.columns:
            vals = meta[col].dropna().astype(str).str.strip().unique()
            all_diseases.update(v for v in vals if v and v != "nan")
    diseases = sorted(all_diseases)

    age_groups: list[str] = []
    if "age_group" in meta.columns:
        age_groups = sorted(meta["age_group"].dropna().unique().tolist())

    sexes: list[str] = []
    if "sex" in meta.columns:
        sexes = sorted(meta["sex"].dropna().unique().tolist())

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
         description="Returns total sample count, country count, disease count, and data version.")
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

    # Count unique diseases from inform0-11 only (not inform-all combinations)
    # 仅统计 inform0-11 的独立疾病（不含 inform-all 组合值）
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    all_diseases: set[str] = set()
    for col in INFORM_COLS:
        if col in meta.columns:
            vals = meta[col].dropna().astype(str).str.strip()
            all_diseases.update(v for v in vals if v and v != "nan" and v != "")
    all_diseases.discard("unknown")
    all_diseases.discard("NC")

    result = {
        "total_samples": int(len(meta)),
        "total_countries": int(meta.loc[meta["country"] != "unknown", "country"].nunique()) if "country" in meta.columns else 0,
        "total_diseases": len(all_diseases),
        "last_updated": version_info.get("last_updated", datetime.now().strftime("%Y-%m-%d")),
        "version": version_info.get("version", f"v1.0_{datetime.now().strftime('%Y%m%d')}"),
    }
    set_cached("data_stats", result)
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
         description="Returns a mapping from raw disease keys to standardized display names with abbreviations.")
@limiter.limit("120/minute")
def get_disease_display_names(request: Request):
    """Return standardized display names / 返回标准化疾病显示名称映射"""
    result: dict[str, str] = {}
    for key, info in DISEASE_ONTOLOGY.items():
        std = info.get("standard_name", "")
        abbr = info.get("abbreviation", "")
        if std and abbr and abbr != std and abbr != key:
            result[key] = f"{std} ({abbr})"
        elif std and std != key:
            result[key] = std
        elif std:
            result[key] = std
    return result


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
    prevalence = present_count / total_count if total_count > 0 else 0

    # Helper: group mean abundance / 辅助函数：按分组计算平均丰度
    def group_means(col_name: str, top_n: int = 30) -> list[dict]:
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
                "prevalence": round(float((vals > 0).sum() / len(vals)), 4),
                "sample_count": len(vals),
            })
        results.sort(key=lambda x: x["mean_abundance"], reverse=True)
        return results[:top_n]

    # Disease distribution (from inform0-11) / 疾病分布（来自inform0-11）
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    disease_data: dict[str, list[float]] = {}
    for col in INFORM_COLS:
        if col not in mm.columns:
            continue
        for disease_name, group_idx in mm.groupby(mm[col].fillna("").astype(str).str.strip()).groups.items():
            if not disease_name or disease_name == "nan" or disease_name == "":
                continue
            if disease_name not in disease_data:
                disease_data[disease_name] = []
            disease_data[disease_name].extend(ga.loc[group_idx].tolist())

    by_disease = []
    for disease_name, vals_list in disease_data.items():
        arr = np.array(vals_list)
        by_disease.append({
            "name": disease_name,
            "mean_abundance": round(float(arr.mean()), 8),
            "prevalence": round(float((arr > 0).sum() / len(arr)), 4),
            "sample_count": len(arr),
        })
    by_disease.sort(key=lambda x: x["mean_abundance"], reverse=True)

    result = {
        "genus": canonical_name,
        "total_samples": total_count,
        "present_samples": present_count,
        "prevalence": round(prevalence, 4),
        "mean_abundance": round(mean_abundance, 8),
        "by_disease": by_disease[:50],
        "by_country": group_means("country", 30),
        "by_age_group": group_means("age_group" if "age_group" in mm.columns else "age", 10),
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

    # Get NC (healthy control) samples / 获取健康对照样本
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    nc_mask = pd.Series(False, index=mm.index)
    for col in INFORM_COLS:
        if col in mm.columns:
            nc_mask |= (mm[col].fillna("").astype(str).str.strip() == "NC")
    nc_vals = ga.loc[nc_mask].values.astype(float)

    if len(nc_vals) < min_samples:
        raise HTTPException(400, f"Too few NC samples ({len(nc_vals)})")

    nc_mean = float(np.mean(nc_vals))

    # Collect disease groups / 收集疾病分组
    disease_samples: dict[str, list] = {}
    for col in INFORM_COLS:
        if col not in mm.columns:
            continue
        for idx, val in mm[col].fillna("").astype(str).str.strip().items():
            if val and val != "nan" and val != "" and val != "NC" and val != "unknown":
                disease_samples.setdefault(val, []).append(idx)

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
            _, p = stats.mannwhitneyu(d_vals, nc_vals, alternative="two-sided")
            p = float(p)
        except Exception:
            p = 1.0

        direction = "enriched" if log2fc > 0 else "depleted"
        results.append({
            "disease": disease_name,
            "n_samples": len(valid_ids),
            "mean_disease": round(d_mean, 6),
            "mean_control": round(nc_mean, 6),
            "log2fc": round(log2fc, 4),
            "p_value": round(p, 8),
            "direction": direction,
        })

    # BH FDR correction / BH FDR校正
    if results:
        from analysis import _bh_correction
        p_vals = [r["p_value"] for r in results]
        adj_p = _bh_correction(p_vals)
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
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    disease_counts: dict[str, int] = {}
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        for val in meta[col].dropna().astype(str).str.strip():
            if val and val != "nan" and val != "":
                disease_counts[val] = disease_counts.get(val, 0) + 1
    result = [{"name": k, "sample_count": v} for k, v in disease_counts.items()]
    result.sort(key=lambda x: x["sample_count"], reverse=True)
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
        entry.update({
            "standard_name": onto.get("standard_name", ""),
            "standard_name_zh": onto.get("standard_name_zh", ""),
            "abbreviation": onto.get("abbreviation", ""),
            "mesh_id": onto.get("mesh_id", ""),
            "icd10": onto.get("icd10", ""),
            "category": onto.get("category", ""),
            "category_zh": onto.get("category_zh", ""),
        })
        enriched.append(entry)
    return {"diseases": enriched}


@app.get("/api/disease-profile",
         summary="Disease microbiome profile",
         description="Top genera for a disease vs healthy controls with fold-change and prevalence.")
@limiter.limit("60/minute")
def disease_profile(request: Request, disease: str, top_n: int = 20):
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

    # Find samples belonging to this disease / 查找该疾病的样本
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    disease_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        disease_mask |= (meta[col].fillna("").astype(str).str.strip() == disease)

    disease_samples = meta.loc[disease_mask]
    if len(disease_samples) == 0:
        raise HTTPException(404, f"Disease '{disease}' not found")

    # Find healthy control samples: NC (Normal Control) in any inform column
    # 查找健康对照样本：inform0-11 中任一列为 "NC" 的样本
    control_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        control_mask |= (meta[col].fillna("").astype(str).str.strip() == "NC")
    control_samples = meta.loc[control_mask]

    # Get abundance for disease and control samples / 获取疾病组和对照组丰度
    disease_keys = disease_samples["sample_key"].dropna().unique()
    control_keys = control_samples["sample_key"].dropna().unique()

    common_disease = abund.index.intersection(disease_keys)
    common_control = abund.index.intersection(control_keys)

    if len(common_disease) == 0:
        raise HTTPException(404, "No abundance data for disease samples")

    disease_abund = abund.loc[common_disease]
    control_abund = abund.loc[common_control] if len(common_control) > 0 else None

    # Normalize to relative abundance (%) per sample
    # 每个样本转换为相对丰度（%），使所有属的丰度之和 = 100%
    disease_totals = disease_abund.sum(axis=1).replace(0, 1)  # avoid div by 0
    disease_rel = disease_abund.div(disease_totals, axis=0) * 100

    control_rel = None
    if control_abund is not None and len(common_control) > 0:
        control_totals = control_abund.sum(axis=1).replace(0, 1)
        control_rel = control_abund.div(control_totals, axis=0) * 100

    # Compute genus-level mean relative abundance for disease group
    # 计算疾病组的属级平均相对丰度
    genus_map: dict[str, list[str]] = {}
    for col in abund.columns:
        g = extract_genus(col)
        genus_map.setdefault(g, []).append(col)

    genus_stats = []
    for genus, cols in genus_map.items():
        if not is_valid_genus(genus):
            continue
        d_vals = disease_rel[cols].sum(axis=1)
        d_mean = float(d_vals.mean())
        d_prev = float((d_vals > 0).sum() / len(d_vals)) if len(d_vals) > 0 else 0

        c_mean = 0.0
        c_prev = 0.0
        if control_rel is not None:
            c_vals = control_rel[cols].sum(axis=1)
            c_mean = float(c_vals.mean())
            c_prev = float((c_vals > 0).sum() / len(c_vals)) if len(c_vals) > 0 else 0

        if d_mean > 0:
            log2fc = float(np.log2((d_mean + 1e-10) / (c_mean + 1e-10))) if c_mean >= 0 else 0.0
            genus_stats.append({
                "genus": genus,
                "disease_mean": round(d_mean, 4),
                "disease_prevalence": round(d_prev, 4),
                "control_mean": round(c_mean, 4),
                "control_prevalence": round(c_prev, 4),
                "log2fc": round(log2fc, 4),
            })

    genus_stats.sort(key=lambda x: x["disease_mean"], reverse=True)

    # Demographics / 人口统计
    def count_by(col_name: str) -> list[dict]:
        if col_name not in disease_samples.columns:
            return []
        counts = disease_samples[col_name].fillna("unknown").astype(str).str.strip().value_counts()
        return [
            {"name": iso_to_name(k) if col_name == "country" else k, "count": int(v)}
            for k, v in counts.items() if k and k != "nan"
        ][:20]

    # Ontology info / 本体信息
    onto = DISEASE_ONTOLOGY.get(disease, {})

    result = {
        "disease": disease,
        "sample_count": len(disease_samples),
        "control_count": len(control_samples),
        "standard_name": onto.get("standard_name", ""),
        "standard_name_zh": onto.get("standard_name_zh", ""),
        "abbreviation": onto.get("abbreviation", ""),
        "mesh_id": onto.get("mesh_id", ""),
        "icd10": onto.get("icd10", ""),
        "category": onto.get("category", ""),
        "category_zh": onto.get("category_zh", ""),
        "top_genera": genus_stats[:top_n],
        "by_country": count_by("country"),
        "by_age_group": count_by("age_group" if "age_group" in disease_samples.columns else "age"),
        "by_sex": count_by("sex"),
    }
    set_cached(cache_key, result)
    return result


# ── Microbe-disease network endpoint / 菌群-疾病网络端点 ─────────────────────

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
    meta = get_metadata()
    abund = get_abundance()

    INFORM_COLS = [f"inform{i}" for i in range(12)]

    # Get top diseases by sample count / 获取样本数最多的疾病
    disease_counts: dict[str, set] = {}
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        for idx, val in meta[col].dropna().items():
            val = str(val).strip()
            if val and val != "nan":
                disease_counts.setdefault(val, set()).add(idx)

    top_d = sorted(disease_counts.items(), key=lambda x: len(x[1]), reverse=True)[:top_diseases]
    disease_names = [d[0] for d in top_d]

    # Build genus map / 构建属映射
    genus_map: dict[str, list[str]] = {}
    for col in abund.columns:
        g = extract_genus(col)
        genus_map.setdefault(g, []).append(col)

    # For each disease, compute mean abundance of each genus
    # 对每个疾病，计算每个属的平均丰度
    edges: list[dict] = []
    genus_set: set[str] = set()

    for disease_name in disease_names:
        sample_indices = disease_counts[disease_name]
        sample_keys = meta.loc[list(sample_indices), "sample_key"].dropna().unique()
        common = abund.index.intersection(sample_keys)
        if len(common) == 0:
            continue
        disease_abund_raw = abund.loc[common]
        # Normalize to relative abundance (%) / 转换为相对丰度
        d_totals = disease_abund_raw.sum(axis=1).replace(0, 1)
        disease_abund = disease_abund_raw.div(d_totals, axis=0) * 100

        # Get top genera for this disease / 获取该疾病的 top 属
        genus_means = []
        for genus, cols in genus_map.items():
            if not is_valid_genus(genus):
                continue
            m = float(disease_abund[cols].sum(axis=1).mean())
            if m > 0:
                genus_means.append((genus, m))
        genus_means.sort(key=lambda x: x[1], reverse=True)

        for genus, mean_val in genus_means[:top_genera]:
            edges.append({
                "source": disease_name,
                "target": genus,
                "weight": round(mean_val, 6),
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

from fastapi.responses import StreamingResponse
import io
import csv as csv_mod


@app.get("/api/download/summary-stats",
         summary="Download summary statistics",
         description="Export aggregated statistics as CSV, JSON, or TSV.")
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
    disease_counts: dict[str, int] = {}
    for col in INFORM_COLS:
        if col in meta.columns:
            for val in meta[col].dropna().astype(str).str.strip():
                if val and val != "nan" and val != "":
                    disease_counts[val] = disease_counts.get(val, 0) + 1
    # Age group stats / 年龄组统计
    age_counts = meta["age_group"].value_counts().to_dict() if "age_group" in meta.columns else {}
    # Sex stats / 性别统计
    sex_counts = meta["sex"].value_counts().to_dict() if "sex" in meta.columns else {}

    if format == "json":
        return {
            "total_samples": len(meta),
            "by_country": country_counts,
            "by_disease": disease_counts,
            "by_age_group": age_counts,
            "by_sex": sex_counts,
        }

    # CSV format: flatten to rows
    rows = []
    for k, v in country_counts.items():
        rows.append({"category": "country", "name": iso_to_name(k), "count": v})
    for k, v in sorted(disease_counts.items(), key=lambda x: x[1], reverse=True):
        rows.append({"category": "disease", "name": k, "count": v})
    for k, v in age_counts.items():
        rows.append({"category": "age_group", "name": k, "count": v})
    for k, v in sex_counts.items():
        rows.append({"category": "sex", "name": k, "count": v})

    sep = "\t" if format == "tsv" else ","
    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=["category", "name", "count"], delimiter=sep)
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)
    ext = "tsv" if format == "tsv" else "csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=summary_stats.{ext}"}
    )


@app.get("/api/download/disease-profile",
         summary="Download disease profile",
         description="Export disease microbiome profile as CSV, JSON, or TSV.")
@limiter.limit("30/minute")
def download_disease_profile_data(request: Request, disease: str, format: str = "csv"):
    """Download disease profile data / 下载疾病画像数据"""
    profile = disease_profile(request, disease, top_n=50)
    rows = profile["top_genera"]

    if format == "json":
        return rows

    sep = "\t" if format == "tsv" else ","
    buf = io.StringIO()
    if rows:
        writer = csv_mod.DictWriter(buf, fieldnames=rows[0].keys(), delimiter=sep)
        writer.writeheader()
        writer.writerows(rows)
    buf.seek(0)
    ext = "tsv" if format == "tsv" else "csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={disease}_profile.{ext}"}
    )


@app.get("/api/download/species-profile",
         summary="Download species profile",
         description="Export genus abundance profile as CSV, JSON, or TSV.")
@limiter.limit("30/minute")
def download_species_profile_data(request: Request, genus: str, format: str = "csv"):
    """Download species profile data / 下载物种画像数据"""
    profile = species_profile(request, genus)

    if format == "json":
        return profile

    rows = profile.get("by_disease", [])
    sep = "\t" if format == "tsv" else ","
    buf = io.StringIO()
    if rows:
        writer = csv_mod.DictWriter(buf, fieldnames=rows[0].keys(), delimiter=sep)
        writer.writeheader()
        writer.writerows(rows)
    buf.seek(0)
    ext = "tsv" if format == "tsv" else "csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={genus}_profile.{ext}"}
    )


@app.get("/api/download/genus-list",
         summary="Download genus list",
         description="Export complete genus name list as CSV, JSON, or TSV.")
@limiter.limit("30/minute")
def download_genus_list(request: Request, format: str = "csv"):
    """Download list of all genera / 下载所有属名列表"""
    genera = get_genus_list()

    if format == "json":
        return {"genera": genera}

    buf = io.StringIO()
    buf.write("genus\n")
    for g in genera:
        buf.write(f"{g}\n")
    buf.seek(0)
    ext = "tsv" if format == "tsv" else "csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=genus_list.{ext}"}
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

    markers = wilcoxon_marker_test(d_filtered, c_filtered, valid_genera, p_threshold)
    markers = [m for m in markers if m["lda_score"] >= lda_threshold]

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

    meta = get_metadata()
    abund = get_abundance()

    INFORM_COLS = [f"inform{i}" for i in range(12)]
    disease_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col in meta.columns:
            disease_mask |= (meta[col].fillna("").astype(str).str.strip() == disease)
    disease_samples = meta.loc[disease_mask]

    control_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col in meta.columns:
            control_mask |= (meta[col].fillna("").astype(str).str.strip() == "NC")
    control_samples = meta.loc[control_mask]

    d_keys = abund.index.intersection(disease_samples["sample_key"].dropna().unique())
    c_keys = abund.index.intersection(control_samples["sample_key"].dropna().unique())

    if len(d_keys) == 0 or len(c_keys) == 0:
        raise HTTPException(404, "Insufficient samples")

    d_raw = abund.loc[d_keys].values.astype(float)
    c_raw = abund.loc[c_keys].values.astype(float)
    d_t = d_raw.sum(axis=1, keepdims=True); d_t[d_t == 0] = 1
    c_t = c_raw.sum(axis=1, keepdims=True); c_t[c_t == 0] = 1
    d_rel = d_raw / d_t * 100
    c_rel = c_raw / c_t * 100

    col_names = abund.columns.tolist()
    results = []

    genus_map: dict[str, list[int]] = {}
    genus_phylum: dict[str, str] = {}
    for j, col in enumerate(col_names):
        g = extract_genus(col)
        if not is_valid_genus(g):
            continue
        genus_map.setdefault(g, []).append(j)
        if g not in genus_phylum:
            genus_phylum[g] = extract_phylum(col)

    for genus, idxs in genus_map.items():
        d_vals = d_rel[:, idxs].sum(axis=1)
        c_vals = c_rel[:, idxs].sum(axis=1)
        mean_d = float(np.mean(d_vals))
        mean_c = float(np.mean(c_vals))

        pseudo = 1e-6
        log2fc = float(np.log2((mean_d + pseudo) / (mean_c + pseudo)))

        try:
            _, p = stats.mannwhitneyu(d_vals, c_vals, alternative="two-sided")
        except Exception:
            p = 1.0

        neg_log10p = -math.log10(max(float(p), 1e-300))

        results.append({
            "genus": genus,
            "phylum": genus_phylum.get(genus, "Unknown"),
            "log2fc": round(log2fc, 4),
            "neg_log10p": round(neg_log10p, 2),
            "p_value": round(float(p), 8),
            "mean_disease": round(mean_d, 6),
            "mean_control": round(mean_c, 6),
        })

    results.sort(key=lambda x: abs(x["log2fc"]), reverse=True)
    result = {
        "disease": disease,
        "n_disease": len(d_keys),
        "n_control": len(c_keys),
        "data": results[:top_n],
    }
    set_cached(cache_key, result)
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

    disease_counts: dict[str, set] = {}
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        for idx, val in meta[col].dropna().items():
            val = str(val).strip()
            if val and val != "nan" and val != "NC":
                disease_counts.setdefault(val, set()).add(idx)

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
         description="Top co-occurring genera for a given genus, based on Spearman correlation across healthy samples.",
         tags=["Species"])
@limiter.limit("30/minute")
def species_cooccurrence(request: Request, genus: str, top_k: int = 10):
    """
    Return top co-occurring genera for a given genus.
    返回给定属的 Top 共现微生物
    """
    if not genus or not genus.strip():
        raise HTTPException(400, "genus parameter is required")

    cache_key = f"species_cooccurrence:{genus.strip().lower()}:{top_k}"
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

    # Use NC samples
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    nc_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col in meta.columns:
            nc_mask |= (meta[col].fillna("").astype(str).str.strip() == "NC")
    sample_keys = meta.loc[nc_mask, "sample_key"].dropna().unique()
    valid_keys = abund.index.intersection(sample_keys)

    np.random.seed(42)
    if len(valid_keys) > 3000:
        valid_keys = valid_keys[np.random.choice(len(valid_keys), 3000, replace=False)]

    if len(valid_keys) < 20:
        return {"genus": canonical, "partners": []}

    raw = abund.loc[valid_keys].values.astype(float)
    totals = raw.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    rel = raw / totals * 100

    col_names = abund.columns.tolist()
    genus_labels = [extract_genus(c) for c in col_names]
    unique_genera = list(dict.fromkeys(genus_labels))

    # Aggregate to genus level
    genus_matrix = np.zeros((rel.shape[0], len(unique_genera)))
    for i, g in enumerate(unique_genera):
        idxs = [j for j, l in enumerate(genus_labels) if l == g]
        genus_matrix[:, i] = rel[:, idxs].sum(axis=1)

    # Find target genus index
    target_idx = None
    for i, g in enumerate(unique_genera):
        if g.lower() == genus.strip().lower():
            target_idx = i
            break
    if target_idx is None:
        return {"genus": canonical, "partners": []}

    target_vec = genus_matrix[:, target_idx]
    # Skip if target has no variance
    if np.std(target_vec) == 0:
        return {"genus": canonical, "partners": []}

    partners = []
    for i, g in enumerate(unique_genera):
        if i == target_idx or not is_valid_genus(g):
            continue
        other_vec = genus_matrix[:, i]
        if np.std(other_vec) == 0 or (other_vec > 0).sum() < 10:
            continue
        r, p = stats.spearmanr(target_vec, other_vec)
        if abs(r) >= 0.2 and p < 0.05:
            partners.append({
                "genus": g,
                "r": round(float(r), 4),
                "p_value": round(float(p), 6),
                "type": "positive" if r > 0 else "negative",
            })

    partners.sort(key=lambda x: abs(x["r"]), reverse=True)
    result = {"genus": canonical, "partners": partners[:top_k]}
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
):
    """
    Compute genus co-occurrence network based on Spearman correlation.
    基于 Spearman 相关性计算属共现网络
    """
    cache_key = f"cooccurrence:{disease}:{min_r}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    meta = get_metadata()
    abund = get_abundance()

    INFORM_COLS = [f"inform{i}" for i in range(12)]

    if disease and disease.strip():
        mask = pd.Series(False, index=meta.index)
        for col in INFORM_COLS:
            if col in meta.columns:
                mask |= (meta[col].fillna("").astype(str).str.strip() == disease.strip())
        sample_keys = meta.loc[mask, "sample_key"].dropna().unique()
    else:
        mask = pd.Series(False, index=meta.index)
        for col in INFORM_COLS:
            if col in meta.columns:
                mask |= (meta[col].fillna("").astype(str).str.strip() == "NC")
        sample_keys = meta.loc[mask, "sample_key"].dropna().unique()

    valid_keys = abund.index.intersection(sample_keys)
    if len(valid_keys) < 10:
        raise HTTPException(400, "Too few samples for correlation analysis")

    np.random.seed(42)
    if len(valid_keys) > max_samples:
        valid_keys = valid_keys[np.random.choice(len(valid_keys), max_samples, replace=False)]

    raw = abund.loc[valid_keys].values.astype(float)
    totals = raw.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    rel = raw / totals * 100

    col_names = abund.columns.tolist()
    genus_labels = [extract_genus(c) for c in col_names]
    unique_genera = list(dict.fromkeys(genus_labels))

    valid_genera = [g for g in unique_genera if is_valid_genus(g)]

    genus_means = []
    genus_indices: dict[str, list[int]] = {}
    for g in valid_genera:
        idxs = [j for j, l in enumerate(genus_labels) if l == g]
        genus_indices[g] = idxs
        mean_val = float(rel[:, idxs].sum(axis=1).mean())
        genus_means.append((g, mean_val))

    genus_means.sort(key=lambda x: x[1], reverse=True)
    top_genera_names = [g[0] for g in genus_means[:top_genera]]

    genus_matrix = np.zeros((len(valid_keys), len(top_genera_names)))
    for i, g in enumerate(top_genera_names):
        genus_matrix[:, i] = rel[:, genus_indices[g]].sum(axis=1)

    edges = spearman_cooccurrence(
        genus_matrix, top_genera_names,
        min_prevalence=0.1, min_abs_r=min_r, max_pairs=500
    )

    node_set = set()
    for e in edges:
        node_set.add(e["source"])
        node_set.add(e["target"])

    nodes = []
    for g in top_genera_names:
        if g in node_set:
            nodes.append({
                "id": g,
                "mean_abundance": round(float(genus_matrix[:, top_genera_names.index(g)].mean()), 4),
            })

    result = {
        "disease": disease or "Healthy (NC)",
        "n_samples": len(valid_keys),
        "n_genera": len(nodes),
        "n_edges": len(edges),
        "min_r": min_r,
        "nodes": nodes,
        "edges": edges,
    }
    set_cached(cache_key, result)
    return result


AGE_GROUP_ORDER = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian", "Unknown"]


@app.get("/api/lifecycle",
         summary="Lifecycle microbiome atlas",
         description="Genus-level composition across 8 life stages from Infant to Centenarian.")
@limiter.limit("60/minute")
def lifecycle_atlas(
    request: Request,
    disease: str = "",
    country: str = "",
    top_genera: int = 15,
):
    """
    Return genus composition across 8 life stages.
    返回 8 个生命阶段的属级组成
    """
    cache_key = f"lifecycle:{disease}:{country}:{top_genera}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    meta = get_metadata()
    abund = get_abundance()

    if "age_group" not in meta.columns:
        raise HTTPException(400, "age_group column not found in metadata")

    filtered = meta.copy()
    INFORM_COLS = [f"inform{i}" for i in range(12)]
    if disease and disease.strip():
        mask = pd.Series(False, index=filtered.index)
        for col in INFORM_COLS:
            if col in filtered.columns:
                mask |= (filtered[col].fillna("").astype(str).str.strip() == disease.strip())
        filtered = filtered[mask]
    else:
        mask = pd.Series(False, index=filtered.index)
        for col in INFORM_COLS:
            if col in filtered.columns:
                mask |= (filtered[col].fillna("").astype(str).str.strip() == "NC")
        filtered = filtered[mask]

    if country and country.strip():
        filtered = filtered[filtered["country"].str.upper() == country.strip().upper()]

    if len(filtered) < 10:
        raise HTTPException(400, "Too few samples after filtering")

    valid_keys = abund.index.intersection(filtered["sample_key"].dropna().unique())
    if len(valid_keys) < 10:
        raise HTTPException(400, "Too few samples with abundance data")

    raw = abund.loc[valid_keys].values.astype(float)
    totals = raw.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    rel = raw / totals * 100

    col_names = abund.columns.tolist()
    genus_labels = [extract_genus(c) for c in col_names]
    unique_genera = list(dict.fromkeys(genus_labels))

    genus_indices: dict[str, list[int]] = {}
    genus_means_list: list[tuple[str, float]] = []
    for g in unique_genera:
        if not is_valid_genus(g):
            continue
        idxs = [j for j, l in enumerate(genus_labels) if l == g]
        genus_indices[g] = idxs
        genus_means_list.append((g, float(rel[:, idxs].sum(axis=1).mean())))

    genus_means_list.sort(key=lambda x: x[1], reverse=True)
    top_genus_names = [g[0] for g in genus_means_list[:top_genera]]

    key_to_age = dict(zip(filtered["sample_key"], filtered["age_group"].fillna("Unknown").astype(str).str.strip()))
    sample_ages = [key_to_age.get(k, "Unknown") for k in valid_keys]

    age_groups_present = [ag for ag in AGE_GROUP_ORDER if ag in set(sample_ages)]
    stacked_data = []

    for ag in age_groups_present:
        ag_indices = [i for i, a in enumerate(sample_ages) if a == ag]
        if len(ag_indices) == 0:
            continue

        ag_rel = rel[ag_indices]
        row: dict = {"age_group": ag, "sample_count": len(ag_indices)}

        for g in top_genus_names:
            idxs = genus_indices[g]
            row[g] = round(float(ag_rel[:, idxs].sum(axis=1).mean()), 4)

        top_sum = sum(row[g] for g in top_genus_names)
        row["Other"] = round(max(0, 100 - top_sum), 4)

        stacked_data.append(row)

    transitions = []
    for i in range(1, len(stacked_data)):
        prev = stacked_data[i - 1]
        curr = stacked_data[i]
        max_change_genus = ""
        max_change = 0.0
        for g in top_genus_names:
            change = abs(curr.get(g, 0) - prev.get(g, 0))
            if change > max_change:
                max_change = change
                max_change_genus = g
        if max_change > 0.5:
            transitions.append({
                "from": prev["age_group"],
                "to": curr["age_group"],
                "genus": max_change_genus,
                "change": round(max_change, 4),
                "direction": "increase" if curr.get(max_change_genus, 0) > prev.get(max_change_genus, 0) else "decrease",
            })

    result = {
        "disease": disease or "Healthy (NC)",
        "country": country or "All",
        "total_samples": len(valid_keys),
        "genera": top_genus_names + ["Other"],
        "data": stacked_data,
        "transitions": transitions,
    }
    set_cached(cache_key, result)
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

    # ── 构建列名映射：属名 -> 丰度矩阵列名 ──
    col_genus_map: dict[str, str] = {}
    for c in abund.columns:
        g = extract_genus(c)
        if g and is_valid_genus(g):
            col_genus_map[g.lower()] = c

    # ── 将用户提交的属名->丰度值对齐到丰度矩阵的列顺序 ──
    query_vector = np.zeros(len(abund.columns), dtype=float)
    matched_genera = 0
    for genus_name, value in req.abundances.items():
        col_name = col_genus_map.get(genus_name.lower().strip())
        if col_name is not None:
            idx = abund.columns.get_loc(col_name)
            query_vector[idx] = float(value)
            matched_genera += 1

    if matched_genera == 0:
        raise HTTPException(400, "No matching genera found in the abundance matrix")

    # ── 调用 analysis.py 中的相似性搜索函数 ──
    results = sample_similarity_search(
        query_vector=query_vector,
        abundance_matrix=abund.values,
        sample_keys=list(abund.index),
        metric=req.metric,
        top_k=req.top_k,
    )

    # ── 补充元数据信息（疾病、国家等） ──
    for item in results:
        key = item["sample_key"]
        if key in meta.index:
            row = meta.loc[key]
            item["disease"] = str(row.get("disease", "Unknown"))
            item["country"] = str(row.get("country", "Unknown"))
        else:
            item["disease"] = "Unknown"
            item["country"] = "Unknown"

    return {
        "metric": req.metric,
        "top_k": req.top_k,
        "matched_genera": matched_genera,
        "total_genera": len(col_genus_map),
        "results": results,
    }


# ── Cross-study meta-analysis / 跨研究元分析 ────────────────────────────────

@app.get("/api/project-list",
         summary="List available projects",
         description="Returns distinct project IDs with sample counts and disease coverage.",
         tags=["Analysis"])
@limiter.limit("60/minute")
async def project_list(request: Request):
    """返回可用项目列表及每个项目的样本数/疾病覆盖。"""
    meta = get_metadata()
    if "project" not in meta.columns:
        raise HTTPException(500, "No 'project' column in metadata")

    INFORM_COLS = [f"inform{i}" for i in range(12)]
    projects = []
    for proj, grp in meta.groupby("project"):
        # Collect all diseases in this project
        diseases_set: set[str] = set()
        for col in INFORM_COLS:
            if col in grp.columns:
                vals = grp[col].dropna().astype(str).str.strip()
                diseases_set.update(v for v in vals if v and v.lower() != "nan")
        has_nc = any(d.upper() == "NC" for d in diseases_set)
        diseases_set.discard("NC")
        diseases_set.discard("nc")
        projects.append({
            "project_id": str(proj),
            "sample_count": len(grp),
            "diseases": sorted(diseases_set)[:20],
            "has_control": has_nc,
        })

    # Sort by sample count descending
    projects.sort(key=lambda x: x["sample_count"], reverse=True)
    return {"projects": projects, "total": len(projects)}


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
    if len(req.project_ids) > 10:
        raise HTTPException(400, "Maximum 10 projects allowed")

    meta = get_metadata()
    abund = get_abundance()
    abund_idx = set(abund.index)
    col_names = abund.columns.tolist()

    # Aggregate columns by taxonomy level
    def _group_by_level(matrix: np.ndarray, cols: list[str], level: str):
        if level == "genus":
            labels = [extract_genus(c) for c in cols]
        else:
            labels = [extract_phylum(c) for c in cols]
        unique_labels = list(dict.fromkeys(labels))
        agg = np.zeros((matrix.shape[0], len(unique_labels)))
        for i, lbl in enumerate(unique_labels):
            idxs = [j for j, l in enumerate(labels) if l == lbl]
            agg[:, i] = matrix[:, idxs].sum(axis=1)
        return agg, unique_labels

    INFORM_COLS = [f"inform{i}" for i in range(12)]
    disease_lower = req.disease.strip().lower()

    per_project_results = []
    taxa_global = None

    for proj_id in req.project_ids:
        proj_meta = meta[meta["project"].astype(str) == proj_id]
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
        nc_mask = pd.Series(False, index=proj_meta.index)
        for col in INFORM_COLS:
            if col in proj_meta.columns:
                nc_mask |= proj_meta[col].fillna("").astype(str).str.strip().str.lower() == "nc"

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
        tot_d = raw_d.sum(axis=1, keepdims=True); tot_d[tot_d == 0] = 1
        tot_c = raw_c.sum(axis=1, keepdims=True); tot_c[tot_c == 0] = 1
        mat_d = raw_d / tot_d * 100
        mat_c = raw_c / tot_c * 100

        agg_d, taxa = _group_by_level(mat_d, col_names, req.taxonomy_level)
        agg_c, _ = _group_by_level(mat_c, col_names, req.taxonomy_level)
        if taxa_global is None:
            taxa_global = taxa

        # Per-taxon differential test
        taxa_results = []
        for i, taxon in enumerate(taxa):
            vals_d = agg_d[:, i]
            vals_c = agg_c[:, i]
            mean_d = float(np.mean(vals_d))
            mean_c = float(np.mean(vals_c))
            pseudo = 1e-6
            log2fc = math.log2((mean_d + pseudo) / (mean_c + pseudo))

            try:
                if req.method == "wilcoxon":
                    _, p = stats.mannwhitneyu(vals_d, vals_c, alternative="two-sided")
                else:
                    _, p = stats.ttest_ind(vals_d, vals_c)
            except Exception:
                p = 1.0

            # Standard error of mean difference
            se = float(np.sqrt(np.var(vals_d) / len(vals_d) + np.var(vals_c) / len(vals_c)))
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

    # Sort by meta p-value, filter significant
    consensus_markers.sort(key=lambda x: x["meta_p"])
    significant_markers = [m for m in consensus_markers if m["meta_p"] < req.p_threshold]

    return {
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
        "consensus_markers": significant_markers[:100],
        "total_significant": len(significant_markers),
        "all_markers": consensus_markers[:200],
    }


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

    # NC (normal control) samples
    nc_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col in meta.columns:
            nc_mask |= meta[col].fillna("").astype(str).str.strip().str.upper() == "NC"
    nc_keys = [k for k in meta.loc[nc_mask, "sample_key"].values if k in abund_idx]

    # Disease samples (any disease that is NOT NC)
    disease_mask = ~nc_mask & meta["disease"].str.strip().str.lower().ne("unknown")
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
    health_genera = []
    disease_genera = []
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
            "p25": round(float(np.percentile(vals_nc, 25)), 6),
            "p75": round(float(np.percentile(vals_nc, 75)), 6),
        }

        if p < 0.05:
            pseudo = 1e-6
            log2fc = math.log2((mean_nc + pseudo) / (mean_dis + pseudo))
            entry = {"genus": genus, "log2fc": round(log2fc, 4), "p_value": round(float(p), 8),
                     "mean_nc": round(mean_nc, 6), "mean_disease": round(mean_dis, 6)}
            if mean_nc > mean_dis:
                health_genera.append(entry)
            else:
                disease_genera.append(entry)

    # Sort by absolute log2fc, take top 50
    health_genera.sort(key=lambda x: abs(x["log2fc"]), reverse=True)
    disease_genera.sort(key=lambda x: abs(x["log2fc"]), reverse=True)

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
    health_matched = []
    disease_matched = []
    per_genus = []

    for genus, value in req.abundances.items():
        g_lower = genus.strip().lower()
        g_title = genus.strip().title()

        # Check if this is a health or disease genus
        if g_lower in health_set:
            h_sum += value
            health_matched.append({"genus": g_title, "abundance": round(value, 6)})
        if g_lower in disease_set:
            d_sum += value
            disease_matched.append({"genus": g_title, "abundance": round(value, 6)})

        # Per-genus deviation from NC reference
        if g_title in nc_stats:
            ref_stat = nc_stats[g_title]
            deviation = value - ref_stat["mean"]
            status = "normal"
            if value > ref_stat["p75"] * 1.5:
                status = "high"
            elif value < ref_stat["p25"] * 0.5:
                status = "low"
            per_genus.append({
                "genus": g_title,
                "user_abundance": round(value, 6),
                "nc_mean": ref_stat["mean"],
                "nc_median": ref_stat["median"],
                "status": status,
            })

    # Calculate raw score
    pseudo = 1e-6
    raw_score = math.log10((h_sum + pseudo) / (d_sum + pseudo))

    # Use empirical percentile calibration from population data
    # 使用群体数据的经验百分位数校准
    pop = _compute_population_gmhi()
    cal = pop.get("calibration", {})
    p5 = cal.get("p5", -2.0)
    p95 = cal.get("p95", 2.0)
    score_range = p95 - p5 if p95 != p5 else 1.0
    normalized = max(0, min(100, (raw_score - p5) / score_range * 100))

    # Determine category
    if normalized >= 70:
        category = "good"
    elif normalized >= 40:
        category = "moderate"
    else:
        category = "attention"

    # Sort per_genus by absolute deviation
    per_genus.sort(key=lambda x: abs(x["user_abundance"] - x["nc_mean"]), reverse=True)

    return {
        "score": round(normalized, 1),
        "raw_score": round(raw_score, 4),
        "category": category,
        "health_genera_matched": len(health_matched),
        "disease_genera_matched": len(disease_matched),
        "health_genera_sum": round(h_sum, 4),
        "disease_genera_sum": round(d_sum, 4),
        "health_genera_detail": health_matched[:20],
        "disease_genera_detail": disease_matched[:20],
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
    nc_mask = pd.Series(False, index=meta.index)
    for col in INFORM_COLS:
        if col in meta.columns:
            nc_mask |= meta[col].fillna("").astype(str).str.strip().str.upper() == "NC"
    nc_keys = [k for k in meta.loc[nc_mask, "sample_key"].values if k in abund_idx]
    disease_mask = ~nc_mask & meta["disease"].str.strip().str.lower().ne("unknown")
    disease_keys = [k for k in meta.loc[disease_mask, "sample_key"].values if k in abund_idx]

    np.random.seed(42)
    nc_sample = list(np.random.choice(nc_keys, min(2000, len(nc_keys)), replace=False)) if len(nc_keys) > 0 else []
    dis_sample = list(np.random.choice(disease_keys, min(2000, len(disease_keys)), replace=False)) if len(disease_keys) > 0 else []

    health_set = {g["genus"].lower() for g in ref["health_genera"]}
    disease_set = {g["genus"].lower() for g in ref["disease_genera"]}

    genus_labels = [extract_genus(c).lower() for c in col_names]
    h_cols = [i for i, g in enumerate(genus_labels) if g in health_set]
    d_cols = [i for i, g in enumerate(genus_labels) if g in disease_set]

    def compute_raw_scores(keys):
        if not keys:
            return np.array([])
        raw = abund.loc[keys].values.astype(float)
        totals = raw.sum(axis=1, keepdims=True)
        totals[totals == 0] = 1
        rel = raw / totals * 100
        h_sums = rel[:, h_cols].sum(axis=1) if h_cols else np.zeros(len(keys))
        d_sums = rel[:, d_cols].sum(axis=1) if d_cols else np.zeros(len(keys))
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
            "p25": round(float(np.percentile(nc_arr, 25)), 1),
            "p75": round(float(np.percentile(nc_arr, 75)), 1),
        },
        "disease_stats": {
            "n": len(disease_keys),  # actual total, not subsample
            "mean": round(float(np.mean(dis_arr)), 1),
            "median": round(float(np.median(dis_arr)), 1),
            "std": round(float(np.std(dis_arr)), 1),
            "p25": round(float(np.percentile(dis_arr, 25)), 1),
            "p75": round(float(np.percentile(dis_arr, 75)), 1),
        },
        "calibration": {
            "p5": round(p5, 4),
            "p95": round(p95, 4),
        },
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
        "health_genera": ref["health_genera"][:20],
        "disease_genera": ref["disease_genera"][:20],
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
