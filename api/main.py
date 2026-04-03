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
    version="1.0.0",
    description="Backend API for differential microbiome analysis",
)

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

# ── Data loading (cached) / 数据加载（缓存） ──────────────────────────────────

@lru_cache(maxsize=1)
def get_metadata() -> pd.DataFrame:
    """Load and clean metadata CSV. / 加载并清理元数据CSV"""
    print(f"Loading metadata from {METADATA_PATH}...")
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

    print(f"Metadata loaded: {len(df)} rows")
    return df


@lru_cache(maxsize=1)
def get_abundance() -> pd.DataFrame:
    """Load abundance CSV (large ~1.5 GB). / 加载丰度CSV（约1.5GB大文件）"""
    print(f"Loading abundance from {ABUNDANCE_PATH}...")
    # First column is sample_id (rownames from R)
    # 第一列是样本ID（来自R的行名）
    df = pd.read_csv(ABUNDANCE_PATH, index_col=0, low_memory=False)
    print(f"Abundance loaded: {df.shape}")
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

@app.get("/api/health")
def health():
    """Health check / 健康检查"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/filter-options")
def filter_options():
    """
    Return available filter option values from metadata.
    返回元数据中可用的筛选选项值
    """
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

    return {
        "countries": countries,
        "diseases": diseases[:500],       # limit for payload size / 限制响应大小
        "age_groups": age_groups,
        "sexes": sexes,
    }


@app.get("/api/data-stats")
def data_stats():
    """
    Return current dataset statistics for homepage display.
    返回当前数据集统计数据供首页展示
    """
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

    return {
        "total_samples": int(len(meta)),
        "total_countries": int(meta["country"].nunique()) if "country" in meta.columns else 0,
        "total_diseases": len(all_diseases),
        "last_updated": version_info.get("last_updated", datetime.now().strftime("%Y-%m-%d")),
        "version": version_info.get("version", f"v1.0_{datetime.now().strftime('%Y%m%d')}"),
    }


@app.post("/api/diff-analysis")
def diff_analysis(req: DiffAnalysisRequest):
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

    # Extract abundance matrices / 提取丰度矩阵
    mat_a = abund.loc[valid_a].values.astype(float)
    mat_b = abund.loc[valid_b].values.astype(float)
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


@app.get("/api/species-search")
def species_search(q: str = ""):
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


@app.get("/api/species-profile")
def species_profile(genus: str):
    """
    Return a comprehensive profile for a given genus:
    为给定属返回综合画像：
    - Total sample count where genus is present / 含该属的总样本数
    - Mean abundance by disease / 各疾病中的平均丰度
    - Mean abundance by country / 各国家中的平均丰度
    - Mean abundance by age group / 各年龄组中的平均丰度
    - Mean abundance by sex / 各性别中的平均丰度
    """
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
    genus_abundance = abund[matching_cols].sum(axis=1)

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

    return {
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


@app.get("/api/disease-list")
def disease_list(q: str = ""):
    """
    Return all diseases with sample counts. Optional search filter.
    返回所有疾病及样本数，可选搜索过滤
    """
    diseases = get_disease_list_cached()
    if q and q.strip():
        q_lower = q.strip().lower()
        diseases = [d for d in diseases if q_lower in d["name"].lower()]
    return {"diseases": diseases}


@app.get("/api/disease-profile")
def disease_profile(disease: str, top_n: int = 20):
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

    # Find healthy control samples (those with no disease in any inform column)
    # 查找健康对照样本（所有inform列均无疾病标注）
    control_mask = pd.Series(True, index=meta.index)
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        control_mask &= (meta[col].fillna("").astype(str).str.strip().isin(["", "nan", "control", "healthy"]))
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

    # Compute genus-level mean abundance for disease group
    # 计算疾病组的属级平均丰度
    genus_map: dict[str, list[str]] = {}
    for col in abund.columns:
        g = extract_genus(col)
        genus_map.setdefault(g, []).append(col)

    genus_stats = []
    for genus, cols in genus_map.items():
        if not is_valid_genus(genus):
            continue
        d_vals = disease_abund[cols].sum(axis=1)
        d_mean = float(d_vals.mean())
        d_prev = float((d_vals > 0).sum() / len(d_vals)) if len(d_vals) > 0 else 0

        c_mean = 0.0
        c_prev = 0.0
        if control_abund is not None and len(common_control) > 0:
            c_vals = control_abund[cols].sum(axis=1)
            c_mean = float(c_vals.mean())
            c_prev = float((c_vals > 0).sum() / len(c_vals)) if len(c_vals) > 0 else 0

        if d_mean > 0:
            log2fc = float(np.log2((d_mean + 1e-10) / (c_mean + 1e-10))) if c_mean >= 0 else 0.0
            genus_stats.append({
                "genus": genus,
                "disease_mean": round(d_mean, 8),
                "disease_prevalence": round(d_prev, 4),
                "control_mean": round(c_mean, 8),
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

    return {
        "disease": disease,
        "sample_count": len(disease_samples),
        "control_count": len(control_samples),
        "top_genera": genus_stats[:top_n],
        "by_country": count_by("country"),
        "by_age_group": count_by("age_group" if "age_group" in disease_samples.columns else "age"),
        "by_sex": count_by("sex"),
    }


# ── Microbe-disease network endpoint / 菌群-疾病网络端点 ─────────────────────

@app.get("/api/network")
def microbe_disease_network(top_diseases: int = 15, top_genera: int = 30):
    """
    Return nodes (diseases + genera) and edges for a force-directed network.
    返回力导向图所需的节点（疾病 + 属）和边
    Edge weight = mean abundance of genus in disease samples.
    """
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
        disease_abund = abund.loc[common]

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

    return {"nodes": nodes, "edges": edges}


# ── Data management endpoints / 数据管理端点 ──────────────────────────────────

def _check_admin(token: str | None):
    """Reject if ADMIN_TOKEN unset or token doesn't match. / 校验admin token，空token也拒绝"""
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        raise HTTPException(401, "Invalid admin token")


@app.get("/api/admin/check")
def admin_check(x_admin_token: str | None = Header(None)):
    """Verify admin token. / 验证管理员token"""
    _check_admin(x_admin_token)
    return {"status": "authorized"}


@app.post("/api/admin/upload-metadata")
async def upload_metadata(
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


@app.post("/api/admin/validate-metadata")
async def validate_metadata_endpoint(
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
