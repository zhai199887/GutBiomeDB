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

    # Extract country from geo_loc_name (format: "china:beijing" → "china")
    # 从 geo_loc_name 提取国家（格式："china:beijing" → "china"）
    if "geo_loc_name" in df.columns:
        df["country"] = df["geo_loc_name"].str.split(":").str[0].str.strip().str.lower()
    else:
        df["country"] = "unknown"

    # Unify disease column name / 统一疾病列名
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
    method: str = "wilcoxon"        # wilcoxon / t-test


# ── Helper functions / 辅助函数 ────────────────────────────────────────────────

def apply_filter(df: pd.DataFrame, f: GroupFilter) -> pd.DataFrame:
    """Filter metadata dataframe by group conditions. / 按条件筛选元数据"""
    result = df.copy()
    if f.country:
        result = result[result["country"].str.lower() == f.country.lower()]
    if f.disease:
        # Exact match on disease field
        # 疾病字段精确匹配
        result = result[result["disease"] == f.disease]
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
    diseases = sorted(meta["disease"].dropna().unique().tolist())

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

    return {
        "total_samples": int(len(meta)),
        "total_countries": int(meta["country"].nunique()),
        "total_diseases": int(meta["disease"].nunique()),
        "last_updated": version_info.get("last_updated", "2026-04-03"),
        "version": version_info.get("version", "v1.0_20260403"),
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
        # Statistical test / 统计检验（单次调用，复用结果计算效应量）
        u_stat = 0.0
        try:
            if req.method == "wilcoxon":
                mwu = stats.mannwhitneyu(vals_a, vals_b, alternative="two-sided")
                u_stat, p = float(mwu.statistic), float(mwu.pvalue)
            else:
                t_res = stats.ttest_ind(vals_a, vals_b)
                u_stat, p = float(t_res.statistic), float(t_res.pvalue)
        except Exception:
            p = 1.0

        # Effect size (rank-biserial correlation for MWU, Cohen's d for t-test)
        # 效应量（MWU的秩双列相关；t检验的Cohen's d）
        n_a, n_b = len(vals_a), len(vals_b)
        if req.method == "wilcoxon":
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
            "adjusted_p": 0.0,   # filled after BH / BH校正后填充
            "effect_size": effect_size,
        })

    # BH correction / BH多重校正
    adj_p = bh_correction(p_values)
    for i, row in enumerate(diff_results):
        row["adjusted_p"] = adj_p[i]

    # Sort by adjusted p-value / 按校正p值排序
    diff_results.sort(key=lambda x: x["adjusted_p"])

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

    return {
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
