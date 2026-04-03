"""
analysis.py — Statistical analysis utilities for Gut Microbiome Atlas
统计分析工具：标志物检验、相关性计算、相似性搜索
"""
import math
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import cdist, braycurtis


def wilcoxon_marker_test(
    disease_matrix: np.ndarray,
    control_matrix: np.ndarray,
    taxa_names: list[str],
    p_threshold: float = 0.05,
) -> list[dict]:
    """
    Wilcoxon rank-sum test + BH FDR correction for biomarker discovery.
    Wilcoxon 秩和检验 + BH FDR 校正用于标志物发现
    """
    n_taxa = len(taxa_names)
    results = []
    p_values = []

    for i in range(n_taxa):
        d_vals = disease_matrix[:, i]
        c_vals = control_matrix[:, i]

        mean_d = float(np.mean(d_vals))
        mean_c = float(np.mean(c_vals))

        if np.std(d_vals) == 0 and np.std(c_vals) == 0:
            p_values.append(1.0)
            results.append(None)
            continue

        try:
            stat, p = stats.mannwhitneyu(d_vals, c_vals, alternative="two-sided")
        except Exception:
            p_values.append(1.0)
            results.append(None)
            continue

        pseudo = 1e-6
        log2fc = math.log2((mean_d + pseudo) / (mean_c + pseudo))

        grand_mean = float(np.mean(np.concatenate([d_vals, c_vals])))
        n_d, n_c = len(d_vals), len(c_vals)
        between_var = (n_d * (mean_d - grand_mean) ** 2 +
                       n_c * (mean_c - grand_mean) ** 2) / (n_d + n_c)
        within_var = (n_d * float(np.var(d_vals)) +
                      n_c * float(np.var(c_vals))) / (n_d + n_c)

        if within_var > 0:
            lda_score = math.log10(1 + abs(between_var / within_var) * abs(mean_d - mean_c) * 1e6)
        else:
            lda_score = math.log10(1 + abs(mean_d - mean_c) * 1e6)

        prev_d = float((d_vals > 0).sum() / n_d) if n_d > 0 else 0
        prev_c = float((c_vals > 0).sum() / n_c) if n_c > 0 else 0

        diff = mean_d - mean_c
        se = float(np.sqrt(np.var(d_vals) / n_d + np.var(c_vals) / n_c))
        ci_low = diff - 1.96 * se
        ci_high = diff + 1.96 * se

        p_values.append(float(p))
        results.append({
            "taxon": taxa_names[i],
            "mean_disease": round(mean_d, 6),
            "mean_control": round(mean_c, 6),
            "log2fc": round(log2fc, 4),
            "lda_score": round(lda_score, 4),
            "p_value": round(float(p), 8),
            "adjusted_p": 0.0,
            "prevalence_disease": round(prev_d, 4),
            "prevalence_control": round(prev_c, 4),
            "enriched_in": "disease" if mean_d > mean_c else "control",
            "ci_low": round(ci_low, 6),
            "ci_high": round(ci_high, 6),
        })

    adj_p = _bh_correction(p_values)
    significant = []
    for i, r in enumerate(results):
        if r is not None:
            r["adjusted_p"] = round(adj_p[i], 8)
            if adj_p[i] < p_threshold:
                significant.append(r)

    significant.sort(key=lambda x: x["lda_score"], reverse=True)
    return significant


def spearman_cooccurrence(
    abundance_matrix: np.ndarray,
    taxa_names: list[str],
    min_prevalence: float = 0.1,
    min_abs_r: float = 0.3,
    max_pairs: int = 500,
) -> list[dict]:
    """
    Spearman correlation-based co-occurrence network.
    基于 Spearman 相关性的共现网络
    """
    n_samples, n_taxa = abundance_matrix.shape

    prevalences = (abundance_matrix > 0).sum(axis=0) / n_samples
    keep = prevalences >= min_prevalence
    filtered_matrix = abundance_matrix[:, keep]
    filtered_names = [taxa_names[i] for i in range(n_taxa) if keep[i]]
    n_filtered = len(filtered_names)

    if n_filtered < 2:
        return []

    edges = []
    for i in range(n_filtered):
        for j in range(i + 1, n_filtered):
            r, p = stats.spearmanr(filtered_matrix[:, i], filtered_matrix[:, j])
            if abs(r) >= min_abs_r and p < 0.05:
                edges.append({
                    "source": filtered_names[i],
                    "target": filtered_names[j],
                    "r": round(float(r), 4),
                    "p_value": round(float(p), 6),
                    "type": "positive" if r > 0 else "negative",
                })

    edges.sort(key=lambda x: abs(x["r"]), reverse=True)
    return edges[:max_pairs]


def sample_similarity_search(
    query_vector: np.ndarray,
    abundance_matrix: np.ndarray,
    sample_keys: list[str],
    metric: str = "braycurtis",
    top_k: int = 10,
) -> list[dict]:
    """
    Find top-K most similar samples to a query abundance vector.
    查找与查询丰度向量最相似的 Top-K 样本
    """
    q_total = query_vector.sum()
    if q_total > 0:
        query_rel = query_vector / q_total * 100
    else:
        return []

    ref_totals = abundance_matrix.sum(axis=1, keepdims=True)
    ref_totals[ref_totals == 0] = 1
    ref_rel = abundance_matrix / ref_totals * 100

    distances = cdist(query_rel.reshape(1, -1), ref_rel, metric=metric)[0]

    top_indices = np.argsort(distances)[:top_k]
    results = []
    for idx in top_indices:
        results.append({
            "sample_key": sample_keys[idx],
            "distance": round(float(distances[idx]), 6),
            "similarity": round(float(1 - distances[idx]), 6),
        })
    return results


def _bh_correction(p_values: list[float]) -> list[float]:
    """Benjamini-Hochberg FDR correction."""
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
