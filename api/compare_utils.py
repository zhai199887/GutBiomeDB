import math
from typing import Sequence

import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import cdist


PSEUDOCOUNT = 1e-6
RANDOM_SEED = 42
MAX_NEG_LOG10_P = 320.0


def extract_genus(col_name: str) -> str:
    parts = str(col_name).split(".")
    return parts[-1].strip() if parts else str(col_name)


def extract_family(col_name: str) -> str:
    parts = str(col_name).split(".")
    if len(parts) > 4 and parts[4].strip():
        return parts[4].strip()
    return extract_genus(col_name)


def extract_phylum(col_name: str) -> str:
    parts = str(col_name).split(".")
    if len(parts) > 1 and parts[1].strip():
        return parts[1].strip()
    return str(col_name)


def relative_abundance_matrix(raw_matrix: np.ndarray) -> np.ndarray:
    matrix = np.asarray(raw_matrix, dtype=float)
    totals = matrix.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1.0
    return matrix / totals * 100.0


def aggregate_by_level(
    matrix: np.ndarray,
    columns: Sequence[str],
    taxonomy_level: str,
) -> tuple[np.ndarray, list[str], dict[str, str]]:
    if taxonomy_level not in {"genus", "family", "phylum"}:
        taxonomy_level = "genus"

    extractor = {
        "genus": extract_genus,
        "family": extract_family,
        "phylum": extract_phylum,
    }[taxonomy_level]

    labels = [extractor(col) or "Unknown" for col in columns]
    unique_labels = list(dict.fromkeys(labels))
    label_to_indices: dict[str, list[int]] = {label: [] for label in unique_labels}
    phylum_map: dict[str, str] = {}

    for idx, label in enumerate(labels):
        label_to_indices[label].append(idx)
        phylum_map.setdefault(label, extract_phylum(columns[idx]) or label)

    aggregated = np.zeros((matrix.shape[0], len(unique_labels)), dtype=float)
    for col_idx, label in enumerate(unique_labels):
        aggregated[:, col_idx] = matrix[:, label_to_indices[label]].sum(axis=1)

    if taxonomy_level == "phylum":
        phylum_map = {label: label for label in unique_labels}

    return aggregated, unique_labels, phylum_map


def shannon_diversity(row: np.ndarray) -> float:
    row = np.asarray(row, dtype=float)
    row = row[row > 0]
    if row.size == 0:
        return 0.0
    p = row / row.sum()
    return float(-np.sum(p * np.log(p)))


def simpson_diversity(row: np.ndarray) -> float:
    row = np.asarray(row, dtype=float)
    row = row[row > 0]
    if row.size == 0:
        return 0.0
    p = row / row.sum()
    return float(1 - np.sum(p ** 2))


def chao1_richness(row: np.ndarray) -> float:
    counts = np.asarray(np.rint(row), dtype=int)
    counts = counts[counts > 0]
    if counts.size == 0:
        return 0.0

    observed = float(counts.size)
    singletons = float(np.sum(counts == 1))
    doubletons = float(np.sum(counts == 2))

    if doubletons == 0:
        return observed + (singletons * (singletons - 1.0)) / 2.0
    return observed + (singletons * singletons) / (2.0 * doubletons)


def bh_correction(p_values: Sequence[float]) -> list[float]:
    n = len(p_values)
    if n == 0:
        return []
    indexed = sorted(enumerate(p_values), key=lambda item: item[1])
    adjusted = [0.0] * n
    prev_adj = 1.0
    for rank, (orig_idx, p_value) in enumerate(reversed(indexed)):
        adj = min(prev_adj, p_value * n / (n - rank))
        adjusted[orig_idx] = min(adj, 1.0)
        prev_adj = adj
    return adjusted


def _neg_log10_from_p_value(p_value: float) -> float:
    p_value = float(np.nan_to_num(p_value, nan=1.0, posinf=1.0, neginf=1.0))
    if p_value <= 0:
        return MAX_NEG_LOG10_P
    return min(MAX_NEG_LOG10_P, max(0.0, -math.log10(p_value)))


def _stable_p_value(p_value: float, neg_log10_p: float) -> float:
    p_value = float(np.nan_to_num(p_value, nan=1.0, posinf=1.0, neginf=1.0))
    if 0 < p_value <= 1.0:
        return p_value
    return 10 ** (-min(MAX_NEG_LOG10_P, max(0.0, neg_log10_p)))


def _wilcoxon_neg_log10_p(a: Sequence[float], b: Sequence[float], p_value: float) -> float:
    if p_value > 0:
        return _neg_log10_from_p_value(p_value)
    try:
        z_stat = float(stats.ranksums(a, b).statistic)
        log10_p = math.log10(2.0) + stats.norm.logsf(abs(z_stat)) / math.log(10.0)
        return min(MAX_NEG_LOG10_P, max(0.0, -float(log10_p)))
    except Exception:
        return MAX_NEG_LOG10_P


def _adaptive_round_p_value(p_value: float) -> float:
    if p_value >= 1e-4:
        return round(p_value, 6)
    return p_value


def _bh_neg_log10(neg_log10_values: Sequence[float]) -> list[float]:
    n = len(neg_log10_values)
    if n == 0:
        return []
    indexed = sorted(enumerate(neg_log10_values), key=lambda item: item[1], reverse=True)
    adjusted = [0.0] * n
    prev_adj = 0.0
    for pos in range(n - 1, -1, -1):
        orig_idx, neg_log10_p = indexed[pos]
        rank = pos + 1
        scaled = max(0.0, neg_log10_p - math.log10(n / rank))
        current = max(prev_adj, scaled)
        adjusted[orig_idx] = min(MAX_NEG_LOG10_P, current)
        prev_adj = current
    return adjusted


def _safe_mannwhitney(a: Sequence[float], b: Sequence[float]) -> tuple[float, float]:
    try:
        result = stats.mannwhitneyu(a, b, alternative="two-sided")
        return float(result.statistic), float(result.pvalue)
    except Exception:
        return 0.0, 1.0


def _safe_ttest(a: Sequence[float], b: Sequence[float]) -> tuple[float, float]:
    try:
        result = stats.ttest_ind(a, b, equal_var=False)
        return float(result.statistic), float(result.pvalue)
    except Exception:
        return 0.0, 1.0


def _diversity_p_value(a: Sequence[float], b: Sequence[float]) -> float:
    _, p_value = _safe_mannwhitney(a, b)
    return round(float(p_value), 6)


def _classical_mds(distance_matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    n_points = distance_matrix.shape[0]
    centering = np.eye(n_points) - np.ones((n_points, n_points)) / n_points
    gram = -0.5 * centering @ (distance_matrix ** 2) @ centering

    eigenvalues, eigenvectors = np.linalg.eigh(gram)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = eigenvectors[:, order]
    positive = np.clip(eigenvalues, a_min=0.0, a_max=None)

    coords = np.zeros((n_points, 2), dtype=float)
    if positive.size:
        components = min(2, eigenvectors.shape[1])
        coords[:, :components] = eigenvectors[:, :components] * np.sqrt(positive[:components])
    return coords, positive


def _clr_transform(matrix: np.ndarray, pseudocount: float = 1.0) -> np.ndarray:
    adjusted = np.asarray(matrix, dtype=float) + pseudocount
    log_matrix = np.log(adjusted)
    return log_matrix - log_matrix.mean(axis=1, keepdims=True)


def _compute_pcoa(
    matrix_a: np.ndarray,
    matrix_b: np.ndarray,
    metric: str,
    max_samples: int,
) -> dict:
    rng = np.random.default_rng(RANDOM_SEED)
    sampled_a = np.asarray(matrix_a, dtype=float)
    sampled_b = np.asarray(matrix_b, dtype=float)

    if len(sampled_a) > max_samples:
        sampled_a = sampled_a[rng.choice(len(sampled_a), max_samples, replace=False)]
    if len(sampled_b) > max_samples:
        sampled_b = sampled_b[rng.choice(len(sampled_b), max_samples, replace=False)]

    combined = np.vstack([sampled_a, sampled_b])
    n_a = len(sampled_a)

    if metric == "aitchison":
        transformed = _clr_transform(combined)
        distance_matrix = cdist(transformed, transformed, metric="euclidean")
    else:
        metric = "braycurtis"
        distance_matrix = cdist(combined, combined, metric="braycurtis")

    distance_matrix = np.nan_to_num(distance_matrix)
    coords, eigenvalues = _classical_mds(distance_matrix)
    total_positive = float(np.sum(eigenvalues[eigenvalues > 0]))

    if total_positive > 0:
        variance = [
            round(float(eigenvalues[0] / total_positive * 100), 2) if len(eigenvalues) > 0 else 0.0,
            round(float(eigenvalues[1] / total_positive * 100), 2) if len(eigenvalues) > 1 else 0.0,
        ]
    else:
        variance = [0.0, 0.0]

    pcoa_coords = []
    for idx, coord in enumerate(coords):
        pcoa_coords.append(
            {
                "x": round(float(coord[0]), 6),
                "y": round(float(coord[1]), 6),
                "group": "A" if idx < n_a else "B",
            }
        )

    return {
        "metric": metric,
        "variance_explained": variance,
        "pcoa_coords": pcoa_coords,
    }


def build_phylum_composition(
    rel_a: np.ndarray,
    rel_b: np.ndarray,
    columns: Sequence[str],
    group_a_name: str,
    group_b_name: str,
    top_n: int = 8,
) -> dict:
    phylum_a, phyla, _ = aggregate_by_level(rel_a, columns, "phylum")
    phylum_b, _, _ = aggregate_by_level(rel_b, columns, "phylum")

    mean_a = phylum_a.mean(axis=0)
    mean_b = phylum_b.mean(axis=0)
    ranking = sorted(
        range(len(phyla)),
        key=lambda idx: max(mean_a[idx], mean_b[idx]),
        reverse=True,
    )

    chosen = ranking[:top_n]
    rows = []
    used_a = 0.0
    used_b = 0.0
    for idx in chosen:
        a_value = float(mean_a[idx])
        b_value = float(mean_b[idx])
        rows.append(
            {
                "phylum": phyla[idx],
                "group_a": round(a_value, 4),
                "group_b": round(b_value, 4),
            }
        )
        used_a += a_value
        used_b += b_value

    other_a = max(0.0, 100.0 - used_a)
    other_b = max(0.0, 100.0 - used_b)
    if other_a > 0.01 or other_b > 0.01:
        rows.append(
            {
                "phylum": "Other",
                "group_a": round(other_a, 4),
                "group_b": round(other_b, 4),
            }
        )

    return {
        "groups": [group_a_name, group_b_name],
        "rows": rows,
    }


def lefse_analysis(
    agg_a: np.ndarray,
    agg_b: np.ndarray,
    taxa: Sequence[str],
    lda_threshold: float = 2.0,
    p_threshold: float = 0.05,
) -> list[dict]:
    results: list[dict] = []
    for idx, taxon in enumerate(taxa):
        vals_a = agg_a[:, idx]
        vals_b = agg_b[:, idx]
        if np.std(vals_a) == 0 and np.std(vals_b) == 0:
            continue

        try:
            _, p_value = stats.kruskal(vals_a, vals_b)
        except Exception:
            continue

        if p_value >= p_threshold:
            continue

        mean_a = float(np.mean(vals_a))
        mean_b = float(np.mean(vals_b))
        grand_mean = float(np.mean(np.concatenate([vals_a, vals_b])))
        n_a, n_b = len(vals_a), len(vals_b)
        between_var = (n_a * (mean_a - grand_mean) ** 2 + n_b * (mean_b - grand_mean) ** 2) / (n_a + n_b)
        within_var = (n_a * float(np.var(vals_a)) + n_b * float(np.var(vals_b))) / (n_a + n_b)

        if within_var > 0:
            lda_score = math.log10(1 + abs(between_var / within_var) * abs(mean_a - mean_b) * 1e6)
        else:
            lda_score = math.log10(1 + abs(mean_a - mean_b) * 1e6)

        if lda_score < lda_threshold:
            continue

        results.append(
            {
                "taxon": str(taxon),
                "lda_score": round(float(lda_score), 4),
                "p_value": round(float(p_value), 6),
                "enriched_group": "A" if mean_a > mean_b else "B",
            }
        )

    results.sort(key=lambda item: item["lda_score"], reverse=True)
    return results[:100]


def permanova_test(
    agg_a: np.ndarray,
    agg_b: np.ndarray,
    n_permutations: int = 999,
    max_samples: int = 300,
) -> dict:
    rng = np.random.default_rng(RANDOM_SEED)
    sampled_a = np.asarray(agg_a, dtype=float)
    sampled_b = np.asarray(agg_b, dtype=float)

    if len(sampled_a) > max_samples:
        sampled_a = sampled_a[rng.choice(len(sampled_a), max_samples, replace=False)]
    if len(sampled_b) > max_samples:
        sampled_b = sampled_b[rng.choice(len(sampled_b), max_samples, replace=False)]

    n_a = len(sampled_a)
    n_b = len(sampled_b)
    combined = np.vstack([sampled_a, sampled_b])
    labels = np.array([0] * n_a + [1] * n_b)
    distance_matrix = np.nan_to_num(cdist(combined, combined, metric="braycurtis"))

    def calc_pseudo_f(distances: np.ndarray, group_labels: np.ndarray) -> tuple[float, float]:
        n_total = len(group_labels)
        groups = np.unique(group_labels)
        k = len(groups)
        ss_total = np.sum(distances ** 2) / (2 * n_total)
        ss_within = 0.0
        for group in groups:
            mask = group_labels == group
            n_group = int(np.sum(mask))
            if n_group > 1:
                sub = distances[np.ix_(mask, mask)]
                ss_within += np.sum(sub ** 2) / (2 * n_group)
        ss_between = ss_total - ss_within
        df_between = k - 1
        df_within = n_total - k
        if df_within <= 0 or ss_within == 0:
            return 0.0, 0.0
        f_stat = (ss_between / df_between) / (ss_within / df_within)
        r_squared = ss_between / ss_total if ss_total > 0 else 0.0
        return float(f_stat), float(r_squared)

    observed_f, r_squared = calc_pseudo_f(distance_matrix, labels)
    count_ge = 0
    for _ in range(n_permutations):
        perm_labels = rng.permutation(labels)
        perm_f, _ = calc_pseudo_f(distance_matrix, perm_labels)
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


def run_compare_analysis(
    abundance_df: pd.DataFrame,
    valid_a: Sequence[str],
    valid_b: Sequence[str],
    taxonomy_level: str,
    method: str,
    group_a_name: str,
    group_b_name: str,
    max_diff_taxa: int | None = 250,
) -> dict:
    columns = abundance_df.columns.tolist()
    raw_a = abundance_df.loc[list(valid_a)].values.astype(float)
    raw_b = abundance_df.loc[list(valid_b)].values.astype(float)

    rel_a = relative_abundance_matrix(raw_a)
    rel_b = relative_abundance_matrix(raw_b)

    rel_agg_a, taxa, phylum_map = aggregate_by_level(rel_a, columns, taxonomy_level)
    rel_agg_b, _, _ = aggregate_by_level(rel_b, columns, taxonomy_level)
    raw_agg_a, _, _ = aggregate_by_level(raw_a, columns, taxonomy_level)
    raw_agg_b, _, _ = aggregate_by_level(raw_b, columns, taxonomy_level)

    base_method = method if method in {"wilcoxon", "t-test"} else "wilcoxon"
    diff_results: list[dict] = []
    p_values: list[float] = []
    neg_log10_p_values: list[float] = []

    for idx, taxon in enumerate(taxa):
        vals_a = rel_agg_a[:, idx]
        vals_b = rel_agg_b[:, idx]
        mean_a = float(np.mean(vals_a))
        mean_b = float(np.mean(vals_b))
        log2fc = math.log2((mean_a + PSEUDOCOUNT) / (mean_b + PSEUDOCOUNT))
        prevalence_a = float(np.mean(vals_a > 0))
        prevalence_b = float(np.mean(vals_b > 0))

        if base_method == "wilcoxon":
            stat, p_value = _safe_mannwhitney(vals_a, vals_b)
            effect_size = float(1 - 2 * stat / (len(vals_a) * len(vals_b))) if len(vals_a) * len(vals_b) > 0 else 0.0
            neg_log10_p = _wilcoxon_neg_log10_p(vals_a, vals_b, p_value)
        else:
            stat, p_value = _safe_ttest(vals_a, vals_b)
            pooled_std = float(np.std(np.concatenate([vals_a, vals_b]), ddof=0))
            effect_size = float((mean_a - mean_b) / pooled_std) if pooled_std > 0 else 0.0
            neg_log10_p = _neg_log10_from_p_value(p_value)

        stable_p_value = _stable_p_value(p_value, neg_log10_p)
        p_values.append(stable_p_value)
        neg_log10_p_values.append(neg_log10_p)
        diff_results.append(
            {
                "taxon": str(taxon),
                "phylum": phylum_map.get(taxon, taxon),
                "tax_level": taxonomy_level,
                "mean_a": round(mean_a, 6),
                "mean_b": round(mean_b, 6),
                "prevalence_a": round(prevalence_a, 4),
                "prevalence_b": round(prevalence_b, 4),
                "log2fc": round(log2fc, 6),
                "p_value": _adaptive_round_p_value(stable_p_value),
                "neg_log10_p": round(neg_log10_p, 4),
                "adjusted_p": 1.0,
                "neg_log10_adjusted_p": 0.0,
                "effect_size": round(effect_size, 6),
                "enriched_in": "A" if mean_a > mean_b else "B",
            }
        )

    adjusted = bh_correction(p_values)
    adjusted_neg_log10 = _bh_neg_log10(neg_log10_p_values)
    for idx, row in enumerate(diff_results):
        stable_adjusted_p = _stable_p_value(adjusted[idx], adjusted_neg_log10[idx])
        row["adjusted_p"] = _adaptive_round_p_value(stable_adjusted_p)
        row["neg_log10_adjusted_p"] = round(adjusted_neg_log10[idx], 4)

    diff_results.sort(key=lambda item: (item["adjusted_p"], -abs(item["effect_size"])))

    shannon_a = [round(shannon_diversity(row), 6) for row in raw_agg_a]
    shannon_b = [round(shannon_diversity(row), 6) for row in raw_agg_b]
    simpson_a = [round(simpson_diversity(row), 6) for row in raw_agg_a]
    simpson_b = [round(simpson_diversity(row), 6) for row in raw_agg_b]
    chao1_a = [round(chao1_richness(row), 6) for row in raw_agg_a]
    chao1_b = [round(chao1_richness(row), 6) for row in raw_agg_b]

    alpha_diversity = {
        "group_a": {
            "shannon": shannon_a[:500],
            "simpson": simpson_a[:500],
            "chao1": chao1_a[:500],
        },
        "group_b": {
            "shannon": shannon_b[:500],
            "simpson": simpson_b[:500],
            "chao1": chao1_b[:500],
        },
    }

    alpha_pvalues = {
        "shannon": _diversity_p_value(shannon_a, shannon_b),
        "simpson": _diversity_p_value(simpson_a, simpson_b),
        "chao1": _diversity_p_value(chao1_a, chao1_b),
    }

    beta_diversity = {
        "default_metric": "braycurtis",
        "metrics": {
            "braycurtis": _compute_pcoa(rel_agg_a, rel_agg_b, "braycurtis", max_samples=150),
            "aitchison": _compute_pcoa(raw_agg_a, raw_agg_b, "aitchison", max_samples=150),
        },
    }

    response = {
        "summary": {
            "group_a_name": group_a_name,
            "group_b_name": group_b_name,
            "group_a_n": len(valid_a),
            "group_b_n": len(valid_b),
            "taxonomy_level": taxonomy_level,
            "method": method,
            "total_taxa": len(taxa),
            "significant_taxa": sum(1 for row in diff_results if row["adjusted_p"] < 0.05),
        },
        "diff_taxa": diff_results if max_diff_taxa is None else diff_results[:max_diff_taxa],
        "alpha_diversity": alpha_diversity,
        "alpha_pvalues": alpha_pvalues,
        "beta_diversity": beta_diversity,
        "phylum_composition": build_phylum_composition(rel_a, rel_b, columns, group_a_name, group_b_name),
    }

    if method == "lefse":
        response["lefse_results"] = lefse_analysis(rel_agg_a, rel_agg_b, taxa)
    if method == "permanova":
        response["permanova"] = permanova_test(rel_agg_a, rel_agg_b)

    return response


def run_spearman_analysis(
    abundance_df: pd.DataFrame,
    sample_keys: Sequence[str],
    taxonomy_level: str,
    max_taxa: int = 18,
) -> dict:
    if not sample_keys:
        return {
            "summary": {"sample_count": 0, "taxonomy_level": taxonomy_level, "max_taxa": max_taxa},
            "taxa": [],
            "matrix": [],
            "p_values": [],
            "edges": [],
        }

    raw = abundance_df.loc[list(sample_keys)].values.astype(float)
    rel = relative_abundance_matrix(raw)
    agg, taxa, phylum_map = aggregate_by_level(rel, abundance_df.columns.tolist(), taxonomy_level)

    means = agg.mean(axis=0)
    prevalence = (agg > 0).mean(axis=0)
    scores = means * prevalence
    top_n = min(max_taxa, len(taxa))
    top_indices = np.argsort(scores)[::-1][:top_n]
    selected_taxa = [taxa[idx] for idx in top_indices]
    selected_phyla = [phylum_map[taxa[idx]] for idx in top_indices]
    selected = agg[:, top_indices]

    matrix = np.eye(top_n, dtype=float)
    p_values = np.zeros((top_n, top_n), dtype=float)
    edges: list[dict] = []

    for i in range(top_n):
        for j in range(i + 1, top_n):
            try:
                r_value, p_value = stats.spearmanr(selected[:, i], selected[:, j])
                r_value = float(np.nan_to_num(r_value))
                p_value = float(np.nan_to_num(p_value, nan=1.0))
            except Exception:
                r_value, p_value = 0.0, 1.0
            matrix[i, j] = matrix[j, i] = r_value
            p_values[i, j] = p_values[j, i] = p_value
            if abs(r_value) >= 0.3 and p_value < 0.05:
                edges.append(
                    {
                        "source": selected_taxa[i],
                        "target": selected_taxa[j],
                        "source_phylum": selected_phyla[i],
                        "target_phylum": selected_phyla[j],
                        "r": round(r_value, 4),
                        "p_value": round(p_value, 6),
                        "type": "positive" if r_value > 0 else "negative",
                    }
                )

    edges.sort(key=lambda item: abs(item["r"]), reverse=True)
    return {
        "summary": {
            "sample_count": len(sample_keys),
            "taxonomy_level": taxonomy_level,
            "max_taxa": top_n,
        },
        "taxa": [
            {
                "taxon": taxon,
                "phylum": selected_phyla[idx],
                "mean_abundance": round(float(selected[:, idx].mean()), 6),
                "prevalence": round(float((selected[:, idx] > 0).mean()), 4),
            }
            for idx, taxon in enumerate(selected_taxa)
        ],
        "matrix": np.round(matrix, 4).tolist(),
        "p_values": np.round(p_values, 6).tolist(),
        "edges": edges[:150],
    }
