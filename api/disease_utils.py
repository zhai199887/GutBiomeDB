from __future__ import annotations

import math
from typing import Sequence

import numpy as np
import pandas as pd
from scipy import stats

from compare_utils import (
    PSEUDOCOUNT,
    aggregate_by_level,
    bh_correction,
    extract_phylum,
    relative_abundance_matrix,
)


INVALID_GENERA = {
    "NA",
    "group",
    "Sedis",
    "Incertae",
    "unclassified",
    "uncultured",
    "Unknown",
    "unknown",
    "noname",
    "002",
    "sp",
}
INFORM_COLS = [f"inform{i}" for i in range(12)]
HEALTHY_CONTROL_ALIASES = {"nc", "healthy control", "helminth uninfected control"}
CONTROL_GROUP_REMAP_LABELS = {
    "Fecal occult blood positive;without severe underlying bowel disease",
}
CONTROL_GROUP_REMAP_LABELS_LOWER = {label.lower() for label in CONTROL_GROUP_REMAP_LABELS}
EXCLUDED_DISEASE_ALIASES = {"dss colitis"}


def is_valid_genus(name: str) -> bool:
    if not name or len(name) < 3:
        return False
    if name in INVALID_GENERA:
        return False
    if name[0].islower():
        return False
    if name.isdigit():
        return False
    return True


def normalize_inform_label(value: object) -> str:
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


def primary_condition_series(meta: pd.DataFrame) -> pd.Series:
    if "inform-all" not in meta.columns:
        return pd.Series("", index=meta.index, dtype=object)
    return meta["inform-all"].fillna("").astype(str).str.strip().map(normalize_inform_label)


def primary_condition_mask(meta: pd.DataFrame, label: str) -> pd.Series:
    normalized = normalize_inform_label(label)
    if not normalized:
        return pd.Series(False, index=meta.index, dtype=bool)
    primary = primary_condition_series(meta)
    return primary.str.lower() == normalized.lower()


def inform_label_mask(meta: pd.DataFrame, label: str) -> pd.Series:
    normalized = normalize_inform_label(label)
    if not normalized:
        return pd.Series(False, index=meta.index, dtype=bool)
    if normalized.lower() == "nc":
        return primary_condition_mask(meta, "NC")

    mask = pd.Series(False, index=meta.index, dtype=bool)
    for col in INFORM_COLS:
        if col not in meta.columns:
            continue
        values = meta[col].fillna("").astype(str).str.strip().map(normalize_inform_label)
        mask |= values.str.lower() == normalized.lower()
    return mask


def disease_mask(meta: pd.DataFrame, disease: str) -> pd.Series:
    return inform_label_mask(meta, disease)


def control_mask(meta: pd.DataFrame) -> pd.Series:
    return primary_condition_mask(meta, "NC")


def _matched_keys(meta: pd.DataFrame, abundance_df: pd.DataFrame) -> list[str]:
    abundance_index = set(str(idx) for idx in abundance_df.index)
    keys = []
    for key in meta["sample_key"].dropna().astype(str):
        if key in abundance_index and key not in keys:
            keys.append(key)
    return keys


def _matched_rows(meta: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    key_set = set(keys)
    return meta.loc[meta["sample_key"].astype(str).isin(key_set)].copy()


def matched_disease_control(
    meta: pd.DataFrame,
    abundance_df: pd.DataFrame,
    disease: str,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    disease_rows = meta.loc[disease_mask(meta, disease)].copy()
    control_rows = meta.loc[control_mask(meta)].copy()
    disease_keys = _matched_keys(disease_rows, abundance_df)
    control_keys = _matched_keys(control_rows, abundance_df)
    return (
        _matched_rows(disease_rows, disease_keys),
        _matched_rows(control_rows, control_keys),
        disease_keys,
        control_keys,
    )


def _dominant_country(meta: pd.DataFrame) -> str:
    if "iso" not in meta.columns or meta.empty:
        return ""
    values = (
        meta["iso"]
        .fillna("")
        .astype(str)
        .str.strip()
    )
    values = values[(values != "") & (values.str.lower() != "unknown")]
    if values.empty:
        return "unknown"
    return str(values.mode().iat[0])


def _top_counts(meta: pd.DataFrame, column: str, top_n: int | None = None) -> list[dict]:
    if column not in meta.columns:
        return []
    values = meta[column].fillna("").astype(str).str.strip()
    values = values[(values != "") & (values.str.lower() != "nan")]
    counts = values.value_counts()
    if top_n is not None:
        counts = counts.head(top_n)
    return [{"name": str(name), "count": int(count)} for name, count in counts.items()]


def log2fc_interval(disease_values: np.ndarray, control_values: np.ndarray) -> tuple[float, float]:
    mean_d = float(np.mean(disease_values))
    mean_c = float(np.mean(control_values))
    safe_d = max(mean_d, 0.0) + PSEUDOCOUNT
    safe_c = max(mean_c, 0.0) + PSEUDOCOUNT
    log2fc = math.log2(safe_d / safe_c)

    n_d = max(len(disease_values), 1)
    n_c = max(len(control_values), 1)
    var_d = float(np.var(disease_values, ddof=1)) if len(disease_values) > 1 else 0.0
    var_c = float(np.var(control_values, ddof=1)) if len(control_values) > 1 else 0.0
    se_ln = math.sqrt((var_d / (n_d * safe_d * safe_d)) + (var_c / (n_c * safe_c * safe_c)))
    se_log2 = se_ln / math.log(2)
    return (round(log2fc - 1.96 * se_log2, 6), round(log2fc + 1.96 * se_log2, 6))


def compute_genus_statistics(
    abundance_df: pd.DataFrame,
    disease_keys: Sequence[str],
    control_keys: Sequence[str],
) -> list[dict]:
    if not disease_keys or not control_keys:
        return []

    columns = abundance_df.columns.tolist()
    disease_raw = abundance_df.loc[list(disease_keys)].values.astype(float)
    control_raw = abundance_df.loc[list(control_keys)].values.astype(float)
    disease_rel = relative_abundance_matrix(disease_raw)
    control_rel = relative_abundance_matrix(control_raw)

    disease_agg, genera, phylum_map = aggregate_by_level(disease_rel, columns, "genus")
    control_agg, _, _ = aggregate_by_level(control_rel, columns, "genus")

    rows: list[dict] = []
    p_values: list[float] = []
    n_d = disease_agg.shape[0]
    n_c = control_agg.shape[0]

    for idx, genus in enumerate(genera):
        if not is_valid_genus(genus):
            continue

        disease_values = disease_agg[:, idx]
        control_values = control_agg[:, idx]
        mean_d = float(np.mean(disease_values))
        mean_c = float(np.mean(control_values))
        if mean_d == 0.0 and mean_c == 0.0:
            continue

        prevalence_d = float((disease_values > 0).mean()) if n_d > 0 else 0.0
        prevalence_c = float((control_values > 0).mean()) if n_c > 0 else 0.0

        try:
            mwu = stats.mannwhitneyu(disease_values, control_values, alternative="two-sided")
            u_stat = float(mwu.statistic)
            p_value = float(mwu.pvalue)
        except Exception:
            u_stat = 0.0
            p_value = 1.0

        log2fc = float(math.log2((mean_d + PSEUDOCOUNT) / (mean_c + PSEUDOCOUNT)))
        effect_size = float(1 - 2 * u_stat / (n_d * n_c)) if n_d and n_c else 0.0
        ci_low, ci_high = log2fc_interval(disease_values, control_values)

        p_values.append(p_value)
        rows.append(
            {
                "genus": genus,
                "phylum": phylum_map.get(genus, extract_phylum(genus)),
                "disease_mean": round(mean_d, 6),
                "disease_prevalence": round(prevalence_d, 4),
                "control_mean": round(mean_c, 6),
                "control_prevalence": round(prevalence_c, 4),
                "log2fc": round(log2fc, 6),
                "p_value": round(p_value, 8),
                "adjusted_p": 1.0,
                "effect_size": round(effect_size, 6),
                "enriched_in": "disease" if log2fc > 0 else ("control" if log2fc < 0 else "none"),
                "ci_low": ci_low,
                "ci_high": ci_high,
            }
        )

    adjusted = bh_correction(p_values)
    for idx, row in enumerate(rows):
        row["adjusted_p"] = round(float(adjusted[idx]), 8)

    rows.sort(key=lambda item: (item["adjusted_p"], -abs(item["log2fc"]), -item["disease_mean"]))
    return rows


def build_disease_profile(
    meta: pd.DataFrame,
    abundance_df: pd.DataFrame,
    disease: str,
    top_n: int = 40,
) -> dict:
    disease_rows, control_rows, disease_keys, control_keys = matched_disease_control(meta, abundance_df, disease)
    if not disease_keys:
        raise ValueError(f"Disease '{disease}' has no matched abundance samples")
    if not control_keys:
        raise ValueError("No matched NC control samples found")

    stats_rows = compute_genus_statistics(abundance_df, disease_keys, control_keys)
    top_genera = sorted(
        stats_rows,
        key=lambda item: (-item["disease_mean"], item["adjusted_p"], -abs(item["log2fc"])),
    )[:top_n]

    study_ids = sorted(str(value) for value in disease_rows["project"].dropna().astype(str).unique()) if "project" in disease_rows.columns else []
    return {
        "disease": disease,
        "sample_count": len(disease_keys),
        "control_count": len(control_keys),
        "n_studies": len(study_ids),
        "study_ids": study_ids,
        "top_genera": top_genera,
        "by_country": _top_counts(disease_rows, "iso", top_n=10),
        "by_age_group": _top_counts(disease_rows, "age_group"),
        "by_sex": _top_counts(disease_rows, "sex"),
    }


def build_lollipop_result(
    meta: pd.DataFrame,
    abundance_df: pd.DataFrame,
    disease: str,
    top_n: int = 120,
) -> dict:
    _, _, disease_keys, control_keys = matched_disease_control(meta, abundance_df, disease)
    if not disease_keys or not control_keys:
        raise ValueError("Insufficient disease or control samples")

    stats_rows = compute_genus_statistics(abundance_df, disease_keys, control_keys)
    rows = []
    for row in sorted(stats_rows, key=lambda item: (item["adjusted_p"], -abs(item["log2fc"]))):
        rows.append(
            {
                "genus": row["genus"],
                "phylum": row["phylum"],
                "log2fc": row["log2fc"],
                "neg_log10p": round(-math.log10(max(float(row["p_value"]), 1e-300)), 4),
                "p_value": row["p_value"],
                "adjusted_p": row["adjusted_p"],
                "mean_disease": row["disease_mean"],
                "mean_control": row["control_mean"],
                "prevalence_disease": row["disease_prevalence"],
                "prevalence_control": row["control_prevalence"],
            }
        )

    return {
        "disease": disease,
        "n_disease": len(disease_keys),
        "n_control": len(control_keys),
        "data": rows[:top_n],
    }


def build_disease_studies(
    meta: pd.DataFrame,
    abundance_df: pd.DataFrame,
    disease: str,
    min_per_group: int = 3,
    top_reference_markers: int = 12,
) -> dict:
    disease_rows, _, disease_keys, control_keys = matched_disease_control(meta, abundance_df, disease)
    if not disease_keys:
        raise ValueError(f"Disease '{disease}' not found")

    global_stats = compute_genus_statistics(abundance_df, disease_keys, control_keys)
    reference_markers = [row for row in global_stats if row["adjusted_p"] < 0.05][:top_reference_markers]
    if not reference_markers:
        reference_markers = global_stats[:top_reference_markers]
    reference_map = {row["genus"]: row for row in reference_markers}

    project_ids = (
        disease_rows["project"].dropna().astype(str).unique().tolist()
        if "project" in disease_rows.columns
        else []
    )
    projects = []

    for project_id in sorted(project_ids):
        project_meta = meta.loc[meta["project"].astype(str) == project_id].copy()
        project_disease_rows = project_meta.loc[disease_mask(project_meta, disease)].copy()
        project_control_rows = project_meta.loc[control_mask(project_meta)].copy()
        project_disease_keys = _matched_keys(project_disease_rows, abundance_df)
        project_control_keys = _matched_keys(project_control_rows, abundance_df)

        project_stats = []
        top_marker = ""
        cscs_score = 0.0
        if len(project_disease_keys) >= min_per_group and len(project_control_keys) >= min_per_group:
            project_stats = compute_genus_statistics(abundance_df, project_disease_keys, project_control_keys)
            if project_stats:
                top_marker = project_stats[0]["genus"]

            weighted_total = 0.0
            weighted_match = 0.0
            project_map = {row["genus"]: row for row in project_stats}
            for genus, ref_row in reference_map.items():
                proj_row = project_map.get(genus)
                if not proj_row:
                    continue
                weight = max(abs(float(ref_row["log2fc"])), 0.25)
                weighted_total += weight
                same_direction = (
                    (ref_row["log2fc"] > 0 and proj_row["log2fc"] > 0)
                    or (ref_row["log2fc"] < 0 and proj_row["log2fc"] < 0)
                    or (ref_row["log2fc"] == 0 and proj_row["log2fc"] == 0)
                )
                if same_direction:
                    weighted_match += weight
            if weighted_total > 0:
                cscs_score = round(weighted_match / weighted_total * 100.0, 1)

        country = _dominant_country(project_disease_rows if not project_disease_rows.empty else project_meta)
        projects.append(
            {
                "project_id": str(project_id),
                "n_disease": len(project_disease_keys),
                "n_control": len(project_control_keys),
                "country": country,
                "pmid": None,
                "cscs_score": cscs_score,
                "top_marker": top_marker,
            }
        )

    projects.sort(key=lambda item: (-item["n_disease"], -item["n_control"], item["project_id"]))
    return {
        "disease": disease,
        "n_projects": len(projects),
        "projects": projects,
    }
