# -*- coding: utf-8 -*-
"""
Process metadata CSV and export frontend summary JSON files.

Input:  D:\\**\\result_with_age_sex_with_age_group_meta.csv
Output: public/data/metadata.json
        public/data/metadata_summary.json
"""

from __future__ import annotations

import glob
import json
from collections import Counter
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "public" / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)
METADATA_OUT = OUT_DIR / "metadata.json"
SUMMARY_OUT = OUT_DIR / "metadata_summary.json"
ABUNDANCE_PATH = ROOT.parent / "data" / "unfiltered_abundance.csv"

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
INFORM_COLS = [f"inform{i}" for i in range(12)]


def locate_source_csv() -> Path:
    matches = sorted(glob.glob(r"D:\**\result_with_age_sex_with_age_group_meta.csv", recursive=True))
    if not matches:
        raise FileNotFoundError("Cannot find result_with_age_sex_with_age_group_meta.csv under D:\\")
    return Path(matches[0])


def read_csv_with_fallbacks(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "gbk", "latin1"):
        try:
            return pd.read_csv(path, encoding=encoding, low_memory=False)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return pd.read_csv(path, low_memory=False)


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


def label_kind(label: object) -> str:
    normalized = normalize_inform_label(label)
    if not normalized:
        return "unknown"
    if normalized.lower() == "nc":
        return "healthy_control"
    if normalized.lower() in SPECIAL_POPULATION_LABELS_LOWER:
        return "special_population"
    return "disease"


def count_unique_projects(df: pd.DataFrame) -> int:
    for column in ("project", "BioProject"):
        if column in df.columns:
            values = df[column].fillna("").astype(str).str.strip()
            values = values[values != ""]
            return int(values.nunique())
    return 0


def count_unique_genera() -> int:
    if not ABUNDANCE_PATH.exists():
        return 0
    columns = pd.read_csv(ABUNDANCE_PATH, nrows=0).columns.tolist()
    if not columns:
        return 0
    head = columns[0].strip().lower()
    data_columns = columns[1:] if head in {"sample_id", "sampleid", "sample", "rownames"} else columns
    return len(data_columns)


def apply_control_group_remap(df: pd.DataFrame) -> None:
    if "inform-all" not in df.columns:
        return
    inform_all = df["inform-all"].fillna("").astype(str).str.strip()
    control_mask = inform_all.str.lower().isin(CONTROL_GROUP_REMAP_LABELS_LOWER)
    if not control_mask.any():
        return
    df.loc[control_mask, "inform-all"] = "NC"
    for col in INFORM_COLS:
        if col in df.columns:
            df.loc[control_mask, col] = ""


def strict_nc_mask(df: pd.DataFrame) -> pd.Series:
    if "inform-all" not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    inform_all = df["inform-all"].fillna("").astype(str).str.strip().map(normalize_inform_label)
    return inform_all == "NC"


def row_inform_labels(row: pd.Series) -> list[str]:
    if normalize_inform_label(row.get("inform-all", "")) == "NC":
        return ["NC"]
    labels: list[str] = []
    for col in INFORM_COLS:
        label = normalize_inform_label(row.get(col, ""))
        if label:
            labels.append(label)
    return labels


def standardize_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna("").astype(str).str.strip()

    if "sex" in df.columns:
        sex = df["sex"].fillna("").astype(str).str.strip().str.lower()
        sex = sex.replace("", "unknown")
        sex = sex.where(sex.isin(["male", "female", "unknown"]), "unknown")
        df["sex"] = sex

    if "iso" in df.columns:
        iso = df["iso"].fillna("").astype(str).str.strip().replace("", "unknown")
        iso = iso.replace({"TW": "CN", "HK": "CN", "MO": "CN"})
        df["iso"] = iso
        df["country"] = iso
    else:
        df["country"] = "unknown"

    for col in INFORM_COLS + ["inform-all"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    apply_control_group_remap(df)

    fill_mask = df["inform-all"].ne("") & df["inform0"].eq("")
    if fill_mask.any():
        df.loc[fill_mask, "inform0"] = df.loc[fill_mask, "inform-all"]

    for col in INFORM_COLS + ["inform-all"]:
        df[col] = df[col].map(normalize_inform_label)

    df["disease"] = df["inform-all"].where(df["inform-all"] != "", "unknown")
    return df


def build_union_counter(df: pd.DataFrame) -> Counter:
    counter: Counter = Counter()
    for _, row in df.iterrows():
        counter.update(row_inform_labels(row))
    return counter


def build_age_disease_cross(df: pd.DataFrame, top_diseases: list[str]) -> list[dict]:
    rows: list[pd.DataFrame] = []
    for col in INFORM_COLS:
        if col not in df.columns:
            continue
        sub = df.loc[df[col].isin(top_diseases), ["age_group", col]].copy()
        if sub.empty:
            continue
        sub = sub.rename(columns={col: "disease"})
        rows.append(sub)
    if not rows:
        return []
    merged = pd.concat(rows, ignore_index=True)
    return (
        merged.groupby(["age_group", "disease"])
        .size()
        .reset_index(name="count")
        .to_dict(orient="records")
    )


def build_country_stats(df: pd.DataFrame) -> dict[str, dict]:
    country_stats: dict[str, dict] = {}
    top_countries = df["country"].value_counts().head(60).index.tolist()
    for iso in top_countries:
        sub = df[df["country"] == iso].copy()
        total = len(sub)
        if total == 0:
            continue

        male_n = int((sub["sex"] == "male").sum())
        female_n = int((sub["sex"] == "female").sum())
        known = male_n + female_n

        top_ages = (
            sub[sub["age_group"] != "Unknown"]["age_group"]
            .value_counts()
            .head(3)
            .to_dict()
        )

        disease_counts: Counter = Counter()
        for _, row in sub.iterrows():
            for value in row_inform_labels(row):
                if label_kind(value) == "disease":
                    disease_counts[value] += 1

        country_stats[str(iso)] = {
            "total": total,
            "sex": {
                "female_pct": round(female_n / known * 100) if known > 0 else None,
                "male_pct": round(male_n / known * 100) if known > 0 else None,
                "known": known,
            },
            "top_ages": top_ages,
            "top_diseases": dict(disease_counts.most_common(3)),
        }

    return country_stats


def main() -> None:
    csv_path = locate_source_csv()
    print(f"CSV path: {csv_path}")
    print("Loading CSV...")
    df_raw = read_csv_with_fallbacks(csv_path)
    print(f"  Loaded {len(df_raw):,} rows x {len(df_raw.columns)} columns")

    df = standardize_metadata(df_raw)

    keep = {
        "srr": "sample_id",
        "country": "country",
        "region": "region",
        "project": "project",
        "age_group": "age_group",
        "sex": "sex",
        "disease": "disease",
    }
    export_frame = pd.DataFrame()
    for source_col, target_col in keep.items():
        export_frame[target_col] = df[source_col] if source_col in df.columns else ""

    print("Writing metadata.json...")
    records = export_frame.to_dict(orient="records")
    with METADATA_OUT.open("w", encoding="utf-8") as handle:
        json.dump(records, handle, ensure_ascii=False, separators=(",", ":"))
    print(f"  Written {len(records):,} records")

    print("Building metadata_summary.json...")
    sex_counts = {str(k): int(v) for k, v in df["sex"].value_counts(dropna=False).items()}
    age_counts = {str(k): int(v) for k, v in df["age_group"].value_counts(dropna=False).items()}
    country_counts = {str(k): int(v) for k, v in df["country"].value_counts(dropna=False).items()}
    region_counts = {str(k): int(v) for k, v in df["region"].value_counts(dropna=False).items()} if "region" in df.columns else {}

    inform0_counter = Counter(label for label in df["inform0"].tolist() if label)
    union_counter = build_union_counter(df)
    unique_labels = set(union_counter)
    non_nc_condition_labels = {
        label for label in unique_labels if label_kind(label) != "healthy_control"
    }
    has_nc_category = any(label_kind(label) == "healthy_control" for label in unique_labels)
    standard_disease_counts = Counter({
        label: count for label, count in union_counter.items() if label_kind(label) == "disease"
    })
    special_population_counts = Counter({
        label: count for label, count in union_counter.items() if label_kind(label) == "special_population"
    })
    healthy_control_counts = Counter({
        label: count for label, count in union_counter.items() if label_kind(label) == "healthy_control"
    })

    top20_diseases = [
        label
        for label, _ in sorted(standard_disease_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:20]
    ]

    summary = {
        "total_samples": int(len(df)),
        "total_projects": count_unique_projects(df),
        "total_genera": count_unique_genera(),
        "total_unique_diseases": int(len(non_nc_condition_labels)),
        "total_non_nc_condition_labels": int(len(non_nc_condition_labels)),
        "total_condition_categories": int(len(non_nc_condition_labels) + int(has_nc_category)),
        "total_unique_countries": int(df.loc[df["country"] != "unknown", "country"].nunique()),
        "age_counts": age_counts,
        "sex_counts": sex_counts,
        "disease_counts": dict(sorted(standard_disease_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:50]),
        "country_counts": country_counts,
        "region_counts": region_counts,
        "age_sex_cross": (
            df.groupby(["age_group", "sex"])
            .size()
            .reset_index(name="count")
            .to_dict(orient="records")
        ),
        "age_disease_cross": build_age_disease_cross(df, top20_diseases),
        "top20_diseases": top20_diseases,
        "country_stats": build_country_stats(df),
        "special_population_counts": dict(sorted(special_population_counts.items(), key=lambda item: (-item[1], item[0].lower()))),
        "healthy_control_counts": dict(sorted(healthy_control_counts.items(), key=lambda item: (-item[1], item[0].lower()))),
        "disease_label_types": {
            "inform0": {
                "healthy_control": {
                    "labels": sum(1 for label in inform0_counter if label_kind(label) == "healthy_control"),
                    "samples": int(sum(count for label, count in inform0_counter.items() if label_kind(label) == "healthy_control")),
                },
                "disease": {
                    "labels": sum(1 for label in inform0_counter if label_kind(label) == "disease"),
                    "samples": int(sum(count for label, count in inform0_counter.items() if label_kind(label) == "disease")),
                },
                "special_population": {
                    "labels": sum(1 for label in inform0_counter if label_kind(label) == "special_population"),
                    "samples": int(sum(count for label, count in inform0_counter.items() if label_kind(label) == "special_population")),
                },
            },
            "inform0_11": {
                "healthy_control": {
                    "labels": sum(1 for label in union_counter if label_kind(label) == "healthy_control"),
                    "samples": int(sum(count for label, count in union_counter.items() if label_kind(label) == "healthy_control")),
                },
                "disease": {
                    "labels": sum(1 for label in union_counter if label_kind(label) == "disease"),
                    "samples": int(sum(count for label, count in union_counter.items() if label_kind(label) == "disease")),
                },
                "special_population": {
                    "labels": sum(1 for label in union_counter if label_kind(label) == "special_population"),
                    "samples": int(sum(count for label, count in union_counter.items() if label_kind(label) == "special_population")),
                },
            },
        },
    }

    with SUMMARY_OUT.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    print(f"  metadata.json: {METADATA_OUT}")
    print(f"  metadata_summary.json: {SUMMARY_OUT}")
    print("Done.")


if __name__ == "__main__":
    main()
