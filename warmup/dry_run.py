#!/usr/bin/env python3
"""Dry-run: enumerate expected cache_keys per endpoint, diff against existing _disk_cache/ files.

Runs ON SYDNEY (/opt/gutbiomedb/warmup/).

Default mode is full enumeration.

If a smaller hot-path subset is needed for a controlled burn-in, opt in explicitly:
  GBDB_WARMUP_HOT_ONLY=1 python3 /opt/gutbiomedb/warmup/dry_run.py
"""
import csv
import json
import os
from collections import Counter
from itertools import combinations, product

CACHE_DIR = "/opt/gutbiomedb/api/_disk_cache"
WARMUP_DIR = "/opt/gutbiomedb/warmup"
API_ENV_PATH = "/opt/gutbiomedb/api/.env"
DEFAULT_METADATA_PATH = "/opt/gutbiomedb/data/metadata.csv"

HOT_ONLY = os.environ.get("GBDB_WARMUP_HOT_ONLY") == "1"
FULL_ENUM = not HOT_ONLY
HOT_TOP_GENERA = [5, 10, 15, 20, 25, 30]
HOT_LIFECYCLE_TOP_N = 15
HOT_LIFECYCLE_DISEASE_LIMIT = 12
HOT_MIN_DISEASE_SAMPLES = 30
HOT_PHENOTYPE_DEFAULTS = [
    ("sex", "female", "male", "genus"),
    ("sex", "female", "male", "phylum"),
    ("age", "Adult", "Older_Adult", "genus"),
    ("age", "Adult", "Older_Adult", "phylum"),
    ("disease", "UC", "CD", "genus"),
    ("disease", "UC", "CD", "phylum"),
]


def cache_path(key: str) -> str:
    safe = key.replace(":", "_").replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe}.json")


def exists(key: str) -> bool:
    return os.path.exists(cache_path(key))


def env_default(key: str, fallback: str) -> str:
    if key in os.environ and os.environ[key]:
        return os.environ[key]
    if os.path.exists(API_ENV_PATH):
        with open(API_ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, value = line.split("=", 1)
                if name == key and value:
                    return value
    return fallback


METADATA_PATH = env_default("METADATA_PATH", DEFAULT_METADATA_PATH)


def load(name: str):
    return json.load(open(os.path.join(WARMUP_DIR, name)))


def iter_row_labels(row: dict[str, str]) -> set[str]:
    labels: set[str] = set()
    primary = (row.get("inform-all") or "").strip()
    if primary and primary != "NC":
        labels.add(primary)
    for idx in range(12):
        value = (row.get(f"inform{idx}") or "").strip()
        if value and value != "NC":
            labels.add(value)
    return labels


def collect_disease_stats(metadata_path: str) -> tuple[Counter[str], dict[str, set[str]]]:
    counts: Counter[str] = Counter()
    age_groups: dict[str, set[str]] = {}
    with open(metadata_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            labels = iter_row_labels(row)
            if not labels:
                continue
            age_group = (row.get("age_group") or "").strip()
            for label in labels:
                counts[label] += 1
                if age_group and age_group != "Unknown":
                    age_groups.setdefault(label, set()).add(age_group)
    return counts, age_groups


fo = load("filter_options.json")
go = load("genus_names.json")
mo = load("metabolism_overview.json")
pl = load("project_list.json")

countries = fo["countries"]
diseases_filter = fo["diseases"]
genera = [g.strip().lower() for g in go["genera"]]
metabolism_cats = [c["category_id"] for c in mo["categories"]]
project_ids = [p["project_id"].lower() for p in pl["projects"]]

print(f"INPUT SIZES: countries={len(countries)}, diseases={len(diseases_filter)}, "
      f"genera={len(genera)}, metabolism={len(metabolism_cats)}, projects={len(project_ids)}")

AGE_GROUPS = ["Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian"]
SEX_GROUPS = ["female", "male"]
DIM_TYPES = ["age", "sex", "disease"]
TAX_LEVELS = ["genus", "phylum"]


def count(endpoint_label: str, keys: list[str], rate_limit_per_min: int):
    expected = len(keys)
    existing = sum(1 for k in keys if exists(k))
    missing = expected - existing
    eta_min = missing / rate_limit_per_min if rate_limit_per_min > 0 else 0
    print(f"  {endpoint_label:35s} expected={expected:>7d}  existing={existing:>6d}  "
          f"missing={missing:>7d}  rate={rate_limit_per_min}/min  ETA={eta_min:>7.1f} min")
    return missing, keys


print("\n=== BATCH 1: small endpoints ===")
total_missing = 0
all_missing: dict[str, list[tuple[int, list[str]]]] = {}


def add(label, keys, rate):
    m, k = count(label, keys, rate)
    all_missing[label] = (rate, [x for x in k if not exists(x)])
    return m


def skip(label: str, rate: int, reason: str) -> None:
    print(f"  {label:35s} expected=      0  existing=     0  missing=      0  rate={rate}/min  SKIP: {reason}")
    all_missing[label] = (rate, [])


total_missing += add("metabolism_profile_v1",
    [f"metabolism_profile_v1:{c}" for c in metabolism_cats], 60)

total_missing += add("disease_profile_v1 (top_n=40)",
    [f"disease_profile_v1:{d}:40" for d in diseases_filter], 60)

total_missing += add("disease_studies_v1",
    [f"disease_studies_v1:{d}" for d in diseases_filter], 60)

total_missing += add("lollipop_v1 (top_n=120)",
    [f"lollipop_v1:{d}:120" for d in diseases_filter], 60)

total_missing += add("biomarker_discovery_v1",
    [f"biomarker_discovery_v1:{d}:{lda}:0.05"
     for d in diseases_filter for lda in [1.5, 2.0, 2.5, 3.0]], 60)

total_missing += add("cooccurrence_v1",
    [f"cooccurrence_v1:{d}:{r}:50:3000:{m}:0.05"
     for d in diseases_filter for r in [0.2, 0.3, 0.4, 0.5] for m in ["sparcc", "spearman"]], 20)

total_missing += add("network_compare_v1",
    [f"network_compare_v1:{d}:{r}:50:3000:{m}:0.05"
     for d in diseases_filter for r in [0.2, 0.3, 0.4, 0.5] for m in ["sparcc", "spearman"]], 20)

if FULL_ENUM:
    total_missing += add("project_detail_v1",
        [f"project_detail_v1:{p}" for p in project_ids], 30)
else:
    skip("project_detail_v1", 30, "zero observed production hits; exclude from safe default plan")

print("\n=== BATCH 2: genus-scale endpoints ===")
if FULL_ENUM:
    total_missing += add("species_profile_v1",
        [f"species_profile_v1:{g}" for g in genera], 60)

    total_missing += add("biomarker_profile_v1 (min_samples=10)",
        [f"biomarker_profile_v1:{g}:10" for g in genera], 30)

    total_missing += add("species_cooccurrence_v1 (NC only)",
        [f"species_cooccurrence_v1:{g}:10:__nc__" for g in genera], 30)

    total_missing += add("phenotype_taxa_profile_v1",
        [f"phenotype_taxa_profile_v1:{g}:{d}" for g in genera for d in DIM_TYPES], 30)
else:
    skip("species_profile_v1", 60, "zero observed production hits; exclude from safe default plan")
    skip("biomarker_profile_v1", 30, "zero observed production hits; exclude from safe default plan")
    skip("species_cooccurrence_v1", 30, "zero observed production hits; exclude from safe default plan")
    skip("phenotype_taxa_profile_v1", 30, "zero observed production hits; exclude from safe default plan")

print("\n=== BATCH 3: phenotype_assoc ===")
if FULL_ENUM:
    assoc_keys: list[str] = []
    age_pairs = list(combinations(AGE_GROUPS, 2))
    sex_pairs = list(combinations(SEX_GROUPS, 2))

    curl_disease_groups = json.load(open(os.path.join(WARMUP_DIR, "phenotype_disease_groups.json")))
    disease_pairs = list(combinations([g["group"] for g in curl_disease_groups["groups"]], 2))
    print(f"  disease-disease pair count: {len(disease_pairs)}")

    for (ga, gb) in age_pairs:
        for tax in TAX_LEVELS:
            assoc_keys.append(f"phenotype_assoc_v1:age:{ga}:{gb}:{tax}:0.1:100")

    for (ga, gb) in sex_pairs:
        for tax in TAX_LEVELS:
            assoc_keys.append(f"phenotype_assoc_v1:sex:{ga}:{gb}:{tax}:0.1:100")

    for (ga, gb) in disease_pairs:
        for tax in TAX_LEVELS:
            assoc_keys.append(f"phenotype_assoc_v1:disease:{ga}:{gb}:{tax}:0.1:100")

    total_missing += add("phenotype_assoc_v1 (age+sex+DD)", assoc_keys, 10)
else:
    hot_assoc_keys = [
        f"phenotype_assoc_v1:{dim}:{group_a}:{group_b}:{tax}:0.1:100"
        for dim, group_a, group_b, tax in HOT_PHENOTYPE_DEFAULTS
    ]
    total_missing += add("phenotype_assoc_v1 (hot defaults)", hot_assoc_keys, 10)

print("\n=== BATCH 4: lifecycle ===")
if FULL_ENUM:
    print("MODE: full enumeration (default)")
    TOP_GENERA = HOT_TOP_GENERA
    disease_and_empty = [""] + diseases_filter
    country_and_empty = [""] + countries

    lifecycle_keys = [
        f"lifecycle_v9:{d}:{c}:{t}:"
        for d in disease_and_empty for c in country_and_empty for t in TOP_GENERA
    ]
    total_missing += add("lifecycle_v9 (C plan)", lifecycle_keys, 60)

    lifecycle_compare_keys = [
        f"lifecycle_compare_v3:{d}:{c}:{t}"
        for d in diseases_filter if d != "NC"
        for c in country_and_empty
        for t in TOP_GENERA
    ]
    total_missing += add("lifecycle_compare_v3 (C plan, excl NC)", lifecycle_compare_keys, 30)
else:
    print("MODE: hot-path plan (GBDB_WARMUP_HOT_ONLY=1)")
    disease_counts, disease_age_groups = collect_disease_stats(METADATA_PATH)
    qualified_hot = [
        name for name, count in disease_counts.most_common()
        if count >= HOT_MIN_DISEASE_SAMPLES
    ]
    lifecycle_hot_diseases = [
        name for name in qualified_hot
        if len(disease_age_groups.get(name, set())) >= 3
    ][:HOT_LIFECYCLE_DISEASE_LIMIT]

    print(
        "  hot lifecycle diseases:",
        ", ".join(lifecycle_hot_diseases) if lifecycle_hot_diseases else "(none)",
    )

    lifecycle_keys = [f"lifecycle_v9:::{top_n}:" for top_n in HOT_TOP_GENERA]
    lifecycle_keys.extend(
        f"lifecycle_v9:{disease}::{HOT_LIFECYCLE_TOP_N}:"
        for disease in lifecycle_hot_diseases
    )
    total_missing += add("lifecycle_v9 (hot subset)", lifecycle_keys, 60)

    lifecycle_compare_keys = [
        f"lifecycle_compare_v3:{disease}::{HOT_LIFECYCLE_TOP_N}"
        for disease in lifecycle_hot_diseases
    ]
    total_missing += add("lifecycle_compare_v3 (hot subset)", lifecycle_compare_keys, 30)

print(f"\n=== GRAND TOTAL MISSING: {total_missing} ===")

# Write per-endpoint missing cache_keys to plan/ for warmup_full.py to consume
plan_dir = os.path.join(WARMUP_DIR, "plan")
os.makedirs(plan_dir, exist_ok=True)
for label, (rate, missing_keys) in all_missing.items():
    safe_label = label.split(" ")[0].replace("/", "_")
    out = os.path.join(plan_dir, f"{safe_label}.json")
    with open(out, "w") as f:
        json.dump({"rate_per_min": rate, "keys": missing_keys}, f)
    print(f"  wrote {out}: {len(missing_keys)} keys @ {rate}/min")

print("\nDone. Next step: review numbers, then run warmup_full.py")
