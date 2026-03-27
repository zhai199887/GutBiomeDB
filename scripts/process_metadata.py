# -*- coding: utf-8 -*-
"""
Process metadata CSV and export JSON files for the frontend.

Input:  D:\483项目\result_with_age_sex_with_age_group_meta.csv
Output: public/data/metadata.json
        public/data/metadata_summary.json
"""

import json
import glob
import os
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
results = glob.glob(r'D:\**\result_with_age_sex_with_age_group_meta.csv', recursive=True)
if not results:
    raise FileNotFoundError("Cannot find result_with_age_sex_with_age_group_meta.csv under D:\\")
CSV_PATH = results[0]
print(f"CSV path: {CSV_PATH}")

OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'public', 'data')
os.makedirs(OUT_DIR, exist_ok=True)

METADATA_OUT = os.path.join(OUT_DIR, 'metadata.json')
SUMMARY_OUT  = os.path.join(OUT_DIR, 'metadata_summary.json')

# ── Load ─────────────────────────────────────────────────────────────────────
print("Loading CSV...")
df = pd.read_csv(CSV_PATH, encoding='latin1', low_memory=False)
print(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")

# ── Select & rename columns ──────────────────────────────────────────────────
keep = {
    'srr':        'sample_id',
    'iso':        'country',
    'region':     'region',
    'project':    'project',
    'age_group':  'age_group',
    'sex':        'sex',
    'inform-all': 'disease',
}
df = df[list(keep.keys())].rename(columns=keep)

# ── Clean disease column ─────────────────────────────────────────────────────
df['disease'] = df['disease'].fillna('unknown').str.strip()
df.loc[df['disease'] == '', 'disease'] = 'unknown'

# ── Clean sex column ─────────────────────────────────────────────────────────
df['sex'] = df['sex'].fillna('unknown').str.strip().str.lower()
df.loc[~df['sex'].isin(['male', 'female']), 'sex'] = 'unknown'

# ── Export metadata.json ─────────────────────────────────────────────────────
print("Writing metadata.json...")
records = df.to_dict(orient='records')
with open(METADATA_OUT, 'w', encoding='utf-8') as f:
    json.dump(records, f, ensure_ascii=False, separators=(',', ':'))
size_mb = os.path.getsize(METADATA_OUT) / 1024 / 1024
print(f"  Written {len(records):,} records → {size_mb:.1f} MB")

# ── Build summary ─────────────────────────────────────────────────────────────
print("Building metadata_summary.json...")

# Global aggregations
age_counts     = df['age_group'].value_counts().to_dict()
sex_counts     = df['sex'].value_counts().to_dict()
disease_counts = df['disease'].value_counts().head(50).to_dict()
country_counts = df['country'].value_counts().to_dict()
region_counts  = df['region'].value_counts().to_dict()

# Age × Sex cross table
age_sex = (
    df.groupby(['age_group', 'sex'])
    .size()
    .reset_index(name='count')
    .to_dict(orient='records')
)

# Age × Disease cross table (Top 20 diseases, excluding unknown)
top20_diseases = (
    df[df['disease'] != 'unknown']['disease']
    .value_counts()
    .head(20)
    .index
    .tolist()
)
age_disease = (
    df[df['disease'].isin(top20_diseases)]
    .groupby(['age_group', 'disease'])
    .size()
    .reset_index(name='count')
    .to_dict(orient='records')
)

# ── Per-country stats (for map tooltip) ─────────────────────────────────────
print("  Computing per-country stats...")

country_stats = {}
top_countries = df['country'].value_counts().head(60).index.tolist()

for iso in top_countries:
    sub = df[df['country'] == iso]
    total = len(sub)
    if total == 0:
        continue

    # Sex ratio
    male_n   = int((sub['sex'] == 'male').sum())
    female_n = int((sub['sex'] == 'female').sum())
    known    = male_n + female_n
    sex_pct = {
        'female_pct': round(female_n / known * 100) if known > 0 else None,
        'male_pct':   round(male_n   / known * 100) if known > 0 else None,
        'known':      known,
    }

    # Top 3 age groups
    top_ages = (
        sub[sub['age_group'] != 'Unknown']['age_group']
        .value_counts()
        .head(3)
        .to_dict()
    )

    # Top 3 diseases (excluding unknown)
    top_diseases = (
        sub[sub['disease'] != 'unknown']['disease']
        .value_counts()
        .head(3)
        .to_dict()
    )

    country_stats[iso] = {
        'total':        total,
        'sex':          sex_pct,
        'top_ages':     top_ages,
        'top_diseases': top_diseases,
    }

# ── Write summary ─────────────────────────────────────────────────────────────
summary = {
    'total_samples':    len(df),
    'age_counts':       age_counts,
    'sex_counts':       sex_counts,
    'disease_counts':   disease_counts,
    'country_counts':   country_counts,
    'region_counts':    region_counts,
    'age_sex_cross':    age_sex,
    'age_disease_cross': age_disease,
    'top20_diseases':   top20_diseases,
    'country_stats':    country_stats,
}

with open(SUMMARY_OUT, 'w', encoding='utf-8') as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
size_kb = os.path.getsize(SUMMARY_OUT) / 1024
print(f"  Written summary → {size_kb:.1f} KB")

print("\nDone.")
print(f"  metadata.json:         {METADATA_OUT}")
print(f"  metadata_summary.json: {SUMMARY_OUT}")
