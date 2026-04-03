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
}
# Also keep inform0-11 columns for individual disease extraction
inform_cols = [f'inform{i}' for i in range(12)]
keep_cols = list(keep.keys()) + [c for c in inform_cols]
df = df[[c for c in keep_cols if c in df.columns]].rename(columns=keep)

# ── Merge TW/HK/MO into CN (Taiwan & Hong Kong → China) ────────────────────
df.loc[df['country'].isin(['TW', 'HK', 'MO']), 'country'] = 'CN'

# ── Extract individual diseases from inform0-11 ─────────────────────────────
# Each sample can have multiple diseases across inform0-11 columns
# inform-all contains combined strings like "IBS;chickenpox" — we don't use it
inform_cols = [f'inform{i}' for i in range(12)]
all_individual_diseases: set[str] = set()
for col in inform_cols:
    if col in df.columns:
        vals = df[col].dropna().astype(str).str.strip()
        all_individual_diseases.update(v for v in vals if v and v != 'nan' and v != '')

# Create a simple 'disease' column from inform0 for backward compatibility
if 'inform0' in df.columns:
    df['disease'] = df['inform0'].fillna('unknown').astype(str).str.strip()
    df.loc[df['disease'] == '', 'disease'] = 'unknown'
else:
    df['disease'] = 'unknown'

# ── Clean sex column ─────────────────────────────────────────────────────────
df['sex'] = df['sex'].fillna('unknown').str.strip().str.lower()
df.loc[~df['sex'].isin(['male', 'female']), 'sex'] = 'unknown'

# ── Export metadata.json (without inform0-11 columns, frontend doesn't need them)
print("Writing metadata.json...")
export_cols = [c for c in df.columns if not c.startswith('inform')]
records = df[export_cols].to_dict(orient='records')
with open(METADATA_OUT, 'w', encoding='utf-8') as f:
    json.dump(records, f, ensure_ascii=False, separators=(',', ':'))
size_mb = os.path.getsize(METADATA_OUT) / 1024 / 1024
print(f"  Written {len(records):,} records → {size_mb:.1f} MB")

# ── Build summary ─────────────────────────────────────────────────────────────
print("Building metadata_summary.json...")

# Global aggregations
age_counts     = df['age_group'].value_counts().to_dict()
sex_counts     = df['sex'].value_counts().to_dict()
# Count individual diseases from inform0-11 (not the combined inform-all)
disease_sample_counts: dict[str, int] = {}
for col in inform_cols:
    if col in df.columns:
        for val in df[col].dropna().astype(str).str.strip():
            if val and val != 'nan' and val != '':
                disease_sample_counts[val] = disease_sample_counts.get(val, 0) + 1
# Sort by count descending, take top 50 for summary
disease_counts = dict(sorted(disease_sample_counts.items(), key=lambda x: x[1], reverse=True)[:50])
country_counts = df['country'].value_counts().to_dict()
region_counts  = df['region'].value_counts().to_dict()

# Age × Sex cross table
age_sex = (
    df.groupby(['age_group', 'sex'])
    .size()
    .reset_index(name='count')
    .to_dict(orient='records')
)

# Age × Disease cross table (Top 20 individual diseases from inform0-11)
top20_diseases = [d for d in sorted(disease_sample_counts.items(), key=lambda x: x[1], reverse=True)
                  if d[0] != 'unknown' and d[0] != 'NC'][:20]
top20_diseases = [d[0] for d in top20_diseases]

# Build age×disease cross table using inform0-11
age_disease_records = []
for col in inform_cols:
    if col in df.columns:
        sub = df[df[col].astype(str).str.strip().isin(top20_diseases)][['age_group', col]].copy()
        sub = sub.rename(columns={col: 'disease'})
        sub['disease'] = sub['disease'].astype(str).str.strip()
        age_disease_records.append(sub)
if age_disease_records:
    age_disease_df = pd.concat(age_disease_records, ignore_index=True)
    age_disease = (
        age_disease_df.groupby(['age_group', 'disease'])
        .size()
        .reset_index(name='count')
        .to_dict(orient='records')
    )
else:
    age_disease = []

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

    # Top 3 diseases from inform0-11 (excluding unknown/NC)
    country_disease_counts: dict[str, int] = {}
    for col in inform_cols:
        if col in sub.columns:
            for val in sub[col].dropna().astype(str).str.strip():
                if val and val != 'nan' and val != '' and val != 'unknown' and val != 'NC':
                    country_disease_counts[val] = country_disease_counts.get(val, 0) + 1
    top_diseases = dict(sorted(country_disease_counts.items(), key=lambda x: x[1], reverse=True)[:3])

    country_stats[iso] = {
        'total':        total,
        'sex':          sex_pct,
        'top_ages':     top_ages,
        'top_diseases': top_diseases,
    }

# ── Write summary ─────────────────────────────────────────────────────────────
all_individual_diseases.discard('unknown')
all_individual_diseases.discard('NC')
all_individual_diseases.discard('nan')
all_individual_diseases.discard('')

summary = {
    'total_samples':    len(df),
    'total_unique_diseases': len(all_individual_diseases),
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
