#!/bin/bash
# gbdb-warmup-rest: 串行跑所有非 assoc/lifecycle 的 plan
set -e
cd /opt/gutbiomedb/warmup
PLANS=(disease_profile_v1 disease_studies_v1 lollipop_v1 biomarker_discovery_v1 cooccurrence_v1 network_compare_v1 project_detail_v1 species_profile_v1 biomarker_profile_v1 species_cooccurrence_v1 phenotype_taxa_profile_v1)
for p in "${PLANS[@]}"; do
  if [ -f "DONE.${p}" ]; then
    echo "[rest_runner] $(date -Iseconds) SKIP $p (DONE marker exists)"
    continue
  fi
  echo "[rest_runner] $(date -Iseconds) START $p"
  /usr/bin/python3 warmup_full.py --plan plan/${p}.json --log logs/${p}.csv --done DONE.${p}
  echo "[rest_runner] $(date -Iseconds) FINISH $p"
done
echo "[rest_runner] $(date -Iseconds) ALL DONE"
