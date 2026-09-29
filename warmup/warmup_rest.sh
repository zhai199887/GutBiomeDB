#!/bin/bash
# Unit D: BATCH 1+2 合并串行 (12 plans, ~19700 keys)
cd /opt/gutbiomedb/warmup
for p in metabolism_profile_v1 disease_profile_v1 disease_studies_v1 lollipop_v1 biomarker_discovery_v1 cooccurrence_v1 network_compare_v1 project_detail_v1 species_profile_v1 biomarker_profile_v1 species_cooccurrence_v1 phenotype_taxa_profile_v1; do
  echo "=== $(date -u +%FT%TZ) START $p ==="
  /usr/bin/python3 /opt/gutbiomedb/warmup/warmup_full.py \
    --plan plan/${p}.json --log logs/${p}.csv --done DONE.${p}
  echo "=== $(date -u +%FT%TZ) DONE $p ==="
done
echo "REST ALL DONE at $(date -u +%FT%TZ)" > /opt/gutbiomedb/warmup/DONE.rest_batch
