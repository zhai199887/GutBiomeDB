#!/usr/bin/env python3
"""Full warmup worker: consume a plan/<endpoint>.json and hit each cache_key.

Usage:
    python3 warmup_full.py --plan /opt/gutbiomedb/warmup/plan/phenotype_assoc_v1.json \\
                           --log /opt/gutbiomedb/warmup/logs/phenotype_assoc_v1.csv \\
                           --done /opt/gutbiomedb/warmup/DONE.phenotype_assoc_v1

Pacing: sleeps 60/rate_per_min seconds between requests; on 429 sleeps 60s.

Stops on KeyboardInterrupt; supports resume by re-checking disk cache per key.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

API_BASE = "http://127.0.0.1:8000"
CACHE_DIR = "/opt/gutbiomedb/api/_disk_cache"


def cache_path(key: str) -> str:
    safe = key.replace(":", "_").replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe}.json")


def build_url(key: str) -> str:
    """Reverse-engineer URL from cache_key prefix.

    Returns GET URL including querystring. Assumes all endpoints accept GET.
    """
    # split off v1/v3/v9 suffix — every cache_key starts with "<prefix>_v<n>:"
    prefix, _, rest = key.partition(":")
    parts = rest.split(":")

    def enc(s: str) -> str:
        return urllib.parse.quote(s, safe="")

    if prefix == "metabolism_profile_v1":
        return f"{API_BASE}/api/metabolism-category-profile?category_id={enc(parts[0])}"
    if prefix == "disease_profile_v1":
        return f"{API_BASE}/api/disease-profile?disease={enc(parts[0])}&top_n={parts[1]}"
    if prefix == "disease_studies_v1":
        return f"{API_BASE}/api/disease-studies?disease={enc(parts[0])}"
    if prefix == "lollipop_v1":
        return f"{API_BASE}/api/lollipop-data?disease={enc(parts[0])}&top_n={parts[1]}"
    if prefix == "biomarker_discovery_v1":
        return (f"{API_BASE}/api/biomarker-discovery"
                f"?disease={enc(parts[0])}&lda_threshold={parts[1]}&p_threshold={parts[2]}")
    if prefix == "cooccurrence_v1":
        return (f"{API_BASE}/api/cooccurrence"
                f"?disease={enc(parts[0])}&min_r={parts[1]}&top_genera={parts[2]}"
                f"&max_samples={parts[3]}&method={parts[4]}&fdr_threshold={parts[5]}")
    if prefix == "network_compare_v1":
        return (f"{API_BASE}/api/network-compare"
                f"?disease={enc(parts[0])}&min_r={parts[1]}&top_genera={parts[2]}"
                f"&max_samples={parts[3]}&method={parts[4]}&fdr_threshold={parts[5]}")
    if prefix == "project_detail_v1":
        return f"{API_BASE}/api/project-detail?project_id={enc(parts[0])}"
    if prefix == "species_profile_v1":
        return f"{API_BASE}/api/species-profile?genus={enc(parts[0])}"
    if prefix == "biomarker_profile_v1":
        return f"{API_BASE}/api/biomarker-profile?genus={enc(parts[0])}&min_samples={parts[1]}"
    if prefix == "species_cooccurrence_v1":
        disease = "" if parts[2] == "__nc__" else parts[2]
        return (f"{API_BASE}/api/species-cooccurrence"
                f"?genus={enc(parts[0])}&top_k={parts[1]}&disease_name={enc(disease)}")
    if prefix == "phenotype_taxa_profile_v1":
        return (f"{API_BASE}/api/phenotype-taxa-profile"
                f"?taxon={enc(parts[0])}&dim_type={parts[1]}")
    if prefix == "phenotype_assoc_v1":
        return (f"{API_BASE}/api/phenotype-association"
                f"?dim_type={parts[0]}&group_a={enc(parts[1])}&group_b={enc(parts[2])}"
                f"&tax_level={parts[3]}&min_prevalence={parts[4]}&top_n={parts[5]}")
    if prefix == "lifecycle_v9":
        # key format: lifecycle_v9:<disease>:<country>:<top_genera>:  (trailing empty fixed_part)
        disease, country, top = parts[0], parts[1], parts[2]
        url = f"{API_BASE}/api/lifecycle?top_genera={top}"
        if disease:
            url += f"&disease={enc(disease)}"
        if country:
            url += f"&country={enc(country)}"
        return url
    if prefix == "lifecycle_compare_v3":
        disease, country, top = parts[0], parts[1], parts[2]
        url = f"{API_BASE}/api/lifecycle-compare?disease={enc(disease)}&top_genera={top}"
        if country:
            url += f"&country={enc(country)}"
        return url

    raise ValueError(f"Unknown cache_key prefix: {prefix}")


def hit(url: str, timeout: int = 120) -> tuple[int, str]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "gbdb-warmup/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()  # drain
            return resp.status, "ok"
    except urllib.error.HTTPError as e:
        return e.code, str(e.reason)[:80]
    except Exception as e:
        return -1, str(e)[:80]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--log", required=True)
    ap.add_argument("--done", required=True)
    args = ap.parse_args()

    with open(args.plan) as f:
        plan = json.load(f)
    rate = plan["rate_per_min"]
    keys = plan["keys"]
    # Use 90% of rate ceiling to stay safely under limiter (empirical buffer)
    interval = 60.0 / (rate * 0.9)

    os.makedirs(os.path.dirname(args.log), exist_ok=True)

    already = set()
    # Resume support: read log, mark previously OK'd keys as done
    if os.path.exists(args.log):
        with open(args.log) as f:
            for row in csv.reader(f):
                if len(row) >= 3 and row[1] == "200":
                    already.add(row[0])

    ok = slow = fail = backoff_429 = skip_hit = skip_done = 0
    t0 = time.time()

    with open(args.log, "a", newline="") as logf:
        w = csv.writer(logf)
        for i, key in enumerate(keys):
            if key in already:
                skip_done += 1
                continue
            # Pre-check: already on disk means set_disk_cached wrote it
            if os.path.exists(cache_path(key)):
                skip_hit += 1
                w.writerow([key, 200, "disk-already", f"{time.time():.0f}"])
                logf.flush()
                continue

            try:
                url = build_url(key)
            except ValueError as e:
                fail += 1
                w.writerow([key, -2, str(e)[:80], f"{time.time():.0f}"])
                logf.flush()
                continue

            t_req = time.time()
            status, reason = hit(url)
            dt = time.time() - t_req

            if status == 429:
                backoff_429 += 1
                w.writerow([key, 429, "backoff-60s", f"{time.time():.0f}"])
                logf.flush()
                time.sleep(60)
                # retry once
                status, reason = hit(url)
                dt = time.time() - t_req

            if status == 200:
                if dt > 5.0:
                    slow += 1
                else:
                    ok += 1
                w.writerow([key, 200, f"{dt:.2f}s", f"{time.time():.0f}"])
            else:
                fail += 1
                w.writerow([key, status, reason, f"{time.time():.0f}"])

            logf.flush()

            # progress heartbeat every 100 keys
            done_count = ok + slow + fail + skip_hit
            total_count = len(keys) - skip_done
            if done_count % 100 == 0 and done_count > 0:
                elapsed = time.time() - t0
                rate_actual = done_count / elapsed if elapsed > 0 else 0
                remaining = total_count - done_count
                eta_sec = remaining / rate_actual if rate_actual > 0 else 0
                sys.stderr.write(
                    f"[{Path(args.plan).stem}] {done_count}/{total_count} "
                    f"ok={ok} slow={slow} fail={fail} 429={backoff_429} skip-hit={skip_hit} "
                    f"rate={rate_actual:.2f}/s ETA={eta_sec/3600:.1f}h\n"
                )
                sys.stderr.flush()

            if i < len(keys) - 1:
                time.sleep(interval)

    # Write DONE marker
    with open(args.done, "w") as f:
        elapsed = time.time() - t0
        summary = {
            "plan": args.plan,
            "total_keys": len(keys),
            "ok": ok,
            "slow": slow,
            "fail": fail,
            "backoff_429": backoff_429,
            "skip_hit_prerun": skip_hit,
            "skip_done_resume": skip_done,
            "elapsed_sec": round(elapsed, 1),
            "elapsed_hr": round(elapsed / 3600, 2),
        }
        json.dump(summary, f, indent=2)
    sys.stderr.write(f"[{Path(args.plan).stem}] DONE. {json.dumps(summary)}\n")


if __name__ == "__main__":
    main()
