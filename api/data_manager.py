"""
data_manager.py – Dataset update and version management
数据更新与版本管理模块
"""

import csv
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# ── Paths / 路径配置 ──────────────────────────────────────────────────────────
METADATA_PATH = os.getenv("METADATA_PATH", "")
ABUNDANCE_PATH = os.getenv("ABUNDANCE_PATH", "")
CHANGELOG_PATH = Path(__file__).parent / "data_changelog.json"
VERSION_PATH   = Path(__file__).parent / "data_version.json"

# Required columns for metadata validation / 元数据必须有的列
REQUIRED_METADATA_COLS = {
    "srr", "project", "geo_loc_name", "age_group", "sex",
    "inform-all",
}


# ── Version helpers / 版本号工具 ───────────────────────────────────────────────

def _read_version() -> dict:
    if VERSION_PATH.exists():
        return json.loads(VERSION_PATH.read_text())
    return {"version": "v1.0_20260403", "last_updated": "2026-04-03", "total_samples": 0}


def _write_version(info: dict):
    VERSION_PATH.write_text(json.dumps(info, indent=2, ensure_ascii=False))


def _append_changelog(entry: dict):
    log: list = []
    if CHANGELOG_PATH.exists():
        log = json.loads(CHANGELOG_PATH.read_text())
    log.append(entry)
    CHANGELOG_PATH.write_text(json.dumps(log, indent=2, ensure_ascii=False))


# ── Validation / 校验 ─────────────────────────────────────────────────────────

def validate_metadata(csv_path: str) -> dict:
    """
    Validate a new metadata CSV file.
    校验新上传的元数据CSV格式
    Returns {"ok": bool, "errors": list[str], "row_count": int}
    """
    errors = []
    row_count = 0

    try:
        df = pd.read_csv(csv_path, encoding="gbk", on_bad_lines="skip", nrows=5)
        actual_cols = set(df.columns.str.strip().str.lower())
        missing = REQUIRED_METADATA_COLS - actual_cols
        if missing:
            errors.append(f"Missing required columns: {missing}")
    except Exception as e:
        return {"ok": False, "errors": [str(e)], "row_count": 0}

    # Count rows efficiently / 快速统计行数
    try:
        with open(csv_path, encoding="gbk", errors="replace") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for _ in reader:
                row_count += 1
    except Exception as e:
        errors.append(f"Row count error: {e}")

    return {"ok": len(errors) == 0, "errors": errors, "row_count": row_count}


def validate_abundance(csv_path: str) -> dict:
    """
    Validate a new abundance CSV file.
    校验新上传的丰度CSV格式
    """
    errors = []
    try:
        df = pd.read_csv(csv_path, index_col=0, nrows=3, low_memory=False)
        row_count_sample = len(df)
        col_count = len(df.columns)
        return {
            "ok": True,
            "errors": [],
            "sample_count_preview": row_count_sample,
            "taxa_count": col_count,
        }
    except Exception as e:
        return {"ok": False, "errors": [str(e)], "sample_count_preview": 0, "taxa_count": 0}


# ── Update / 数据合并更新 ─────────────────────────────────────────────────────

def update_metadata(new_csv_path: str) -> dict:
    """
    Merge new metadata CSV into existing dataset.
    合并新元数据CSV到现有数据集
    """
    validation = validate_metadata(new_csv_path)
    if not validation["ok"]:
        return {"success": False, "errors": validation["errors"]}

    # Load existing / 加载现有数据
    existing = pd.read_csv(METADATA_PATH, encoding="gbk", on_bad_lines="skip", low_memory=False)
    new_data = pd.read_csv(new_csv_path, encoding="gbk", on_bad_lines="skip", low_memory=False)

    # Check for duplicate sample IDs / 检查重复样本ID
    if "srr" in existing.columns and "srr" in new_data.columns:
        overlap = set(existing["srr"]) & set(new_data["srr"])
        if overlap:
            return {
                "success": False,
                "errors": [f"{len(overlap)} duplicate SRR IDs found: {list(overlap)[:5]}..."]
            }

    # Backup existing / 备份现有数据
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = METADATA_PATH.replace(".csv", f"_backup_{timestamp}.csv")
    shutil.copy2(METADATA_PATH, backup_path)

    # Merge and save / 合并并保存
    merged = pd.concat([existing, new_data], ignore_index=True)
    merged.to_csv(METADATA_PATH, index=False, encoding="gbk")

    # Update version / 更新版本号
    version_tag = f"v1.{datetime.now().strftime('%Y%m%d')}"
    _write_version({
        "version": version_tag,
        "last_updated": datetime.now().strftime("%Y-%m-%d"),
        "total_samples": len(merged),
    })

    # Log the update / 记录更新日志
    _append_changelog({
        "action": "update_metadata",
        "timestamp": datetime.now().isoformat(),
        "added_rows": len(new_data),
        "total_after": len(merged),
        "backup": backup_path,
        "version": version_tag,
    })

    return {
        "success": True,
        "added_rows": len(new_data),
        "total_rows": len(merged),
        "version": version_tag,
        "backup": backup_path,
    }


def get_data_stats() -> dict:
    """
    Return current dataset statistics.
    返回当前数据集统计信息
    """
    version = _read_version()
    try:
        meta = pd.read_csv(
            METADATA_PATH, encoding="gbk", on_bad_lines="skip",
            usecols=["geo_loc_name", "inform-all"],
            low_memory=False,
        )
        country_count = meta["geo_loc_name"].str.split(":").str[0].nunique() if "geo_loc_name" in meta.columns else 0
        disease_count = meta["inform-all"].nunique() if "inform-all" in meta.columns else 0
        total = len(meta)
    except Exception:
        total = version.get("total_samples", 0)
        country_count = 0
        disease_count = 0

    return {
        "total_samples": total,
        "total_countries": country_count,
        "total_diseases": disease_count,
        "version": version.get("version", "v1.0"),
        "last_updated": version.get("last_updated", "2026-04-03"),
    }


if __name__ == "__main__":
    # Quick test / 快速测试
    print(get_data_stats())
