"""
data_manager.py - Metadata validation, upload, and version tracking
"""
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = {"srr", "project", "iso", "age_group", "sex"}
VERSION_FILE = Path(__file__).parent / "data_version.json"
METADATA_PATH = os.getenv("METADATA_PATH", "")


def _read_version() -> dict:
    if VERSION_FILE.exists():
        with open(VERSION_FILE) as f:
            return json.load(f)
    return {"version": "v1.0", "last_updated": datetime.now().strftime("%Y-%m-%d"), "history": []}


def _write_version(info: dict):
    with open(VERSION_FILE, "w") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)


def validate_metadata(csv_path: str) -> dict:
    try:
        df = pd.read_csv(csv_path, encoding="utf-8", nrows=100, low_memory=False)
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(csv_path, encoding="gbk", nrows=100, low_memory=False)
        except Exception as e:
            return {"valid": False, "errors": [f"Cannot read CSV: {e}"]}
    except Exception as e:
        return {"valid": False, "errors": [f"Cannot read CSV: {e}"]}

    columns = set(c.strip() for c in df.columns)
    missing = REQUIRED_COLUMNS - columns
    if missing:
        return {"valid": False, "errors": [f"Missing required columns: {', '.join(sorted(missing))}"], "columns": sorted(columns)}

    try:
        full_df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        full_df = pd.read_csv(csv_path, encoding="gbk", low_memory=False)

    return {"valid": True, "rows": len(full_df), "columns": sorted(columns)}


def update_metadata(csv_path: str) -> dict:
    if not METADATA_PATH or not os.path.exists(METADATA_PATH):
        return {"status": "error", "message": "METADATA_PATH not configured or file not found"}

    try:
        new_df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        new_df = pd.read_csv(csv_path, encoding="gbk", low_memory=False)

    new_df.columns = [c.strip() for c in new_df.columns]
    missing = REQUIRED_COLUMNS - set(new_df.columns)
    if missing:
        return {"status": "error", "message": f"Missing columns: {', '.join(sorted(missing))}"}

    try:
        existing = pd.read_csv(METADATA_PATH, encoding="gbk", low_memory=False)
    except UnicodeDecodeError:
        existing = pd.read_csv(METADATA_PATH, encoding="utf-8", low_memory=False)

    old_count = len(existing)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["srr"], keep="last")
    new_count = len(combined)
    combined.to_csv(METADATA_PATH, index=False, encoding="gbk")

    ver_info = _read_version()
    ver_num = int(ver_info.get("version", "v1.0").replace("v", "").split(".")[0]) if "version" in ver_info else 1
    new_ver = f"v{ver_num}.{len(ver_info.get('history', [])) + 1}_{datetime.now().strftime('%Y%m%d')}"
    ver_info["version"] = new_ver
    ver_info["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    ver_info.setdefault("history", []).append({
        "version": new_ver, "date": datetime.now().isoformat(),
        "action": "upload_merge", "added_rows": new_count - old_count, "total_rows": new_count,
    })
    _write_version(ver_info)

    return {"status": "ok", "message": f"Merged successfully. {new_count - old_count} new rows added.",
            "new_rows": new_count - old_count, "total_rows": new_count}
