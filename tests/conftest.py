"""Shared pytest fixtures for cache_audit tests."""
import json
from pathlib import Path
import pytest


@pytest.fixture
def tmp_hash_file(tmp_path: Path) -> Path:
    """Writable empty directory for hash-file tests."""
    return tmp_path / ".endpoint_source_hashes.json"


@pytest.fixture
def seeded_hash_file(tmp_hash_file: Path) -> Path:
    """Hash file pre-populated with one endpoint."""
    tmp_hash_file.write_text(json.dumps({
        "_meta": {"seeded_at": "2026-04-17T00:00:00Z", "schema_version": 1},
        "disease_profile": {
            "hash": "a7b3c1",
            "cache_key_version": "v1",
            "source_status": "source",
            "last_seen_utc": "2026-04-17T00:00:00Z",
        },
    }))
    return tmp_hash_file
