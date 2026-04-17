"""Integration tests for cache_audit wired into api/main.py."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))

from fastapi.testclient import TestClient


def test_main_app_startup_populates_cache_audit_report(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_AUDIT_HASH_FILE", str(tmp_path / ".endpoint_source_hashes.json"))
    if "main" in sys.modules:
        del sys.modules["main"]
    import main  # noqa: E402

    with TestClient(main.app) as client:
        assert main.app.state.cache_audit_report is not None
        report = main.app.state.cache_audit_report
        assert report.total == 55
        assert report.tracked == 20
        assert len(report.seeded) == 20
        assert len(report.stale) == 0
        assert len(report.unknown) == 0
