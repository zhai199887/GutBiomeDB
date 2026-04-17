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
        assert report.total == 56
        assert report.tracked == 20
        assert len(report.seeded) == 20
        assert len(report.stale) == 0
        assert len(report.unknown) == 0


def test_health_omits_cache_audit_fields_when_clean(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_AUDIT_HASH_FILE", str(tmp_path / ".h.json"))
    if "main" in sys.modules:
        del sys.modules["main"]
    import main

    with TestClient(main.app) as client:
        r = client.get("/api/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert "stale_cache_warnings" not in body
        assert body.get("seeded_count") == 20
        assert "unknown_count" not in body


def test_rehash_seed_full_requires_admin_token(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_AUDIT_HASH_FILE", str(tmp_path / ".h.json"))
    if "main" in sys.modules:
        del sys.modules["main"]
    import main
    monkeypatch.setattr(main, "ADMIN_TOKEN", "secret123")

    with TestClient(main.app) as client:
        r = client.post("/api/admin/rehash-seed")
        assert r.status_code == 401
        r = client.post("/api/admin/rehash-seed", headers={"X-Admin-Token": "wrong"})
        assert r.status_code == 401
        r = client.post("/api/admin/rehash-seed", headers={"X-Admin-Token": "secret123"})
        assert r.status_code == 200
        assert r.json()["reset"] == "all"


def test_rehash_seed_endpoint_param_resets_single(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_AUDIT_HASH_FILE", str(tmp_path / ".h.json"))
    if "main" in sys.modules:
        del sys.modules["main"]
    import main
    monkeypatch.setattr(main, "ADMIN_TOKEN", "secret123")

    with TestClient(main.app) as client:
        client.get("/api/health")
        r = client.post(
            "/api/admin/rehash-seed?endpoint=disease_profile",
            headers={"X-Admin-Token": "secret123"},
        )
        assert r.status_code == 200
        assert r.json()["reset"] == "disease_profile"


def test_rehash_seed_unknown_endpoint_returns_404(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_AUDIT_HASH_FILE", str(tmp_path / ".h.json"))
    if "main" in sys.modules:
        del sys.modules["main"]
    import main
    monkeypatch.setattr(main, "ADMIN_TOKEN", "secret123")

    with TestClient(main.app) as client:
        r = client.post(
            "/api/admin/rehash-seed?endpoint=does_not_exist",
            headers={"X-Admin-Token": "secret123"},
        )
        assert r.status_code == 404
