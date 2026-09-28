"""Unit tests for api/cache_audit.py pure functions."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))

from cache_audit import ast_hash, extract_cache_key_version


def _sample_original():
    x = 0.05
    return x


def _sample_changed_literal():
    x = 0.01
    return x


def _compile_fn(src: str, name: str = "_sample"):
    """Compile src into a module-like namespace and return the named function.

    Uses a synthetic filename routed through linecache so inspect.getsource()
    can recover the original source.
    """
    import linecache
    import types

    filename = f"<_compile_fn:{name}:{abs(hash(src))}>"
    lines = src.splitlines(True)
    linecache.cache[filename] = (len(src), None, lines, filename)
    code = compile(src, filename, "exec")
    mod = types.ModuleType("_compiled_test_mod")
    mod.__file__ = filename
    exec(code, mod.__dict__)
    return mod.__dict__[name]


def test_ast_hash_returns_6_hex_chars_and_source_status():
    h, status = ast_hash(_sample_original)
    assert len(h) == 6
    assert all(c in "0123456789abcdef" for c in h)
    assert status == "source"


def test_ast_hash_ignores_comment_edits():
    src_plain = "def fn():\n    x = 0.05\n    return x\n"
    src_commented = "def fn():\n    x = 0.05  # threshold\n    return x\n"
    fn_a = _compile_fn(src_plain, "fn")
    fn_b = _compile_fn(src_commented, "fn")
    h1, _ = ast_hash(fn_a)
    h2, _ = ast_hash(fn_b)
    assert h1 == h2, "adding a comment must not change AST hash"


def test_ast_hash_detects_literal_change():
    h1, _ = ast_hash(_sample_original)
    h2, _ = ast_hash(_sample_changed_literal)
    assert h1 != h2, "changing 0.05 -> 0.01 must change AST hash"


def _fn_with_fstring_v1():
    cache_key = f"disease_profile_v1:{42}:{100}"
    return cache_key


def _fn_with_plain_string_v2():
    cache_key = "project_list_v2"
    return cache_key


def _fn_with_no_cache_key():
    x = 1
    return x


def _fn_with_versionless_cache_key():
    cache_key = "metabolism_overview"
    return cache_key


def test_extract_version_fstring():
    name, ver = extract_cache_key_version(_fn_with_fstring_v1)
    assert name == "disease_profile"
    assert ver == "v1"


def test_extract_version_plain_string():
    name, ver = extract_cache_key_version(_fn_with_plain_string_v2)
    assert name == "project_list"
    assert ver == "v2"


def test_extract_version_missing_cache_key():
    name, ver = extract_cache_key_version(_fn_with_no_cache_key)
    assert name is None
    assert ver is None


def test_extract_version_unversioned_cache_key_returns_name_but_no_version():
    name, ver = extract_cache_key_version(_fn_with_versionless_cache_key)
    assert name == "metabolism_overview"
    assert ver is None


from fastapi import FastAPI
from cache_audit import scan_endpoints, EndpointAudit


def _make_fake_app():
    app = FastAPI()

    @app.get("/api/foo")
    def foo():
        cache_key = f"foo_v1:{1}"
        return {"k": cache_key}

    @app.get("/api/bar")
    def bar():
        return {}

    @app.get("/api/baz")
    def baz():
        return {"ok": True}
    baz._no_cache_tracking = True

    return app


def test_scan_endpoints_classifies_three_states():
    app = _make_fake_app()
    audits = scan_endpoints(app)
    by_name = {a.fn_name: a for a in audits}
    assert by_name["foo"].status == "tracked"
    assert by_name["foo"].cache_key_name == "foo"
    assert by_name["foo"].version == "v1"
    assert by_name["bar"].status == "unknown"
    assert by_name["baz"].status == "no_cache_by_design"


def test_scan_endpoints_skips_non_apiroute_entries():
    app = _make_fake_app()
    audits = scan_endpoints(app)
    names = {a.fn_name for a in audits}
    assert names == {"foo", "bar", "baz"}


from cache_audit import compute_report


def _tracked_audit(name: str, version: str, hex6: str) -> EndpointAudit:
    return EndpointAudit(
        path=f"/api/{name}", method="GET", fn_name=name,
        status="tracked", cache_key_name=name, version=version,
        current_hash=hex6, source_status="source",
    )


def test_compute_report_first_run_seeds_all():
    audits = [_tracked_audit("disease_profile", "v1", "abc123")]
    report = compute_report(audits, prior={})
    assert len(report.stale) == 0
    assert report.seeded == ["disease_profile"]


def test_compute_report_unchanged_returns_zero_stale():
    audits = [_tracked_audit("disease_profile", "v1", "abc123")]
    prior = {"disease_profile": {"hash": "abc123", "cache_key_version": "v1"}}
    report = compute_report(audits, prior=prior)
    assert len(report.stale) == 0
    assert report.seeded == []


def test_compute_report_hash_changed_version_unchanged_is_stale():
    audits = [_tracked_audit("disease_profile", "v1", "def456")]
    prior = {"disease_profile": {"hash": "abc123", "cache_key_version": "v1"}}
    report = compute_report(audits, prior=prior)
    assert len(report.stale) == 1
    assert report.stale[0]["name"] == "disease_profile"
    assert report.stale[0]["prior"] == "abc123"
    assert report.stale[0]["current"] == "def456"
    assert report.stale[0]["version"] == "v1"


def test_compute_report_hash_changed_version_bumped_is_clean():
    audits = [_tracked_audit("disease_profile", "v2", "def456")]
    prior = {"disease_profile": {"hash": "abc123", "cache_key_version": "v1"}}
    report = compute_report(audits, prior=prior)
    assert len(report.stale) == 0


def test_compute_report_collects_unknown_and_legacy():
    audits = [
        EndpointAudit("/a", "GET", "a", "unknown"),
        EndpointAudit("/b", "GET", "b", "legacy_unversioned", cache_key_name="b"),
    ]
    report = compute_report(audits, prior={})
    assert report.unknown == ["a"]
    assert report.legacy_unversioned == ["b"]


from cache_audit import detect_cache_key_collisions, DuplicateCacheKeyError
import pytest


def test_detect_collisions_raises_on_duplicate():
    audits = [
        _tracked_audit("foo", "v1", "aaa"),
        _tracked_audit("foo", "v1", "bbb"),
    ]
    with pytest.raises(DuplicateCacheKeyError) as excinfo:
        detect_cache_key_collisions(audits)
    msg = str(excinfo.value)
    assert "foo" in msg
    assert "v1" in msg


def test_detect_collisions_passes_when_versions_differ():
    audits = [
        _tracked_audit("foo", "v1", "aaa"),
        _tracked_audit("foo", "v2", "bbb"),
    ]
    detect_cache_key_collisions(audits)


def test_detect_collisions_ignores_non_tracked_status():
    audits = [
        EndpointAudit("/a", "GET", "a", "unknown"),
        EndpointAudit("/b", "GET", "b", "unknown"),
    ]
    detect_cache_key_collisions(audits)


from cache_audit import persist, load_prior


def test_load_prior_missing_file_returns_empty_dict(tmp_hash_file):
    assert load_prior(tmp_hash_file) == {}


def test_load_prior_corrupted_returns_empty_dict(tmp_hash_file):
    tmp_hash_file.write_text("{ not valid json")
    assert load_prior(tmp_hash_file) == {}


def test_persist_writes_upsert_with_meta(tmp_hash_file, seeded_hash_file):
    audits = [
        _tracked_audit("network", "v1", "zzz999"),
    ]
    persist(audits, seeded_hash_file)
    raw = json.loads(seeded_hash_file.read_text())
    assert raw["_meta"]["schema_version"] == 1
    assert "disease_profile" in raw
    assert raw["network"]["hash"] == "zzz999"
    assert raw["network"]["cache_key_version"] == "v1"


def test_persist_preserves_hash_when_source_unavailable(seeded_hash_file):
    audits = [
        EndpointAudit(
            path="/api/disease-profile", method="GET", fn_name="disease_profile",
            status="source_unavailable", cache_key_name="disease_profile",
            version="v1", current_hash="", source_status="source_unavailable",
        ),
    ]
    persist(audits, seeded_hash_file)
    raw = json.loads(seeded_hash_file.read_text())
    assert raw["disease_profile"]["hash"] == "a7b3c1"
    assert raw["disease_profile"]["source_status"] == "source_unavailable"


from cache_audit import reset_endpoint


def test_reset_endpoint_updates_one_leaves_others(seeded_hash_file):
    raw = json.loads(seeded_hash_file.read_text())
    raw["network"] = {"hash": "oldnet", "cache_key_version": "v1",
                       "source_status": "source", "last_seen_utc": "x"}
    seeded_hash_file.write_text(json.dumps(raw))

    audits = [
        _tracked_audit("disease_profile", "v1", "newhash"),
        _tracked_audit("network", "v1", "newnet"),
    ]
    reset_endpoint("disease_profile", seeded_hash_file, audits)

    updated = json.loads(seeded_hash_file.read_text())
    assert updated["disease_profile"]["hash"] == "newhash"
    assert updated["network"]["hash"] == "oldnet"


def test_reset_endpoint_raises_when_name_not_in_audits(seeded_hash_file):
    audits = [_tracked_audit("other", "v1", "zzz")]
    with pytest.raises(KeyError):
        reset_endpoint("disease_profile", seeded_hash_file, audits)


from cache_audit import run


def test_run_first_invocation_seeds_and_returns_report(tmp_hash_file):
    app = _make_fake_app()
    report = run(app, tmp_hash_file)
    assert report.total == 3
    assert report.tracked == 1
    assert "foo" in report.seeded
    assert "bar" in report.unknown
    assert len(report.stale) == 0
    assert report.elapsed_ms >= 0.0
    raw = json.loads(tmp_hash_file.read_text())
    assert "foo" in raw


def test_run_second_invocation_sees_no_changes(tmp_hash_file):
    app = _make_fake_app()
    run(app, tmp_hash_file)
    report = run(app, tmp_hash_file)
    assert len(report.stale) == 0
    assert len(report.seeded) == 0


def test_run_raises_on_duplicate_cache_key(tmp_hash_file):
    app = FastAPI()

    @app.get("/a")
    def a():
        cache_key = "dup_v1"
        return {}

    @app.get("/b")
    def b():
        cache_key = "dup_v1"
        return {}

    with pytest.raises(DuplicateCacheKeyError):
        run(app, tmp_hash_file)
