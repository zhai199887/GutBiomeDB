"""Unit tests for api/cache_audit.py pure functions."""
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
