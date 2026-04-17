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
