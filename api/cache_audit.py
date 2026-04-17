"""Per-endpoint source-hash observability.

Pure functions. No FastAPI import at module level so unit tests can import
without spinning up the full app.
"""
from __future__ import annotations

import ast
import hashlib
import inspect
import re
import textwrap
from dataclasses import dataclass
from typing import Any, Callable, Literal

_VERSION_RE = re.compile(r"^([A-Za-z0-9_]+?)_v(\d+)(?::|$)")

Status = Literal[
    "tracked",
    "legacy_unversioned",
    "no_cache_by_design",
    "unknown",
    "ast_parse_failed",
    "source_unavailable",
]


@dataclass
class EndpointAudit:
    path: str
    method: str
    fn_name: str
    status: Status
    cache_key_name: str | None = None
    version: str | None = None
    current_hash: str = ""
    source_status: str = ""


def ast_hash(fn: Callable[..., Any]) -> tuple[str, str]:
    """Return (hex6, source_status).

    source_status:
      - "source"              : inspect.getsource + ast.parse succeeded
      - "source_unavailable"  : inspect.getsource raised OSError (PyInstaller / .pyc-only)
      - "ast_parse_failed"    : ast.parse raised SyntaxError (unlikely in real code)
    """
    try:
        src = inspect.getsource(fn)
    except OSError:
        return ("", "source_unavailable")
    try:
        tree = ast.parse(textwrap.dedent(src))
    except SyntaxError:
        return ("", "ast_parse_failed")
    normal = ast.dump(tree, annotate_fields=False)
    digest = hashlib.sha256(normal.encode("utf-8")).hexdigest()[:6]
    return (digest, "source")


def extract_cache_key_version(fn: Callable[..., Any]) -> tuple[str | None, str | None]:
    """Walk the function body for `cache_key = <str|f-string>` and parse '<name>_v<N>'.

    Returns (name, 'v<N>') on match, (name, None) if assigned but unversioned,
    (None, None) if no cache_key assignment present.
    """
    try:
        src = inspect.getsource(fn)
        tree = ast.parse(textwrap.dedent(src))
    except (OSError, SyntaxError):
        return (None, None)

    literal: str | None = None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        if node.targets[0].id != "cache_key":
            continue
        literal = _first_literal_prefix(node.value)
        break

    if literal is None:
        return (None, None)

    m = _VERSION_RE.match(literal)
    if not m:
        bare = literal.split(":", 1)[0]
        return (bare or None, None)

    return (m.group(1), f"v{m.group(2)}")


def _first_literal_prefix(value: ast.AST) -> str | None:
    """Extract the literal-string head of a Constant / JoinedStr / BinOp(+) node."""
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    if isinstance(value, ast.JoinedStr):
        for part in value.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                return part.value
            break
        return None
    if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Add):
        return _first_literal_prefix(value.left)
    return None


def scan_endpoints(app: Any) -> list[EndpointAudit]:
    """Reflect over FastAPI app.routes and classify each user-defined endpoint."""
    from fastapi.routing import APIRoute

    audits: list[EndpointAudit] = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        fn = route.endpoint
        fn_name = getattr(fn, "__name__", "<unknown>")
        methods = sorted(route.methods or {"GET"})
        method = methods[0]

        if getattr(fn, "_no_cache_tracking", False):
            audits.append(EndpointAudit(
                path=route.path, method=method, fn_name=fn_name,
                status="no_cache_by_design",
            ))
            continue

        name, version = extract_cache_key_version(fn)
        hex6, src_status = ast_hash(fn)

        if src_status == "source_unavailable":
            status: Status = "source_unavailable"
        elif src_status == "ast_parse_failed":
            status = "ast_parse_failed"
        elif name is None:
            status = "unknown"
        elif version is None:
            status = "legacy_unversioned"
        else:
            status = "tracked"

        audits.append(EndpointAudit(
            path=route.path, method=method, fn_name=fn_name,
            status=status, cache_key_name=name, version=version,
            current_hash=hex6, source_status=src_status,
        ))
    return audits
