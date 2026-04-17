"""Per-endpoint source-hash observability.

Pure functions. No FastAPI import at module level so unit tests can import
without spinning up the full app.
"""
from __future__ import annotations

import ast
import hashlib
import inspect
from typing import Any, Callable


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
        tree = ast.parse(src)
    except SyntaxError:
        return ("", "ast_parse_failed")
    normal = ast.dump(tree, annotate_fields=False)
    digest = hashlib.sha256(normal.encode("utf-8")).hexdigest()[:6]
    return (digest, "source")
