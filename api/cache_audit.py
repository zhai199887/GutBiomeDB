"""Per-endpoint source-hash observability.

Pure functions. No FastAPI import at module level so unit tests can import
without spinning up the full app.
"""
from __future__ import annotations

import ast
import hashlib
import inspect
import json
import re
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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


@dataclass
class AuditReport:
    total: int
    tracked: int
    elapsed_ms: float
    stale: list[dict]
    seeded: list[str]
    unknown: list[str]
    legacy_unversioned: list[str]
    source_unavailable: list[str]
    ast_parse_failed: list[str]


def compute_report(audits: list[EndpointAudit], prior: dict) -> AuditReport:
    """Diff current audits against prior hash-file contents.

    prior: dict mapping endpoint name -> {"hash": str, "cache_key_version": str}.
           _meta key is ignored by this function.
    """
    stale: list[dict] = []
    seeded: list[str] = []
    unknown: list[str] = []
    legacy_unversioned: list[str] = []
    source_unavailable: list[str] = []
    ast_parse_failed: list[str] = []
    tracked_count = 0

    for a in audits:
        if a.status == "unknown":
            unknown.append(a.fn_name)
            continue
        if a.status == "legacy_unversioned":
            legacy_unversioned.append(a.cache_key_name or a.fn_name)
            continue
        if a.status == "source_unavailable":
            source_unavailable.append(a.fn_name)
            continue
        if a.status == "ast_parse_failed":
            ast_parse_failed.append(a.fn_name)
            continue
        if a.status != "tracked":
            continue

        tracked_count += 1
        assert a.cache_key_name is not None and a.version is not None
        entry = prior.get(a.cache_key_name)
        if entry is None:
            seeded.append(a.cache_key_name)
            continue
        if entry.get("hash") != a.current_hash and entry.get("cache_key_version") == a.version:
            stale.append({
                "name": a.cache_key_name,
                "prior": entry.get("hash", ""),
                "current": a.current_hash,
                "version": a.version,
            })

    return AuditReport(
        total=len(audits),
        tracked=tracked_count,
        elapsed_ms=0.0,
        stale=stale,
        seeded=seeded,
        unknown=unknown,
        legacy_unversioned=legacy_unversioned,
        source_unavailable=source_unavailable,
        ast_parse_failed=ast_parse_failed,
    )


class DuplicateCacheKeyError(Exception):
    """Two endpoints use the same (cache_key_name, version). Fail-fast at startup."""


def detect_cache_key_collisions(audits: list[EndpointAudit]) -> None:
    seen: dict[tuple[str, str], str] = {}
    for a in audits:
        if a.status != "tracked":
            continue
        assert a.cache_key_name is not None and a.version is not None
        key = (a.cache_key_name, a.version)
        if key in seen:
            raise DuplicateCacheKeyError(
                f"cache_key '{a.cache_key_name}_{a.version}' used by "
                f"{seen[key]} AND {a.fn_name}"
            )
        seen[key] = a.fn_name


def load_prior(prior_file: Path) -> dict:
    """Read the hash file. Return empty dict on missing/corrupted, not an error."""
    try:
        raw = prior_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def persist(audits: list[EndpointAudit], prior_file: Path) -> None:
    """Upsert each trackable audit into the hash file, preserve existing entries not touched."""
    existing = load_prior(prior_file)
    out: dict = dict(existing)
    out.setdefault("_meta", {
        "seeded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "schema_version": 1,
    })
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for a in audits:
        if a.status not in ("tracked", "source_unavailable"):
            continue
        if a.cache_key_name is None:
            continue
        prior_entry = existing.get(a.cache_key_name, {})
        if a.status == "source_unavailable":
            out[a.cache_key_name] = {
                **prior_entry,
                "source_status": "source_unavailable",
                "last_seen_utc": now,
            }
        else:
            out[a.cache_key_name] = {
                "hash": a.current_hash,
                "cache_key_version": a.version,
                "source_status": "source",
                "last_seen_utc": now,
            }

    prior_file.parent.mkdir(parents=True, exist_ok=True)
    prior_file.write_text(json.dumps(out, indent=2), encoding="utf-8")


def reset_endpoint(name: str, prior_file: Path, audits: list[EndpointAudit]) -> None:
    """Rewrite a single entry in prior_file using the current audit for `name`.

    Raises KeyError if no audit has cache_key_name == name.
    """
    match = next(
        (a for a in audits
         if a.status == "tracked" and a.cache_key_name == name),
        None,
    )
    if match is None:
        raise KeyError(name)

    existing = load_prior(prior_file)
    existing[name] = {
        "hash": match.current_hash,
        "cache_key_version": match.version,
        "source_status": "source",
        "last_seen_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    prior_file.parent.mkdir(parents=True, exist_ok=True)
    prior_file.write_text(json.dumps(existing, indent=2), encoding="utf-8")


def run(app: Any, prior_file: Path) -> AuditReport:
    """Top-level orchestration. Raises DuplicateCacheKeyError (and nothing else on purpose)."""
    start = time.perf_counter()
    audits = scan_endpoints(app)

    detect_cache_key_collisions(audits)

    prior = load_prior(prior_file)
    report = compute_report(audits, prior)
    persist(audits, prior_file)
    report.elapsed_ms = (time.perf_counter() - start) * 1000.0
    return report
