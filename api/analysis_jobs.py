"""Small persistent job registry for long-running analysis requests.

Jobs are intentionally keyed by an opaque ID rather than a user account. The
browser stores the ID locally and can resume polling after navigation. Request
payloads are not persisted; this keeps user-uploaded analysis inputs in memory
only and avoids changing the API's existing data-handling boundary.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


JobRunner = Callable[[], Any]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class AnalysisJobStore:
    """Thread-safe job metadata/result store with atomic per-job snapshots."""

    def __init__(self, root: Path | None = None, max_workers: int = 1):
        configured = os.getenv("GBDB_ANALYSIS_JOB_DIR", "").strip()
        self.root = Path(configured) if configured else (root or Path(__file__).parent / "_analysis_jobs")
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="analysis-job")
        self._load_snapshots()

    def _path(self, job_id: str) -> Path:
        return self.root / f"{job_id}.json"

    def _write(self, record: dict[str, Any]) -> None:
        path = self._path(record["job_id"])
        temporary = path.with_suffix(".json.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False, separators=(",", ":"), default=str)
            os.replace(temporary, path)
        except Exception:
            logging.exception("Could not persist analysis job %s", record.get("job_id"))
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def _load_snapshots(self) -> None:
        for path in self.root.glob("job_*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                job_id = str(record.get("job_id", ""))
                if not job_id:
                    continue
                if record.get("status") in {"queued", "running"}:
                    record["status"] = "failed"
                    record["error"] = "Server restarted before this job completed."
                    record["updated_at"] = _now()
                    self._write(record)
                self._jobs[job_id] = record
            except (OSError, ValueError, TypeError):
                logging.warning("Ignoring invalid analysis job snapshot: %s", path)

    def submit(self, kind: str, runner: JobRunner) -> dict[str, Any]:
        job_id = f"job_{secrets.token_urlsafe(9).replace('-', '_').replace('_', '')[:12]}"
        record = {
            "job_id": job_id,
            "kind": kind,
            "status": "queued",
            "created_at": _now(),
            "updated_at": _now(),
        }
        with self._lock:
            self._jobs[job_id] = record
            self._write(record)
        self._executor.submit(self._run, job_id, runner)
        return self.public(record)

    def _run(self, job_id: str, runner: JobRunner) -> None:
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                return
            record["status"] = "running"
            record["updated_at"] = _now()
            self._write(record)
        try:
            result = runner()
        except Exception as exc:  # the API exposes a stable failed status to pollers
            logging.exception("Analysis job %s failed", job_id)
            with self._lock:
                record = self._jobs[job_id]
                record.update({"status": "failed", "error": str(exc), "updated_at": _now()})
                self._write(record)
            return
        with self._lock:
            record = self._jobs[job_id]
            record.update({"status": "completed", "result": result, "updated_at": _now()})
            self._write(record)

    @staticmethod
    def public(record: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in record.items()
            if key in {"job_id", "kind", "status", "created_at", "updated_at", "error", "result"}
        }

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._jobs.get(job_id)
            return None if record is None else self.public(record)
