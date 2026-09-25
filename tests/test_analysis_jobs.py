"""Unit tests for the trackable analysis-job registry."""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))

from analysis_jobs import AnalysisJobStore  # noqa: E402


def _wait_for(store: AnalysisJobStore, job_id: str):
    deadline = time.time() + 3
    while time.time() < deadline:
        job = store.get(job_id)
        if job and job["status"] in {"completed", "failed"}:
            return job
        time.sleep(0.01)
    raise AssertionError("analysis job did not finish")


def test_job_completes_and_persists_result(tmp_path):
    store = AnalysisJobStore(tmp_path, max_workers=1)
    submitted = store.submit("test", lambda: {"value": 42})
    assert submitted["job_id"].startswith("job_")
    completed = _wait_for(store, submitted["job_id"])
    assert completed["status"] == "completed"
    assert completed["result"] == {"value": 42}


def test_job_failure_is_trackable(tmp_path):
    def fail():
        raise ValueError("example failure")

    store = AnalysisJobStore(tmp_path, max_workers=1)
    submitted = store.submit("test", fail)
    failed = _wait_for(store, submitted["job_id"])
    assert failed["status"] == "failed"
    assert failed["error"] == "example failure"


def test_restart_marks_incomplete_jobs_failed(tmp_path):
    snapshot = tmp_path / "job_restart.json"
    snapshot.write_text(
        '{"job_id":"job_restart","kind":"test","status":"running",'
        '"created_at":"2026-01-01T00:00:00+00:00",'
        '"updated_at":"2026-01-01T00:00:00+00:00"}',
        encoding="utf-8",
    )
    store = AnalysisJobStore(tmp_path, max_workers=1)
    recovered = store.get("job_restart")
    assert recovered is not None
    assert recovered["status"] == "failed"
