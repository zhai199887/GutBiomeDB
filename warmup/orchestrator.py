#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path


WARMUP_DIR = Path("/opt/gutbiomedb/warmup")
PLAN_DIR = WARMUP_DIR / "plan"
LOG_DIR = WARMUP_DIR / "logs"
STATE_DIR = WARMUP_DIR / ".orchestrator"
STATE_DIR.mkdir(exist_ok=True)
BLOCKED_KEYS_PATH = STATE_DIR / "blocked_keys.jsonl"

API_HEALTH_URL = "http://127.0.0.1:8000/api/health"

PLAN_ORDER = [
    "disease_profile_v1",
    "disease_studies_v1",
    "lollipop_v1",
    "cooccurrence_v1",
    "network_compare_v1",
    "project_detail_v1",
    "biomarker_discovery_v1",
    "species_profile_v1",
    "biomarker_profile_v1",
    "species_cooccurrence_v1",
    "phenotype_taxa_profile_v1",
    "lifecycle_v9",
    "lifecycle_compare_v3",
    "phenotype_assoc_v1",
]

LIGHT_LANE_PLANS = {"biomarker_discovery_v1"}
PLAN_BATCH_SIZE = {
    "biomarker_discovery_v1": int(os.environ.get("GBDB_WARMUP_BATCH_BIOMARKER", "4")),
    "cooccurrence_v1": int(os.environ.get("GBDB_WARMUP_BATCH_COOCCURRENCE", "4")),
    "network_compare_v1": int(os.environ.get("GBDB_WARMUP_BATCH_NETWORK_COMPARE", "4")),
    "project_detail_v1": int(os.environ.get("GBDB_WARMUP_BATCH_PROJECT_DETAIL", "8")),
    "species_profile_v1": int(os.environ.get("GBDB_WARMUP_BATCH_SPECIES_PROFILE", "8")),
    "biomarker_profile_v1": int(os.environ.get("GBDB_WARMUP_BATCH_BIOMARKER_PROFILE", "4")),
    "species_cooccurrence_v1": int(os.environ.get("GBDB_WARMUP_BATCH_SPECIES_COOCCURRENCE", "4")),
    "phenotype_taxa_profile_v1": int(os.environ.get("GBDB_WARMUP_BATCH_PHENOTYPE_TAXA", "4")),
}
PLAN_MEMORY_MAX = {
    "biomarker_discovery_v1": os.environ.get("GBDB_WARMUP_MEMORYMAX_BIOMARKER", "3G"),
}

MAX_ACTIVE_JOBS = 2
MAX_ACTIVE_PER_LANE = 1

READY_MAX_MEM_MB = 10240
READY_MIN_AVAIL_MB = 10240
READY_MAX_HEALTH_MS = 100
READY_MAX_SWAP_MB = int(os.environ.get("GBDB_WARMUP_READY_MAX_SWAP_MB", "6144"))
READY_MIN_UPTIME_SEC = 3600

SECOND_LANE_MAX_MEM_MB = int(os.environ.get("GBDB_WARMUP_SECOND_LANE_MAX_MEM_MB", "10240"))
SECOND_LANE_MIN_AVAIL_MB = int(os.environ.get("GBDB_WARMUP_SECOND_LANE_MIN_AVAIL_MB", "8192"))
SECOND_LANE_MAX_HEALTH_MS = int(os.environ.get("GBDB_WARMUP_SECOND_LANE_MAX_HEALTH_MS", "100"))
SECOND_LANE_MAX_SWAP_MB = int(os.environ.get("GBDB_WARMUP_SECOND_LANE_MAX_SWAP_MB", "6144"))

RUN_MAX_MEM_MB = 14336
RUN_MIN_AVAIL_MB = 4096
RUN_MAX_HEALTH_MS = 500
RUN_MAX_SWAP_MB = int(os.environ.get("GBDB_WARMUP_RUN_MAX_SWAP_MB", "6144"))

INITIAL_READY_STABLE_SAMPLES = 6
INTER_KEY_STABLE_SAMPLES = 2
READY_STABLE_INTERVAL_SEC = 10
RUN_POLL_SEC = 5
FAILURE_BACKOFF_SEC = 21600
MAX_KEY_FAILS = 2
IDLE_SLEEP_SEC = 15


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}", flush=True)


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def run_ok(cmd: list[str]) -> bool:
    return subprocess.run(cmd, text=True, capture_output=True).returncode == 0


def cache_path(key: str) -> Path:
    safe = key.replace(":", "_").replace("/", "_")
    return Path("/opt/gutbiomedb/api/_disk_cache") / f"{safe}.json"


def meminfo() -> dict[str, int]:
    out: dict[str, int] = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, value = line.split(":", 1)
            out[key] = int(value.strip().split()[0])
    return out


def service_memory_mb() -> int:
    out = run(["systemctl", "show", "gutbiomedb.service", "-p", "MemoryCurrent", "--value"]).stdout.strip()
    if not out or out == "[not set]":
        return 0
    try:
        return int(int(out) / 1024 / 1024)
    except ValueError:
        return 0


def worker_count() -> int:
    out = run(
        [
            "bash",
            "-lc",
            "systemctl list-units 'gbdb-warmup-key-*' --state=active,activating --no-legend --plain "
            "| awk 'NF {count++} END {print count+0}'",
        ],
        check=False,
    ).stdout.strip()
    try:
        return int(out)
    except ValueError:
        return 0


def health_probe() -> tuple[int, int]:
    t0 = time.time()
    try:
        with urllib.request.urlopen(API_HEALTH_URL, timeout=5) as resp:
            resp.read()
            code = getattr(resp, "status", 200)
    except urllib.error.HTTPError as exc:
        code = exc.code
    except Exception:
        return 0, 999999
    return code, int((time.time() - t0) * 1000)


def titlecase_active() -> bool:
    return run(["systemctl", "is-active", "gbdb-titlecase-warmup.service"], check=False).stdout.strip() in {
        "active",
        "activating",
    }


def service_uptime_sec() -> int:
    out = run(
        ["systemctl", "show", "gutbiomedb.service", "-p", "ActiveEnterTimestampMonotonic", "--value"],
        check=False,
    ).stdout.strip()
    if not out or out == "0":
        return 0
    try:
        start_us = int(out)
    except ValueError:
        return 0
    with open("/proc/uptime") as f:
        uptime_sec = float(f.read().split()[0])
    now_us = int(uptime_sec * 1_000_000)
    return max(0, int((now_us - start_us) / 1_000_000))


def probe() -> dict[str, int]:
    mi = meminfo()
    code, health_ms = health_probe()
    return {
        "mem_mb": service_memory_mb(),
        "avail_mb": int(mi.get("MemAvailable", 0) / 1024),
        "swap_mb": int((mi.get("SwapTotal", 0) - mi.get("SwapFree", 0)) / 1024),
        "health_code": code,
        "health_ms": health_ms,
        "uptime_sec": service_uptime_sec(),
        "worker_count": worker_count(),
        "titlecase_active": 1 if titlecase_active() else 0,
    }


def append_log_row(path: Path, key: str, status: int, reason: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="") as f:
        csv.writer(f).writerow([key, status, reason[:80], f"{time.time():.0f}"])


def stop_titlecase_if_needed() -> None:
    if titlecase_active():
        log("titlecase-stop: gbdb-titlecase-warmup.service")
        subprocess.run(["systemctl", "stop", "gbdb-titlecase-warmup.service"], text=True, capture_output=True)


def safe_window_ready(p: dict[str, int]) -> bool:
    return (
        p["worker_count"] == 0
        and p["titlecase_active"] == 0
        and p["health_code"] == 200
        and p["health_ms"] < READY_MAX_HEALTH_MS
        and p["mem_mb"] < READY_MAX_MEM_MB
        and p["avail_mb"] > READY_MIN_AVAIL_MB
        and p["swap_mb"] < READY_MAX_SWAP_MB
        and p["uptime_sec"] >= READY_MIN_UPTIME_SEC
    )


def wait_for_safe_window(reason: str, stable_samples: int) -> None:
    stable = 0
    while True:
        stop_titlecase_if_needed()
        p = probe()
        log(
            "ready-check: "
            f"mem={p['mem_mb']}MB avail={p['avail_mb']}MB swap={p['swap_mb']}MB "
            f"health={p['health_code']} health_ms={p['health_ms']} uptime={p['uptime_sec']}s "
            f"workers={p['worker_count']} titlecase_active={p['titlecase_active']} "
            f"reason={reason} stable={stable}/{stable_samples}"
        )
        if safe_window_ready(p):
            stable += 1
            if stable >= stable_samples:
                return
        else:
            stable = 0
        time.sleep(READY_STABLE_INTERVAL_SEC)


def prelaunch_ready() -> bool:
    stop_titlecase_if_needed()
    p = probe()
    log(
        "prelaunch: "
        f"mem={p['mem_mb']}MB avail={p['avail_mb']}MB swap={p['swap_mb']}MB "
        f"health={p['health_code']} health_ms={p['health_ms']} uptime={p['uptime_sec']}s "
        f"workers={p['worker_count']} titlecase_active={p['titlecase_active']}"
    )
    return safe_window_ready(p)


def ensure_ready() -> None:
    if prelaunch_ready():
        return
    wait_for_safe_window("prelaunch", INITIAL_READY_STABLE_SAMPLES)


def plan_lane(name: str) -> str:
    return "light" if name in LIGHT_LANE_PLANS else "heavy"


def plan_batch_size(name: str) -> int:
    return max(1, PLAN_BATCH_SIZE.get(name, 1))


def plan_memory_max(name: str) -> str:
    return PLAN_MEMORY_MAX.get(name, "4G")


def lane_counts(active_jobs: dict[str, "ActiveJob"]) -> dict[str, int]:
    counts = {"heavy": 0, "light": 0}
    for job in active_jobs.values():
        counts[job.lane] += 1
    return counts


def launch_window_ok(p: dict[str, int], active_jobs: dict[str, "ActiveJob"], lane: str) -> bool:
    if p["titlecase_active"] == 1 or p["health_code"] != 200 or p["uptime_sec"] < READY_MIN_UPTIME_SEC:
        return False
    if len(active_jobs) >= MAX_ACTIVE_JOBS:
        return False
    if lane_counts(active_jobs)[lane] >= MAX_ACTIVE_PER_LANE:
        return False
    if not active_jobs:
        return safe_window_ready(p)
    return (
        p["worker_count"] < MAX_ACTIVE_JOBS
        and p["health_ms"] < SECOND_LANE_MAX_HEALTH_MS
        and p["mem_mb"] < SECOND_LANE_MAX_MEM_MB
        and p["avail_mb"] > SECOND_LANE_MIN_AVAIL_MB
        and p["swap_mb"] < SECOND_LANE_MAX_SWAP_MB
    )


def run_guard_ok(p: dict[str, int]) -> bool:
    return (
        p["titlecase_active"] == 0
        and p["health_code"] == 200
        and p["health_ms"] < RUN_MAX_HEALTH_MS
        and p["mem_mb"] < RUN_MAX_MEM_MB
        and p["avail_mb"] > RUN_MIN_AVAIL_MB
        and p["swap_mb"] < RUN_MAX_SWAP_MB
        and p["worker_count"] <= MAX_ACTIVE_JOBS
    )


def unit_is_active(unit: str) -> bool:
    return run(["systemctl", "is-active", unit], check=False).stdout.strip() in {"active", "activating"}


def unit_name(name: str) -> str:
    return f"gbdb-warmup-key-{name.replace('_', '-')}"


@dataclass
class PlanState:
    name: str
    rate_per_min: int
    keys: list[str]
    queue: deque[str]
    fail_count: dict[str, int] = field(default_factory=dict)
    last_fail_ts: dict[str, float] = field(default_factory=dict)
    blocked: set[str] = field(default_factory=set)

    @property
    def log_path(self) -> Path:
        return LOG_DIR / f"{self.name}.csv"

    @property
    def done_path(self) -> Path:
        return WARMUP_DIR / f"DONE.{self.name}"


@dataclass
class ActiveJob:
    state: PlanState
    keys: list[str]
    lane: str
    unit: str
    plan_path: Path
    done_path: Path
    log_offset: int
    tripped: bool = False
    tripped_reason: str = ""


def load_blocked_keys() -> dict[str, set[str]]:
    blocked: dict[str, set[str]] = {}
    if not BLOCKED_KEYS_PATH.exists():
        return blocked
    with BLOCKED_KEYS_PATH.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            plan = row.get("plan")
            key = row.get("key")
            if not plan or not key:
                continue
            blocked.setdefault(plan, set()).add(key)
    return blocked


def append_blocked_key(state: PlanState, key: str, reason: str) -> None:
    row = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "plan": state.name,
        "key": key,
        "reason": reason[:160],
        "fail_count": state.fail_count.get(key, 0),
    }
    with BLOCKED_KEYS_PATH.open("a") as f:
        f.write(json.dumps(row, ensure_ascii=True) + "\n")


def load_plan_state(name: str, blocked_keys: set[str]) -> PlanState:
    plan_path = PLAN_DIR / f"{name}.json"
    data = json.loads(plan_path.read_text())
    keys = list(data["keys"])
    rate_per_min = max(1, int(data.get("rate_per_min", 1)))

    success: set[str] = set()
    fail_count: dict[str, int] = {}
    log_path = LOG_DIR / f"{name}.csv"
    if log_path.exists():
        with log_path.open() as f:
            for row in csv.reader(f):
                if len(row) < 2:
                    continue
                key = row[0]
                status = row[1]
                if status == "200":
                    success.add(key)
                else:
                    fail_count[key] = fail_count.get(key, 0) + 1

    pending_buckets: dict[int, list[str]] = {}
    for key in keys:
        if key in success or cache_path(key).exists() or key in blocked_keys:
            continue
        pending_buckets.setdefault(fail_count.get(key, 0), []).append(key)

    queue: deque[str] = deque()
    for bucket in sorted(pending_buckets):
        queue.extend(pending_buckets[bucket])

    return PlanState(
        name=name,
        rate_per_min=rate_per_min,
        keys=keys,
        queue=queue,
        fail_count=fail_count,
        blocked=set(blocked_keys),
    )


def write_done_if_complete(state: PlanState) -> None:
    if state.queue or state.done_path.exists() or state.blocked:
        return
    summary = {
        "plan": str(PLAN_DIR / f"{state.name}.json"),
        "total_keys": len(state.keys),
        "ok_or_cached": len(state.keys),
        "orchestrated_by": "orchestrator.py",
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    state.done_path.write_text(json.dumps(summary, indent=2) + "\n")
    log(f"plan-done: {state.name}")


def make_temp_plan(state: PlanState, keys: list[str]) -> tuple[Path, Path]:
    plan_path = STATE_DIR / f"{state.name}.current.json"
    done_path = STATE_DIR / f"DONE.{state.name}.current.json"
    plan_path.write_text(json.dumps({"rate_per_min": state.rate_per_min, "keys": keys}, indent=2) + "\n")
    if done_path.exists():
        done_path.unlink()
    return plan_path, done_path


def prepend_keys(state: PlanState, keys: list[str]) -> None:
    for key in reversed(keys):
        state.queue.appendleft(key)


def pop_ready_batch(state: PlanState) -> list[str]:
    if not state.queue:
        return []
    batch: list[str] = []
    batch_size = plan_batch_size(state.name)
    remaining = len(state.queue)
    now = time.time()
    while state.queue and remaining > 0 and len(batch) < batch_size:
        key = state.queue.popleft()
        remaining -= 1
        fail_count = state.fail_count.get(key, 0)
        if fail_count >= 1 and (now - state.last_fail_ts.get(key, 0)) < FAILURE_BACKOFF_SEC:
            state.queue.append(key)
            continue
        batch.append(key)
    return batch


def launch_batch(state: PlanState, keys: list[str]) -> ActiveJob | None:
    plan_path, done_path = make_temp_plan(state, keys)
    unit = unit_name(state.name)
    log_offset = state.log_path.stat().st_size if state.log_path.exists() else 0
    subprocess.run(["systemctl", "stop", unit], text=True, capture_output=True)
    cmd = [
        "systemd-run",
        "--unit",
        unit,
        f"--property=MemoryMax={plan_memory_max(state.name)}",
        "--property=CPUWeight=50",
        "--working-directory=/opt/gutbiomedb/warmup",
        "/usr/bin/python3",
        "warmup_full.py",
        "--plan",
        str(plan_path),
        "--log",
        str(state.log_path),
        "--done",
        str(done_path),
    ]
    out = run(cmd, check=False)
    if out.returncode != 0:
        for key in keys:
            append_log_row(state.log_path, key, -8, "systemd-run-launch-failed")
        for path in (plan_path, done_path):
            if path.exists():
                path.unlink()
        log(f"launch-failed: unit={unit} rc={out.returncode} stderr={out.stderr.strip()[:200]}")
        return None
    log(
        f"launch: unit={unit} plan={state.name} lane={plan_lane(state.name)} "
        f"batch={len(keys)} first_key={keys[0]}"
    )
    return ActiveJob(
        state=state,
        keys=keys,
        lane=plan_lane(state.name),
        unit=unit,
        plan_path=plan_path,
        done_path=done_path,
        log_offset=log_offset,
    )


def job_rows(job: ActiveJob) -> dict[str, tuple[int, str]]:
    rows: dict[str, tuple[int, str]] = {}
    if not job.state.log_path.exists():
        return rows
    with job.state.log_path.open() as f:
        f.seek(job.log_offset)
        for row in csv.reader(f):
            if len(row) < 3:
                continue
            key = row[0]
            if key not in job.keys:
                continue
            try:
                status = int(row[1])
            except ValueError:
                continue
            rows[key] = (status, row[2])
    return rows


def is_permanent_failure(status: int | None) -> bool:
    return status is not None and 400 <= status < 500 and status != 429


def finalize_job(job: ActiveJob) -> tuple[int, int]:
    result = run(["systemctl", "show", job.unit, "-p", "Result", "--value"], check=False).stdout.strip()
    status = run(["systemctl", "show", job.unit, "-p", "ExecMainStatus", "--value"], check=False).stdout.strip()
    rows = job_rows(job)
    ok_count = 0
    fail_count = 0
    blocked_count = 0
    now = time.time()

    for key in job.keys:
        row_status, row_reason = rows.get(key, (None, "no-log-row"))
        if cache_path(key).exists() or row_status == 200:
            ok_count += 1
            continue

        if job.tripped and row_status is None:
            job.state.last_fail_ts[key] = now
            job.state.queue.append(key)
            fail_count += 1
            reason = job.tripped_reason or "circuit-break"
            log(f"key-requeue: plan={job.state.name} key={key} fail_count=guard-stop reason={reason}")
            continue

        next_fail = job.state.fail_count.get(key, 0) + 1
        job.state.fail_count[key] = next_fail
        job.state.last_fail_ts[key] = now
        reason = row_reason if row_reason else "unknown-failure"

        if is_permanent_failure(row_status):
            job.state.blocked.add(key)
            append_blocked_key(job.state, key, f"permanent-{row_status}:{reason}")
            blocked_count += 1
            log(f"key-blocked: plan={job.state.name} key={key} status={row_status} reason={reason}")
            continue

        fail_count += 1
        if next_fail >= MAX_KEY_FAILS:
            job.state.blocked.add(key)
            append_blocked_key(job.state, key, f"max-failures:{reason}")
            blocked_count += 1
            log(f"key-blocked: plan={job.state.name} key={key} reason=max-failures:{reason}")
        else:
            job.state.queue.append(key)
            log(f"key-requeue: plan={job.state.name} key={key} fail_count={next_fail} reason={reason}")

    log(
        f"unit-exit: unit={job.unit} result={result or 'unknown'} exec_status={status or 'unknown'} "
        f"tripped={int(job.tripped)} ok={ok_count} fail={fail_count} blocked={blocked_count}"
    )
    subprocess.run(["systemctl", "reset-failed", job.unit], text=True, capture_output=True)
    for path in (job.plan_path, job.done_path):
        if path.exists():
            path.unlink()
    return fail_count, blocked_count


def stop_active_jobs(active_jobs: dict[str, ActiveJob], reason: str) -> None:
    for job in active_jobs.values():
        if job.tripped:
            continue
        job.tripped = True
        job.tripped_reason = reason
        log(f"circuit-break: stop unit={job.unit} plan={job.state.name} reason={reason}")
        subprocess.run(["systemctl", "stop", job.unit], text=True, capture_output=True)


def maybe_launch_lane(states: list[PlanState], active_jobs: dict[str, ActiveJob], lane: str) -> bool:
    p = probe()
    if not launch_window_ok(p, active_jobs, lane):
        return False

    active_plans = {job.state.name for job in active_jobs.values()}
    for state in states:
        if state.name in active_plans or not state.queue or plan_lane(state.name) != lane:
            continue
        batch = pop_ready_batch(state)
        if not batch:
            continue
        job = launch_batch(state, batch)
        if job is None:
            prepend_keys(state, batch)
            return False
        active_jobs[job.unit] = job
        return True
    return False


def poll_active_jobs(active_jobs: dict[str, ActiveJob]) -> tuple[bool, bool]:
    if not active_jobs:
        return False, False

    stop_titlecase_if_needed()
    p = probe()
    counts = lane_counts(active_jobs)
    log(
        "run-guard: "
        f"active_total={len(active_jobs)} heavy={counts['heavy']} light={counts['light']} "
        f"mem={p['mem_mb']}MB avail={p['avail_mb']}MB swap={p['swap_mb']}MB "
        f"health={p['health_code']} health_ms={p['health_ms']} workers={p['worker_count']}"
    )
    if not run_guard_ok(p):
        stop_active_jobs(
            active_jobs,
            (
                f"mem={p['mem_mb']} avail={p['avail_mb']} swap={p['swap_mb']} "
                f"health={p['health_code']} health_ms={p['health_ms']} workers={p['worker_count']}"
            ),
        )

    any_failures = False
    any_blocked = False
    finished_units = [unit for unit, job in active_jobs.items() if not unit_is_active(unit)]
    for unit in finished_units:
        job = active_jobs.pop(unit)
        fail_count, blocked_count = finalize_job(job)
        any_failures = any_failures or fail_count > 0 or job.tripped
        any_blocked = any_blocked or blocked_count > 0
    if not finished_units:
        time.sleep(RUN_POLL_SEC)
    return any_failures, any_blocked


def load_states() -> list[PlanState]:
    states: list[PlanState] = []
    blocked_by_plan = load_blocked_keys()
    for name in PLAN_ORDER:
        done_path = WARMUP_DIR / f"DONE.{name}"
        if done_path.exists():
            states.append(PlanState(name=name, rate_per_min=1, keys=[], queue=deque()))
            continue
        states.append(load_plan_state(name, blocked_by_plan.get(name, set())))
    return states


def main() -> int:
    log("orchestrator-py start")
    states = load_states()
    active_jobs: dict[str, ActiveJob] = {}

    while True:
        pending = [s for s in states if s.queue]
        if not pending and not active_jobs:
            blocked_total = sum(len(s.blocked) for s in states)
            if blocked_total:
                log(f"all runnable keys complete; blocked_keys={blocked_total}")
            else:
                log("all plans complete")
            return 0

        if not active_jobs:
            ensure_ready()

        launched = False
        for lane in ("heavy", "light"):
            launched = maybe_launch_lane(states, active_jobs, lane) or launched

        any_failures, _any_blocked = poll_active_jobs(active_jobs)

        if not active_jobs and any_failures and not prelaunch_ready():
            wait_for_safe_window("post-failure", INTER_KEY_STABLE_SAMPLES)

        for state in states:
            write_done_if_complete(state)

        if not active_jobs and not launched:
            time.sleep(IDLE_SLEEP_SEC)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log(f"fatal: {type(exc).__name__}: {exc}")
        raise
