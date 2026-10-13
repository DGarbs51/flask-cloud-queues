"""Async queue throughput test: dispatch N jobs, time how fast a worker drains them.

app.py calls install() once with its registry, telemetry and logger. throughput.py never
imports app, so there is no import cycle and the math below tests without Redis.

Run state is one Redis hash per run (<KEY_PREFIX>throughput:<run>): "count", "started_at", plus per
job uuid "d:" deliveries, "w:" wait (s) and "f:" finish time. The job writes with HSETNX, so
a redelivery only bumps "d:" and counts as a duplicate.
"""

from __future__ import annotations

import asyncio
import json
import math
import threading
import time
import uuid

from laravel_cloud_queues import current_job

from telemetry import KEY_PREFIX, TIMEOUT

PREFIX = KEY_PREFIX + "throughput:"
ACTIVE = PREFIX + "active"  # holds the run id of the one run allowed at a time
# Delete the lock only if this run still owns it.
RELEASE = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0"
TTL = 3600
DEFAULT_COUNT = 1000
MAX_COUNT = 10_000
# 50 jobs/s: one worker process clears this easily; less means the queue or workers are starved.
MIN_JOBS_PER_S = 50
# 15 s: a 1000-job burst at the minimum rate waits ~20 s at the tail; this flags it as slow, not lost.
MAX_P95_WAIT_MS = 15_000

_telemetry = _log = tick = None  # set by install()
_tasks: set[asyncio.Task[None]] = set()


def deadline(count: int) -> float:
    """Seconds a run may take before unprocessed jobs count as lost."""
    return min(300, 30 + count * 0.05)


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p / 100 * len(ordered)) - 1)]


def verdict(count: int, processed: int, duplicates: int, jobs_per_s: float | None, p95_ms: float | None) -> str:
    if processed < count or duplicates:
        return "fail"
    if (jobs_per_s or 0) < MIN_JOBS_PER_S or (p95_ms or 0) > MAX_P95_WAIT_MS:
        return "warn"
    return "pass"


def parse_count(data: dict) -> int | str:
    """The validated count, or an error message."""
    count = data.get("count", DEFAULT_COUNT)
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= MAX_COUNT:
        return f"count must be an integer from 1 to {MAX_COUNT}"
    return count


def install(registry, telemetry, log) -> None:
    global _telemetry, _log, tick
    _telemetry, _log = telemetry, log

    async def _tick(run: str, queued_at: float) -> None:
        started = time.time()
        await asyncio.sleep(0)
        uid = current_job().uuid
        pipe = _telemetry.store.pipeline()
        pipe.hincrby(PREFIX + run, "d:" + uid, 1)
        # Wait spans web and worker clocks, so it includes any clock skew between them.
        pipe.hsetnx(PREFIX + run, "w:" + uid, started - queued_at)
        pipe.hsetnx(PREFIX + run, "f:" + uid, time.time())
        # A job delivered after the run expired recreates the hash; give it a TTL so it can't leak.
        pipe.expire(PREFIX + run, TTL, nx=True)
        pipe.execute()

    tick = registry.job(name="throughput.tick")(_tick)


def _dispatch(run: str, count: int) -> None:
    try:
        for _ in range(count):
            tick.dispatch(run, time.time())
    except Exception as exc:
        _log.exception("throughput dispatch failed", extra=dict(run=run))
        pipe = _telemetry.store.pipeline()
        pipe.hset(PREFIX + run, "error", str(exc)[:200])
        pipe.expire(PREFIX + run, TTL, nx=True)  # never leave a TTL-less hash behind
        pipe.execute()


async def _adispatch(run: str, count: int) -> None:
    try:
        for _ in range(count):
            async with asyncio.timeout(TIMEOUT):
                await tick.dispatch_async(run, time.time())
    except Exception as exc:
        _log.exception("throughput dispatch failed", extra=dict(run=run))
        pipe = _telemetry.astore.pipeline()
        pipe.hset(PREFIX + run, "error", str(exc)[:200])
        pipe.expire(PREFIX + run, TTL, nx=True)
        async with asyncio.timeout(TIMEOUT):
            await pipe.execute()


def _dispatch_done(task: asyncio.Task[None]) -> None:
    _tasks.discard(task)
    if not task.cancelled() and (exc := task.exception()) is not None:
        _log.error("throughput dispatch error could not be saved", exc_info=exc)


def start(data: dict) -> tuple[int, dict]:
    """POST /api/throughput: returns (status, body)."""
    count = parse_count(data)
    if isinstance(count, str):
        return 400, {"error": count}
    store, run = _telemetry.store, uuid.uuid4().hex
    ttl = int(deadline(count)) + 60
    # One run at a time across every web process and replica. status() frees the lock when the
    # run finishes; the TTL frees it if nobody polls.
    if not store.set(ACTIVE, run, nx=True, ex=ttl):
        return 409, {"error": "a throughput run is already active", "run": store.get(ACTIVE)}
    pipe = store.pipeline()
    pipe.hset(PREFIX + run, mapping={"count": count, "started_at": time.time()})
    pipe.expire(PREFIX + run, TTL)
    pipe.execute()
    _log.info("throughput started", extra=dict(run=run, count=count))
    # The POST must return fast (gunicorn's worker timeout is 30 s), so dispatch off-request.
    threading.Thread(target=_dispatch, args=(run, count), daemon=True).start()
    return 202, {"run": run}


async def astart(data: dict) -> tuple[int, dict]:
    """POST /api/throughput on the ASGI request loop."""
    count = parse_count(data)
    if isinstance(count, str):
        return 400, {"error": count}
    store, run = _telemetry.astore, uuid.uuid4().hex
    ttl = int(deadline(count)) + 60
    async with asyncio.timeout(TIMEOUT):
        if not await store.set(ACTIVE, run, nx=True, ex=ttl):
            return 409, {"error": "a throughput run is already active", "run": await store.get(ACTIVE)}
        pipe = store.pipeline()
        pipe.hset(PREFIX + run, mapping={"count": count, "started_at": time.time()})
        pipe.expire(PREFIX + run, TTL)
        await pipe.execute()
    _log.info("throughput started", extra=dict(run=run, count=count))
    task = asyncio.create_task(_adispatch(run, count))
    _tasks.add(task)
    task.add_done_callback(_dispatch_done)
    return 202, {"run": run}


def status(run: str) -> tuple[int, dict]:
    """GET /api/throughput/<run>: returns (status, body)."""
    store = _telemetry.store
    raw = store.hgetall(PREFIX + run)
    code, body = _status(raw)
    if code != 200 or "result" in raw or body["state"] == "running":
        return code, body
    # First finisher freezes the result so later polls (and late jobs) cannot change it.
    pipe = store.pipeline()
    pipe.hsetnx(PREFIX + run, "result", json.dumps(body))
    pipe.expire(PREFIX + run, TTL, nx=True)  # the run may have expired since HGETALL
    if not pipe.execute()[0]:
        # Another process froze it first, maybe from a different snapshot; its answer is the answer.
        return 200, json.loads(store.hget(PREFIX + run, "result"))
    store.eval(RELEASE, 1, ACTIVE, run)
    _log.info("throughput finished", extra=dict(run=run, verdict=body["verdict"],
                                                jobs_per_s=body["jobs_per_s"], p95_ms=body["wait_ms"]["p95"]))
    return 200, body


async def astatus(run: str) -> tuple[int, dict]:
    """GET /api/throughput/<run> on the ASGI request loop."""
    store = _telemetry.astore
    async with asyncio.timeout(TIMEOUT):
        raw = await store.hgetall(PREFIX + run)
        code, body = _status(raw)
        if code != 200 or "result" in raw or body["state"] == "running":
            return code, body
        pipe = store.pipeline()
        pipe.hsetnx(PREFIX + run, "result", json.dumps(body))
        pipe.expire(PREFIX + run, TTL, nx=True)
        if not (await pipe.execute())[0]:
            return 200, json.loads(await store.hget(PREFIX + run, "result"))
        await store.eval(RELEASE, 1, ACTIVE, run)
    _log.info("throughput finished", extra=dict(run=run, verdict=body["verdict"],
                                                jobs_per_s=body["jobs_per_s"], p95_ms=body["wait_ms"]["p95"]))
    return 200, body


def _status(raw: dict) -> tuple[int, dict]:
    if not raw:
        return 404, {"error": "unknown run"}
    if "result" in raw:
        return 200, json.loads(raw["result"])
    if "count" not in raw:  # recreated by a late job after the run expired
        return 404, {"error": "unknown run"}
    count, started = int(raw["count"]), float(raw["started_at"])
    waits = [float(v) for k, v in raw.items() if k.startswith("w:")]
    finishes = [float(v) for k, v in raw.items() if k.startswith("f:")]
    duplicates = sum(int(v) - 1 for k, v in raw.items() if k.startswith("d:"))
    processed = len(finishes)
    span = max(finishes) - started if finishes else 0
    jobs_per_s = round(processed / span, 1) if span > 0 else None
    wait_ms = {p: round(percentile(waits, n) * 1000, 1) if waits else None for p, n in (("p50", 50), ("p95", 95))}
    failed = "error" in raw
    done = failed or processed >= count or time.time() - started > deadline(count)
    body = dict(state="running", count=count, processed=processed, jobs_per_s=jobs_per_s, wait_ms=wait_ms,
                lost=None, duplicates=duplicates, verdict=None)
    if not done:
        return 200, body
    body.update(state="failed" if failed else "done", lost=count - processed,
                verdict="fail" if failed else verdict(count, processed, duplicates, jobs_per_s, wait_ms["p95"]))
    if failed:
        body["error"] = raw["error"]
    return 200, body
