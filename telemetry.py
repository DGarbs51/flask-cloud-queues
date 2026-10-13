"""Job telemetry and the deployment check. Shared canary contract with python-cloud-queues; keep JSON shapes and check cases in sync.

The package emits no lifecycle events outside managed mode, so jobs record their own
events into the environment's Valkey under DEMO_KEY_PREFIX (default ``lcq-demo:``).
In ``redis`` mode queue depth comes from the package's Redis keys; in other modes
depth is not shown.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import platform
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager

import redis
import redis.asyncio

from laravel_cloud_queues import ConfigurationError, Registry, __version__, current_job

KEY_PREFIX = os.environ.get("DEMO_KEY_PREFIX", "lcq-demo:")
EVENTS = KEY_PREFIX + "events"
STATS = KEY_PREFIX + "stats"
CHECK = KEY_PREFIX + "check"
JOB = KEY_PREFIX + "job:"
TIMEOUT = 5
KEEP_EVENTS = 200
JOB_TTL = 86_400
CHECK_DEADLINE = 240  # the timeout case needs a worker restart plus the 60 s Redis lease
PYTHON = platform.python_version()
WORKER = f"{os.environ.get('WORKER_LABEL') or socket.gethostname()}:{os.getpid()}"

# Dispatch kinds the check runs, one case each. Every app maps these to its jobs.
CHECK_KINDS = ("quick", "async", "delayed", "flaky", "failing", "timeout", "burst")
DELAY_SECONDS = 5
BURST_SIZE = 20
TIMEOUT_SECONDS = 3


class Telemetry:
    def __init__(self, registry: Registry, framework: str) -> None:
        self.registry = registry
        self.framework = framework
        self._astore: redis.asyncio.Redis | None = None

    def _url(self) -> str | None:
        config = self.registry.config.redis
        return config.url if config else os.environ.get("REDIS_URL")

    @functools.cached_property
    def store(self) -> redis.Redis:
        url = self._url()
        if not url:
            raise RuntimeError("Attach a Valkey cache (REDIS_URL) to store demo telemetry.")
        # Timeouts bound a stalled Valkey: async jobs call this on the worker's loop, where a hang would stop the
        # lease renewer (laravel-cloud-queues renews async handlers from a task, not a thread).
        return redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=TIMEOUT, socket_timeout=TIMEOUT)

    async def aopen(self) -> None:
        """Create the request-loop client; apps can boot without an attached cache."""
        try:
            url = self._url()
        except ConfigurationError:
            if (os.environ.get("LARAVEL_CLOUD_QUEUES_BACKEND") is not None
                    or os.environ.get("LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG") is not None):
                raise
            # Telemetry is optional when booting without a queue backend.
            url = os.environ.get("REDIS_URL")
        if url and self._astore is None:
            max_connections = int(os.environ.get("REDIS_MAX_CONNECTIONS", "50"))
            if max_connections < 1:
                raise ValueError("REDIS_MAX_CONNECTIONS must be positive")
            self._astore = redis.asyncio.Redis.from_url(
                url, decode_responses=True,
                max_connections=max_connections,
                socket_connect_timeout=TIMEOUT, socket_timeout=TIMEOUT,
            )

    async def aclose(self) -> None:
        if self._astore is not None:
            async with asyncio.timeout(TIMEOUT):
                await self._astore.aclose()
            self._astore = None

    @property
    def astore(self) -> redis.asyncio.Redis:
        if self._astore is None:
            raise RuntimeError("Async demo telemetry needs REDIS_URL and aopen() in ASGI lifespan.")
        return self._astore

    def record(self, event: str, **fields: object) -> None:
        pipe = self.store.pipeline()
        self._record(pipe, event, **fields)
        pipe.execute()

    def _record(self, pipe, event: str, **fields: object) -> None:
        entry = json.dumps({"event": event, "at": time.time(), **fields})
        pipe.lpush(EVENTS, entry)
        pipe.ltrim(EVENTS, 0, KEEP_EVENTS - 1)
        pipe.hincrby(STATS, event, 1)
        if "uuid" in fields:
            key = f"{JOB}{fields['uuid']}"
            pipe.rpush(key, entry)
            pipe.expire(key, JOB_TTL)

    @contextmanager
    def tracked(self) -> Iterator[None]:
        """Record started/processed/released/failed for the current delivery."""
        job = current_job()
        base = {
            "job": job.job_name,
            "uuid": job.uuid,
            "attempt": job.attempt,
            "worker": WORKER,
            "python": PYTHON,
        }
        self.record("started", **base)
        start = time.monotonic()
        try:
            yield
        except Exception as exc:
            final = job.attempt >= job.max_tries
            ms = round((time.monotonic() - start) * 1000)
            self.record("failed" if final else "released", **base, ms=ms, error=str(exc)[:200])
            raise
        self.record("processed", **base, ms=round((time.monotonic() - start) * 1000))

    def queued(self, job_name: str, uuid: str, at: float, delay: int) -> None:
        self.record("queued", at=at, job=job_name, uuid=uuid, delay=delay)

    async def aqueued(self, job_name: str, uuid: str, at: float, delay: int) -> None:
        pipe = self.astore.pipeline()
        self._record(pipe, "queued", at=at, job=job_name, uuid=uuid, delay=delay)
        async with asyncio.timeout(TIMEOUT):
            await pipe.execute()

    def snapshot(self) -> dict[str, object]:
        pipe = self._snapshot_pipeline(self.store)
        return self._snapshot(pipe.execute(), self.evaluate())

    async def asnapshot(self) -> dict[str, object]:
        pipe = self._snapshot_pipeline(self.astore)
        async with asyncio.timeout(TIMEOUT):
            values = await pipe.execute()
        return self._snapshot(values, await self.aevaluate())

    def _snapshot_pipeline(self, store):
        config = self.registry.config
        pipe = store.pipeline()
        if config.redis:
            pending = f"{config.redis.prefix}queues:{config.redis.queue}"
            pipe.llen(pending)
            pipe.zcard(f"{pending}:delayed")
            pipe.zcard(f"{pending}:reserved")
        pipe.hgetall(STATS)
        pipe.lrange(EVENTS, 0, 99)
        return pipe

    def _snapshot(self, values, check: dict[str, object] | None) -> dict[str, object]:
        config = self.registry.config
        *depth, counts, events = values
        parsed = sorted((json.loads(e) for e in events), key=lambda e: e["at"], reverse=True)
        return {
            "framework": self.framework,
            "python": PYTHON,
            "version": __version__,
            "mode": config.mode,
            "queue": config.redis.queue if config.redis else None,
            "depth": dict(zip(("ready", "delayed", "reserved"), depth)) if depth else None,
            "counts": {k: int(v) for k, v in counts.items()},
            "events": parsed,
            "check": check,
        }

    def reset(self) -> None:
        self.store.delete(EVENTS, STATS, CHECK)

    async def areset(self) -> None:
        async with asyncio.timeout(TIMEOUT):
            await self.astore.delete(EVENTS, STATS, CHECK)

    def save_check(self, cases: dict[str, list[str]]) -> None:
        self.store.set(CHECK, json.dumps({"at": time.time(), "cases": cases}))

    async def asave_check(self, cases: dict[str, list[str]]) -> None:
        async with asyncio.timeout(TIMEOUT):
            await self.astore.set(CHECK, json.dumps({"at": time.time(), "cases": cases}))

    def evaluate(self) -> dict[str, object] | None:
        raw = self.store.get(CHECK)
        if raw is None:
            return None
        run = json.loads(raw)
        pipe = self._check_pipeline(self.store, run)
        return self._evaluate(run, pipe.execute())

    async def aevaluate(self) -> dict[str, object] | None:
        async with asyncio.timeout(TIMEOUT):
            raw = await self.astore.get(CHECK)
        if raw is None:
            return None
        run = json.loads(raw)
        pipe = self._check_pipeline(self.astore, run)
        async with asyncio.timeout(TIMEOUT):
            histories = await pipe.execute()
        return self._evaluate(run, histories)

    def _check_pipeline(self, store, run):
        pipe = store.pipeline()
        for uuids in run["cases"].values():
            for uuid in uuids:
                pipe.lrange(f"{JOB}{uuid}", 0, -1)
        return pipe

    def _evaluate(self, run, histories) -> dict[str, object]:
        expired = time.time() - run["at"] > CHECK_DEADLINE
        histories = iter([[json.loads(e) for e in h] for h in histories])

        results, seen = [], []
        for kind, uuids in run["cases"].items():
            jobs = [next(histories) for _ in uuids]
            seen += [e for h in jobs for e in h]
            ok, detail = EXPECTATIONS[kind](jobs)
            if ok is None and expired:
                ok, detail = False, f"gave up after {CHECK_DEADLINE} s: {detail}"
            results.append({"case": kind, "ok": ok, "detail": detail})
        results.append({"case": "workers", **_workers(seen, expired)})

        oks = [r["ok"] for r in results]
        status = "fail" if False in oks else "pass" if all(oks) else "running"
        return {"status": status, "started_at": run["at"], "results": results}


Verdict = tuple[bool | None, str]  # None: still waiting


def _outcomes(history: list[dict[str, object]]) -> list[tuple[object, object]]:
    return [(e["event"], e.get("attempt")) for e in history if e["event"] != "queued"]


def _processed_first_try(jobs: list[list[dict[str, object]]]) -> Verdict:
    outcomes = _outcomes(jobs[0])
    if ("processed", 1) in outcomes:
        return True, "processed on attempt 1"
    if outcomes and outcomes[-1][0] in ("released", "failed"):
        return False, f"{outcomes[-1][0]} on attempt {outcomes[-1][1]}"
    return None, "waiting for a worker"


def _delayed(jobs: list[list[dict[str, object]]]) -> Verdict:
    ok, detail = _processed_first_try(jobs)
    if not ok:
        return ok, detail
    by_event = {e["event"]: e for e in jobs[0]}
    waited = by_event["started"]["at"] - by_event["queued"]["at"]
    # 0.1 s tolerance for clock skew between the web and worker containers.
    if waited < DELAY_SECONDS - 0.1:
        return False, f"started {waited:.1f} s after dispatch, expected >= {DELAY_SECONDS} s"
    return True, f"started {waited:.1f} s after dispatch"


def _flaky(jobs: list[list[dict[str, object]]]) -> Verdict:
    outcomes = _outcomes(jobs[0])
    if ("released", 1) in outcomes and ("processed", 2) in outcomes:
        return True, "released on attempt 1, processed on attempt 2"
    if any(event == "failed" for event, _ in outcomes) or ("processed", 1) in outcomes:
        return False, f"unexpected outcomes {outcomes}"
    return None, "waiting for the retry"


def _failing(jobs: list[list[dict[str, object]]]) -> Verdict:
    outcomes = _outcomes(jobs[0])
    if ("released", 1) in outcomes and ("failed", 2) in outcomes:
        return True, "released on attempt 1, failed on attempt 2"
    if any(event == "processed" for event, _ in outcomes):
        return False, f"unexpected outcomes {outcomes}"
    return None, "waiting for the final attempt"


def _timeout(jobs: list[list[dict[str, object]]]) -> Verdict:
    starts = {e["attempt"]: e for e in jobs[0] if e["event"] == "started"}
    if any(e["event"] == "processed" for e in jobs[0]):
        return False, "a job that exceeds its timeout was reported processed"
    if 1 in starts and 2 in starts:
        gap = starts[2]["at"] - starts[1]["at"]
        return True, f"worker exited on timeout; attempt 2 redelivered {gap:.0f} s later"
    if 1 in starts:
        return None, "attempt 1 timed out; waiting for the restart and redelivery"
    return None, "waiting for a worker"


def _burst(jobs: list[list[dict[str, object]]]) -> Verdict:
    processed = [sum(e["event"] == "processed" for e in h) for h in jobs]
    if any(n > 1 for n in processed):
        return False, f"{sum(n > 1 for n in processed)} job(s) processed more than once"
    done = sum(processed)
    if done == len(jobs):
        return True, f"{done}/{len(jobs)} processed exactly once"
    return None, f"{done}/{len(jobs)} processed"


def _workers(events: list[dict[str, object]], expired: bool) -> dict[str, object]:
    workers = {(e["worker"], e.get("python")) for e in events if "worker" in e}
    names = ", ".join(sorted(f"{w} (Python {p})" for w, p in workers))
    if not workers:
        return {"ok": False if expired else None, "detail": "no worker has run a job"}
    mismatched = sorted(str(p) for _, p in workers if p != PYTHON)
    if mismatched:
        return {"ok": False, "detail": f"workers on Python {mismatched}, web on {PYTHON}: {names}"}
    return {"ok": True, "detail": f"{len({w for w, _ in workers})} worker(s): {names}"}


EXPECTATIONS = {
    "quick": _processed_first_try,
    "async": _processed_first_try,
    "delayed": _delayed,
    "flaky": _flaky,
    "failing": _failing,
    "timeout": _timeout,
    "burst": _burst,
}
