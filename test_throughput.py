"""Throughput math and contract checks; the live section runs only when REDIS_URL is reachable.
Run: uv run --env-file .env python test_throughput.py
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import telemetry as telemetry_module
import throughput as t

assert t.PREFIX == telemetry_module.KEY_PREFIX + "throughput:"
assert t.ACTIVE == t.PREFIX + "active"
for key in (telemetry_module.EVENTS, telemetry_module.STATS, telemetry_module.CHECK, telemetry_module.JOB):
    assert key.startswith(telemetry_module.KEY_PREFIX)

# percentile: nearest rank
assert t.percentile([5], 95) == 5
assert t.percentile(list(range(1, 101)), 50) == 50
assert t.percentile(list(range(1, 101)), 95) == 95
assert t.percentile([3, 1, 2], 100) == 3

# verdict
assert t.verdict(10, 10, 0, 500.0, 100.0) == "pass"
assert t.verdict(10, 9, 0, 500.0, 100.0) == "fail"  # lost
assert t.verdict(10, 10, 1, 500.0, 100.0) == "fail"  # duplicates
assert t.verdict(10, 10, 0, t.MIN_JOBS_PER_S - 1, 100.0) == "warn"
assert t.verdict(10, 10, 0, 500.0, t.MAX_P95_WAIT_MS + 1) == "warn"
assert t.verdict(10, 10, 0, None, None) == "warn"  # finished in zero time: rate unknown

# count validation
assert t.parse_count({}) == t.DEFAULT_COUNT
assert t.parse_count({"count": 1}) == 1 and t.parse_count({"count": t.MAX_COUNT}) == t.MAX_COUNT
for bad in (0, -1, t.MAX_COUNT + 1, 1.5, "10", True, None):
    assert isinstance(t.parse_count({"count": bad}), str), bad

# deadline grows with count and is bounded
assert t.deadline(1) < t.deadline(1000) < t.deadline(t.MAX_COUNT) <= 300

# start() rejects a bad count before touching Redis
t._telemetry = None
assert t.start({"count": 0})[0] == 400


async def async_contracts() -> None:
    assert (await t.astart({"count": 0}))[0] == 400
    with patch.dict(os.environ, {}, clear=True):
        optional = telemetry_module.Telemetry(telemetry_module.Registry(), "test")
        await optional.aopen()
        assert optional._astore is None
        await optional.aclose()
        os.environ["REDIS_URL"] = "redis://localhost:6379/0"
        await optional.aopen()
        assert optional.astore.connection_pool.max_connections == 50
        await optional.aclose()
        os.environ["LARAVEL_CLOUD_QUEUES_BACKEND"] = "invalid"
        try:
            await optional.aopen()
        except telemetry_module.ConfigurationError:
            pass
        else:
            raise AssertionError("invalid queue configuration was hidden")
    registry = SimpleNamespace(config=SimpleNamespace(redis=SimpleNamespace(url="redis://localhost:6379/0")))
    telemetry = telemetry_module.Telemetry(registry, "test")
    with patch.dict(os.environ, {"REDIS_MAX_CONNECTIONS": "7"}):
        await telemetry.aopen()
    first = telemetry.astore
    assert first.connection_pool.max_connections == 7
    await telemetry.aopen()
    assert telemetry.astore is first
    await telemetry.aclose()
    await telemetry.aclose()
    try:
        telemetry.astore
    except RuntimeError:
        pass
    else:
        raise AssertionError("closed async client was still available")
    await telemetry.aopen()
    assert telemetry.astore is not first
    await telemetry.aclose()
    with patch.dict(os.environ, {"REDIS_MAX_CONNECTIONS": "0"}):
        try:
            await telemetry.aopen()
        except ValueError:
            pass
        else:
            raise AssertionError("zero Redis connection limit was accepted")
    registry.config.redis = None
    with patch.dict(os.environ, {"REDIS_URL": ""}):
        await telemetry.aopen()
    assert telemetry._astore is None

    async def blocked(*args, **kwargs):
        await asyncio.Event().wait()

    # A stalled external operation must release the request through a timeout.
    telemetry._astore = SimpleNamespace(delete=blocked)
    with patch.object(telemetry_module, "TIMEOUT", 0.01):
        try:
            await telemetry.areset()
        except TimeoutError:
            pass
        else:
            raise AssertionError("async telemetry did not time out")
    t._telemetry = SimpleNamespace(astore=SimpleNamespace(set=blocked))
    with patch.object(t, "TIMEOUT", 0.01):
        try:
            await t.astart({"count": 1})
        except TimeoutError:
            pass
        else:
            raise AssertionError("async throughput start did not time out")
    assert not t._tasks

    # Dispatch failures are recorded even when the failure is a timeout.
    pipe = Mock(execute=AsyncMock(return_value=[1, True]))
    t._telemetry = SimpleNamespace(astore=SimpleNamespace(pipeline=lambda: pipe))
    t._log = Mock()
    t.tick = SimpleNamespace(dispatch_async=blocked)
    with patch.object(t, "TIMEOUT", 0.01):
        await t._adispatch("timed-out", 1)
    pipe.hset.assert_called_once()
    assert pipe.hset.call_args.args[:2] == (t.PREFIX + "timed-out", "error")
    pipe.execute.assert_awaited_once()
    t._log.exception.assert_called_once()

    # Task retention and cancellation also work without a Redis server.
    store = SimpleNamespace(set=AsyncMock(return_value=True), pipeline=lambda: pipe)
    t._telemetry = SimpleNamespace(astore=store)
    gate, calls = asyncio.Event(), []

    async def dispatch_async(run, queued_at):
        await gate.wait()
        calls.append((run, queued_at))

    t.tick = SimpleNamespace(dispatch_async=dispatch_async)
    code, body = await t.astart({"count": 3})
    assert code == 202 and len(t._tasks) == 1 and not calls
    gate.set()
    await asyncio.gather(*t._tasks)
    assert not t._tasks and len(calls) == 3 and all(run == body["run"] for run, _ in calls)
    gate.clear()
    await t.astart({"count": 1})
    tasks = list(t._tasks)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert not t._tasks


asyncio.run(async_contracts())

# live: seed a finished run by hand and read it back through status()
url = os.environ.get("REDIS_URL")
store = None
if url:
    import redis

    try:
        store = redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)
        store.ping()
    except redis.RedisError:
        store = None
if store is None:
    print("throughput math and async contract checks passed (live section skipped: REDIS_URL not reachable)")
    raise SystemExit(0)

# Own key namespace: never touch a real run's lock or hash.
t.PREFIX = telemetry_module.KEY_PREFIX + f"throughput-test-{uuid.uuid4().hex}:"
t.ACTIVE = t.PREFIX + "active"
messages = []
t._telemetry = SimpleNamespace(store=store)
t._log = SimpleNamespace(info=lambda msg, extra=None: messages.append((msg, extra)))
assert t.status("nope" + uuid.uuid4().hex)[0] == 404

run, now = uuid.uuid4().hex, time.time()
key = t.PREFIX + run
fields = {"count": 4, "started_at": now - 2}
for i in range(4):
    fields |= {f"d:j{i}": 1 + (i == 0), f"w:j{i}": 0.1 * (i + 1), f"f:j{i}": now - 1 + i * 0.1}
store.hset(key, mapping=fields)
store.expire(key, 60)
store.set(t.ACTIVE, run, ex=60)
try:
    code, body = t.status(run)
    assert code == 200 and body["state"] == "done" and body["verdict"] == "fail", body  # one duplicate
    assert (body["processed"], body["lost"], body["duplicates"]) == (4, 0, 1), body
    assert body["wait_ms"] == {"p50": 200.0, "p95": 400.0}, body
    assert body["jobs_per_s"] == round(4 / 1.3, 1), body
    assert store.get(t.ACTIVE) is None  # finishing frees the lock
    assert [m for m, _ in messages] == ["throughput finished"]
    store.hset(key, "f:late", now)  # a late job after the verdict must not change it
    assert t.status(run) == (200, body) and len(messages) == 1
    # A second process that loses the freeze returns the stored result, not its own.
    store.hdel(key, "result")
    store.hset(key, "result", json.dumps({**body, "verdict": "pass"}))
    assert t.status(run)[1]["verdict"] == "pass"
    # Lock: NX only; a second start is refused while one is active.
    store.set(t.ACTIVE, "other", ex=60)
    assert t.start({"count": 1})[0] == 409
    assert store.get(t.ACTIVE) == "other"
    # A hash recreated by a late job (no count) reads as unknown.
    store.delete(key)
    store.hset(key, "f:late", now)
    assert t.status(run)[0] == 404
finally:
    store.delete(key, t.ACTIVE)


async def async_live() -> None:
    namespace = telemetry_module.KEY_PREFIX + f"telemetry-test-{uuid.uuid4().hex}:"
    with patch.multiple(telemetry_module, EVENTS=namespace + "events", STATS=namespace + "stats",
                        CHECK=namespace + "check", JOB=namespace + "job:"):
        registry = SimpleNamespace(config=SimpleNamespace(
            mode="redis", redis=SimpleNamespace(url=url, prefix=namespace, queue="test")))
        telemetry = telemetry_module.Telemetry(registry, "test")
        telemetry.store = store
        t._telemetry = telemetry
        t._log = Mock()
        keys = [telemetry_module.EVENTS, telemetry_module.STATS, telemetry_module.CHECK, t.ACTIVE]
        uid = uuid.uuid4().hex
        keys.append(telemetry_module.JOB + uid)
        try:
            # Worker telemetry works before ASGI lifespan opens any async client.
            job = SimpleNamespace(job_name="test.quick", uuid=uid, attempt=1, max_tries=1)
            with patch.object(telemetry_module, "current_job", return_value=job):
                with telemetry.tracked():
                    pass
            await telemetry.aopen()
            await telemetry.aqueued("test.quick", uid, time.time() - 1, 0)
            telemetry.save_check({"quick": [uid]})
            assert await telemetry.aevaluate() == telemetry.evaluate()
            assert (await telemetry.aevaluate())["status"] == "pass"
            snapshot = await telemetry.asnapshot()
            assert snapshot == telemetry.snapshot()
            assert snapshot["counts"] == {"queued": 1, "started": 1, "processed": 1}
            assert [e["event"] for e in snapshot["events"]] == ["processed", "started", "queued"]
            assert 0 < store.ttl(telemetry_module.JOB + uid) <= telemetry_module.JOB_TTL
            await telemetry.asave_check({"quick": [uid]})
            assert telemetry.evaluate() == await telemetry.aevaluate()
            await telemetry.areset()
            assert await telemetry.aevaluate() is None
            assert telemetry.snapshot()["counts"] == {}
            telemetry.queued("test.quick", uid, time.time(), 0)
            assert (await telemetry.asnapshot())["counts"] == {"queued": 1}
            telemetry.reset()
            assert (await telemetry.asnapshot())["counts"] == {}

            assert (await t.astatus("nope" + uuid.uuid4().hex))[0] == 404
            run, now = uuid.uuid4().hex, time.time()
            key = t.PREFIX + run
            keys.append(key)
            fields = {"count": 4, "started_at": now - 2}
            for i in range(4):
                fields |= {f"d:j{i}": 1 + (i == 0), f"w:j{i}": 0.1 * (i + 1), f"f:j{i}": now - 1 + i * 0.1}
            await telemetry.astore.hset(key, mapping=fields)
            await telemetry.astore.expire(key, 60)
            await telemetry.astore.set(t.ACTIVE, run, ex=60)
            code, body = await t.astatus(run)
            assert code == 200 and body["state"] == "done" and body["verdict"] == "fail", body
            assert (body["processed"], body["lost"], body["duplicates"]) == (4, 0, 1), body
            assert body["wait_ms"] == {"p50": 200.0, "p95": 400.0}, body
            assert body["jobs_per_s"] == round(4 / 1.3, 1), body
            assert store.get(t.ACTIVE) is None
            await telemetry.astore.hset(key, "f:late", now)
            assert await t.astatus(run) == t.status(run) == (200, body)
            await telemetry.astore.hset(key, "result", json.dumps({**body, "verdict": "pass"}))
            assert (await t.astatus(run))[1]["verdict"] == "pass"
            await telemetry.astore.set(t.ACTIVE, "other", ex=60)
            assert (await t.astart({"count": 1}))[0] == 409
            assert store.get(t.ACTIVE) == "other"
            # Finishing an old run cannot delete a newer run's lock.
            await telemetry.astore.hdel(key, "result")
            await t.astatus(run)
            assert store.get(t.ACTIVE) == "other"
            await telemetry.astore.delete(key, t.ACTIVE)
            await telemetry.astore.hset(key, "f:late", now)
            assert (await t.astatus(run))[0] == 404

            # POST returns while dispatch is pending, and the task stays strongly referenced.
            gate = asyncio.Event()
            calls = []

            async def dispatch_async(run, queued_at):
                await gate.wait()
                calls.append((run, queued_at))

            t.tick = SimpleNamespace(dispatch_async=dispatch_async)
            code, body = await t.astart({"count": 3})
            assert code == 202 and len(t._tasks) == 1 and not calls
            run, key = body["run"], t.PREFIX + body["run"]
            keys.append(key)
            assert store.hget(key, "count") == "3" and store.ttl(key) > 0
            assert t.start({"count": 1})[0] == 409
            assert (await t.astatus(run))[1]["state"] == "running"
            gate.set()
            await asyncio.gather(*t._tasks)
            assert not t._tasks and len(calls) == 3 and all(r == run for r, _ in calls)

            async def failed_dispatch(*args):
                raise RuntimeError("dispatch broke")

            t.tick = SimpleNamespace(dispatch_async=failed_dispatch)
            await telemetry.astore.delete(t.ACTIVE)
            code, body = await t.astart({"count": 2})
            assert code == 202
            run, key = body["run"], t.PREFIX + body["run"]
            keys.append(key)
            await asyncio.gather(*t._tasks)
            assert not t._tasks
            code, body = await t.astatus(run)
            assert code == 200 and body["state"] == "failed" and body["error"] == "dispatch broke"
            assert body["verdict"] == "fail" and body["lost"] == 2 and store.ttl(key) > 0
            assert store.get(t.ACTIVE) is None

            t.tick = SimpleNamespace(dispatch_async=lambda *args: asyncio.Event().wait())
            code, body = await t.astart({"count": 1})
            assert code == 202
            keys.append(t.PREFIX + body["run"])
            tasks = list(t._tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            assert not t._tasks
        finally:
            for task in list(t._tasks):
                task.cancel()
            await asyncio.gather(*t._tasks, return_exceptions=True)
            await telemetry.aclose()
            store.delete(*keys)


asyncio.run(async_live())
store.close()
print("throughput checks passed (sync and async live sections ran)")
