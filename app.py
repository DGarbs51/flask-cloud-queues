"""Flask WSGI canary for Laravel Cloud. Worker target: app:registry."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import socket
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qsl

from flask import Blueprint, Flask, Response, g, request
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge
from werkzeug.middleware.proxy_fix import ProxyFix
from laravel_cloud_logging import ALERT, EMERGENCY, NOTICE, wsgi_middleware
from laravel_cloud_queues import Registry, current_job

import checks
import logs
import throughput
from telemetry import BURST_SIZE, CHECK_DEADLINE, CHECK_KINDS, DELAY_SECONDS, KEY_PREFIX, TIMEOUT_SECONDS, Telemetry

LOG_ROLE = "worker" if "work" in sys.argv else "web"
log = logs.setup(LOG_ROLE)
registry = Registry()
telemetry = Telemetry(registry, "Flask")
throughput.install(registry, telemetry, log)
INDEX = Path(__file__).with_name("index.html")
MAX_BODY = 64 * 1024
SERVER = os.environ.get("WEB_SERVER", "gunicorn")

MARKER = re.compile(r"[A-Za-z0-9-]{1,64}")
# /api/log-test levels. DEBUG is below the default LOG_LEVEL=INFO, so it must not reach Cloud.
LOG_TEST_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "notice": NOTICE, "warning": logging.WARNING,
                   "error": logging.ERROR, "critical": logging.CRITICAL, "alert": ALERT, "emergency": EMERGENCY}
LOG_TEST_UNICODE = "日本語 · émoji 🚀"
STREAM_CHUNKS = 6  # /api/stream sends one a second; web.streaming times when each arrives (#8)
SLOW_MAX = 300  # /api/slow cap in seconds: above Cloud's 60 s HTTP timeout, so the timeout test (#9) can cross it
# /api/upload's own cap (#10): above Cloudflare's 500 MiB edge limit, so the edge is what an upload test hits first.
UPLOAD_MAX = 512 * 1024 * 1024
UPLOAD_CHUNK = 1024 * 1024
CPU_ROUNDS = 100_000  # /api/cpu: chained sha256 rounds, ~20 ms on one core; fixed so every run does the same work
REDIS_HITS = KEY_PREFIX + "redis-hits"  # /api/redis INCRs this


class Boom(Exception):
    """Raised by /api/boom through Flask to the logging middleware and server."""


@registry.job(name="demo.quick")
def quick() -> None:
    with telemetry.tracked():
        time.sleep(random.uniform(0.05, 0.3))


@registry.job(name="demo.async")
async def async_job() -> None:
    with telemetry.tracked():
        await asyncio.sleep(random.uniform(0.05, 0.3))


@registry.job(name="demo.slow")
def slow() -> None:
    with telemetry.tracked():
        time.sleep(3)


@registry.job(name="demo.flaky", tries=3, backoff=[2])
def flaky() -> None:
    with telemetry.tracked():
        if current_job().attempt == 1:
            raise RuntimeError("flaky job fails on its first attempt")


@registry.job(name="demo.failing", tries=2, backoff=[1])
def failing() -> None:
    with telemetry.tracked():
        raise RuntimeError("this job always fails")


@registry.job(name="demo.timeout", tries=2, timeout=TIMEOUT_SECONDS)
def timeout() -> None:
    # Exceeds its timeout: the worker exits 124 and the platform restarts it.
    with telemetry.tracked():
        time.sleep(TIMEOUT_SECONDS + 7)


DISPATCHES = {  # kind: (job, delay seconds, how many)
    "quick": (quick, 0, 1),
    "async": (async_job, 0, 1),
    "slow": (slow, 0, 1),
    "delayed": (quick, DELAY_SECONDS, 1),
    "flaky": (flaky, 0, 1),
    "failing": (failing, 0, 1),
    "timeout": (timeout, 0, 1),
    "burst": (quick, 0, BURST_SIZE),
}


def dispatch(kind: str) -> list[str]:
    job, delay, count = DISPATCHES[kind]
    uuids = []
    for _ in range(count):
        at = time.time()
        receipt = job.options(delay=delay).dispatch()
        telemetry.queued(job.name, receipt.uuid, at, delay)
        uuids.append(receipt.uuid)
    return uuids


CHECK_LOCK = KEY_PREFIX + "check-lock"


def run_check() -> bool:
    """Start the queue check at most once per CHECK_DEADLINE across every web process and replica.

    Its timeout case restarts a worker, so repeated starts would keep workers cycling. Within the
    cooldown the page just shows the latest check.
    """
    if not telemetry.store.set(CHECK_LOCK, "1", nx=True, ex=CHECK_DEADLINE):
        return False
    telemetry.save_check({kind: dispatch(kind) for kind in CHECK_KINDS})
    return True


def instance() -> dict:
    """Which deployment, instance and worker answered: DEPLOY_MARKER is set before each test deploy (#7)."""
    return {"deploy": os.environ.get("DEPLOY_MARKER", ""), "pod": socket.gethostname(), "pid": os.getpid()}


def stream_headers(query: str) -> list[tuple[str, str]]:
    """GET /api/stream: server-sent events, no Content-Length. ?accel=no adds nginx's per-response buffering opt-out."""
    headers = [("Content-Type", "text/event-stream")]
    if ("accel", "no") in parse_qsl(query):
        headers.append(("X-Accel-Buffering", "no"))
    return headers


def stream_chunk(i: int) -> bytes:
    return f"data: {json.dumps(dict(i=i, sent=round(time.time(), 3)))}\n\n".encode()


def log_test(marker: str) -> None:
    """One line per level, plus context, multi-line, non-English and a chained exception. Used by logs_check.py (#17)."""
    for name, level in LOG_TEST_LEVELS.items():
        log.log(level, "log-test %s", name, extra=dict(marker=marker, case=name))
    log.info("log-test extra", extra=dict(marker=marker, case="extra", order={"id": 42, "items": ["a", "b"]}))
    log.info("log-test multi-line\nsecond line\nthird line", extra=dict(marker=marker, case="multiline"))
    log.info("log-test unicode %s", LOG_TEST_UNICODE, extra=dict(marker=marker, case="unicode"))
    try:
        try:
            raise KeyError("inner cause")
        except KeyError as exc:
            raise ValueError("log-test outer") from exc
    except ValueError:
        log.exception("log-test exception", extra=dict(marker=marker, case="exception"))


def _boom_outer(marker: str) -> None:
    _boom_middle(marker)


def _boom_middle(marker: str) -> None:
    _boom_inner(marker)


def _boom_inner(marker: str) -> None:
    raise Boom(f"uncaught boom {marker}")


def started(server: str) -> None:
    global SERVER
    SERVER = server
    log.info("startup", extra=dict(server=server, pid=os.getpid(), web_concurrency=os.environ.get("WEB_CONCURRENCY")))


def _reject_constant(value: str):
    raise ValueError(f"invalid JSON constant: {value}")


def json_response(status: int, data: object) -> Response:
    return Response(json.dumps(data, allow_nan=False), status=status, content_type="application/json")


api = Blueprint("api", __name__, url_prefix="/api")


@api.get("/ping")
def ping():
    return json_response(200, {"ok": True, **instance()})


@api.get("/cpu")
def cpu():
    digest = b"lcq"
    for _ in range(CPU_ROUNDS):
        digest = hashlib.sha256(digest).digest()
    return json_response(200, {"ok": True, "rounds": CPU_ROUNDS, "digest": digest.hex(), **instance()})


@api.get("/slow")
def slow_request():
    value = request.args.get("seconds", "")
    if not value.isascii() or not value.isdigit() or len(value) > 3 or not 1 <= int(value) <= SLOW_MAX:
        return json_response(400, {"error": f"seconds must be 1-{SLOW_MAX}"})
    seconds = int(value)
    time.sleep(seconds)
    return json_response(200, {"ok": True, "slept": seconds, **instance()})


@api.get("/stats")
def stats():
    return json_response(200, {**telemetry.snapshot(), "server": SERVER})


@api.get("/redis")
def redis_hits():
    return json_response(200, {"ok": True, "hits": telemetry.store.incr(REDIS_HITS), **instance()})


@api.get("/checks")
def platform_checks():
    # Copy headers before checks.run() sends them to daemon threads outside Flask's request context.
    return json_response(200, checks.run(dict(request.headers)))


@api.get("/packages")
def packages():
    return json_response(200, checks.inventory())


@api.post("/check")
def queue_check():
    if not run_check():
        return json_response(409, {"error": "a queue check is already running"})
    return json_response(200, {"ok": True})


@api.post("/throughput")
def throughput_start():
    return json_response(*throughput.start(g.data))


@api.get("/throughput/<run>")
def throughput_status(run):
    return json_response(*throughput.status(run))


@api.post("/suite-results")
def suite_results():
    return json_response(*checks.save_suite(g.data))


@api.post("/reset")
def reset():
    telemetry.reset()
    return json_response(200, {"ok": True})


@api.get("/log-test")
@api.get("/boom")
@api.get("/boom-thread")
def logging_probe():
    marker = request.args.get("marker", "")
    if not MARKER.fullmatch(marker):
        return json_response(400, {"error": "marker must be 1-64 letters, digits or -"})
    if request.path == "/api/log-test":
        log_test(marker)
    elif request.path == "/api/boom":
        _boom_outer(marker)
    else:
        thread = threading.Thread(target=_boom_outer, args=(marker,), name="boom-thread")
        thread.start()
        thread.join()
    return json_response(200, {"ok": True, "marker": marker})


def stream():
    for i in range(STREAM_CHUNKS):
        if i:
            time.sleep(1)
        yield stream_chunk(i)


@api.get("/stream")
def streaming():
    return Response(stream(), headers=stream_headers(request.query_string.decode("latin-1")))


@api.post("/upload")
def upload():
    length = request.content_length
    if length is not None and length > UPLOAD_MAX:
        return json_response(413, {"error": "body exceeds 512 MiB"})
    request.max_content_length = UPLOAD_MAX + 1
    count = 0
    while count <= UPLOAD_MAX:
        chunk = request.stream.read(min(UPLOAD_CHUNK, UPLOAD_MAX + 1 - count))
        if not chunk:
            break
        count += len(chunk)
    if count > UPLOAD_MAX:
        return json_response(413, {"error": "body exceeds 512 MiB"})
    return json_response(200, {"bytes": count})


def create_app() -> Flask:
    application = Flask(__name__, static_folder=None)
    # Let the deliberate Boom probe reach the logging middleware and the server's error path.
    application.config["PROPAGATE_EXCEPTIONS"] = True
    application.register_blueprint(api)

    @application.get("/")
    def index():
        return Response(INDEX.read_bytes(), content_type="text/html; charset=utf-8")

    @application.before_request
    def prepare_request():
        g.started = time.monotonic()
        supplied = request.headers.get("X-Request-ID", "")
        g.request_id = supplied if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", supplied) else uuid.uuid4().hex
        g.log_token = logs.CONTEXT.set({**logs.CONTEXT.get(), "request_id": g.request_id})
        # ProxyFix rewrites REMOTE_ADDR; this check needs the actual upstream peer.
        original = request.environ.get("werkzeug.proxy_fix.orig", {})
        g.peer_token = checks.PEER.set(original.get("REMOTE_ADDR", request.remote_addr or ""))
        raw_length = request.environ.get("CONTENT_LENGTH", "")
        try:
            length = int(raw_length or 0)
        except ValueError:
            length = -1
        is_upload = request.method == "POST" and request.path == "/api/upload"
        if length < 0:
            return json_response(400, {"error": "invalid Content-Length"})
        chunked = "chunked" in request.headers.get("Transfer-Encoding", "").lower()
        if chunked and not raw_length:
            if SERVER == "uwsgi":
                # uWSGI doesn't de-chunk wsgi.input; Cloud's nginx supplies Content-Length.
                return json_response(400, {"error": "invalid Content-Length"})
            # hypercorn de-chunks and buffers the input but omits this WSGI extension.
            request.environ["wsgi.input_terminated"] = True
        if is_upload:
            return None
        if length > MAX_BODY:
            return json_response(400, {"error": "body exceeds 64 KiB"})
        request.max_content_length = MAX_BODY + 1
        body = request.stream.read(MAX_BODY + 1)
        if len(body) > MAX_BODY:
            return json_response(400, {"error": "body exceeds 64 KiB"})
        g.data = {}
        if request.method == "POST":
            # JSON requires a CORS preflight; another origin cannot dispatch with a simple POST.
            if request.mimetype != "application/json":
                return json_response(415, {"error": "expected application/json"})
            try:
                data = json.loads(body, parse_constant=_reject_constant) if body else {}
            except (ValueError, UnicodeError, RecursionError):
                data = None
            if not isinstance(data, dict):
                return json_response(400, {"error": "expected a JSON object"})
            g.data = data

    @application.after_request
    def finish_request(response):
        level = logging.DEBUG if request.method == "GET" and request.path == "/api/stats" else logging.INFO
        if response.status_code >= 400:
            level = logging.ERROR if response.status_code >= 500 else logging.WARNING
        log.log(level, "access", extra=dict(method=request.method, path=request.path, status=response.status_code,
                                            bytes=response.content_length or 0,
                                            duration_ms=round((time.monotonic() - g.started) * 1000, 2)))
        response.headers["X-Request-ID"] = g.request_id
        return response

    @application.teardown_request
    def clear_context(error):
        if "peer_token" in g:
            checks.PEER.reset(g.peer_token)
        if "log_token" in g:
            logs.CONTEXT.reset(g.log_token)

    @application.errorhandler(Exception)
    def failed(error):
        if isinstance(error, Boom):
            raise error
        if isinstance(error, RequestEntityTooLarge):
            return json_response(413 if request.path == "/api/upload" else 400, {"error": "body exceeds limit"})
        if isinstance(error, HTTPException):
            return json_response(error.code, {"error": "not found" if error.code == 404 else error.description})
        log.exception("Request failed: %s %s", request.method, request.path)
        return json_response(503, {"error": "service unavailable"})

    application.wsgi_app = wsgi_middleware(ProxyFix(application.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1))
    return application


app = create_app()


def self_check() -> None:
    from unittest.mock import patch

    client = app.test_client()
    with patch.dict(os.environ, DEPLOY_MARKER="m-1"):
        response = client.get("/api/ping?x=1", headers={"X-Request-ID": "abc"})
    assert response.status_code == 200 and response.headers["X-Request-ID"] == "abc"
    assert response.json == {"ok": True, "deploy": "m-1", "pod": socket.gethostname(), "pid": os.getpid()}
    with patch("time.sleep") as sleep:
        assert client.get("/api/slow?seconds=2").json["slept"] == 2
        assert sleep.call_args.args == (2,)
        for bad in ("0", "301", "-1", "1.5", "x", "", "9" * 5000, "²"):
            assert client.get(f"/api/slow?seconds={bad}").status_code == 400
        assert sleep.call_count == 1
    assert client.post("/api/upload", data=b"hello").json == {"bytes": 5}
    assert client.post("/api/upload", environ_overrides={"CONTENT_LENGTH": str(UPLOAD_MAX + 1)}).status_code == 413
    assert client.post("/api/upload", environ_overrides={"CONTENT_LENGTH": "-1"}).status_code == 400
    assert client.get("/").status_code == 200
    assert client.get("/nope").json == {"error": "not found"}
    assert client.post("/api/check", data=b"{}", content_type="text/plain").status_code == 415
    for bad in (b"[]", b"NaN", b'{"count": NaN}', b"{", b"\xff"):
        assert client.post("/api/check", data=bad, content_type="application/json").status_code == 400
    assert client.post("/api/check", data=b"x" * (MAX_BODY + 1), content_type="application/json").status_code == 400
    assert client.get("/", headers={"X-Request-ID": "bad id!"}).headers["X-Request-ID"] != "bad id!"
    with patch.object(telemetry, "snapshot", side_effect=RuntimeError("redis down")):
        assert client.get("/api/stats").status_code == 503
    with patch(__name__ + ".run_check", return_value=True):
        assert client.post("/api/check", json={}).status_code == 200
    with patch(__name__ + ".run_check", return_value=False):
        assert client.post("/api/check", json={}).status_code == 409
    assert client.post("/api/dispatch/timeout", json={}).status_code == 404
    assert client.get("/api/log-test?marker=bad marker!").status_code == 400
    assert client.get("/api/log-test?marker=m-1").status_code == 200
    try:
        client.get("/api/boom?marker=m-1")
        raise AssertionError("Boom must reach the middleware and server")
    except Boom:
        pass
    # Trust Cloud's last forwarded values while retaining the original peer for the upstream check.
    with patch.object(checks, "run", side_effect=lambda headers: {"peer": checks.PEER.get(), "scheme": request.scheme,
                                                               "client": request.remote_addr}):
        response = client.get("/api/checks", headers={"X-Forwarded-For": "203.0.113.4", "X-Forwarded-Proto": "https"},
                              environ_overrides={"REMOTE_ADDR": "::1"})
        assert response.json == {"peer": "::1", "scheme": "https", "client": "203.0.113.4"}, response.json
    assert logs.CONTEXT.get() == {} and checks.PEER.get() == ""
    print("Flask router contract checks passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        # Shared checks import app; use this module rather than registering a second set of jobs.
        sys.modules["app"] = sys.modules[__name__]
        logs.self_check()
        self_check()
        raise SystemExit(0)
    raise SystemExit(__doc__)

