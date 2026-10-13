"""Boot the real app under every server in serve.py, started the way Cloud starts it.

No Redis needed: only routes that don't touch the queue are called.
Run: uv run python test_servers.py
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from serve import COMMANDS, SINGLE_PROCESS

ROOT = Path(__file__).resolve().parent
# uWSGI prints its own boot and shutdown lines from C, outside Python logging: always plain text
# (laravel-cloud-logging README "Limits"). Every other server must write JSON only.
JSON_LOGS = set(COMMANDS) - {"uwsgi"}


def free_port() -> int:
    with socket.socket(socket.AF_INET6) as sock:
        sock.bind(("::1", 0))
        return sock.getsockname()[1]


def request(port, method, path, body=b"", headers=None, host="::1"):
    conn = http.client.HTTPConnection(host, port, timeout=10)
    try:
        conn.request(method, path, body=body, headers=headers or {},
                     encode_chunked=(headers or {}).get("Transfer-Encoding") == "chunked")
        response = conn.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        conn.close()

# Servers that finish a request in flight when SIGTERM arrives (#7).
DRAINS = {"gunicorn", "granian-wsgi"}


def check(server: str) -> None:
    port = free_port()
    env = {**os.environ, "PORT": str(port), "WEB_CONCURRENCY": "2", "PYTHONUNBUFFERED": "1"}
    env.pop("LARAVEL_CLOUD", None)  # logs to stdout, not the Cloud socket
    with tempfile.TemporaryFile() as out:
        proc = subprocess.Popen([sys.executable, "serve.py", server], cwd=ROOT, env=env,
                                stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 20
            while True:
                assert proc.poll() is None, f"{server} exited {proc.returncode}"
                try:
                    if request(port, "GET", "/api/ping")[0] == 200:
                        break
                except OSError:
                    pass
                assert time.monotonic() < deadline, f"{server} not ready on [::]:{port}"
                time.sleep(0.1)
            status, headers, body = request(port, "GET", "/api/ping", headers={"Cloud-Request-ID": "cr-test"})
            pong = json.loads(body)
            assert status == 200 and pong["ok"] and pong["pid"] and headers.get("x-request-id"), (status, pong)
            assert request(port, "GET", "/")[0] == 200
            # Cloud's nginx connects to 127.0.0.1, so [::] must not be IPv6-only.
            assert request(port, "GET", "/api/ping", host="127.0.0.1")[0] == 200, f"{server} unreachable over IPv4"
            assert request(port, "GET", "/nope")[0] == 404
            assert request(port, "POST", "/api/check", b"{}", {"Content-Type": "text/plain"})[0] == 415
            # Chunked bodies (no Content-Length) get the same JSON checks as sized ones.
            chunked = request(port, "POST", "/api/check", iter([b"[", b"]"]),
                              {"Content-Type": "application/json", "Transfer-Encoding": "chunked"})
            assert chunked[0] == 400, f"{server} chunked body: {chunked}"
            # /api/stream reaches the client chunk by chunk: the first before the second is sent (#8).
            conn = http.client.HTTPConnection("::1", port, timeout=10)
            started = time.monotonic()
            conn.request("GET", "/api/stream?accel=no")
            response = conn.getresponse()
            first = response.readline()
            assert first.startswith(b"data:") and time.monotonic() - started < 0.9, f"{server} buffered /api/stream"
            assert response.getheader("x-accel-buffering") == "no"
            conn.close()
            # The server's own error path logs an exception the app doesn't catch (#17).
            # uWSGI closes the connection without a response instead of sending a 500 (nginx turns it into a 502).
            try:
                boom = request(port, "GET", "/api/boom?marker=t-boom", headers={"Cloud-Request-ID": "cr-boom"})[0]
            except http.client.RemoteDisconnected:
                boom = None
            assert boom == (None if server == "uwsgi" else 500), f"{server} /api/boom: {boom}"
            assert request(port, "GET", "/api/boom-thread?marker=t-thread")[0] == 200
            # /api/upload counts bodies far past the 64 KiB cap, sized or chunked, without truncating them (#10).
            # uWSGI can't read chunked bodies, so it rejects them (Cloud's nginx always sends a Content-Length):
            # send it a tiny one, since it answers before reading and a big one would end in a broken pipe.
            size = 3 * 1024 * 1024 + 7
            chunks = [b"u" * 65536] * (size // 65536) + [b"u" * (size % 65536)]
            upload = {"Content-Type": "application/octet-stream"}
            assert json.loads(request(port, "POST", "/api/upload", b"".join(chunks), upload)[2]) == {"bytes": size}
            # hypercorn's WSGI wrapper buffers the body and answers an empty 400 above its 16 MiB wsgi_max_body_size.
            over = request(port, "POST", "/api/upload", b"u" * (16 * 1024 * 1024 + 1), upload)
            expected = (400, b"") if server == "hypercorn-wsgi" else (200, b'{"bytes": 16777217}')
            assert (over[0], over[2]) == expected, f"{server} 16 MiB + 1 upload: {over[0]} {over[2][:80]}"
            chunked = {**upload, "Transfer-Encoding": "chunked"}
            status, _, body = request(port, "POST", "/api/upload", [b"u"] if server == "uwsgi" else chunks, chunked)
            expected = (400, {"error": "invalid Content-Length"}) if server == "uwsgi" else (200, {"bytes": size})
            assert (status, json.loads(body)) == expected, f"{server} chunked upload: {status} {body}"
            big = b"x" * (64 * 1024 + 1)
            assert request(port, "POST", "/api/check", big, {"Content-Type": "application/json"})[0] == 400
            # SIGTERM with a slow request in flight: a draining server finishes it before exiting (#7).
            slow: list = []

            def call_slow() -> None:
                try:
                    slow.append(request(port, "GET", "/api/slow?seconds=2")[0])
                except OSError as exc:  # http.client.RemoteDisconnected is a ConnectionResetError
                    slow.append(type(exc).__name__)

            caller = threading.Thread(target=call_slow)
            caller.start()
            time.sleep(0.5)
            os.killpg(proc.pid, signal.SIGTERM)
            caller.join(timeout=30)
            # Locally, with their serve.py flags, only these finish the in-flight request. The others drop it:
            # waitress has no SIGTERM handler, and uWSGI, hypercorn exit without waiting for it.
            drained = server in DRAINS
            assert (slow == [200]) == drained, f"{server} slow request during SIGTERM: {slow}"
            clean = -signal.SIGTERM if server == "waitress" else 0
            assert proc.wait(timeout=40) == clean, f"{server} exit code {proc.returncode}"
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
        out.seek(0)
        lines = [line for line in out.read().decode(errors="replace").splitlines() if line.strip()]
    plain = [line for line in lines if not line.startswith("{")]
    assert server not in JSON_LOGS or not plain, f"{server} wrote plain-text log lines:\n" + "\n".join(plain)
    records = [json.loads(line) for line in lines if line.startswith("{")]
    access = [r for r in records if r["message"] == "access" and r["context"].get("cloud_request_id") == "cr-test"]
    assert len(access) == 1 and access[0]["context"]["path"] == "/api/ping", records
    for marker in ("t-boom", "t-thread"):
        errors = [r for r in records if f"uncaught boom {marker}" in json.dumps(r) and r["level"] >= 400]
        assert len(errors) == 1, f"{server}: expected one ERROR-or-worse record for {marker}, saw {len(errors)}"
        # Cloud only shows a structured exception when the record carries one (laravel-cloud-python-logging#38).
        assert "exception" in errors[0]["context"], f"{server}: {marker} record: {errors[0]}"
    # The middleware logs a request's uncaught exception while its ID is set, so it can be traced to the request.
    boom = next(r for r in records if "uncaught boom t-boom" in json.dumps(r))
    assert (boom["extra"].get("logger"), boom["message"], boom["level_name"]) == (
        "uncaught", "Uncaught exception in GET /api/boom", "ERROR"), f"{server}: {boom}"
    assert boom["context"].get("cloud_request_id") == "cr-boom", f"{server}: no request ID on {boom}"
    startups = {r["context"].get("pid") for r in records if r["message"] == "startup"}
    expected = 1 if server in SINGLE_PROCESS else 2
    assert len(startups) == expected, f"{server}: expected {expected} workers from WEB_CONCURRENCY, saw {startups}"
    print(f"{server} ok")


if __name__ == "__main__":
    # What the Cloud build command does; serve.py's commands read it.
    subprocess.run(["laravel-cloud-logging-config", str(ROOT / "logging.json")], check=True)
    for name in sys.argv[1:] or COMMANDS:
        check(name)
    print("test_servers ok")
