"""Contract tests for checks.py (no network, no Redis, no database).

Run: uv run --env-file .env python test_checks.py
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from unittest.mock import patch


import app
import checks
import cloud_suite

SECRET = "hunter2-s3cret"
STATUSES = {"pass", "warn", "fail", "skip"}
KEYS = {"id", "label", "status", "detail", "help"}


def check_shape(result: dict) -> list[dict]:
    assert list(result) == ["groups"]
    rows = [row for group in result["groups"] for row in group["checks"]]
    assert [g["name"] for g in result["groups"]] == ["Web", "Logging", "Runtime", "Services"]
    assert all(set(row) == KEYS and row["status"] in STATUSES for row in rows), rows
    assert all(isinstance(v, str) for row in rows for v in row.values())
    assert len({row["id"] for row in rows}) == len(rows) == len(checks.CHECKS)
    json.dumps(result)
    return rows


def offline_run(headers: dict, **env: str) -> list[dict]:
    """Run every check with the network-facing ones stubbed out."""
    with patch.dict(os.environ, env), patch("urllib.request.urlopen", side_effect=OSError("offline")), \
            patch("socket.getaddrinfo", return_value=[("addr",)]):
        return check_shape(checks.run(headers))


def main() -> None:
    # Coverage gate (#19): every entry says where it applies and how heavy it is, and every full-tier
    # entry has its cloud_suite.py job (and every job an entry), so the runner can't drift from the list.
    assert all(len(entry) == 6 and callable(entry[4]) and entry[5] in ("quick", "full") for entry in checks.CHECKS)
    assert all(callable(entry[3]) for entry in checks.CHECKS if entry[5] == "quick")
    assert all(entry[4] is checks.everywhere or entry[4].__doc__ for entry in checks.CHECKS), "a skip needs its reason"
    assert {entry[1] for entry in checks.CHECKS if entry[5] == "full"} == set(cloud_suite.JOBS)

    with patch.object(checks, "_redis", return_value=None):
        rows = {row["id"]: row for row in offline_run({})}
    # Never raises: the stubbed network call becomes a fail row carrying the exception class.
    assert rows["runtime.https"]["status"] == "fail" and rows["runtime.https"]["detail"].startswith("OSError")
    # Not an ASGI server (here: none started), so applies() skips the async check with its reason.
    assert rows["web.event_loop"]["status"] == "skip" and "no event loop" in rows["web.event_loop"]["detail"]
    # A full-tier row with no suite result yet says how to get one.
    assert rows["logging.cloud_viewer"]["status"] == "skip"
    assert "cloud_suite.py --tier full" in rows["logging.cloud_viewer"]["detail"]
    assert rows["runtime.tmp"]["status"] == rows["runtime.subprocess"]["status"] == rows["runtime.threads"]["status"] == "pass"
    assert rows["web.proto"]["status"] == "skip"  # off Cloud

    # ASGI path: a measured value passes under 0.5 s and fails over it.
    with patch.object(app, "SERVER", "uvicorn"):
        for seconds, status in ((0.21, "pass"), (3.0, "fail")):
            checks.LOOP_SECONDS.set(seconds)
            assert checks.web_event_loop({})[0] == status
    checks.LOOP_SECONDS.set(None)
    with patch.object(app, "SERVER", "gunicorn"):
        assert {r["id"]: r for r in offline_run({})}["web.websocket"]["detail"].startswith("Not run on gunicorn")

    # Suite results: cloud_suite.py posts full-tier rows back; the row then shows the latest one.
    store: dict = {}
    fake = type("FakeRedis", (), {"get": lambda self, k: store.get(k), "set": lambda self, k, v: store.__setitem__(k, v)})()
    with patch.object(checks, "_redis", return_value=fake):
        for bad in ({}, {"web.server": {"status": "pass", "detail": ""}}, {"logging.cloud_viewer": {"status": "ok", "detail": ""}},
                    {"logging.cloud_viewer": {"status": "pass"}}, {"logging.cloud_viewer": "pass"}):
            assert checks.save_suite(bad)[0] == 400, bad
        assert checks.save_suite({"logging.cloud_viewer": {"status": "fail", "detail": "noise: 3 plain line(s)"}})[0] == 200
        posted = {r["id"]: r for r in offline_run({})}["logging.cloud_viewer"]
        assert posted["status"] == "fail" and posted["detail"].startswith("noise: 3 plain line(s) (cloud_suite.py, "), posted

    # On Cloud the proxy headers are required, matched case-insensitively.
    cloud = {"LARAVEL_CLOUD": "1", "LARAVEL_CLOUD_LOG_SOCKET": "unix:///nonexistent.sock", "WEB_CONCURRENCY": "1"}
    bad = {r["id"]: r["status"] for r in offline_run({"x-forwarded-proto": "http"}, **cloud)}
    assert bad["web.proto"] == bad["web.forwarded_for"] == bad["web.request_id"] == "fail"
    assert bad["logging.socket"] == "fail"
    good = {r["id"]: r["status"] for r in offline_run(
        {"X-Forwarded-Proto": "https", "x-forwarded-for": "1.2.3.4", "Cloud-Request-Id": "r"}, **cloud)}
    assert good["web.proto"] == good["web.forwarded_for"] == good["web.request_id"] == "pass"

    # One check blowing up never takes the endpoint down.
    with patch("tempfile.NamedTemporaryFile", side_effect=PermissionError("read-only")):
        broken = {r["id"]: r for r in offline_run({})}
    assert broken["runtime.tmp"]["status"] == "fail" and broken["runtime.tmp"]["detail"] == "PermissionError: read-only"
    assert broken["runtime.threads"]["status"] == "pass"

    # Credentials never reach detail: neither from the URL nor from an exception message that echoes them.
    url = f"mysql://app:{SECRET}@127.0.0.1:1/db"
    with patch("pymysql.connect", side_effect=RuntimeError(f"cannot connect with {url} ({SECRET})")):
        secret_rows = offline_run({}, DATABASE_URL=url)
    database = next(r for r in secret_rows if r["id"] == "services.database")
    assert database["status"] == "fail" and database["detail"].startswith("RuntimeError")
    assert SECRET not in json.dumps(secret_rows), database

    # Unsupported scheme fails cleanly; no DATABASE_URL skips.
    assert next(r for r in offline_run({}, DATABASE_URL="sqlite:///x") if r["id"] == "services.database")["status"] == "fail"
    with patch.dict(os.environ):
        os.environ.pop("DATABASE_URL", None)
        assert next(r for r in offline_run({}) if r["id"] == "services.database")["status"] == "skip"

    # A malformed service URL fails its own rows, never the whole list.
    bad_url = {r["id"]: r["status"] for r in offline_run({}, DATABASE_URL="mysql://[broken")}
    assert bad_url["services.database"] == "fail" and bad_url["runtime.threads"] == "pass"

    # No Redis URL at all is a skip, not a fail.
    with patch.dict(os.environ):
        for name in ("REDIS_URL", "LARAVEL_CLOUD_QUEUES_REDIS_URL"):
            os.environ.pop(name, None)
        assert next(r for r in offline_run({}) if r["id"] == "services.redis")["status"] == "skip"

    # A hung check is reported at the deadline instead of hanging the endpoint.
    import threading
    release = threading.Event()
    with patch.object(checks, "DEADLINE", 0.5), patch("pymysql.connect", side_effect=lambda **_: release.wait(10)):
        started = time.monotonic()
        hung = {r["id"]: r for r in offline_run({}, DATABASE_URL="mysql://u@127.0.0.1:1/db")}
        assert time.monotonic() - started < 3
        # While it is still stuck, the next request reports it instead of starting another probe.
        before = threading.active_count()
        again = {r["id"]: r for r in offline_run({}, DATABASE_URL="mysql://u@127.0.0.1:1/db")}
        assert again["services.database"]["status"] == "warn", again["services.database"]
        assert threading.active_count() <= before
    release.set()
    assert hung["services.database"]["status"] == "fail" and "no answer" in hung["services.database"]["detail"]
    deadline = time.monotonic() + 5
    while "services.database" in checks._running and time.monotonic() < deadline:
        time.sleep(0.05)
    assert "services.database" not in checks._running

    # Listening port: read from /proc on Linux, judged on [::] vs IPv4-only.
    with patch.dict(os.environ, PORT="8000"), patch.object(checks.Path, "is_file", return_value=True):
        with patch.object(checks, "_listeners", return_value={"::"}):
            assert checks.web_port({})[0] == "pass"
        with patch.object(checks, "_listeners", return_value={"0.0.0.0"}):
            assert checks.web_port({})[0] == "fail"
        with patch.object(checks, "_listeners", return_value=set()):
            assert checks.web_port({})[0] == "warn"
    # /proc/net parsing: port 8000 = 0x1F40, state 0A = LISTEN, 01 = ESTABLISHED.
    head = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
    tables = {
        "tcp6": head + "   0: 00000000000000000000000000000000:1F40 00000000000000000000000000000000:0000 0A 0:0 00:0 0 1000 0 1\n",
        "tcp": head + "   0: 0100007F:1F40 00000000:0000 0A 0:0 00:0 0 1000 0 2\n"
                      "   1: 00000000:1F41 00000000:0000 0A 0:0 00:0 0 1000 0 3\n"
                      "   2: 00000000:1F40 0100007F:9999 01 0:0 00:0 0 1000 0 4\n",
    }
    with patch.object(checks.Path, "read_text", lambda self: tables[self.name]):
        assert checks._listeners(8000) == {"::", "0100007F"}
        assert checks._listeners(8001) == {"0.0.0.0"}
    with patch.dict(os.environ, PORT="nope"):
        assert checks.web_port({})[0] == "warn"

    # CPU/memory rows warn when Python sees more than the cgroup allows (the node), pass when capped.
    cgroup = {"cpu.max": "200000 100000", "memory.max": str(4096 * 2**20)}
    with patch.object(checks, "_cgroup", cgroup.get):
        with patch("os.cpu_count", return_value=16):
            assert checks.cpu_limit({})[0] == "warn"
        with patch("os.cpu_count", return_value=2):
            assert checks.cpu_limit({})[0] == "pass"
        sizes = {"SC_PAGE_SIZE": 4096}
        with patch("os.sysconf", lambda name: sizes.get(name, 126511 * 256)):
            assert checks.memory_limit({})[0] == "warn"
        with patch("os.sysconf", lambda name: sizes.get(name, 4096 * 256)):
            status, detail, _ = checks.memory_limit({})
            assert status == "pass" and "sysconf reports 4096 MiB" in detail, detail

    # WebSocket check: skipped off Cloud; on Cloud a 101 with the right accept key passes.
    assert checks.web_websocket({"host": "x"})[0] == "skip"  # LARAVEL_CLOUD unset
    with patch.dict(os.environ, LARAVEL_CLOUD="1"):

        class FakeSock:
            def __init__(self, reply):
                self.reply = [reply, b""]
            def __enter__(self):
                return self
            def __exit__(self, *exc):
                return False
            def sendall(self, data):
                pass
            def recv(self, n):
                return self.reply.pop(0)
            def wrap_socket(self, sock, server_hostname):
                return sock

        key = base64.b64encode(b"k" * 16).decode()
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
        upgraded = b"HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n"
        for reply, expected in ((upgraded, "pass"), (b"HTTP/1.1 404 Not Found\r\n\r\n", "fail")):
            with patch("os.urandom", return_value=b"k" * 16), \
                    patch("socket.create_connection", return_value=FakeSock(reply)), \
                    patch("ssl.create_default_context", return_value=FakeSock(b"")):
                assert checks.web_websocket({"Host": "app.example"})[0] == expected

        # IPv6 upstream: ::1 passes, 127.0.0.1 warns.
        for peer, expected in (("::1", "pass"), ("127.0.0.1", "warn"), ("::ffff:127.0.0.1", "warn"), ("10.0.0.9", "warn")):
            checks.PEER.set(peer)
            assert checks.web_upstream_ipv6({})[0] == expected, peer
        checks.PEER.set("::ffff:127.0.0.1")
        assert "over IPv4 (127.0.0.1)" in checks.web_upstream_ipv6({})[1]
        # uvicorn rewrites the peer to X-Real-IP's client only when nginx came from trusted ::1.
        for server, expected in (("uvicorn", "pass"), ("granian-asgi", "warn")):
            with patch.object(app, "SERVER", server):
                checks.PEER.set("2600:1f16::2c")
                assert checks.web_upstream_ipv6({"x-real-ip": "2600:1f16::2c"})[0] == expected, server
                assert checks.web_upstream_ipv6({"x-real-ip": "2600:1f16::99"})[0] == "warn", server
        checks.PEER.set("")
    assert checks.web_upstream_ipv6({})[0] == "skip"

    # WEB_CONCURRENCY vs Cloud's generic-Python formula max(1, min(2c + 1, MiB // 150)) (#13).
    assert checks.web_concurrency({})[0] == "skip"  # off Cloud
    mib = 2**20 // 4096  # pages per MiB at a 4096-byte page
    for cores, memory, value, status, limited in ((2, 4096, "5", "pass", "cpu"), (2, 512, "3", "pass", "memory"),
                                                  (1, 256, "1", "pass", "memory"), (2, 4096, "3", "warn", "cpu"),
                                                  (2, 4096, "", "warn", "")):
        with patch.dict(os.environ, LARAVEL_CLOUD="1", WEB_CONCURRENCY=value), \
                patch.object(checks, "_cgroup", {"cpu.max": f"{cores}00000 100000"}.get), \
                patch("os.cpu_count", return_value=cores), patch("os.sysconf", lambda name: 4096 if name == "SC_PAGE_SIZE" else memory * mib):
            got, detail, _ = checks.web_concurrency({})
            assert got == status and f"{limited}-limited" in detail, (cores, memory, value, detail)
    with patch.dict(os.environ, LARAVEL_CLOUD="1", WEB_CONCURRENCY="5"), patch.object(checks, "_cgroup", {"cpu.max": "200000 100000"}.get), \
            patch("os.cpu_count", return_value=16):
        assert "above the cgroup CPU limit" in checks.web_concurrency({})[1]  # the bootstrap didn't run
    with patch.dict(os.environ, LARAVEL_CLOUD="1", WEB_CONCURRENCY="1"), patch.object(app, "SERVER", "gunicorn"), \
            patch.object(checks, "_cgroup", {"cpu.max": "100000 100000"}.get), patch("os.cpu_count", return_value=1), \
            patch("os.sysconf", lambda name: 4096 if name == "SC_PAGE_SIZE" else 4096 * mib):
        assert checks.web_concurrency({})[0] == "warn"  # WSGI has no hand-set exception

    for server in ("gunicorn", "uwsgi", "granian-wsgi", "hypercorn-wsgi", "waitress"):
        with patch.dict(os.environ, LARAVEL_CLOUD="1", WEB_CONCURRENCY="20" if server != "waitress" else ""), \
                patch.object(app, "SERVER", server), patch.object(checks, "_cgroup", {"cpu.max": "200000 100000"}.get), \
                patch("os.cpu_count", return_value=2), patch("os.sysconf", lambda name: 4096 if name == "SC_PAGE_SIZE" else 4096 * mib):
            assert checks.web_concurrency({})[0] == "pass", server

    # Streaming (#8): six chunks a second apart; scored on when they arrive.
    assert checks._streamed([0.1, 1.1, 2.1, 3.1, 4.1, 5.1]) == (True, "first chunk at 0.10 s, largest gap 1.00 s (streamed)")
    assert checks._streamed([5.1, 5.1, 5.1, 5.1, 5.1, 5.1])[1].endswith("(buffered: all at once)")
    assert checks._streamed([0.1, 0.1, 0.1, 3.1, 3.1, 5.1])[1].endswith("(in bursts)")
    assert checks._streamed([0.1, 1.1]) == (False, "2 of 6 chunks arrived")
    streamed, buffered = [0.1, 1.1, 2.1, 3.1, 4.1, 5.1], [5.1] * 6
    with patch.dict(os.environ, LARAVEL_CLOUD="1"):
        for plain, opted, expected in ((streamed, streamed, "pass"), (buffered, streamed, "pass"), (buffered, buffered, "fail")):
            with patch.object(checks, "_arrivals", lambda host, path: opted if "accel=no" in path else plain):
                assert checks.web_streaming({"host": "app.example"})[0] == expected
    with patch("time.sleep"):  # the WSGI generator: one chunk per send, no Content-Length
        import wsgi
        assert len(list(app.stream())) == app.STREAM_CHUNKS
    assert app.stream_headers("accel=no") == [("Content-Type", "text/event-stream"), ("X-Accel-Buffering", "no")]

    # Static files (#14): exact bytes from nginx, Cloud's cache headers, blocked files refused, the rest reaching the app.
    def answers(**override):
        def get(host, path):
            name = path.lstrip("/")
            if name in override:
                return override[name]
            if name in checks.STATIC:
                cache = {"cache-control": checks.static_cache(name)} if checks.static_cache(name) else {}
                return 200, cache, (checks.PUBLIC / name).read_bytes()
            if name in checks.STATIC_BLOCKED:
                return 403, {}, b"<html>403 Forbidden</html>"
            return 404, {"x-request-id": "r"}, b'{"error": "not found"}'
        return get
    with patch.dict(os.environ, LARAVEL_CLOUD="1"):
        for override, expected in (({}, "pass"), ({"static/cloud-check.css": (200, {}, (checks.PUBLIC / "static/cloud-check.css").read_bytes())}, "warn"),
                                   ({"cloud-check.sql": (200, {}, (checks.PUBLIC / "cloud-check.sql").read_bytes())}, "fail"),
                                   ({"robots.txt": (404, {"x-request-id": "r"}, b'{"error": "not found"}')}, "fail"),
                                   ({"cloud-check-missing.txt": (404, {}, b"<html>nginx 404</html>")}, "fail")):
            with patch.object(checks, "_get", answers(**override)):
                status, detail, _ = checks.web_static({"host": "app.example"})
                assert status == expected, (override, detail)
                assert expected == "pass" or next(iter(override)) in detail, detail
    # Types without a fixed value get the operator's NGINX_CACHE_DEFAULT, or no header when it is unset.
    for env, sent, expected in (("no-cache, private", "no-cache, private", "pass"), ("no-cache, private", None, "warn"),
                                ("", None, "pass"), ("", "no-cache, private", "warn")):
        with patch.dict(os.environ, LARAVEL_CLOUD="1", NGINX_CACHE_DEFAULT=env):
            robots = (200, {"cache-control": sent} if sent else {}, (checks.PUBLIC / "robots.txt").read_bytes())
            with patch.object(checks, "_get", answers(**{"robots.txt": robots})):
                status, detail, _ = checks.web_static({"host": "app.example"})
                assert status == expected, (env, sent, detail)

    # Latest patch and end of life (#12), from endoflife.date.
    import io
    import sys as _sys
    running = f"{_sys.version_info.major}.{_sys.version_info.minor}"
    # Patch lag only warns past PATCH_LAG_DAYS; end of life always warns.
    from datetime import date, timedelta
    recent, stale = (str(date.today() - timedelta(days=d)) for d in (5, checks.PATCH_LAG_DAYS + 1))
    for latest, released, eol, expected in ((f"{running}.{_sys.version_info.micro}", recent, "2999-01-01", "pass"),
                                            (f"{running}.{_sys.version_info.micro + 1}", recent, "2999-01-01", "pass"),
                                            (f"{running}.{_sys.version_info.micro + 2}", stale, "2999-01-01", "warn"),
                                            (f"{running}.{_sys.version_info.micro}", recent, "2000-01-01", "warn")):
        cycle = json.dumps(dict(latest=latest, latestReleaseDate=released, eol=eol)).encode()
        with patch("urllib.request.urlopen", return_value=io.BytesIO(cycle)), patch.object(checks.Path, "read_text", return_value=running):
            status, detail, _ = checks.python_version({})
        assert status == expected, detail
    assert "release(s) behind" in detail or "end of life" in detail

    # Standard library (#11): required modules fail the row, optional ones only warn.
    for missing, expected in (({}, "pass"), ({"_curses": "ModuleNotFoundError"}, "warn"), ({"_lzma": "ModuleNotFoundError"}, "fail")):
        probe = json.dumps(dict(missing=missing, encodings=["utf-8", "UTF-8"]))
        with patch("subprocess.run", return_value=subprocess.CompletedProcess([], 0, probe, "")):
            status, detail, _ = checks.stdlib_complete({})
        assert status == expected and all(name in detail for name in missing), detail
    assert checks.stdlib_complete({})[0] in ("pass", "warn")  # the real probe

    # Installed packages vs uv.lock: this venv is uv's, so it matches; a changed version fails.
    assert checks.packages_match({})[0] == "pass"
    real = checks.inventory()
    site = next(iter(real["packages"]))
    image = checks.sysconfig.get_paths(vars={"base": sys.base_prefix, "platbase": sys.base_prefix})["purelib"]
    cases = (({site: dict(real["packages"][site], redis="0.0.1")}, "fail"),
             # Two locked versions (one per Python range) are both fine.
             ({site: dict(real["packages"][site], websockets="16.1.1")}, "pass"),
             # The image's setuptools only matters when it would import ahead of the app's copy.
             ({**real["packages"], image: {"setuptools": "79.0.1", "pip": "24.0"}}, "pass"),
             ({image: {"setuptools": "79.0.1"}, **real["packages"]}, "pass"),  # 3.11: the app needs no setuptools
             ({image: {"certifi": "1999.1.1"}, **real["packages"]}, "warn"))
    read_text = checks.Path.read_text
    def fixture_text(path, *args, **kwargs):
        content = read_text(path, *args, **kwargs)
        return content + '\n[[package]]\nname = "websockets"\nversion = "16.1.1"\n' if path.name == "uv.lock" else content
    for packages, expected in cases:
        with patch.object(checks, "inventory", return_value=dict(real, packages=packages)), \
                patch.object(checks.Path, "read_text", fixture_text):
            status, detail, _ = checks.packages_match({})
        assert status == expected, (expected, detail)
    response = app.app.test_client().get("/api/packages")
    assert response.status_code == 200 and set(response.json) == {"python", "native", "packages"}

    # The endpoint finishes fast even with every network check stubbed.
    started = time.monotonic()
    offline_run({})
    assert time.monotonic() - started < 15

    # Through the router (handle) under the sync/WSGI path.
    with patch("urllib.request.urlopen", side_effect=OSError("offline")):
        response = app.app.test_client().get("/api/checks")
    assert response.status_code == 200
    check_shape(response.json)
    response = app.app.test_client().post("/api/suite-results", json={"web.server": {}})
    assert response.status_code == 400, response.data
    print("checks contract passed")


if __name__ == "__main__":
    main()
