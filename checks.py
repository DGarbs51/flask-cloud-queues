"""Platform checks behind GET /api/checks.

Each check is a small function returning (status, detail, help); CHECKS lists them in display order.
run() never raises: a check that throws becomes a fail row, every detail is scrubbed of credentials,
and the checks run in parallel under a 12 s deadline (WSGI daemon threads).

CHECKS is the one registry the page, /api/checks and cloud_suite.py share (#19). Each entry has
applies(server, python), false where the check doesn't fit (the row is a skip with the reason), and a tier:
quick runs here on every request; full needs the Cloud CLI, so cloud_suite.py runs it and posts the result
back, and the row shows that.
"""

from __future__ import annotations

import base64
import contextvars
import hashlib
import http.client
import importlib.metadata
import json
import os
import platform
import re
import socket
import ssl
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

import certifi

import serve
import telemetry

TIMEOUT = 5
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
CLOUD_ONLY = "Only checked on Laravel Cloud."
# The async check measures 50 concurrent 0.2 s sleeps; sync callers may supply a measurement.
LOOP_SECONDS: contextvars.ContextVar[float | None] = contextvars.ContextVar("loop_seconds", default=None)
# app.py sets this before ProxyFix replaces the peer to the address the request came from: nginx's side of the upstream connection.
PEER: contextvars.ContextVar[str] = contextvars.ContextVar("peer", default="")
SUITE_KEY = telemetry.KEY_PREFIX + "suite:"  # + check id: the latest cloud_suite.py result for a full-tier row
# Cloudflare's browser integrity check rejects Python's default user agent with 403.
BROWSER = {"User-Agent": "Mozilla/5.0"}
USERINFO = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/@]*@")

Result = tuple[str, str, str]


def on_cloud() -> bool:
    return os.environ.get("LARAVEL_CLOUD") == "1"


def _scrub(text: str) -> str:
    text = USERINFO.sub(r"\1", text)
    for name in ("DATABASE_URL", "REDIS_URL"):
        try:
            password = urlsplit(os.environ.get(name) or "").password
        except ValueError:  # unparsable URL: its own check reports that
            continue
        for secret in (password, unquote(password or "")):
            if secret:
                text = text.replace(secret, "***")
    return text


def _redis():
    """The telemetry Redis client with bounded timeouts, or None when no Valkey is attached."""
    import app

    if not (os.environ.get("REDIS_URL") or os.environ.get("LARAVEL_CLOUD_QUEUES_REDIS_URL")):
        return None
    client = app.telemetry.store
    client.connection_pool.connection_kwargs.update(socket_connect_timeout=TIMEOUT, socket_timeout=TIMEOUT)
    return client


def _database() -> tuple[str, str, int] | None:
    """(kind, host, port) from DATABASE_URL, or None when unset. Raises on an unsupported scheme."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    parts = urlsplit(url)
    if parts.scheme == "mysql":
        return "mysql", parts.hostname or "", parts.port or 3306
    if parts.scheme in ("postgres", "postgresql"):
        return "postgres", parts.hostname or "", parts.port or 5432
    raise ValueError(f"DATABASE_URL scheme {parts.scheme!r} is not mysql or postgres")


def _header(headers, name: str) -> str:
    return next((v for k, v in headers.items() if k.lower() == name), "")


# Web


def web_server(headers) -> Result:
    import app

    expected = os.environ.get("WEB_CONCURRENCY")
    if not expected:
        if on_cloud():
            return "warn", f"{app.SERVER}, WEB_CONCURRENCY is not set", (
                "Cloud sets WEB_CONCURRENCY to match the instance size. Without it the server runs a single process.")
        return "pass", f"{app.SERVER}, 1 process (WEB_CONCURRENCY not set)", ""
    wanted = int(expected)
    if app.SERVER in serve.SINGLE_PROCESS:
        return "pass", f"{app.SERVER}, 1 process (it has no process count, so WEB_CONCURRENCY={wanted} does not apply)", ""
    # Workers are the children of one parent (the server's master); a single process has no parent.
    count = _siblings(os.getppid()) if wanted > 1 else 1
    detail = f"{app.SERVER}, {count} process(es), WEB_CONCURRENCY={wanted}"
    if count == wanted:
        return "pass", detail, ""
    return "warn", detail, "The server is running a different number of processes than WEB_CONCURRENCY asks for. A worker may be restarting; reload to check again."


def web_concurrency(headers) -> Result:
    import app

    if not on_cloud() or _cgroup("cpu.max") is None:
        return "skip", "Not on Laravel Cloud, so nothing sets WEB_CONCURRENCY.", CLOUD_ONLY
    # Cloud's generic Python / WSGI profile (laravel/cloud PythonProfile::webConcurrency), from the inputs Python sees:
    # cloud-bootstrap caps os.cpu_count() and sysconf at the pod's cgroup limits.
    cores = os.cpu_count() or 1
    mib = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // 2**20
    if app.SERVER in serve.SINGLE_PROCESS:
        return "pass", f"{app.SERVER} runs one process; WEB_CONCURRENCY does not apply", ""
    expected = max(1, min(2 * cores + 1, mib // 150))
    inputs = f"{cores} vCPU, {mib} MiB, {'cpu' if 2 * cores + 1 <= mib // 150 else 'memory'}-limited; FastAPI would get {max(1, cores)}"
    quota, period = _cgroup("cpu.max").split()
    if quota != "max" and cores > -(-int(quota) // int(period)):
        return "warn", f"os.cpu_count()={cores} is above the cgroup CPU limit ({int(quota) / int(period):g} vCPU)", (
            "Python sees the node's CPUs, so the cloud-bootstrap capping didn't run (check the build command copies it) "
            "and the formula's inputs are wrong.")
    value = os.environ.get("WEB_CONCURRENCY")
    if not value:
        return "warn", f"WEB_CONCURRENCY is not set; formula gives {expected} ({inputs})", (
            "Cloud writes WEB_CONCURRENCY at deploy time from the App instance size. Without an App instance it isn't set.")
    if int(value) == expected:
        return "pass", f"WEB_CONCURRENCY={value}, formula gives {expected} ({inputs})", ""
    if app.SERVER in serve.ASGI and int(value) == max(1, cores):
        # Set by hand on the async environments: one worker per whole core. Cloud has no ASGI case to compare with.
        return "pass", (f"WEB_CONCURRENCY={value}, hand-set; Cloud's generic Python formula gives {expected} ({inputs}). "
                        "Cloud has no ASGI case for a non-FastAPI app"), ""
    # Proposed sync rule for process-model WSGI servers (Notion: "Python WEB_CONCURRENCY defaults by server and
    # interface"): memory decides, after a max(256 MiB, 25%) reserve, capped at 16 per core.
    sync = min(max(1, (mib - max(256, mib // 4)) // 150), 16 * cores)
    if app.SERVER not in serve.ASGI and app.SERVER not in serve.SINGLE_PROCESS and int(value) == sync:
        return "pass", (f"WEB_CONCURRENCY={value}, hand-set to the proposed sync rule; Cloud's generic Python formula "
                        f"gives {expected} ({inputs})"), ""
    return "warn", f"generic Python formula gives {expected} ({inputs}); WEB_CONCURRENCY={value} differs (a hand-set override may explain it)", (
        "Cloud sizes WEB_CONCURRENCY from the nominal instance size at deploy, while Python sees the pod's cgroup. A mismatch "
        "means one of them is wrong, the instance was resized without a redeploy, or WEB_CONCURRENCY was set by hand.")


def _siblings(parent: int) -> int:
    """Child processes of parent, ignoring multiprocessing's resource tracker (uvicorn spawns one)."""
    if Path("/proc/self").is_dir():
        procs = []
        for pid in filter(str.isdigit, os.listdir("/proc")):
            try:
                ppid = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[1]
                procs.append((ppid, Path(f"/proc/{pid}/cmdline").read_text()))
            except OSError:
                continue
    else:
        out = subprocess.run(["ps", "-A", "-o", "pid=,ppid=,command="], capture_output=True, text=True, timeout=TIMEOUT, check=True).stdout
        procs = [(line.split()[1], line) for line in out.splitlines() if line.strip()]
    return sum(ppid == str(parent) and "resource_tracker" not in cmd for ppid, cmd in procs)


def web_port(headers) -> Result:
    port = os.environ.get("PORT", "")
    if not port.isdigit():
        return "warn", f"PORT is {port or 'not set'}", "Cloud tells the server which port to listen on through PORT. Use `--port $PORT` in the start command."
    if not Path("/proc/net/tcp6").is_file():
        return "skip", f"PORT={port}; listening sockets can't be read here", "Only checked on Linux, which includes Laravel Cloud."
    listeners = _listeners(int(port))
    if "::" in listeners:
        return "pass", f"listening on [::]:{port}", ""
    if listeners:
        return "fail", f"listening on {', '.join(sorted(listeners))} port {port}, not [::]", (
            "Cloud's startup probes connect over IPv6 (nginx over 127.0.0.1), so the server must listen on dual-stack `::`. "
            "Use `--host ::` (uvicorn) or `--bind [::]:$PORT` (gunicorn).")
    return "warn", f"nothing found listening on port {port}", "The server may be listening on a different port than PORT. Use `--port $PORT` in the start command."


def _listeners(port: int) -> set[str]:
    """Addresses in LISTEN state on port, from /proc/net/tcp6 and /proc/net/tcp ("::", "0.0.0.0", or other)."""
    found = set()
    for table, wildcard in (("tcp6", "0" * 32), ("tcp", "0" * 8)):
        for line in Path("/proc/net", table).read_text().splitlines()[1:]:
            local, state = line.split()[1], line.split()[3]
            address, hex_port = local.split(":")
            if state == "0A" and int(hex_port, 16) == port:  # 0A = LISTEN
                found.add(("::" if table == "tcp6" else "0.0.0.0") if address == wildcard else address)
    return found


def _proxy_header(name: str, expected: str | None = None):
    def check(headers) -> Result:
        if not on_cloud():
            return "skip", "Not on Laravel Cloud, so no proxy sits in front of this app.", CLOUD_ONLY
        value = _header(headers, name.lower())
        if not value:
            return "fail", f"{name} header is missing", "Requests should reach the app through Cloud's proxy, which adds this header. Check the app is opened through its Cloud URL."
        if expected and value.lower() != expected:
            return "fail", f"{name}={value}, expected {expected}", "Cloud's proxy should forward HTTPS requests. Check the app is opened through its https:// Cloud URL."
        return "pass", f"{name} present" + (f" ({value})" if expected else ""), ""

    return check


def web_event_loop(headers) -> Result:
    seconds = LOOP_SECONDS.get()
    detail = f"50 concurrent 0.2 s sleeps finished in {seconds:.2f} s"
    if seconds < 0.5:
        return "pass", detail, ""
    return "fail", detail, "Awaiting many tasks at once took far longer than one sleep, so something is blocking the async event loop. Async requests will queue behind each other."


def web_websocket(headers) -> Result:
    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so there is no proxy chain to test.", CLOUD_ONLY
    host = _header(headers, "host").split(":")[0]
    if not host:
        return "fail", "request has no Host header", FAIL_HELP
    # A raw RFC 6455 handshake to this app's public URL: Cloudflare, Envoy and the pod's nginx must all
    # pass Upgrade: websocket through for the app to answer 101.
    key = base64.b64encode(os.urandom(16)).decode()
    request = (f"GET /ws/echo HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
    context = ssl.create_default_context()
    with socket.create_connection((host, 443), timeout=TIMEOUT) as raw, context.wrap_socket(raw, server_hostname=host) as sock:
        sock.sendall(request.encode())
        response = b""
        while b"\r\n\r\n" not in response and len(response) < 16384:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
    return _websocket_result(host, key, response)


def _websocket_result(host: str, key: str, response: bytes) -> Result:
    status_line = response.split(b"\r\n", 1)[0].decode(errors="replace")
    accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
    if " 101 " in status_line + " " and accept.encode() in response:
        return "pass", f"wss://{host}/ws/echo upgraded ({status_line})", ""
    return "fail", f"wss://{host}/ws/echo answered {status_line or 'nothing'}", (
        "A proxy between the browser and the app dropped the WebSocket upgrade, so WebSockets (live updates, "
        "chat, Django Channels) can't connect. The pod's nginx must forward the Upgrade and Connection headers.")


def _arrivals(host: str, path: str) -> list[float]:
    """Seconds from sending the request until each `data:` line of path arrived."""
    import app

    # A fully buffered stream arrives only when it ends, STREAM_CHUNKS - 1 seconds in: wait past that.
    conn = http.client.HTTPSConnection(host, timeout=app.STREAM_CHUNKS + 3, context=ssl.create_default_context())
    try:
        started = time.monotonic()
        conn.request("GET", path, headers=BROWSER)
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f"{path} answered {response.status}")
        arrivals: list[float] = []
        while len(arrivals) < app.STREAM_CHUNKS and (line := response.readline()):
            if line.startswith(b"data:"):
                arrivals.append(time.monotonic() - started)
        return arrivals
    finally:
        conn.close()


def _streamed(arrivals: list[float]) -> tuple[bool, str]:
    """(arrived as sent, detail). Chunks leave one a second; as sent = each within 0.5 s of that."""
    import app

    if len(arrivals) < app.STREAM_CHUNKS:
        return False, f"{len(arrivals)} of {app.STREAM_CHUNKS} chunks arrived"
    first = arrivals[0]
    gap = max(b - a for a, b in zip(arrivals, arrivals[1:]))
    if first < 1.5 and all(abs(t - first - i) <= 0.5 for i, t in enumerate(arrivals)):
        verdict = "streamed"
    elif arrivals[-1] - first < 1:
        verdict = "buffered: all at once"
    else:
        verdict = "in bursts"
    return verdict == "streamed", f"first chunk at {first:.2f} s, largest gap {gap:.2f} s ({verdict})"


def web_streaming(headers) -> Result:
    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so no proxy can buffer the response.", CLOUD_ONLY
    host = _header(headers, "host").split(":")[0]
    if not host:
        return "fail", "request has no Host header", FAIL_HELP
    # Through the public URL, with and without nginx's per-response opt-out, at the same time.
    with ThreadPoolExecutor(2) as pool:
        (plain, plain_detail), (opted, opted_detail) = pool.map(
            lambda path: _streamed(_arrivals(host, path)), ("/api/stream", "/api/stream?accel=no"))
    return _streaming_result((plain, plain_detail), (opted, opted_detail))


def _streaming_result(plain_result, opted_result) -> Result:
    plain, plain_detail = plain_result
    opted, opted_detail = opted_result
    detail = f"default: {plain_detail}; with X-Accel-Buffering: no: {opted_detail}"
    # The documented opt-out is what apps rely on; buffering without it (uWSGI's --http-socket, SE-303) is expected.
    if plain or opted:
        return "pass", detail, ""
    return "fail", detail, (
        "Something between the browser and the app buffers streamed responses even with X-Accel-Buffering: no, "
        "so server-sent events and AI chat look frozen and then arrive all at once.")


PUBLIC = Path(__file__).with_name("public")
# Fixtures under public/ (#14), with the Cache-Control Cloud's nginx adds by content type. None: the type has no fixed
# value, so nginx sends NGINX_CACHE_DEFAULT, which Cloud's operator sets to "no-cache, private" for now (SE-290).
STATIC = {
    "cloud-check.txt": None,
    "static/cloud-check.css": "public, max-age=31536000, immutable",
    "static/cloud-check.js": "public, max-age=31536000, immutable",
    "static/cloud-check.png": "private, max-age=86400, stale-while-revalidate=604800",
    "robots.txt": None,
    ".well-known/cloud-check.txt": None,
}
STATIC_BLOCKED = [".cloud-check-secret", "cloud-check.sql", "cloud-check.log"]  # nginx must refuse these


def static_cache(path: str) -> str | None:
    return STATIC[path] or os.environ.get("NGINX_CACHE_DEFAULT") or None


def _get(host: str, path: str) -> tuple[int, dict[str, str], bytes]:
    conn = http.client.HTTPSConnection(host, timeout=TIMEOUT, context=ssl.create_default_context())
    try:
        conn.request("GET", path, headers=BROWSER)
        response = conn.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        conn.close()


def web_static(headers) -> Result:
    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so no nginx serves public/.", CLOUD_ONLY
    host = _header(headers, "host").split(":")[0]
    if not host:
        return "fail", "request has no Host header", FAIL_HELP
    paths = [*STATIC, *STATIC_BLOCKED, "cloud-check-missing.txt"]
    with ThreadPoolExecutor(len(paths)) as pool:
        answers = dict(zip(paths, pool.map(lambda path: _get(host, "/" + path), paths)))
    return _static_result(answers)


def _static_result(answers) -> Result:
    broken, cache = [], []
    # The app has no route for these paths, so the file's exact bytes can only have come from nginx.
    for path in STATIC:
        status, got, body = answers[path]
        expected = static_cache(path)
        if status != 200 or body != (PUBLIC / path).read_bytes():
            broken.append(f"{path}: {status}, {'wrong content' if status == 200 else 'not served'}")
        elif got.get("cache-control") != expected:
            cache.append(f"{path}: Cache-Control={got.get('cache-control')}, expected {expected}")
    for path in STATIC_BLOCKED:
        status, _, body = answers[path]
        if status == 200 or (PUBLIC / path).read_bytes().strip() in body:
            broken.append(f"{path}: {status}, should be blocked (LEAKED)")
    status, got, body = answers["cloud-check-missing.txt"]
    if status != 404 or b"not found" not in body or "x-request-id" not in got:
        broken.append(f"cloud-check-missing.txt: {status}, didn't reach the app")
    if broken:
        return "fail", "; ".join(broken + cache), (
            "nginx should serve files under public/ itself, refuse dotfiles, backups, logs and SQL dumps, and pass "
            "anything else to the app. A leaked blocked file is a security problem.")
    if cache:
        return "warn", "; ".join(cache), "Files are served, but not with the Cache-Control Cloud's nginx sets by content type."
    return "pass", f"{len(STATIC)} files served by nginx with Cloud's cache headers, {len(STATIC_BLOCKED)} blocked, unknown paths reach the app", ""


def web_upstream_ipv6(headers) -> Result:
    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so no nginx sits in front of this app.", CLOUD_ONLY
    import app

    peer = PEER.get()
    # A dual-stack listener (`--host ::`) sees an IPv4 client as ::ffff:127.0.0.1; it is still IPv4.
    peer = peer.removeprefix("::ffff:")
    if peer == "::1":
        return "pass", "nginx reaches the app over IPv6 (::1)", ""
    # uvicorn's proxy headers replace the peer with X-Forwarded-For when the connection comes from a trusted address,
    # by default 127.0.0.1 and ::1. On its `--host ::` listener nginx's IPv4 arrives as ::ffff:127.0.0.1, which isn't
    # trusted, so a rewritten peer (the client nginx saw, X-Real-IP) means nginx connected from ::1.
    if app.SERVER == "uvicorn" and peer and peer == _header(headers, "x-real-ip"):
        return "pass", "nginx reaches the app over IPv6 (::1); uvicorn replaced the peer with X-Forwarded-For", ""
    if peer == "127.0.0.1":
        return "warn", "nginx reaches the app over IPv4 (127.0.0.1)", (
            "This app works because it listens on both IPv4 and IPv6, but an app that listens on IPv6 only "
            "(for example `--host ::` with IPV6_V6ONLY) would be unreachable. nginx should try [::1] first.")
    return "warn", f"request came from {peer or 'an unknown address'}, not the pod's nginx", (
        "Expected nginx on the same pod (::1 or 127.0.0.1).")


# Logging


def log_handler(headers) -> Result:
    import logging

    from laravel_cloud_logging import CloudHandler

    handlers = [type(h).__name__ for h in logging.getLogger().handlers]
    if any(isinstance(h, CloudHandler) for h in logging.getLogger().handlers):
        return "pass", f"root logger handlers: {', '.join(handlers)}", ""
    return "fail", f"root logger handlers: {', '.join(handlers) or 'none'}", (
        "laravel-cloud-logging is not installed on the root logger, so logs will not be structured for Cloud. Call configure() at startup.")


def log_socket(headers) -> Result:
    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so logs go to stdout.", CLOUD_ONLY
    address = os.environ.get("LARAVEL_CLOUD_LOG_SOCKET") or "unix:///tmp/cloud-init.sock"
    try:
        if address.startswith("unix://"):
            sock, target = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM), address[len("unix://"):]
        else:
            host, _, port = address.removeprefix("tcp://").rpartition(":")
            sock, target = socket.socket(socket.AF_INET, socket.SOCK_STREAM), (host, int(port))
        with sock:
            sock.settimeout(2)
            sock.connect(target)
    except Exception as exc:
        return "fail", f"could not connect to {address}: {type(exc).__name__}", (
            "Logs fall back to stdout and may lose their levels (everything looks like info). Check the Cloud log socket is available to this process.")
    return "pass", f"connected to {address}", ""


# Runtime


PATCH_LAG_DAYS = 30


def _python_pin() -> str | Result:
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    detail = f"running Python {sys.version.split()[0]}"
    pinned = Path(__file__).with_name(".python-version")
    if not pinned.is_file():
        return "skip", detail + ", no .python-version file", "Add a .python-version file so the expected Python version is written down."
    wanted = pinned.read_text().strip()
    if not (wanted == running or wanted.startswith(running + ".")):
        return "warn", f"{detail}, .python-version is {wanted}", "The Python running this app is not the one pinned in .python-version. Check the Python version set for this Cloud environment."
    return detail + f", .python-version is {wanted}"


def python_version(headers) -> Result:
    detail = _python_pin()
    if not isinstance(detail, str):
        return detail
    running = python_minor()
    # The newest patch release and end of life for this major.minor (#12).
    try:
        with urllib.request.urlopen(f"https://endoflife.date/api/python/{running}.json", timeout=TIMEOUT,
                                    context=ssl.create_default_context(cafile=certifi.where())) as response:
            cycle = json.load(response)
    except Exception as exc:
        return "pass", f"{detail}; latest-patch comparison skipped ({type(exc).__name__})", ""
    return _python_release(detail, cycle)


def _python_release(detail: str, cycle: dict) -> Result:
    running = python_minor()
    latest, notes, problems = cycle["latest"], [], []
    behind = int(latest.split(".")[2]) - sys.version_info.micro
    if behind > 0:
        days = (date.today() - date.fromisoformat(cycle["latestReleaseDate"])).days
        # A patch arrives with the next base-image bump; only a lag past PATCH_LAG_DAYS means the bumps have stalled.
        (problems if days > PATCH_LAG_DAYS else notes).append(
            f"latest is {latest} ({behind} release(s) behind; {latest} released {days} days ago)")
    if isinstance(cycle.get("eol"), str) and date.fromisoformat(cycle["eol"]) <= date.today():
        problems.append(f"Python {running} reached end of life on {cycle['eol']}")
    if problems:
        return "warn", f"{detail}; " + "; ".join(problems + notes), (
            f"Cloud's Python comes from its base image, so a patch release arrives with the next base-image bump, and "
            f".python-version can't pin one (Cloud reads only major.minor). Behind by more than {PATCH_LAG_DAYS} days means "
            "the bumps have stalled. Past end of life there are no security fixes.")
    return "pass", f"{detail}; " + "; ".join(notes) if notes else f"{detail} (latest {running})", ""


def outbound_https(headers) -> Result:
    with urllib.request.urlopen("https://pypi.org/simple/", timeout=TIMEOUT, context=ssl.create_default_context()) as response:
        response.read(1)
        return "pass", f"GET https://pypi.org/simple/ returned {response.status}", ""


def tmp_writable(headers) -> Result:
    with tempfile.NamedTemporaryFile(dir="/tmp") as file:
        file.write(b"ok")
        file.flush()
    return "pass", "wrote and removed a file in /tmp", ""


STDLIB = ["sqlite3", "_sqlite3", "ssl", "_ssl", "hashlib", "_hashlib", "lzma", "_lzma", "bz2", "_bz2", "zlib",
          "ctypes", "_ctypes", "zoneinfo", "decimal", "_decimal", "uuid", "_uuid"]
# Not needed on a server, so missing ones only warn. tkinter isn't checked: a GUI toolkit is never usable on Cloud
# (no display), and the slim image leaves it out.
STDLIB_OPTIONAL = ["readline", "curses", "_curses", "dbm", "_dbm"]
# Run in a child so imports with side effects (readline) never touch the web process.
STDLIB_PROBE = """
import importlib, json, locale, sys
missing = {}
for name in sys.argv[1:]:
    try:
        importlib.import_module(name)
    except Exception as exc:
        missing[name] = type(exc).__name__
try:
    from zoneinfo import ZoneInfo
    ZoneInfo("America/New_York")
except Exception as exc:
    missing["tz data"] = type(exc).__name__
print(json.dumps(dict(missing=missing, encodings=[sys.getfilesystemencoding(), locale.getpreferredencoding(False)])))
"""


def stdlib_complete(headers) -> Result:
    out = subprocess.run([sys.executable, "-c", STDLIB_PROBE, *STDLIB, *STDLIB_OPTIONAL], capture_output=True, text=True,
                         timeout=TIMEOUT, check=True).stdout
    probe = json.loads(out)
    native = inventory()["native"]
    versions = f"OpenSSL {native['openssl'].split()[1]}, SQLite {native['sqlite']}"
    required = {name: error for name, error in probe["missing"].items() if name not in STDLIB_OPTIONAL}
    not_utf8 = [e for e in probe["encodings"] if e.lower().replace("-", "") != "utf8"]
    if required or not_utf8:
        missing = ", ".join(f"{name} ({error})" for name, error in required.items())
        return "fail", "; ".join(filter(None, [missing and f"missing: {missing}", not_utf8 and f"encodings: {probe['encodings']}"])), (
            "Part of the standard library this Python was built without (or text that isn't UTF-8) breaks apps that "
            "work locally: sqlite3 for Django, ssl and hashlib for HTTPS and passwords, lzma/bz2 for archives.")
    if probe["missing"]:
        return "warn", f"{len(STDLIB)} required modules import, tz data and UTF-8 OK, {versions}; optional missing: {', '.join(probe['missing'])}", (
            "Only modules a server rarely needs are missing (terminal and GUI support).")
    return "pass", f"{len(STDLIB) + len(STDLIB_OPTIONAL)} modules import, tz data and UTF-8 OK, {versions}", ""


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def inventory() -> dict:
    """GET /api/packages: this Python, the native libraries it's built against, and every installed package by location."""
    try:
        import sqlite3
        sqlite = sqlite3.sqlite_version
    except ImportError:
        sqlite = None
    import pyexpat
    import zlib

    packages: dict[str, dict[str, str]] = {}
    for dist in importlib.metadata.distributions():
        packages.setdefault(str(dist.locate_file("")), {})[_canonical(dist.metadata["Name"])] = dist.version
    return {
        "python": dict(version=sys.version, executable=sys.executable, prefix=sys.prefix, platform=platform.platform(),
                       libc=" ".join(platform.libc_ver()), free_threaded=bool(sysconfig.get_config_var("Py_GIL_DISABLED"))),
        "native": dict(openssl=ssl.OPENSSL_VERSION, sqlite=sqlite, zlib=zlib.ZLIB_RUNTIME_VERSION, expat=pyexpat.EXPAT_VERSION),
        "packages": packages,
    }


def packages_match(headers) -> Result:
    lock = Path(__file__).with_name("uv.lock").read_text()
    # uv.lock can pin one package at several versions, one per range of Python versions.
    locked: dict[str, set[str]] = {}
    for name, version in re.findall(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"', lock):
        locked.setdefault(name, set()).add(version)
    block = re.search(r"^dependencies = \[(.*?)^\]", Path(__file__).with_name("pyproject.toml").read_text(), re.S | re.M)
    direct = [_canonical(name) for name in re.findall(r'^\s*"([A-Za-z0-9_.-]+)', block.group(1), re.M)]
    # The image's own packages (pip, setuptools, ...) live in the interpreter's site-packages; the app's anywhere else.
    image_dir = sysconfig.get_paths(vars={"base": sys.base_prefix, "platbase": sys.base_prefix})["purelib"]
    app_copies: dict[str, str] = {}
    image_copies: dict[str, str] = {}
    imported: dict[str, str] = {}  # sys.path order: the first copy is the one imported
    for location, names in inventory()["packages"].items():
        image = os.path.realpath(location) == os.path.realpath(image_dir)
        for name, version in names.items():
            (image_copies if image else app_copies).setdefault(name, version)
            imported.setdefault(name, "image" if image else "app")
    # The app installed its own copy, but the image's comes first on sys.path.
    shadowed = [f"{name} {image_copies[name]}" for name in app_copies if name in image_copies and imported[name] == "image"]
    missing = [name for name in direct if name not in app_copies and name not in image_copies]
    wrong = [f"{name} {version} (locked {', '.join(sorted(locked[name]))})" for name, version in app_copies.items()
             if name in locked and version not in locked[name]]
    extra = sorted(f"{name} {version}" for name, version in app_copies.items() if name not in locked)
    detail = f"{len(app_copies) - len(extra)} app packages match uv.lock" + (f"; not in uv.lock: {', '.join(extra)}" if extra else "") + (
        f"; image preinstalls {', '.join(f'{n} {v}' for n, v in sorted(image_copies.items()))}" if image_copies else "")
    if missing or wrong:
        return "fail", "; ".join(filter(None, [missing and f"missing: {', '.join(missing)}", wrong and f"wrong version: {', '.join(wrong)}"])), (
            "The installed packages don't match uv.lock, so the build didn't install what the lock file pins.")
    if shadowed:
        return "warn", f"{detail}; the image's copy is imported instead of the app's: {', '.join(shadowed)}", (
            "A package the image preinstalls comes first on sys.path, so the app imports it instead of the version uv.lock pins.")
    return "pass", detail, ""


def _cgroup(name: str) -> str | None:
    try:
        return Path("/sys/fs/cgroup", name).read_text().strip()
    except OSError:
        return None


def cpu_limit(headers) -> Result:
    value = _cgroup("cpu.max")
    if value is None:
        return "skip", "cgroup v2 cpu.max not present", "No container CPU limit can be read here (normal on a laptop)."
    quota, period = value.split()
    if quota == "max":
        return "pass", f"no CPU limit, os.cpu_count()={os.cpu_count()}", ""
    limit = -(-int(quota) // int(period))  # whole CPUs, rounded up
    detail = f"CPU limit {int(quota) / int(period):g} vCPU, os.cpu_count()={os.cpu_count()}"
    if (os.cpu_count() or 0) > limit:
        return "warn", detail, (
            "Python sees the whole server's CPUs, not this instance's, so default pool sizes (multiprocessing, "
            "ProcessPoolExecutor, NumPy/OpenBLAS threads) start too many workers. Set PYTHON_CPU_COUNT (3.13+) "
            "and size pools explicitly.")
    return "pass", detail, ""


def memory_limit(headers) -> Result:
    value = _cgroup("memory.max")
    if value is None:
        return "skip", "cgroup v2 memory.max not present", "No container memory limit can be read here (normal on a laptop)."
    if value == "max":
        return "pass", "no memory limit", ""
    seen = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    detail = f"memory limit {int(value) / 2**20:.0f} MiB, sysconf reports {seen / 2**20:.0f} MiB"
    if seen > int(value):
        return "warn", detail, (
            "Python sees the whole server's memory, not this instance's limit, so anything sized from physical "
            "memory (caches, worker counts) can overshoot and get the process killed.")
    return "pass", detail, ""


def subprocess_run(headers) -> Result:
    out = subprocess.run([sys.executable, "-c", "print(1)"], capture_output=True, text=True, timeout=TIMEOUT, check=True).stdout
    assert out.strip() == "1", out
    return "pass", "started a child Python and read its output", ""


def threads_run(headers) -> Result:
    done = []
    threads = [threading.Thread(target=done.append, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
    if len(done) != 4:
        return "fail", f"{len(done)} of 4 threads finished", "Threads did not finish, which breaks anything that uses background work."
    return "pass", "started and joined 4 threads", ""


# Services


def redis_ping(headers) -> Result:
    client = _redis()
    if client is None:
        return "skip", "REDIS_URL is not set", "Attach a Valkey cache to this environment to test it and to run queue jobs."
    client.ping()
    return "pass", "PING returned PONG", ""


def _database_options() -> tuple[str, dict] | None:
    database = _database()
    if database is None:
        return None
    kind, host, port = database
    parts = urlsplit(os.environ["DATABASE_URL"])
    user, password, name = unquote(parts.username or ""), unquote(parts.password or ""), parts.path.lstrip("/")
    tls = host not in LOCAL_HOSTS
    verify = os.environ.get("DB_SSL_VERIFY") != "0"
    if kind == "mysql":
        context = None
        if tls:
            context = ssl.create_default_context(cafile=certifi.where())
            if not verify:  # Cloud's ProxySQL certificate is self-signed: encrypted, not authenticated
                context.check_hostname, context.verify_mode = False, ssl.CERT_NONE
        options = dict(host=host, port=port, user=user, password=password, database=name or None,
                       ssl=context, connect_timeout=TIMEOUT, read_timeout=TIMEOUT)
    else:
        extra = {}
        if tls:
            extra = dict(sslmode="verify-full", sslrootcert=certifi.where()) if verify else dict(sslmode="require")
        options = dict(host=host, port=port, user=user, password=password, dbname=name or None,
                       connect_timeout=TIMEOUT, options=f"-c statement_timeout={TIMEOUT * 1000}", **extra)
    return kind, options


def _database_result(kind: str, options: dict) -> Result:
    host, port = options["host"], options["port"]
    tls = host not in LOCAL_HOSTS
    verify = os.environ.get("DB_SSL_VERIFY") != "0"
    security = ("TLS, certificate not verified" if not verify else "TLS") if tls else "no TLS (local host)"
    return "pass", f"SELECT 1 on {kind} at {host}:{port}, {security}", ""


def database_query(headers) -> Result:
    configured = _database_options()
    if configured is None:
        return "skip", "DATABASE_URL is not set", "Attach a database to this environment to test it."
    kind, options = configured
    if kind == "mysql":
        import pymysql

        conn = pymysql.connect(**options)
    else:
        import psycopg

        conn = psycopg.connect(**options)
    with conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        assert tuple(cursor.fetchone()) == (1,)
    return _database_result(kind, options)


def dns_hosts(headers) -> Result:
    client, database = _redis(), _database()
    hosts = {}
    if client is not None:
        hosts["redis"] = client.connection_pool.connection_kwargs.get("host")
    if database is not None:
        hosts[database[0]] = database[1]
    hosts = {label: host for label, host in hosts.items() if host}
    if not hosts:
        return "skip", "no Redis or database host configured", "Attach a Valkey cache or database to test its DNS name."
    found = {}
    for label, host in hosts.items():
        # getaddrinfo has no timeout of its own, so a daemon thread bounds it.
        answer: list = []
        # Bind per iteration: a lookup that outlives its timeout must not land in the next host's answer.
        worker = threading.Thread(target=lambda out=answer, name=host: out.append(socket.getaddrinfo(name, None)), daemon=True)
        worker.start()
        worker.join(TIMEOUT)
        found[label] = bool(answer)
    detail = ", ".join(f"{hosts[label]} {'resolved' if ok else 'did not resolve'}" for label, ok in found.items())
    if all(found.values()):
        return "pass", detail, ""
    return "fail", detail, "The app cannot look up the hostname of an attached service, so it cannot connect to it. Check the service is attached to this environment."


# Suite results (full tier)


def suite_result(id: str) -> Result:
    """The latest result cloud_suite.py posted for a full-tier row."""
    client = _redis()
    saved = client.get(SUITE_KEY + id) if client is not None else None
    return _suite_result(saved)


def _suite_result(saved) -> Result:
    if not saved:
        return "skip", "run `cloud_suite.py --tier full`", (
            "This check needs the Cloud CLI, so cloud_suite.py runs it from outside the app and posts the result here.")
    row = json.loads(saved)
    return row["status"], f"{row['detail']} (cloud_suite.py, {row['at']})", (
        "Run `uv run python cloud_suite.py --tier full <environment>` for the full report.")


def _suite_error(data: dict) -> tuple[int, dict] | None:
    full = {entry[1] for entry in CHECKS if entry[5] == "full"}
    if not data or not all(id in full and isinstance(row, dict) and row.get("status") in ("pass", "warn", "fail", "skip")
                           and isinstance(row.get("detail"), str) for id, row in data.items()):
        return 400, {"error": f"expected {{check id: {{status, detail}}}} for {', '.join(sorted(full))}"}


def save_suite(data: dict) -> tuple[int, dict]:
    """POST /api/suite-results: {check id: {"status", "detail"}} for full-tier rows, from cloud_suite.py."""
    error = _suite_error(data)
    if error:
        return error
    client = _redis()
    if client is None:
        return 503, {"error": "REDIS_URL is not set"}
    at = f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC"
    for id, row in data.items():
        client.set(SUITE_KEY + id, json.dumps({"status": row["status"], "detail": row["detail"][:300], "at": at}))
    return 200, {"ok": True}


# Where a check applies. The docstring is the skip reason.


def everywhere(server: str, python: str) -> bool:
    return True


def asgi_only(server: str, python: str) -> bool:
    """WSGI servers have no event loop or WebSockets."""
    return server in serve.ASGI


def upload_pair(server: str, python: str) -> bool:
    """The body limit is a proxy setting, so the suite uploads its ladder on gunicorn only."""
    return server == "gunicorn"


FAIL_HELP = "The check could not complete. See the detail for the error, then check the network and service settings of this environment."

CHECKS = [  # (group, id, title, fn, applies, tier); full-tier rows have no fn here: their job is in cloud_suite.JOBS
    ("Web", "web.server", "Server and processes", web_server, everywhere, "quick"),
    ("Web", "web.concurrency", "WEB_CONCURRENCY matches Cloud's formula", web_concurrency, everywhere, "quick"),
    ("Web", "web.port", "Listening port", web_port, everywhere, "quick"),
    ("Web", "web.proto", "Proxy sends X-Forwarded-Proto: https", _proxy_header("X-Forwarded-Proto", "https"), everywhere, "quick"),
    ("Web", "web.forwarded_for", "Proxy sends X-Forwarded-For", _proxy_header("X-Forwarded-For"), everywhere, "quick"),
    ("Web", "web.request_id", "Proxy sends Cloud-Request-ID", _proxy_header("Cloud-Request-ID"), everywhere, "quick"),
    ("Web", "web.event_loop", "Async event loop not blocked", web_event_loop, asgi_only, "quick"),
    ("Web", "web.websocket", "WebSocket upgrade through Cloud's proxy", web_websocket, asgi_only, "quick"),
    ("Web", "web.upstream_ipv6", "nginx reaches the app over IPv6", web_upstream_ipv6, everywhere, "quick"),
    ("Web", "web.streaming", "Streamed responses arrive as sent", web_streaming, everywhere, "quick"),
    ("Web", "web.static", "Static files served by nginx", web_static, everywhere, "quick"),
    ("Web", "web.upload_limit", "Largest upload through Cloud's proxy", None, upload_pair, "full"),
    ("Logging", "logging.handler", "Cloud logging handler installed", log_handler, everywhere, "quick"),
    ("Logging", "logging.socket", "Cloud log socket reachable", log_socket, everywhere, "quick"),
    ("Logging", "logging.cloud_viewer", "Logs render in Cloud's log viewer", None, everywhere, "full"),
    ("Runtime", "runtime.python", "Python version is pinned and current", python_version, everywhere, "quick"),
    ("Runtime", "runtime.stdlib", "Python standard library complete", stdlib_complete, everywhere, "quick"),
    ("Runtime", "runtime.packages", "Installed packages match uv.lock", packages_match, everywhere, "quick"),
    ("Runtime", "runtime.https", "Outbound HTTPS", outbound_https, everywhere, "quick"),
    ("Runtime", "runtime.tmp", "/tmp writable", tmp_writable, everywhere, "quick"),
    ("Runtime", "runtime.cpu", "CPU limit", cpu_limit, everywhere, "quick"),
    ("Runtime", "runtime.memory", "Memory limit", memory_limit, everywhere, "quick"),
    ("Runtime", "runtime.subprocess", "Subprocess", subprocess_run, everywhere, "quick"),
    ("Runtime", "runtime.threads", "Threads", threads_run, everywhere, "quick"),
    ("Services", "services.redis", "Valkey / Redis", redis_ping, everywhere, "quick"),
    ("Services", "services.database", "Database", database_query, everywhere, "quick"),
    ("Services", "services.dns", "DNS for service hosts", dns_hosts, everywhere, "quick"),
]


def python_minor() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def _one(entry, headers) -> dict[str, str]:
    import app

    _, id, label, fn, applies, tier = entry
    try:
        if not applies(app.SERVER, python_minor()):
            status, detail, help = "skip", f"Not run on {app.SERVER}: {applies.__doc__}", (
                "This check doesn't apply to this environment's server or Python version.")
        elif tier == "full":
            status, detail, help = suite_result(id)
        else:
            status, detail, help = fn(headers)
    except Exception as exc:
        status, detail, help = "fail", f"{type(exc).__name__}: {exc}", FAIL_HELP
    return _row(entry, (status, detail, help))


def _row(entry, result: Result) -> dict[str, str]:
    status, detail, help = result
    return {"id": entry[1], "label": entry[2], "status": status, "detail": _scrub(detail)[:300], "help": help}


def _groups(rows: list[dict]) -> dict:
    groups: dict[str, list] = {}
    for entry, row in zip(CHECKS, rows):
        groups.setdefault(entry[0], []).append(row)
    return {"groups": [{"name": name, "checks": rows} for name, rows in groups.items()]}


def _busy_result() -> Result:
    return "warn", "an earlier run of this check has not finished yet", (
        "This check was still running from a previous request. If it stays like this, the service it talks to has stopped responding.")


def _deadline_result() -> Result:
    return "fail", f"no answer within {DEADLINE} s", (
        "This check hung. The service it talks to accepted the connection but stopped responding.")


DEADLINE = 12  # seconds for the whole list; a check still running then is reported, not awaited
# Checks still running (possibly hung) from any request. A hung check keeps its one thread; later
# requests report it instead of starting another, so stuck threads can't pile up.
_running: set[str] = set()
_running_lock = threading.Lock()


def _record(entry, headers, results: dict) -> None:
    try:
        results[entry[1]] = _one(entry, headers)
    finally:
        with _running_lock:
            _running.discard(entry[1])


def run(headers) -> dict:
    results: dict[str, dict] = {}
    threads, busy = [], set()
    for entry in CHECKS:
        with _running_lock:
            if entry[1] in _running:
                busy.add(entry[1])
                continue
            _running.add(entry[1])
        # Preserve the upstream peer and logging context in daemon threads; a hung probe cannot block shutdown.
        thread = threading.Thread(target=contextvars.copy_context().run, args=(_record, entry, headers, results), daemon=True)
        thread.start()
        threads.append(thread)
    deadline = time.monotonic() + DEADLINE
    for thread in threads:
        thread.join(max(0, deadline - time.monotonic()))
    rows = []
    for entry in CHECKS:
        id = entry[1]
        if id in results:
            rows.append(results[id])
        elif id in busy:
            rows.append(_row(entry, _busy_result()))
        else:
            rows.append(_row(entry, _deadline_result()))
    return _groups(rows)
