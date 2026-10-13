"""Start the web server named in .web-server, or the one given: python serve.py [server]

Cloud start command for every environment: python serve.py
Each server branch differs from main only in .web-server (and .python-version, to test another
Python), so switching server or version is a commit, not a settings change.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Bind [::]:$PORT, dual-stack: Cloud's startup probes connect over IPv6, the pod's nginx over 127.0.0.1.
# gunicorn reads WEB_CONCURRENCY itself; the others get it as a flag. waitress uses one process.
# Logging flags follow laravel-cloud-logging's server table. logging.json is written by the Cloud build command
# `laravel-cloud-logging-config logging.json`, so main-process lines (boot, workers, shutdown) are JSON too.
COMMANDS = {
    "gunicorn": "gunicorn wsgi:app --bind [::]:{port}",
    # Import in each worker, after forking; keep threads for checks and dispatch.
    "uwsgi": "uwsgi --http-socket [::]:{port} --module wsgi:app --master --processes {workers} --lazy-apps "
             "--enable-threads --die-on-term --need-app --disable-logging",
    # waitress uses IPv6-only sockets, and must retain Cloud's proxy headers.
    "waitress": "waitress-serve --listen=[::]:{port} --listen=0.0.0.0:{port} --no-clear-untrusted-proxy-headers wsgi:app",
    "granian-wsgi": "granian --interface wsgi --host :: --port {port} --workers {workers} --log-config logging.json wsgi:app",
    "hypercorn-wsgi": "hypercorn --bind [::]:{port} --workers {workers} --log-config json:logging.json wsgi:app",
}
# Seconds a server waits for in-flight requests on SIGTERM: under Cloud's 30 s budget minus its 5 s pre-drain.
GRACE = 20
SINGLE_PROCESS = {"waitress"}
ASGI = {name for name, command in COMMANDS.items() if "asgi:app" in command}


def command(server: str) -> list[str]:
    port = os.environ.get("PORT", "8001")
    workers = os.environ.get("WEB_CONCURRENCY", "1")
    return COMMANDS[server].format(port=port, workers=workers, grace=GRACE).split()


if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) > 1 else Path(__file__).with_name(".web-server").read_text().strip()
    if server not in COMMANDS:
        raise SystemExit(f"unknown server {server!r}; expected one of {', '.join(COMMANDS)}")
    argv = command(server)
    os.environ["WEB_SERVER"] = server  # app.py reports it as SERVER
    os.execvp(argv[0], argv)
