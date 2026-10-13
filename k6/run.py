"""Run a k6 script and append one JSON line to results/k6/runs.jsonl: the run's UTC window, for correlating with dashboards.

Run: uv run python k6/run.py [--cloud] k6/concurrency.js -e BASE_URL=... -e ENV_NAME=server-gunicorn -e PASS=A -e K6_BYPASS=...
Without --cloud it is `k6 run` (use localhost); --cloud is `k6 cloud run` on Grafana Cloud k6.
Every -e key except K6_BYPASS (the secret, passed to k6 but never logged) goes into the log line.
--cloud refuses to start without -e SIZE, WEB_CONCURRENCY and THRESHOLDS (the plan logs them per run).
"commit" is the local HEAD; also pass -e COMMIT=<the environment branch's sha>, which is logged in "env".
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

LOG = Path("results/k6/runs.jsonl")


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main() -> None:
    args = sys.argv[1:]
    cloud = "--cloud" in args
    args = [a for a in args if a != "--cloud"]
    script = next((a for a in args if a.endswith(".js")), None)
    if not script:
        raise SystemExit(__doc__)
    env = dict(a.split("=", 1) for prev, a in zip(args, args[1:]) if prev == "-e" and "=" in a)
    # Cloud runs omit system env vars by default; pass only the bypass secret, never the whole shell.
    if "K6_BYPASS" not in env and os.environ.get("K6_BYPASS"):
        env["K6_BYPASS"] = os.environ["K6_BYPASS"]
        args += ["-e", "K6_BYPASS=" + env["K6_BYPASS"]]
    if cloud and (missing := [k for k in ("SIZE", "WEB_CONCURRENCY", "THRESHOLDS") if k not in env]):
        raise SystemExit(f"--cloud needs -e {', -e '.join(missing)}")
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    start = utc()
    code = subprocess.run(["k6", *(["cloud", "run"] if cloud else ["run"]), *args]).returncode
    logged = {k: v for k, v in env.items() if k != "K6_BYPASS"}
    row = dict(start_utc=start, end_utc=utc(), script=script, mode="cloud" if cloud else "local", commit=head, exit_code=code, env=logged)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps(row) + "\n")
    sys.exit(code)


if __name__ == "__main__":
    main()
