"""Deploy drain test (#7): keep slow and quick requests in flight through a deploy, then score what survived.

Run: uv run python scripts/deploy_drain.py [env ...]   (default: every environment, in parallel)
Per environment: set a new DEPLOY_MARKER, start a deploy, and keep starting a /api/slow every SLOW_EVERY seconds and
PING_RATE /api/ping a second until the deploy has finished and only the new marker has answered for SETTLE seconds.
Prints one row per environment and writes every request to results/drain-<UTC time>.json.
"""

from __future__ import annotations

import http.client
import json
import ssl
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from logs_check import APP, HEADERS  # noqa: E402

# Slow requests stay under the HTTP timeout (default 20 s): past it, nginx gives up and retries them on its backup
# upstream, which is the same app (SE-304). About 3 in flight: under the 5 sync workers of gunicorn and uWSGI and the
# 4 threads of waitress on pro.g-2vcpu-4gb, so pings still get through.
SLOW_SECONDS, SLOW_EVERY, PING_RATE = 15, 5, 10
SETTLE = 30
FINISHED = ("deployment.succeeded", "deployment.failed", "build.failed", "deployment.cancelled")
BROWSER = {"User-Agent": "Mozilla/5.0"}  # Cloudflare's browser integrity check refuses Python's default


def cloud(*args: str):
    """The first JSON value the CLI prints (deploy prints two), retrying on the API's 120/min rate limit."""
    for attempt in range(6):
        out = subprocess.run(["cpx", "laravel/cloud-cli", *args, "--json", "-n"], capture_output=True, text=True, timeout=180)
        if not out.returncode:
            return json.JSONDecoder().raw_decode(out.stdout.strip())[0]
        if "Too Many Attempts" not in out.stdout + out.stderr:
            break
        time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"{args[0]} failed: {out.stdout[-500:]} {out.stderr[-500:]}")


def get(host: str, path: str, timeout: float) -> dict:
    started = time.time()
    conn = http.client.HTTPSConnection(host, timeout=timeout, context=ssl.create_default_context())
    try:
        conn.request("GET", path, headers=HEADERS)
        response = conn.getresponse()
        body = response.read()
        try:
            data = json.loads(body)
        except ValueError:
            data = {}
        return dict(start=started, end=time.time(), status=response.status, deploy=data.get("deploy"), pod=data.get("pod"))
    except (OSError, http.client.HTTPException) as exc:
        return dict(start=started, end=time.time(), status=type(exc).__name__, deploy=None, pod=None)
    finally:
        conn.close()


def drain(name: str, env_id: str, url: str) -> dict:
    host = urlsplit(url).hostname
    old = get(host, "/api/ping", 60)["deploy"]  # also wakes a sleeping environment
    new = uuid.uuid4().hex[:12]
    cloud("env:vars", env_id, "--action=append", "--key=DEPLOY_MARKER", f"--value={new}", "--force")
    records: list[dict] = []
    lock, stop = threading.Lock(), threading.Event()
    pool = ThreadPoolExecutor(128)

    def record(kind: str, path: str, timeout: float) -> None:
        result = {**get(host, path, timeout), "kind": kind}
        with lock:
            records.append(result)

    def load() -> None:
        started, n = time.time(), 0
        while not stop.is_set():
            if n % (SLOW_EVERY * PING_RATE) == 0:
                pool.submit(record, "slow", f"/api/slow?seconds={SLOW_SECONDS}", SLOW_SECONDS + 70)
            pool.submit(record, "ping", "/api/ping", 5)
            n += 1
            time.sleep(max(0.0, started + n / PING_RATE - time.time()))

    loader = threading.Thread(target=load)
    loader.start()
    time.sleep(10)  # a baseline on the old deployment
    deployment = cloud("deploy", APP, name, "--no-wait")["deployment_id"]
    status, deadline = "", time.time() + 1800
    while status not in FINISHED and time.time() < deadline:
        time.sleep(15)
        status = cloud("deployment:get", deployment, "--fields=status")["status"]
    while status == "deployment.succeeded" and time.time() < deadline:
        with lock:
            answered = [r for r in records if r["kind"] == "ping" and r["status"] == 200]
        first_new = min((r["end"] for r in answered if r["deploy"] == new), default=None)
        recent = [r for r in answered if r["end"] > time.time() - SETTLE]
        if first_new and time.time() - first_new > SETTLE and recent and all(r["deploy"] == new for r in recent):
            break
        time.sleep(2)
    stop.set()
    loader.join()
    pool.shutdown(wait=True)
    return dict(env=name, old=old, new=new, deployment=deployment, status=status, **score(records, old, new), records=records)


def score(records: list[dict], old: str | None, new: str) -> dict:
    slow = [r for r in records if r["kind"] == "slow"]
    pings = [r for r in records if r["kind"] == "ping"]
    first_new = min((r["end"] for r in records if r["deploy"] == new), default=None)
    old_answers = [r["end"] for r in records if r["status"] == 200 and r["deploy"] == old]
    killed = [r for r in slow if r["status"] != 200]
    good = sorted(r["end"] for r in pings if r["status"] == 200)
    old_slow = [r for r in slow if r["status"] == 200 and r["deploy"] == old]
    result = dict(
        slow_started=len(slow), slow_finished=len(slow) - len(killed),
        slow_killed_old=sum(1 for r in killed if first_new is None or r["start"] < first_new),
        slow_killed_new=sum(1 for r in killed if first_new is not None and r["start"] >= first_new),
        killed_with=sorted({str(r["status"]) for r in killed}),
        quick_failures=sum(1 for r in pings if r["status"] != 200),
        quick_failed_with=sorted({str(r["status"]) for r in pings if r["status"] != 200}),
        longest_gap=round(max((b - a for a, b in zip(good, good[1:])), default=0.0), 2),
        # Old and new deployments both answering: from the first new answer to the last old one.
        cutover=round(max(old_answers) - first_new, 2) if first_new and old_answers else None,
        # How long after the new deployment went live a slow request on the old one still finished.
        old_drain=round(max(r["end"] for r in old_slow) - first_new, 2) if first_new and old_slow else None,
    )
    result["verdict"] = "fail" if killed else "warn" if result["quick_failures"] else "pass"
    return result


def self_check() -> None:
    """Scoring without Cloud: python scripts/deploy_drain.py --self-check"""
    def r(kind, start, end, status=200, deploy="old"):
        return dict(kind=kind, start=start, end=end, status=status, deploy=deploy, pod="p")
    clean = [r("ping", t, t + 0.1, deploy="old" if t < 50 else "new") for t in range(100)]
    clean += [r("slow", 30, 50), r("slow", 45, 65, deploy="new")]
    s = score(clean, "old", "new")
    assert (s["verdict"], s["slow_finished"], s["slow_killed_old"], s["longest_gap"]) == ("pass", 2, 0, 1.0), s
    assert (s["cutover"], s["old_drain"]) == (-0.1, -0.1), s  # the last old answer came just before the first new one
    s = score([*clean, r("slow", 40, 41, status="RemoteDisconnected", deploy=None)], "old", "new")
    assert (s["verdict"], s["slow_killed_old"], s["killed_with"]) == ("fail", 1, ["RemoteDisconnected"]), s
    s = score([p for p in clean if not 60 <= p["start"] < 63] + [r("ping", 61, 66, status=502, deploy=None)], "old", "new")
    assert (s["verdict"], s["quick_failures"], s["longest_gap"]) == ("warn", 1, 4.0), s
    print("deploy_drain self-check passed")


def main() -> None:
    if sys.argv[1:] == ["--self-check"]:
        return self_check()
    envs = [e for e in cloud("env:list", APP) if not sys.argv[1:] or e["name"] in sys.argv[1:]]
    # ponytail: 4 environments at a time keeps the laptop's threads and connections and the shared 120/min API budget sane.
    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(lambda e: drain(e["name"], e["id"], e["url"]), envs))
    out = Path("results") / f"drain-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=1))
    cols = ("verdict", "status", "slow_finished", "slow_started", "slow_killed_old", "slow_killed_new", "killed_with",
            "quick_failures", "quick_failed_with", "longest_gap", "cutover", "old_drain")
    for r in sorted(results, key=lambda r: r["env"]):
        print(f"{r['env']:<22}", "  ".join(f"{c}={r[c]}" for c in cols), flush=True)
    print(f"wrote {out}")
    sys.exit(1 if any(r["verdict"] == "fail" or r["status"] != "deployment.succeeded" for r in results) else 0)


if __name__ == "__main__":
    main()
