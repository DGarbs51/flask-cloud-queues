"""Regression suite (#19): every registered check on every environment, saved and diffed against the last run.

Run: uv run python cloud_suite.py --tier quick|full [env ...]
Environments named k6-* (load tests) are skipped unless named.
quick reads GET /api/checks from every environment in parallel. full also runs each full-tier entry's job
(JOBS below, Cloud CLI) where it applies, and posts the result back so the page's row shows it.
Writes results/<UTC time>.json, compares it with the previous file of the same tier and exits 1 on any
regression (pass to fail, or a count that got worse) or any failure that isn't a known finding.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import checks
import logs_check
import upload_check

RESULTS = Path("results")
# Fail on every environment until base-images releases its nginx fixes. Reported as they are, but not a failed run.
KNOWN: dict[str, str] = {}  # failures known not to be regressions, e.g. {"web.proto": "known, #15"}


def call(method: str, url: str, body: dict | None = None):
    # Cloudflare's browser integrity check rejects Python's default user agent with 403.
    request = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, method=method,
                                     headers={**logs_check.HEADERS, "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def cloud_viewer(env: str, url: str) -> tuple[str, str, dict]:
    """logs_check.py on one environment. Its known findings (KNOWN_UNCAUGHT, uWSGI's banner) already pass."""
    report = logs_check.check(env, url, None)
    results = report["results"]
    failed = [name for name, (ok, _) in results.items() if not ok]
    noted = [f"{name}: {detail}" for name, (ok, detail) in results.items() if not ok or "known" in detail]
    detail = "; ".join(noted) or f"all {len(results)} log checks pass"
    if failed:
        detail = f"failed {', '.join(failed)}: {detail}"
    return "fail" if failed else "pass", detail, {"plain_lines": len(report["plain"])}


# Full-tier jobs by registry id: (env name, env URL) -> (status, detail, metrics). Higher metrics are worse.
JOBS = {"logging.cloud_viewer": cloud_viewer, "web.upload_limit": lambda env, url: upload_check.score(upload_check.probe(url))}


def run_env(env: str, url: str, tier: str) -> dict:
    stats = call("GET", f"{url}/api/stats")
    server, python = stats["server"], ".".join(stats["python"].split(".")[:2])
    live = {row["id"]: row for group in call("GET", f"{url}/api/checks")["groups"] for row in group["checks"]}
    rows, posted = {}, {}
    for _, id, _label, _fn, applies, entry_tier in checks.CHECKS:
        if entry_tier == "quick":
            row = live.get(id) or {"status": "fail", "detail": "not in /api/checks: is this commit deployed?"}
            rows[id] = dict(status=row["status"], detail=row["detail"])
        elif tier == "full" and not applies(server, python):
            rows[id] = posted[id] = dict(status="skip", detail=f"Not run on {server}: {applies.__doc__}")
        elif tier == "full":
            try:
                status, detail, metrics = JOBS[id](env, url)
            except Exception as exc:
                status, detail, metrics = "fail", f"{type(exc).__name__}: {exc}", {}
            posted[id] = dict(status=status, detail=detail)
            rows[id] = dict(posted[id], metrics=metrics)
    if posted:
        try:
            call("POST", f"{url}/api/suite-results", posted)
        except Exception as exc:  # the page just keeps its older result
            print(f"{env}: couldn't post results back: {exc}", file=sys.stderr)
    return dict(server=server, python=python, checks=rows)


def safe_run(env: str, url: str, tier: str) -> dict:
    try:
        return run_env(env, url, tier)
    except Exception as exc:
        return dict(error=f"{type(exc).__name__}: {exc}", checks={})


def previous(tier: str) -> tuple[Path | None, dict]:
    for path in sorted(RESULTS.glob("2*.json"), reverse=True):  # results/<UTC time>.json; logs_check writes logs-*.json
        data = json.loads(path.read_text())
        if data.get("tier") == tier:
            return path, data
    return None, {"envs": {}}


def regressions(before: dict, now: dict) -> dict[tuple[str, str], str]:
    found = {}
    for env, result in now["envs"].items():
        for id, row in result["checks"].items():
            old = before["envs"].get(env, {}).get("checks", {}).get(id)
            if not old:
                continue
            if old["status"] == "pass" and row["status"] == "fail":
                found[env, id] = "pass -> fail"
            for name, value in row.get("metrics", {}).items():
                if value > old.get("metrics", {}).get(name, value):
                    found[env, id] = f"{name} {old['metrics'][name]} -> {value}"
    return found


def failures(now: dict) -> list[str]:
    bad = [f"{env}: {result['error']}" for env, result in now["envs"].items() if "error" in result]
    return bad + [f"{env} {id}: {row['detail']}" for env, result in now["envs"].items()
                  for id, row in result["checks"].items() if row["status"] == "fail" and id not in KNOWN]


def report(now: dict, found: dict, baseline: Path | None) -> None:
    print(f"{'environment':<22}{'server':<16}{'python':<8}{'pass':>5}{'warn':>5}{'fail':>5}{'skip':>5}  not passing (! = regression)")
    for env, result in now["envs"].items():
        if "error" in result:
            print(f"{env:<22}ERROR {result['error'][:120]}")
            continue
        statuses = [row["status"] for row in result["checks"].values()]
        notes = [("!" if (env, id) in found else "") + f"{id} {row['status']}" + (f" ({KNOWN[id]})" if id in KNOWN else "")
                 for id, row in result["checks"].items() if row["status"] in ("warn", "fail") or (env, id) in found]
        print(f"{env:<22}{result['server']:<16}{result['python']:<8}" + "".join(f"{statuses.count(s):>5}" for s in ("pass", "warn", "fail", "skip"))
              + "  " + ", ".join(notes))
    print(f"\nregressions vs {baseline}: {len(found)}" if baseline else "\nno earlier run of this tier to compare with")
    for (env, id), change in found.items():
        print(f"  ! {env} {id}: {change}: {now['envs'][env]['checks'][id]['detail'][:200]}")
    for line in failures(now):
        print(f"  FAIL {line[:240]}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", choices=("quick", "full"), required=True)
    parser.add_argument("envs", nargs="*")
    args = parser.parse_args()
    urls = {e["name"]: e["url"] for e in logs_check.cloud("env:list", logs_check.APP)}
    envs = args.envs or sorted(name for name in urls if not name.startswith("k6-"))  # a k6 run may be in progress
    started = datetime.now(timezone.utc)
    with ThreadPoolExecutor(len(envs)) as pool:
        results = dict(zip(envs, pool.map(lambda env: safe_run(env, urls[env], args.tier), envs)))
    now = dict(tier=args.tier, at=started.isoformat(timespec="seconds"), envs=results)

    baseline, before = previous(args.tier)
    found = regressions(before, now)
    report(now, found, baseline)
    out = RESULTS / f"{started:%Y%m%dT%H%M%SZ}.json"
    RESULTS.mkdir(exist_ok=True)
    out.write_text(json.dumps(now, indent=2, ensure_ascii=False))
    print(f"\nwrote {out}")
    sys.exit(1 if found or failures(now) else 0)


if __name__ == "__main__":
    main()
