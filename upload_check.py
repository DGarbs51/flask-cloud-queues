"""Upload limit check (#10): the largest request body that gets through Cloud's proxy chain to the app.

Run: uv run python upload_check.py [env ...]   (default: every environment, two at a time)
POSTs zero-filled bodies of each of SIZES to /api/upload with curl (HTTP/2, as browsers send), then one just over
the edge limit with no Content-Length. Records the status, the bytes the app counted, the time and which layer
answered. Prints one row per environment and writes results/upload-<UTC time>.json.
cloud_suite.py runs the same check as the full-tier row web.upload_limit.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from logs_check import APP, HEADERS, cloud

MB, MiB = 10**6, 2**20
# Cloudflare's request body limit on Cloud's zone (measured 2026-10-05). It rejects anything larger from the
# Content-Length with its own 413, before the body is sent. The pod's nginx allows 2048M, so it's never reached.
EDGE = 500 * MiB
SIZES = [MB, 10 * MB, 25 * MB, 50 * MB, 100 * MB, 200 * MB, EDGE, EDGE + 1]
UNSIZED = EDGE + MiB  # sent without a Content-Length: the edge counts the stream and still answers 413, once it's all sent


def layer(status: int, body: bytes) -> str:
    """Who answered: the app's JSON, nginx's or Cloudflare's HTML error page (both say server: cloudflare), or the
    web server itself with an empty body (hypercorn's WSGI wrapper above its 16 MiB wsgi_max_body_size)."""
    try:
        json.loads(body)
        return "app"
    except ValueError:
        pass
    for name in ("cloudflare", "nginx"):
        if f"<center>{name}</center>".encode() in body:
            return name
    return "none" if not status else "server" if not body.strip() else "unknown"


def upload(url: str, path: Path, size: int, sized: bool = True) -> dict:
    with open(path, "rb") as file, tempfile.NamedTemporaryFile() as out:
        # -T streams the file instead of loading it; reading stdin, curl doesn't know the length, so no Content-Length.
        args = ["curl", "-sS", "-X", "POST", "-T", str(path) if sized else "-", "-o", out.name, "-w", "%{json}",
                "-A", HEADERS["User-Agent"], "-H", "Content-Type: application/octet-stream", "--max-time", "300",
                f"{url}/api/upload"]
        if HEADERS.get("X-K6-Bypass"):
            args[1:1] = ["-H", "X-K6-Bypass: " + HEADERS["X-K6-Bypass"]]
        run = subprocess.run(args, stdin=None if sized else file, capture_output=True, text=True)
        meta = json.loads(run.stdout or "{}")
        body = Path(out.name).read_bytes()
    status = meta.get("http_code") or 0
    try:
        counted = json.loads(body).get("bytes")
    except (ValueError, AttributeError):
        counted = None
    return dict(size=size, sized=sized, status=status, bytes=counted, layer=layer(status, body),
                seconds=round(meta.get("time_total", 0), 2), sent=meta.get("size_upload"),
                error=run.stderr.strip()[-200:] or None, body=body[:120].decode(errors="replace"))


def probe(url: str) -> list[dict]:
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "body"
        for size in [*SIZES, UNSIZED]:
            with open(path, "wb") as file:
                file.truncate(size)  # sparse: no disk or memory cost
            rows.append(upload(url, path, size, sized=size != UNSIZED))
    return rows


def human(size: int) -> str:
    return f"{size // MiB} MiB" if size % MiB == 0 else f"{size} B" if size < MB or size % MB else f"{size // MB} MB"


def score(rows: list[dict]) -> tuple[str, str, dict]:
    """(status, detail, metrics) as cloud_suite's JOBS return. Fails if the limit or the rejecting layer changes."""
    sized = [r for r in rows if r["sized"]]
    ok = [r for r in sized if r["status"] == 200 and r["bytes"] == r["size"]]
    truncated = [r for r in rows if r["status"] == 200 and r["bytes"] != r["size"]]
    failed = [r for r in sized if r not in ok]
    largest = max((r["size"] for r in ok), default=0)
    first = min(failed, key=lambda r: r["size"], default=None)
    unsized = next((r for r in rows if not r["sized"]), None)
    detail = f"largest {human(largest)} ({largest} B)" if largest else "no upload succeeded"
    if ok:
        detail += f" in {max(r['seconds'] for r in ok if r['size'] == largest)} s"
    if first:
        detail += f"; {human(first['size'])}: {first['status'] or first['error']} from {first['layer']}"
    if unsized:
        detail += (f"; {human(unsized['size'])} without Content-Length: {unsized['status'] or unsized['error']}"
                   f" from {unsized['layer']}" + (f", app counted {unsized['bytes']} B" if unsized["bytes"] is not None else ""))
    if truncated:
        cut = ", ".join(f"{human(r['size'])} -> {r['bytes']} B" for r in truncated)
        detail = f"TRUNCATED {cut}; " + detail
    edge = [r for r in (first, unsized) if r]
    expected = largest == EDGE and first and first["size"] == EDGE + 1 and all(r["layer"] == "cloudflare" for r in edge)
    if not expected or truncated:
        return "fail", detail, {"truncated": len(truncated)}
    # Cloudflare sometimes answers an over-limit upload with its own 502 instead of 413 (about 1 in 7): same limit.
    return ("pass" if all(r["status"] == 413 for r in edge) else "warn"), detail, {"truncated": 0}


def self_check() -> None:
    """Scoring without Cloud: python upload_check.py --self-check"""
    def r(size, status, counted=None, where="app", sized=True):
        return dict(size=size, sized=sized, status=status, bytes=counted if counted is not None else (size if status == 200 else None),
                    layer=where, seconds=1.0, sent=size, error=None, body="")
    good = [r(s, 200) for s in SIZES[:-1]] + [r(EDGE + 1, 413, where="cloudflare"), r(UNSIZED, 413, where="cloudflare", sized=False)]
    status, detail, metrics = score(good)
    assert status == "pass" and detail.startswith("largest 500 MiB (524288000 B) in 1.0 s; 524288001 B: 413 from cloudflare"), detail
    assert metrics == {"truncated": 0}
    status, detail, metrics = score([*good[:3], r(25 * MB, 200, counted=5), *good[4:]])
    assert (status, metrics["truncated"]) == ("fail", 1) and detail.startswith("TRUNCATED 25 MB -> 5 B"), detail
    status, detail, _ = score([*good[:5], r(200 * MB, 413, where="nginx"), *good[6:]])
    assert status == "fail" and "200 MB: 413 from nginx" in detail, detail  # a lower limit, from another layer
    status, detail, _ = score([*good[:-2], r(EDGE + 1, 502, where="cloudflare"), good[-1]])
    assert status == "warn" and "524288001 B: 502 from cloudflare" in detail, detail
    status, detail, _ = score([*good[:2], *(r(s, 400, where="server") for s in SIZES[2:-1]), *good[-2:]])
    assert status == "fail" and detail.startswith("largest 10 MB") and "25 MB: 400 from server" in detail, detail
    status, detail, _ = score([*good[:-1], r(UNSIZED, 200, sized=False)])
    assert status == "fail" and "without Content-Length: 200 from app" in detail, detail  # the edge let it through
    assert layer(413, b"<hr><center>cloudflare</center>") == "cloudflare" and layer(0, b"") == "none"
    assert layer(400, b"") == "server" and layer(400, b"oops") == "unknown"
    assert layer(400, b'{"error": "x"}') == "app" and layer(413, b"<center>nginx</center>") == "nginx"
    print("upload_check self-check passed")


def main() -> None:
    if sys.argv[1:] == ["--self-check"]:
        return self_check()
    urls = {e["name"]: e["url"] for e in cloud("env:list", APP)}
    envs = sys.argv[1:] or sorted(urls)
    # ponytail: two environments at a time keeps a laptop's uplink from turning upload time into the HTTP timeout.
    with ThreadPoolExecutor(2) as pool:
        reports = list(pool.map(lambda env: dict(env=env, rows=probe(urls[env])), envs))
    for report in reports:
        report["status"], report["detail"], _ = score(report["rows"])
        print(f"{report['env']:<22}{report['status']:<6}{report['detail']}", flush=True)
    out = Path("results") / f"upload-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(reports, indent=1))
    print(f"wrote {out}")
    sys.exit(0 if all(r["status"] == "pass" for r in reports) else 1)


if __name__ == "__main__":
    main()
