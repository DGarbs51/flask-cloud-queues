"""Local queue and 1,000-job throughput proof. Run: uv run --env-file .env python test_queue.py.

Starts and stops its own web server and worker. Restarting exit 124 reproduces Cloud's
worker supervision, which the deliberately timed-out job requires. Uses only Flask's Redis DB 1.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from test_servers import request

ROOT = Path(__file__).resolve().parent


def main():
    assert os.environ.get("REDIS_URL") == "redis://127.0.0.1:6379/1", "use Flask's isolated local .env"
    assert os.environ.get("DEMO_KEY_PREFIX") == "flask-demo:", "use Flask's telemetry namespace"
    assert os.environ.get("LARAVEL_CLOUD_QUEUES_BACKEND") == "redis", "local proof requires Redis mode"
    assert os.environ.get("PORT") == "8001", "use Flask's isolated port"
    root = ROOT / "results"
    root.mkdir(exist_ok=True)
    worker = web = None
    exits = []
    with (root / "local-web.log").open("w") as web_log, (root / "local-worker.log").open("w") as worker_log:
        def start_worker():
            return subprocess.Popen([str(Path(sys.executable).with_name("laravel-cloud-queues")), "work",
                                     "--queue=default", "--sleep=0"], cwd=ROOT, stdout=worker_log,
                                    stderr=subprocess.STDOUT, start_new_session=True)

        def supervise():
            nonlocal worker
            if worker.poll() is not None:
                code = worker.returncode
                exits.append(code)
                assert code == 124, f"unexpected worker exit {code}; see results/local-worker.log"
                worker = start_worker()

        def get(path):
            status, _, body = request(8001, "GET", path)
            assert status == 200, (path, status, body)
            return json.loads(body)

        try:
            web = subprocess.Popen([sys.executable, "serve.py"], cwd=ROOT, stdout=web_log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 20
            while True:
                assert web.poll() is None, "web exited; see results/local-web.log"
                try:
                    if get("/api/ping")["ok"]:
                        break
                except OSError:
                    pass
                assert time.monotonic() < deadline, "web did not start"
                time.sleep(0.1)
            worker = start_worker()
            status, _, body = request(8001, "POST", "/api/check", b"{}", {"Content-Type": "application/json"})
            assert status in (200, 409), (status, body)
            deadline = time.monotonic() + 255
            while True:
                supervise()
                check = get("/api/stats")["check"]
                if check and check["status"] != "running":
                    break
                assert time.monotonic() < deadline, check
                time.sleep(1)
            assert check["status"] == "pass", check
            print("/api/check:", json.dumps(check), flush=True)
            status, _, body = request(8001, "POST", "/api/throughput", b'{"count": 1000}',
                                      {"Content-Type": "application/json"})
            assert status == 202, (status, body)
            run = json.loads(body)["run"]
            deadline = time.monotonic() + 90
            while True:
                supervise()
                throughput = get("/api/throughput/" + run)
                if throughput["state"] != "running":
                    break
                assert time.monotonic() < deadline, throughput
                time.sleep(0.25)
            assert throughput["verdict"] == "pass", throughput
            print("1,000-job throughput:", json.dumps(throughput), flush=True)
            services = [row for group in get("/api/checks")["groups"] if group["name"] == "Services" for row in group["checks"]]
            assert all(row["status"] == "pass" for row in services), services
            proof = {"check": check, "throughput": throughput, "worker_exits": exits, "services": services}
            (root / "local-queue.json").write_text(json.dumps(proof, indent=2))
            print("local Services:", json.dumps(services), flush=True)
            print("test_queue ok", flush=True)
        finally:
            for process in (worker, web):
                if process is not None and process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=25)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()


if __name__ == "__main__":
    main()
