# flask-cloud-queues

A Flask WSGI app that tells you whether Flask and Python work on a
[Laravel Cloud](https://cloud.laravel.com) deployment. Deploy it, open it, press **Run all
checks**, and read the result: every row is pass, warning, fail or skipped, with a plain
explanation of anything that isn't a pass.

It checks the things a real Python app depends on:

| Group | What it checks |
|---|---|
| Web | server and process count vs `WEB_CONCURRENCY`, `WEB_CONCURRENCY` vs Cloud's sizing formula, `PORT`, the proxy headers (`X-Forwarded-Proto`, `X-Forwarded-For`, `Cloud-Request-ID`), the async event loop isn't blocked, a WebSocket upgrade through Cloud's proxy (ASGI servers only), that the pod's nginx reaches the app over IPv6, that streamed responses (`/api/stream`, server-sent events) arrive as sent with and without `X-Accel-Buffering: no`, that nginx serves `public/` itself with Cloud's cache headers while refusing dotfiles, logs and SQL dumps, and (from `cloud_suite.py --tier full`) the largest upload that gets through |
| Logging | [laravel-cloud-logging](https://pypi.org/project/laravel-cloud-logging/) is installed, Cloud's log socket is reachable, and (from `cloud_suite.py --tier full`) logs render correctly in Cloud's log viewer |
| Runtime | Python version vs `.python-version`, the newest patch release and end of life ([endoflife.date](https://endoflife.date/python)), the standard library's C extensions, time-zone data and UTF-8, installed packages vs `uv.lock`, outbound HTTPS, `/tmp`, CPU and memory limits, subprocesses, threads |
| Services | Valkey/Redis `PING`, database `SELECT 1` (MySQL or Postgres, from `DATABASE_URL`), DNS for both |
| Queue | seven jobs through [laravel-cloud-queues](https://pypi.org/project/laravel-cloud-queues/): plain, async, delayed, retried, failing, timed out and a burst of 20 |
| Throughput | N async jobs (default 1,000) through the queue: jobs per second, p95 wait, lost and duplicated jobs |

Checks that only make sense on Cloud (proxy headers, streaming, static files, log socket, cgroup
limits) are skipped when you run locally. `GET /api/packages` lists this Python, the native
libraries it's built against (OpenSSL, SQLite, zlib, expat) and every installed package by location.

## Deploy to Laravel Cloud

One application, one environment per branch. Each branch differs from `main` only in
`.python-version` (which Cloud reads to pick the runtime) or `.web-server` (which `serve.py`
reads to pick the web server). `main` is Python 3.14 on gunicorn. Flask uses sync HTTP handlers, an API blueprint,
a plain queue `Registry`, and Werkzeug `ProxyFix` for Cloud’s forwarded headers.

| Branch | Python | Web server |
|---|---|---|
| `python-3.11` … `python-3.14` | 3.11 … 3.14 | gunicorn |
| `server-uwsgi`, `server-waitress` | `main`'s (3.14) | WSGI |
| `server-granian-wsgi`, `server-hypercorn-wsgi` | `main`'s (3.14) | WSGI |

Eight environments in total. There are no ASGI servers or dedicated k6 environments.
Before using the Cloud scripts, the deploy lead must replace `APP = "TODO"` in both
`logs_check.py` and `scripts/ship.py` with this application's Cloud id.

These are the servers Cloud's [Python deploy guide](https://cloud.laravel.com/docs/deploy-guides/python#run-a-production-server)
supports. To test a server on another Python, change `.python-version` on its branch and push.

Pushing a branch deploys its environment. To ship `main` to every environment (or the ones named):

```sh
uv run python scripts/ship.py [env ...]
```

It merges `origin/main` into each branch in a scratch worktree and pushes. On a conflict `main` wins
except for the branch's own `.python-version` or `.web-server`; anything it can't resolve stops the run.
Then it waits until every environment's latest deployment is at its branch head and finished, and exits
non-zero if any deploy failed or is still running after 30 minutes. You need the
[Cloud CLI](https://cloud.laravel.com/docs/cli) (`cpx laravel/cloud-cli`), logged in.

Every environment uses the same settings, so results compare across versions:

- **Start command:** `python serve.py`. It execs the server named in `.web-server` with the
  command listed in `serve.py` (`[::]:$PORT`, workers from `WEB_CONCURRENCY`).
- **App cluster:** `pro.g-2vcpu-4gb`, autoscaling 1–6 replicas (CPU 60%, memory 70%), scale-to-zero off.
- **`WEB_CONCURRENCY`** set by hand to the proposed per-server defaults (Notion: "Python WEB_CONCURRENCY defaults by
  server and interface"): WSGI process-model servers (gunicorn, uwsgi, granian
  and hypercorn WSGI) `clamp((MiB − max(256, MiB/4)) / 150, 1, 16 × cores)`, `20` on this size; waitress
  unset (single process). `serve.py` passes it to every server that takes a worker count.
- **Managed queue** named `default`: Flex, `mq.flex.512mb`, 0–100 workers. Cloud injects
  `LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`, which selects managed mode, and runs
  `laravel-cloud-queues work --queue=default --sleep=0` with no target, so `pyproject.toml` names it
  (`[tool.laravel-cloud-queues] target`). Don't set `LARAVEL_CLOUD_QUEUES_BACKEND`: an explicit backend
  wins over the injected config.
- **Valkey cache** attached with hibernation off (Cloud injects `REDIS_URL`): the jobs' telemetry and the `services.redis` row use it.
- **MySQL:** one cluster per app, with one logical database per environment. Attach each database
  to its own environment (Cloud injects `DATABASE_URL`). Set `DB_SSL_VERIFY=0` for Cloud’s self-signed database proxy.
- **`DEMO_KEY_PREFIX=flask-demo:`** isolates the canary telemetry namespace. The package is locked
  to `laravel-cloud-queues` 0.5.0, with the Redis extra for telemetry.
- Deploy command empty. Build command: the `cloud-bootstrap` copy is temporary (see
  [cloud-bootstrap/](cloud-bootstrap/README.md)); `laravel-cloud-logging-config logging.json`
  writes the logging config granian and hypercorn read with `--log-config`, so their
  main-process lines are JSON too. `serve.py` won't start those servers without it.
  `if [ -d cloud-bootstrap ]; then mkdir -p "$(python -m site --user-site)" && cp cloud-bootstrap/laravel_cloud_bootstrap.py cloud-bootstrap/zz_laravel_cloud_bootstrap.pth "$(python -m site --user-site)/"; fi && laravel-cloud-logging-config logging.json`

The server must accept both IPv6 and IPv4 on `$PORT`: Cloud's startup probes connect over IPv6,
while the pod's nginx currently connects to `127.0.0.1`. `[::]` is dual-stack for most servers;
waitress makes it IPv6-only, so `serve.py` also gives it `0.0.0.0`. The commands in `serve.py` bind both address families.

The page header shows which server answered. Under WSGI servers the async and WebSocket rows
are skipped. waitress uses threads and has no process count, so it runs one process
whatever `WEB_CONCURRENCY` says, and waitress exits on SIGTERM without draining requests.

The app has no login. Protect it at the network level.

## Run locally

You need [uv](https://docs.astral.sh/uv/), Valkey/Redis (Laravel Herd bundles one on
`127.0.0.1:6379`) and MySQL. Local isolation uses Redis DB **1**, prefix
**flask-demo:**, database **flask_cloud_queues**, and port **8001**. Create the local database
with `mysql -h 127.0.0.1 -u root -e "CREATE DATABASE IF NOT EXISTS flask_cloud_queues"`.
Never flush another canary’s Redis database.

```sh
cp .env.example .env
uv run --env-file .env python serve.py   # http://localhost:8001
uv run --env-file .env laravel-cloud-queues work --queue=default --sleep=0   # second terminal; more for more workers
```

In a terminal, logs print as readable lines; piped output and Cloud get the JSON lines Cloud
parses (`LOG_FORMAT=json` or `line` forces either). To read, filter or follow a running process
like `php artisan pail`, run it through laravel-cloud-logging's viewer:

```sh
uv run --env-file .env python -m laravel_cloud_logging.pretty -- python serve.py
uv run --env-file .env python -m laravel_cloud_logging.pretty --level warning -- laravel-cloud-queues work --queue=default --sleep=0
```

It keeps the command's exit code and forwards `SIGTERM`, so supervisors still see crashes.
`solo.yml` runs Web and Worker this way.

Locally nothing restarts a worker that exits, so the queue's timeout case only passes with a
restart loop: `while true; do uv run --env-file .env laravel-cloud-queues work --queue=default --sleep=0; done`.

## Regression suite

After shipping, run every check on every environment, save the results and compare them with the last run (#19):

```sh
uv run python cloud_suite.py --tier quick [env ...]   # every row of GET /api/checks, all environments in parallel
uv run python cloud_suite.py --tier full [env ...]    # also the full-tier jobs (Cloud CLI): logs in Cloud's viewer
```

Each run writes `results/<UTC time>.json` (one row per environment and check: pass, warn, fail or skip,
with the detail) and diffs it against the previous file of the same tier. A regression is a check that
went from pass to fail, or a count that got worse (e.g. plain-text log lines). It prints one table with
regressions marked `!` and exits non-zero on any regression or failure.

`checks.CHECKS` is the one registry. Each entry has `applies(server, python)` (where it doesn't apply,
the row is a skip with the reason) and a tier: `quick` runs in the app on every **Run all checks**;
`full` needs the Cloud CLI, so its job lives in `cloud_suite.JOBS` and the suite posts its result back to
the row (`POST /api/suite-results`). Until then the row says to run `cloud_suite.py --tier full`. The
app never calls the Cloud API itself. `test_checks.py` fails if an entry is missing either field, or a
full-tier entry and its job don't match.

## Upload limit

**The largest request body Cloud accepts is 500 MiB (524,288,000 bytes).** Cloudflare's edge sets it,
in front of everything in the environment, so no environment setting raises it. One byte more gets
Cloudflare's own `413 Payload Too Large` page, straight away, from the `Content-Length`. Without a
`Content-Length` (a chunked or streamed upload), Cloudflare counts the bytes and returns the same `413`
once the body passes the limit. The pod's nginx allows 2048M, so it's never the one that refuses.

Your web server can set a lower limit. hypercorn's WSGI mode (`hypercorn wsgi:app`) holds the whole body
in memory and answers an empty `400` above **16 MiB**; raise `wsgi_max_body_size` in a `-c` config file
(there's no CLI flag). The other servers here don’t cap it below Cloudflare’s 500 MiB.

An upload under the limit can still fail on the environment's HTTP timeout (5–60 s) on a slow link.
For bigger files, or slow uploaders, upload straight to object storage with a presigned URL.

`POST /api/upload` counts the body in 1 MiB chunks without keeping it and returns `{"bytes": n}` (its
own cap is 512 MiB). To measure it on every environment:

```sh
uv run python upload_check.py [env ...]   # 1 MB to 500 MiB + 1 byte, then 501 MiB with no Content-Length
```

## HTTP load

Throughput through the queue is on the page. For requests per second through Cloud's ingress,
run [k6](https://k6.io) from your machine:

```sh
k6 run -e BASE_URL=https://your-app.laravel.cloud k6/http.js
```

It ramps to 50 virtual users and fails if more than 1% of requests fail or p95 latency is over
500 ms.

The Grafana Cloud k6 load tests (`k6 cloud run`, one load zone near us-east-2) hit the same
environments as the regression suite. Load-test requests and the suite scripts send `X-K6-Bypass`
from the `K6_BYPASS` environment variable, which skips the edge rate limits and browser challenge.
Each load script has a `maxVUs`, wall-clock and VUh cap (see its header). Keep the secret in the
shell environment, never in a URL or a committed file:

```sh
uv run python k6/run.py --cloud k6/probe.js -e BASE_URL=https://<Flask host> -e ENV_NAME=server-gunicorn \
  -e SIZE=pro.g-2vcpu-4gb -e WEB_CONCURRENCY=20 -e THRESHOLDS=A -e COMMIT=<env branch sha>
# also k6/concurrency.js (-e PASS=A|B) and k6/throughput.js (-e PASS=A|B -e ROUTE=ping|redis|cpu)
```

`k6/run.py` appends each run's UTC window and every `-e` value except `K6_BYPASS` to `results/k6/runs.jsonl`; `--cloud` needs `SIZE`, `WEB_CONCURRENCY` and `THRESHOLDS`, without it the run is a local `k6 run`.

## Tests

```sh
uv run python app.py --self-check              # router and logging
uv run --env-file .env python test_checks.py   # check contract
uv run --env-file .env python test_throughput.py
uv run python test_servers.py [server ...]     # boots every server in serve.py the way Cloud does
uv run --python 3.11 python test_servers.py # same WSGI tests on the oldest supported Python
uv run --python 3.14 python test_servers.py # restore the main runtime
uv run --env-file .env python test_queue.py # supervised local worker, seven cases and 1,000 jobs
uv run python logs_check.py [env ...]          # on Cloud: levels, exceptions, request IDs, plain lines (#17)
uv run python upload_check.py [env ...]        # on Cloud: the largest upload through Cloud's proxy (#10)
uv run python cloud_suite.py --tier full       # on Cloud: everything above, diffed against the last run
```

## Files

- `app.py`: Flask app factory, sync API blueprint, request validation and logging, queue jobs and the queue check.
- `serve.py`, `.web-server`: the start command; picks the server.
- `wsgi.py`, `gunicorn.conf.py`: the Flask WSGI entrypoint and gunicorn’s settings.
- `checks.py`: the check registry and the Web, Logging, Runtime and Services checks behind `GET /api/checks`.
- `cloud_suite.py`: the regression suite; `logs_check.py`: its Cloud log viewer job; `upload_check.py`: its upload limit job. `scripts/ship.py`: ship `main` everywhere.
- `throughput.py`: the throughput test behind `/api/throughput`.
- `telemetry.py`: job telemetry and the queue check verdicts.
- `logs.py`: logging setup through laravel-cloud-logging.
- `public/`: static fixtures the `web.static` check fetches through Cloud's nginx (the repo's `public/` is the webroot).
- `index.html`: the page (Tailwind from a CDN, no build step).
- `test_queue.py`: starts and stops its own local web server and supervised worker, and saves proof in `results/local-queue.json`.
- `k6/http.js`: HTTP load script. `k6/{probe,concurrency,throughput}.js`, `k6/lib.js`, `k6/run.py`: the Cloud load tests. `solo.yml`: local Web, Worker and test processes for [Solo](https://soloterm.com).
