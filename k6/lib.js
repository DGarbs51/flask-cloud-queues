// Shared by the Grafana Cloud k6 load tests (concurrency.js, throughput.js, probe.js).
//   k6 cloud run -e BASE_URL=https://flask-cloud-queues-k6-<name>-<suffix>.laravel-demo.cloud \
//     -e ENV_NAME=server-gunicorn -e PASS=A -e K6_BYPASS=<secret> k6/concurrency.js
// Local (localhost only): k6 run -e BASE_URL=http://localhost:8001 -e ENV_NAME=local -e PASS=A k6/concurrency.js
import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

export const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8001').replace(/\/$/, '');
const LOCAL = /^https?:\/\/(localhost|127\.0\.0\.1|\[::1\])(:|\/|$)/.test(BASE_URL);
const PARAMS = { headers: __ENV.K6_BYPASS ? { 'X-K6-Bypass': __ENV.K6_BYPASS } : {}, tags: {} };

// Distinct pod values per minute (from /api/ping and /api/slow) are the replica count over time.
const pods = new Counter('pods_seen');

// Grafana Cloud k6 load zone near us-east-2 (Ohio), one zone only.
export const CLOUD = { distribution: { ohio: { loadZone: 'amazon:us:columbus', percent: 100 } } };

// Hard caps. VUh upper bound = sum(maxVUs x scenario duration) / 3600; setup() refuses a run over VUH_CAP.
// maxDuration is the wall-clock cap in seconds: iterations abort the run past it (scenario durations already bound it).
export const VUH_CAP = Number(__ENV.VUH_CAP || 150);

const seconds = (d) => Number(d.slice(0, -1)) * { s: 1, m: 60, h: 3600 }[d.slice(-1)];

// scenarios: k6 scenarios object. Returns the VUh upper bound from each scenario's maxVUs and stages / duration.
export function vuhEstimate(scenarios) {
  let total = 0;
  for (const s of Object.values(scenarios)) {
    const length = s.stages ? s.stages.reduce((sum, st) => sum + seconds(st.duration), 0) : seconds(s.duration);
    total += (s.maxVUs * length) / 3600;
  }
  return total;
}

export function wallSeconds(scenarios) {
  return Math.max(...Object.values(scenarios).map((s) =>
    seconds(s.startTime || '0s') + (s.stages ? s.stages.reduce((sum, st) => sum + seconds(st.duration), 0) : seconds(s.duration)) + seconds(s.gracefulStop || '30s')));
}

// Call from setup(). stages is a printable description of the shape; logs the run window's start.
export function begin(script, scenarios, stages, { passRequired = true } = {}) {
  if (!LOCAL && !__ENV.K6_BYPASS) exec.test.abort('K6_BYPASS is not set: the edge would block a non-localhost BASE_URL');
  if (!__ENV.ENV_NAME) exec.test.abort('ENV_NAME is not set (e.g. server-gunicorn)');
  if (passRequired && !['A', 'B'].includes(__ENV.PASS)) exec.test.abort('PASS must be A or B');
  const vuh = vuhEstimate(scenarios);
  if (vuh > VUH_CAP) exec.test.abort(`VUh upper bound ${vuh.toFixed(1)} is over VUH_CAP=${VUH_CAP}`);
  const run = { script, environment: __ENV.ENV_NAME, base_url: BASE_URL, pass: __ENV.PASS || '-', stages,
    start_utc: new Date().toISOString(), vuh_upper_bound: Number(vuh.toFixed(1)), wall_cap_s: wallSeconds(scenarios) };
  console.log(`k6-run-start ${JSON.stringify(run)}`);
  return run;
}

// Call from teardown(run).
export function end(run) {
  console.log(`k6-run-end ${JSON.stringify({ ...run, end_utc: new Date().toISOString() })}`);
}

// GET path with the bypass header. Aborts the run past the wall-clock cap.
export function get(path, cap, name) {
  if (exec.instance.currentTestRunDuration > cap * 1000) exec.test.abort(`wall-clock cap of ${cap} s reached`);
  const res = http.get(`${BASE_URL}${path}`, { ...PARAMS, tags: { name: name || path } });
  check(res, { [`GET ${name || path} is 200`]: (r) => r.status === 200 });
  if (res.status === 200 && res.headers['Content-Type'] && res.headers['Content-Type'].includes('json')) {
    const pod = res.json('pod');
    if (pod) pods.add(1, { pod });
  }
  return res;
}

// One GET /api/stats every 10 s beside the load, for the answering pod's loop_lag_ms {p50, p99}. Logged per sample,
// because a k6 metric can't hold the JSON. Each sample comes from whichever pod answered.
export function statsScenario(duration, startTime = '0s') {
  return { executor: 'constant-arrival-rate', exec: 'stats', rate: 1, timeUnit: '10s', duration, startTime,
    preAllocatedVUs: 2, maxVUs: 5, gracefulStop: '30s' };
}

export function sampleStats(cap) {
  const res = get('/api/stats', cap, '/api/stats');
  if (res.status === 200) {
    console.log(`k6-stats ${JSON.stringify({ at: new Date().toISOString(), server: res.json('server'), loop_lag_ms: res.json('loop_lag_ms') })}`);
  }
}

// Stop early when most requests fail: a blocked edge or a dead environment costs VUh for nothing.
export const SAFETY = { http_req_failed: [{ threshold: 'rate<0.5', abortOnFail: true, delayAbortEval: '60s' }] };
