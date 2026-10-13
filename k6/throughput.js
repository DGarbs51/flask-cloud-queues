// Throughput on one route, ramping arrival rate: 10 min up, 10 min hold, 5 min down (the plan's shape).
// Not evidence for the in-flight trigger; that is concurrency.js.
//   k6 cloud run -e ROUTE=ping|redis|cpu [-e RATE=<peak req/s>] ... k6/throughput.js
// Defaults: ping 400/s, redis 300/s, cpu 50/s. Caps: maxVUs 300 / 300 / 150, wall-clock 1800 s.
// VUh upper bound = (maxVUs + 5 for the stats sampler) x 25 min / 60: ping 127, redis 127, cpu 65 (VUH_CAP 150).
import { CLOUD, SAFETY, begin, end, get, sampleStats, statsScenario } from './lib.js';

const ROUTES = { ping: [400, 300], redis: [300, 300], cpu: [50, 150] }; // [peak req/s, maxVUs]
const ROUTE = __ENV.ROUTE;
if (!ROUTES[ROUTE]) throw new Error('ROUTE must be ping, redis or cpu');
const [DEFAULT_RATE, MAX_VUS] = ROUTES[ROUTE];
const RATE = Number(__ENV.RATE || DEFAULT_RATE);
const WALL_CAP_S = 1800;

export const scenarios = {
  [ROUTE]: {
    executor: 'ramping-arrival-rate', exec: 'hit', startRate: 1, timeUnit: '1s',
    preAllocatedVUs: 50, maxVUs: MAX_VUS, gracefulStop: '30s',
    stages: [
      { duration: '10m', target: RATE },
      { duration: '10m', target: RATE },
      { duration: '5m', target: 0 },
    ],
  },
  stats: statsScenario('25m'),
};

export const options = { scenarios, thresholds: SAFETY, cloud: { name: `throughput ${ROUTE} ${__ENV.ENV_NAME} pass ${__ENV.PASS}`, distribution: CLOUD.distribution } };

export function setup() {
  return begin('throughput.js', scenarios, `/api/${ROUTE}: 10m up to ${RATE}/s, 10m hold, 5m down`);
}

export function hit() {
  get(`/api/${ROUTE}`, WALL_CAP_S);
}

export function stats() {
  sampleStats(WALL_CAP_S);
}

export function teardown(run) {
  end(run);
}
