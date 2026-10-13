// In-flight concurrency vs Cloud's HTTP autoscaling trigger: GET /api/slow?seconds=1 held just below, then just
// above, 95 in flight per pod (floor(0.7 x 4096 / 30) on 4 GiB). By Little's law in flight = arrival rate x 1 s.
// Starts at 1 pod. HPA ignores ratios within 10% of the target, so the load steps are: 85/s (below target), 100/s
// (above 95 but inside the tolerance), 115/s (clearly above it); then "down" shows scale-in. Same shape for PASS=A and B.
//   below 120 s at 85/s | inside 120 s at 100/s | above 120 s at 115/s | down 360 s at 30/s   (steps > 1 min, scale-down > 300 s)
// Caps: maxVUs 250 x 3 load steps (115/s survives 2 s latency) and 60, wall-clock 900 s.
// VUh upper bound: (3 x 250 x 120 + 60 x 360 + 5 x 720 for the stats sampler)/3600 = 32.
import { CLOUD, SAFETY, begin, end, get, sampleStats, statsScenario } from './lib.js';

const WALL_CAP_S = 900;
const PATH = '/api/slow?seconds=1';
const step = (rate, duration, startTime, maxVUs) => ({
  executor: 'constant-arrival-rate', exec: 'slow', rate, timeUnit: '1s', duration, startTime,
  preAllocatedVUs: Math.min(maxVUs, rate + 20), maxVUs, gracefulStop: '30s',
});

export const scenarios = {
  below: step(85, '120s', '0s', 250),
  inside: step(100, '120s', '120s', 250),
  above: step(115, '120s', '240s', 250),
  down: step(30, '360s', '360s', 60),
  stats: statsScenario('720s'),
};

export const options = { scenarios, thresholds: SAFETY, cloud: { name: `concurrency ${__ENV.ENV_NAME} pass ${__ENV.PASS}`, distribution: CLOUD.distribution } };

export function setup() {
  return begin('concurrency.js', scenarios, 'below 85/s 120 s; inside 100/s 120 s; above 115/s 120 s; down 30/s 360 s');
}

export function slow() {
  get(PATH, WALL_CAP_S, '/api/slow?seconds=1');
}

export function stats() {
  sampleStats(WALL_CAP_S);
}

export function teardown(run) {
  end(run);
}
