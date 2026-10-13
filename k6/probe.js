// 2-minute edge probe before the real runs: /api/ping ramping to 300 req/s from one load zone.
// Any 403/429 here points at the WAF rules or Cloud's ingress, not the app.
// Caps: maxVUs 150, wall-clock 300 s. VUh upper bound: 150 x 120 s / 3600 = 5.
import { CLOUD, SAFETY, begin, end, get } from './lib.js';

const WALL_CAP_S = 300;

export const scenarios = {
  probe: {
    executor: 'ramping-arrival-rate', exec: 'ping', startRate: 10, timeUnit: '1s',
    preAllocatedVUs: 30, maxVUs: 150, gracefulStop: '30s',
    stages: [
      { duration: '90s', target: 300 },
      { duration: '30s', target: 300 },
    ],
  },
};

export const options = { scenarios, thresholds: SAFETY, cloud: { name: `probe ${__ENV.ENV_NAME}`, distribution: CLOUD.distribution } };

export function setup() {
  return begin('probe.js', scenarios, '/api/ping: 90s up to 300/s, 30s hold', { passRequired: false });
}

export function ping() {
  get('/api/ping', WALL_CAP_S);
}

export function teardown(run) {
  end(run);
}
