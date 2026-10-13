// HTTP requests/s and latency against a deployed app.
//   k6 run -e BASE_URL=https://your-app.laravel.cloud k6/http.js
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8001').replace(/\/$/, '');

export const options = {
  stages: [
    { duration: '10s', target: 20 },
    { duration: '20s', target: 50 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

export default function () {
  for (const path of ['/api/ping', '/']) {
    const res = http.get(`${BASE_URL}${path}`, { headers: __ENV.K6_BYPASS ? { 'X-K6-Bypass': __ENV.K6_BYPASS } : {} });
    check(res, { [`GET ${path} is 200`]: (r) => r.status === 200 });
  }
  sleep(0.1);
}
