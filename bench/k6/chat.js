// Throughput + latency through the gateway at a fixed request rate.
//
//   k6 run -e RATE=1000 -e DURATION=30s bench/k6/chat.js
//   k6 run -e RATE=1000 -e TARGET=http://localhost:9001/v1 bench/k6/chat.js   # direct to mock, for comparison
//
// constant-arrival-rate (open model) instead of a fixed number of VUs (closed
// model): with fixed VUs, a slow server makes clients send *less*, hiding the
// slowdown ("coordinated omission"). Here the rate is fixed and latency shows
// the truth.
import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const TARGET = __ENV.TARGET || 'http://localhost:8080/v1';
const MODEL = __ENV.MODEL || 'mock-openai-style';
const overhead = new Trend('gateway_overhead_ms', true);
const upstream = new Trend('gateway_upstream_ms', true);

export const options = {
  discardResponseBodies: false,
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 500),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: Number(__ENV.VUS || 200),
      maxVUs: Number(__ENV.MAX_VUS || 2000),
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(90)', 'p(99)', 'max'],
};

const body = JSON.stringify({
  model: MODEL,
  messages: [{ role: 'user', content: 'Say hello in one sentence.' }],
});
const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const res = http.post(`${TARGET}/chat/completions`, body, params);
  check(res, { 'status 200': (r) => r.status === 200 });
  // The gateway reports its own overhead per request; this is the precise
  // number, free of client-side and network noise.
  const o = res.headers['X-Gateway-Overhead-Ms'];
  if (o) overhead.add(Number(o));
  const u = res.headers['X-Gateway-Upstream-Ms'];
  if (u) upstream.add(Number(u));
}
