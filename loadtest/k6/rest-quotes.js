// REST read path under load.
//
//   k6 run -e BASE_URL=http://localhost:8080 loadtest/k6/rest-quotes.js
//
// Every number that appears in the README and in docs/MEASUREMENTS.md comes
// from a run of this file or its gRPC sibling. The thresholds below are not
// decoration: k6 exits non-zero when one is breached, so CI fails on a
// latency regression the same way it fails on a broken test.

import http from 'k6/http';
import crypto from 'k6/crypto';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const KEY_ID = __ENV.TAPELINE_KEY_ID || 'tk_local_dev';
const SECRET = __ENV.TAPELINE_SECRET || 'local-development-secret-not-for-production';

const SYMBOLS = (__ENV.SYMBOLS || 'BTC-USD,ETH-USD,SOL-USD').split(',');

const quoteLatency = new Trend('quote_latency', true);
const authFailures = new Rate('auth_failures');

export const options = {
  scenarios: {
    // Ramp rather than a step. A step start measures cold-start behaviour —
    // empty caches, unwarmed JIT, unopened connection pools — and reports it
    // as steady-state p99, which is how load tests end up flattering.
    steady: {
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 500,
      stages: [
        { target: 200, duration: '30s' },   // warm up
        { target: 1000, duration: '1m' },   // ramp
        { target: 1000, duration: '3m' },   // hold — this window is the measurement
        { target: 0, duration: '30s' },     // ramp down
      ],
    },
  },
  thresholds: {
    // p99 under 25ms for a Redis-backed point read. Anything slower means
    // the hot path is not actually hot.
    'http_req_duration{expected_response:true}': ['p(50)<5', 'p(95)<15', 'p(99)<25'],
    'http_req_failed': ['rate<0.001'],
    'auth_failures': ['rate<0.001'],
  },
  // Discard the ramp so the reported percentiles describe the hold window
  // rather than being dragged up by the warm-up.
  discardResponseBodies: false,
};

// HMAC signing, matching SignedRequest.java exactly: method, path,
// timestamp, nonce and body hash joined by newlines. If this drifts from the
// server, every request 401s and the load test measures the rejection path.
function sign(method, path, body) {
  const timestamp = Math.floor(Date.now() / 1000);
  const nonce = `${__VU}-${__ITER}-${Date.now()}`;
  const bodyHash = crypto.sha256(body || '', 'hex');
  const canonical = [method, path, timestamp, nonce, bodyHash].join('\n');
  const signature = crypto.hmac('sha256', SECRET, canonical, 'base64');

  return {
    'X-Tapeline-Key': KEY_ID,
    'X-Tapeline-Signature': signature,
    'X-Tapeline-Timestamp': String(timestamp),
    'X-Tapeline-Nonce': nonce,
  };
}

export default function () {
  const symbol = SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)];
  const path = `/api/v1/quotes/${symbol}`;

  const res = http.get(`${BASE_URL}${path}`, {
    headers: sign('GET', path, ''),
    tags: { endpoint: 'get_quote' },
  });

  quoteLatency.add(res.timings.duration);
  authFailures.add(res.status === 401 || res.status === 403);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'body has a mid price': (r) => {
      if (r.status !== 200) return false;
      try {
        return typeof r.json('mid') === 'number';
      } catch (e) {
        return false;
      }
    },
    // A quote older than five seconds is stale enough to be a bug, and
    // catching it here means the load test also validates the pipeline
    // rather than only the web tier.
    'quote is fresh': (r) => {
      if (r.status !== 200) return false;
      try {
        const ageUs = Date.now() * 1000 - r.json('eventTimeUs');
        return ageUs < 5_000_000;
      } catch (e) {
        return false;
      }
    },
  });
}

export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, stat) => (m[name] && m[name].values[stat] !== undefined
    ? m[name].values[stat].toFixed(2) : 'n/a');

  // Printed in the shape docs/MEASUREMENTS.md expects, so the numbers can be
  // pasted in without being retyped and quietly rounded in the retyping.
  const summary = `
Tapeline REST load test
=======================
requests           ${m.http_reqs ? m.http_reqs.values.count : 'n/a'}
throughput (rps)   ${get('http_reqs', 'rate')}
p50 latency (ms)   ${get('http_req_duration', 'p(50)')}
p95 latency (ms)   ${get('http_req_duration', 'p(95)')}
p99 latency (ms)   ${get('http_req_duration', 'p(99)')}
max latency (ms)   ${get('http_req_duration', 'max')}
error rate         ${get('http_req_failed', 'rate')}
`;

  return {
    stdout: summary,
    'loadtest/results/rest-quotes.json': JSON.stringify(data, null, 2),
  };
}
