// gRPC streaming path under load.
//
//   k6 run -e GRPC_ADDR=localhost:9090 loadtest/k6/grpc-stream.js
//
// This measures a different thing from the REST test and the difference
// matters. REST measures request latency; this measures how many concurrent
// long-lived subscriptions one replica sustains, and whether quotes keep
// flowing to all of them. A server can have an excellent p99 on point reads
// and still fall over at 500 open streams.

import grpc from 'k6/net/grpc';
import crypto from 'k6/crypto';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const ADDR = __ENV.GRPC_ADDR || 'localhost:9090';
const KEY_ID = __ENV.TAPELINE_KEY_ID || 'tk_local_dev';
const SECRET = __ENV.TAPELINE_SECRET || 'local-development-secret-not-for-production';

const quotesReceived = new Counter('quotes_received');
const firstQuoteLatency = new Trend('first_quote_latency', true);
const quoteFreshness = new Trend('quote_freshness_us');

const client = new grpc.Client();
client.load(['../../proto'], 'tapeline/v1/marketdata.proto');

export const options = {
  scenarios: {
    subscribers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { target: 100, duration: '30s' },
        { target: 500, duration: '1m' },
        { target: 500, duration: '3m' },   // the measurement window
        { target: 0, duration: '30s' },
      ],
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    // Time from opening the stream to the first quote. The server sends a
    // snapshot on subscribe precisely so this stays small; if it regresses,
    // the snapshot-on-open path has broken.
    'first_quote_latency': ['p(95)<250'],
    'grpc_req_duration': ['p(99)<500'],
  },
};

function metadata() {
  const timestamp = Math.floor(Date.now() / 1000);
  const nonce = `${__VU}-${__ITER}-${Date.now()}`;
  // gRPC signs the empty body: request messages arrive after the headers, so
  // signing the payload would mean buffering it before authenticating. The
  // tradeoff is documented in AuthInterceptor.java.
  const emptyBodyHash = crypto.sha256('', 'hex');
  const canonical = [
    'POST',
    '/tapeline.v1.MarketData/StreamQuotes',
    timestamp,
    nonce,
    emptyBodyHash,
  ].join('\n');

  return {
    'x-tapeline-key': KEY_ID,
    'x-tapeline-signature': crypto.hmac('sha256', SECRET, canonical, 'base64'),
    'x-tapeline-timestamp': String(timestamp),
    'x-tapeline-nonce': nonce,
  };
}

export default function () {
  client.connect(ADDR, { plaintext: true });

  const openedAt = Date.now();
  let received = 0;
  let sawFirst = false;

  const stream = new grpc.Stream(
    client,
    'tapeline.v1.MarketData/StreamQuotes',
    { metadata: metadata() },
  );

  stream.on('data', (quote) => {
    received += 1;
    quotesReceived.add(1);

    if (!sawFirst) {
      sawFirst = true;
      firstQuoteLatency.add(Date.now() - openedAt);
    }

    // Freshness measured on the client, which is the only place it means
    // anything to a user.
    if (quote.eventTimeUs) {
      quoteFreshness.add(Date.now() * 1000 - Number(quote.eventTimeUs));
    }
  });

  stream.on('error', (e) => {
    // RESOURCE_EXHAUSTED here is the rate limiter doing its job under 500
    // concurrent subscriptions, not a failure of the server.
    if (e && e.code !== 8) {
      check(null, { 'stream error was expected': () => false });
    }
  });

  stream.write({
    symbols: ['BTC-USD', 'ETH-USD'],
    maxUpdatesHz: 10,
  });

  // Hold the subscription open. This is the point of the test: streams that
  // are opened and closed immediately measure connection setup, not the
  // sustained fan-out cost.
  sleep(30);

  stream.end();
  check(received, { 'received at least one quote': (n) => n > 0 });

  client.close();
}

export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, stat) => (m[name] && m[name].values[stat] !== undefined
    ? m[name].values[stat].toFixed(2) : 'n/a');

  return {
    stdout: `
Tapeline gRPC streaming load test
=================================
peak concurrent streams   ${get('vus_max', 'value')}
quotes received           ${m.quotes_received ? m.quotes_received.values.count : 'n/a'}
quote rate (per sec)      ${get('quotes_received', 'rate')}
first-quote p95 (ms)      ${get('first_quote_latency', 'p(95)')}
freshness p99 (us)        ${get('quote_freshness_us', 'p(99)')}
`,
    'loadtest/results/grpc-stream.json': JSON.stringify(data, null, 2),
  };
}
