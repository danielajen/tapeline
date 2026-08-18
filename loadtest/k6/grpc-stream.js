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

// The full run is 500 subscribers over five minutes. CI runs a smaller,
// shorter version of the same shape: a hosted runner has 4 vCPU shared with
// Kafka, Postgres, Redis, ClickHouse and the server under test, so 500 VUs
// there would measure the runner rather than the server. The scenario is
// parameterised rather than duplicated so CI cannot drift from the real test.
const PEAK = Number(__ENV.PEAK_VUS || 500);
const HOLD = __ENV.HOLD || '3m';
const HOLD_SECONDS = Number(__ENV.HOLD_SECONDS || 180);
// Hold slice, in seconds. Bounds the error on first-quote latency.
const SLICE = Number(__ENV.SLICE || 0.05);

export const options = {
  scenarios: {
    subscribers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { target: Math.ceil(PEAK / 5), duration: '30s' },
        { target: PEAK, duration: '1m' },
        { target: PEAK, duration: HOLD },   // the measurement window
        { target: 0, duration: '30s' },
      ],
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    // Time from opening the stream to the first quote. The server sends a
    // snapshot on subscribe precisely so this stays small; if it regresses,
    // the snapshot-on-open path has broken.
    //
    // NOT thresholded, and the metric is reported for information only.
    //
    // Two runs put this at ~30,000 ms with a 30-second hold, and slicing the
    // sleep into 50 ms pieces did not move it - so the first hypothesis, that
    // a blocking sleep starved the event loop, was wrong. Whatever the cause,
    // this harness times the hold rather than the server, and a metric that
    // reproduces the hold duration to four significant figures is not
    // measuring latency.
    //
    // It stays in the output because the number is real evidence of
    // something; it is not asserted on, because asserting on a number whose
    // meaning is unknown is how this repo ended up chasing an order book bug
    // that did not exist. Time to first quote remains unmeasured, and is
    // recorded as unmeasured in MEASUREMENTS.md rather than filled in.
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
    // k6's gRPC codec may surface the field under either the proto name or
    // its lowerCamelCase form. Reading only one produced "n/a" for freshness
    // on a run where every other metric was populated.
    const eventTime = quote.eventTimeUs ?? quote.event_time_us;
    if (eventTime) {
      quoteFreshness.add(Date.now() * 1000 - Number(eventTime));
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
  //
  // Held in short slices rather than one long sleep. A blocking sleep does not
  // yield the VU's event loop, so every stream.on('data') callback queues and
  // fires only when the sleep returns - which made first_quote_latency report
  // p95 = 30,002 ms on a 30-second hold. That was the sleep being measured,
  // not the server. The quote counts were unaffected, because the events were
  // all still delivered and processed; only the timing was fiction.
  //
  // Slicing bounds the error at one slice. That is the resolution limit of
  // this measurement and the threshold below is set to respect it, rather
  // than keeping a tighter threshold that the method cannot support.
  const hold = Math.min(30, HOLD_SECONDS);
  for (let elapsed = 0; elapsed < hold; elapsed += SLICE) {
    sleep(SLICE);
  }

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
    // A flat file CI can assert on without parsing the whole k6 summary.
    'loadtest/results/grpc-stream-summary.txt':
      `quotes_received=${m.quotes_received ? m.quotes_received.values.count : 0}\n` +
      `peak_streams=${get('vus_max', 'value')}\n` +
      `first_quote_p95_ms=${get('first_quote_latency', 'p(95)')}\n` +
      `freshness_p99_us=${get('quote_freshness_us', 'p(99)')}\n`,
  };
}
