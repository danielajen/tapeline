# Measurements

**Every number in this file that is written as `[measure]` is unmeasured.**

That is deliberate and it is the most important thing in the document. A
made-up p99 is worse than no p99, because the follow-up question in an
interview is always "how did you measure that?" — and the answer has to be a
command someone could run, not a recollection.

Each row below names the command that produces it and the metric it reads.
Run the command, paste the number, and note the hardware. Until then the
placeholder stands.

---

## How to produce these numbers

```bash
make up                       # start the stack
make submit-jobs              # submit the three Flink jobs
sleep 600                     # ten minutes of real venue data
make smoke                    # verify the pipeline end to end first

make load                     # REST: throughput and latency
make load-grpc                # gRPC: concurrent streams and freshness
make chaos                    # broker, taskmanager, redis, network
```

Record the hardware. A p99 from a laptop and a p99 from an EKS node group are
different claims, and conflating them is the kind of thing that unravels in a
follow-up question.

**Environment for the numbers below:** `[measure: CPU, RAM, container runtime,
whether the Flink cluster was local or on EKS]`

---

## Verified — measured 17 August 2026

Hardware: MacBook Air, Apple silicon, 8 GB RAM, 8 CPUs. Docker allocated
~4 GB, which is why the stack was measured in stages under
`deploy/docker-compose.lowmem.yml` rather than all at once. Numbers below
come from a live run against Coinbase Advanced Trade, Kraken v2 and Binance
combined-stream, 3 symbols (BTC-USD, ETH-USD, SOL-USD), 6 Kafka topics.

### Ingestion — 120 second steady-state run

| Metric | Measured | Source |
|---|---|---|
| Events published to Kafka | **48,036** | `tapeline_ingest_events_published_total` |
| Events received from venues | **48,039** | `tapeline_ingest_events_received_total` |
| Sustained throughput | **400 events/sec** | published / 120s |
| Decode errors | **0** | `tapeline_ingest_decode_errors_total` |
| Publish errors | **0** | `tapeline_ingest_publish_errors_total` |
| Duplicates suppressed | **0** | `tapeline_ingest_duplicates_dropped_total` |
| Sequence gaps | **2** | `tapeline_ingest_sequence_gaps_total` |
| Unaccounted (received − published) | **3** | in-flight batch at scrape time, 0.006% |

Per venue: Kraken 21,377 (178/s), Binance 18,216 (152/s), Coinbase 8,443 (70/s).

**What 400/sec is and is not.** It is what three exchanges actually emitted for
three symbols during the window. It is *not* a capacity measurement — the
pipeline was never saturated, CPU was not the limit, and Kafka publish latency
never became a bottleneck. Measuring capacity needs a synthetic generator, and
that has not been built. Quoting this as a throughput ceiling would be wrong.

### Source lag — venue event time to local receipt

| Venue / kind | Mean | Samples |
|---|---|---|
| Kraken book | **29.0 ms** | 21,082 |
| Binance book | **50.1 ms** | 3,240 |
| Binance trade | **53.5 ms** | 14,976 |
| Coinbase book | **54.0 ms** | 6,364 |
| Coinbase trade | 3,434 ms | 2,036 |
| Kraken trade | 65,413 ms | 298 |

The last two rows are **not** pipeline lag. Both venues replay recent
historical trades in the subscribe snapshot, and those carry genuinely old
event timestamps. With only 298 Kraken trade samples in the window, a handful
of snapshot rows dominates the mean. The book figures — continuous streams
with no snapshot replay — are the honest measure of transport lag.

### Serving tier — k6 load test, 110 s

Quote topic fed at 300/sec by `tools/quotegen` so the measurement isolates the
serving path — Kafka consume, Redis cache, HMAC verification, nonce check,
token-bucket rate limit, JSON response — rather than depending on Flink.

| Metric | Measured |
|---|---|
| Requests | **192,660** |
| Sustained rate | **1,751 req/sec** |
| p50 latency | **350 µs** |
| p95 latency | **14.6 ms** |
| p99 latency | **139 ms** |
| Error rate | **0.05%** (101 of 192,660) |
| Quotes consumed from Kafka | **7,848, zero decode failures** |

### The second load point — where it stops scaling

A second run pushed the arrival rate to 3,000/sec to find the knee:

| Metric | Run A (target 2,500/s) | Run B (target 3,000/s) |
|---|---|---|
| Achieved throughput | **1,751 req/sec** | **2,010 req/sec** |
| p50 | **350 µs** | **465 µs** |
| p95 | **14.6 ms** | **410 ms** |
| p99 | **139 ms** | **1.47 s** |
| Error rate | 0.05% | 0.23% |
| Requests | 192,660 | 301,558 |

**This is the useful measurement.** Pushing past ~1,750 req/sec buys 15% more
throughput and costs a 10x worse p99. The knee is real and it is where the
service should be run. Quoting 2,010 req/sec without the latency it came with
would be technically true and materially misleading.

Both runs sign every request with a fresh nonce, so the figures cover HMAC
verification, the Redis nonce claim and the token bucket — not just a cache
read.

**The p99 is the honest weak spot.** A 350 µs median against a 139 ms p99 is a
long tail, not a slow service: JVM GC pauses plus the Postgres API-key lookup
on a cache miss. Reporting the median alone would be flattering and wrong.
Reducing it means pre-warming the key cache and tuning the heap, neither of
which has been done.

Every request was HMAC-signed with a fresh nonce, so the figure includes the
full authentication path — signature verification, Redis nonce claim, and
token-bucket evaluation — not just a cache read.

### ClickHouse OLAP tier

45,000 one-second bars loaded (3 symbols x 3 venues x 5,000 windows) into the
schema in `deploy/clickhouse/01-bars.sql`.

| Query | Measured |
|---|---|
| Range scan, 1,000 bars for one symbol over 10 min | **45-48 ms**, five consecutive runs |
| Full aggregate over 45,000 bars, grouped by symbol | sub-second |
| Rows loaded | **45,000** |

The range scan is the query `WindowQueryService` issues. ~47 ms against
ClickHouse versus a **350 us** median from the Redis hot path is the whole
argument for the two-tier design: they are ~130x apart, and serving range
queries out of the cache would mean either thousands of key lookups or one
enormous serialized blob per symbol.

The serving tier was not wired to ClickHouse during the load test - its OLAP
datasource pointed at Postgres, and the endpoint under test only touches
Redis. So the numbers above are ClickHouse measured directly, not measured
through `QueryWindows`.

### Chaos — Kafka broker killed mid-stream

| Metric | Measured |
|---|---|
| Broker outage duration | **31 s** |
| Time to resume publishing after restart | **20 s** |
| Ingestion process survived | **yes** |
| Events dropped | **0** |
| Duplicates introduced on recovery | **0** |
| Unaccounted after recovery | 4 of 23,971 (0.017%, in-flight) |

During the outage the fan-in buffer filled and backpressure reached the venue
sockets, which is the designed behaviour — bounded memory rather than
unbounded buffering.

**This experiment failed on its first run.** See `POSTMORTEM.md`.

### Schema registry

Verified live: `GET /config` returns `{"compatibilityLevel":"FULL"}`, and all
three schemas registered on startup (trade → id 1, book_delta → 2,
chain_transfer → 3).

### Test suites

| Suite | Result |
|---|---|
| Go | **76 tests**, race-detector clean, **76%** statement coverage |
| Scala | **55 tests** |
| Java unit | **55 tests** |
| Java integration (Testcontainers: real Kafka + Postgres) | **10 tests** |

---

## Still unmeasured

Stated plainly so nothing here gets quoted as if it were measured.

- **Flink now runs in CI**, on a 16 GB GitHub-hosted runner, on every commit
  that touches `stream/`. `.github/workflows/flink-e2e.yml` seeds
  `md.book.v1` with 8,000 deterministic deltas, submits the book job, and
  **fails the build** unless the job is RUNNING, at least one checkpoint has
  completed, and at least one quote was produced. Checkpoint duration and
  state size are printed and uploaded as an artifact on every run. That is a
  better arrangement than a one-off local run: it cannot silently stop being
  true.

- **Flink ran locally too, but could not be sustained on this hardware.** A real Flink
  1.20.1 cluster was started on the host (outside Docker, to use memory Docker
  was not holding), the book job was submitted successfully, and it read
  records from `md.book.v1` and passed them into the order-book operator.
  Getting that far required fixing four separate defects — see POSTMORTEM 3.
  It then failed under `AskTimeoutException` between TaskManager and
  JobManager: 8 GB total, with Docker holding 4 GB and Flink needing ~2.8 GB
  on top, is not enough. **No checkpoint ever completed and no quote was ever
  produced**, so nothing about checkpoint duration, state size, or exactly-once
  throughput is measured.
- **Backfill correctness.** The comparison query in `BACKFILL.md` is unrun; it
  depends on the lakehouse job, which depends on a sustained Flink cluster.
- **Monolith vs per-topic comparison.** Both jobs exist; neither has been run,
  so the checkpoint-duration claim remains unquantified.
- **Terraform has never been applied** to a live AWS account.

## Ingestion

| Metric | Value | Source |
|---|---|---|
| Sustained messages/sec into Kafka | `[measure]` | `rate(tapeline_ingest_events_published_total[1m])` |
| Peak messages/sec | `[measure]` | same, `max_over_time` across the run |
| Source lag p50 / p99 | `[measure]` | `tapeline_ingest_source_lag_seconds` |
| Kafka publish latency p99 | `[measure]` | `tapeline_ingest_publish_latency_seconds` |
| Sequence gaps per hour, per venue | `[measure]` | `tapeline_ingest_sequence_gaps_total` |
| Duplicates suppressed per hour | `[measure]` | `tapeline_ingest_duplicates_dropped_total` |
| Reconnects per venue per hour | `[measure]` | `tapeline_ingest_reconnects_total` |
| Resident memory at steady state | `[measure]` | `docker stats` or `container_memory_working_set_bytes` |

Note the gaps and duplicates alongside the throughput. A throughput number
without them is a claim about volume, not about correctness.

---

## Stream processing

| Metric | Value | Source |
|---|---|---|
| Checkpoint duration p99, book job | `[measure]` | `flink_jobmanager_job_lastCheckpointDuration` |
| Checkpoint size, book job | `[measure]` | `flink_jobmanager_job_lastCheckpointSize` |
| Order book state size per key | `[measure]` | RocksDB metrics, or checkpoint size / key count |
| Quotes emitted per second | `[measure]` | `numRecordsOutPerSecond` on the book operator |
| Crossed books per hour | `[measure]` | `tapeline_book_crossed_books` |
| Divergence alerts per day | `[measure]` | `tapeline_divergence_alerts_emitted` |

### The monolith comparison

This is the measurement behind the architecture story in
`DESIGN_DECISIONS.md#d2`, and it is worth running properly because it is the
strongest interview material in the project. Run the same workload twice:

```bash
# Before
flink run -c io.tapeline.stream.TapelineJob stream/target/tapeline-stream-0.1.0.jar monolith

# After
make submit-jobs
```

| Metric | Monolithic | Per-topic | Source |
|---|---|---|---|
| Checkpoint duration p99 | `[measure]` | `[measure]` | `lastCheckpointDuration` |
| Failed checkpoints per hour | `[measure]` | `[measure]` | `numberOfFailedCheckpoints` |
| Max sustainable throughput | `[measure]` | `[measure]` | the point where lag starts growing |
| Restart blast radius | 3 stages | 1 stage | observed |
| CPU at equal throughput | `[measure]` | `[measure]` | container CPU |

The honest expectation: the per-topic version will use **more** total CPU and
have **worse** end-to-end latency, because of the extra Kafka hop. What it
buys is checkpoint stability and independent failure. If the numbers do not
show that, the story in the design doc is wrong and needs rewriting — which
is the point of measuring rather than asserting.

---

## Serving

| Metric | Value | Source |
|---|---|---|
| REST `GET /quotes/{symbol}` p50 / p95 / p99 | `[measure]` | `make load` |
| Sustained request rate at the p99 target | `[measure]` | k6 `http_reqs` rate |
| Concurrent gRPC streams per replica | `[measure]` | `make load-grpc` |
| Time to first quote on subscribe, p95 | `[measure]` | k6 `first_quote_latency` |
| Quote freshness at the client, p99 | `[measure]` | k6 `quote_freshness_us` |
| Drops for slow clients under load | `[measure]` | `tapeline_serving_stream_dropped_total` |
| Heap at steady state | `[measure]` | `jvm_memory_used_bytes` |

The k6 thresholds currently assert p99 under 25 ms for the REST read path. If
a run breaches that, k6 exits non-zero and CI fails — so this row cannot
quietly drift.

---

## Chaos experiments

Run by `make chaos`. Each experiment states a hypothesis before it runs; the
outcome column records whether the hypothesis held, including when it did
not.

| Experiment | Hypothesis | Recovery | Events lost | Held? |
|---|---|---|---|---|
| Kafka broker killed mid-stream | Producer retries; zero loss; recovery under 60s | `[measure]` | `[measure]` | `[measure]` |
| Flink TaskManager killed | Restores from checkpoint; book state survives | `[measure]` | `[measure]` | `[measure]` |
| Redis killed | Rate limiter fails open; API stays up | `[measure]` | n/a | `[measure]` |
| Network partition to Kafka | Buffer applies backpressure; depth plateaus | `[measure]` | `[measure]` | `[measure]` |

**Measuring "events lost" properly.** Compare the delta in
`tapeline_ingest_events_received_total` against the delta in
`tapeline_ingest_events_published_total` across the experiment window. Equal
deltas is the zero-loss claim. Any shortfall is real loss and must be
explained before the number appears anywhere near a resume.

The broker experiment failed its hypothesis on the first run. That failure is
written up in `POSTMORTEM.md`, and the write-up is worth more than the
eventual pass.

---

## Backfill

See `BACKFILL.md` for the correctness proof. The numbers:

| Metric | Value | Source |
|---|---|---|
| Events replayed per second | `[measure]` | bars emitted × trades per bar, over wall time |
| One day replayed in | `[measure]` | job wall time |
| Bars matching the streaming run | `[measure: expect 100%]` | the comparison query in `BACKFILL.md` |
| Iceberg files scanned vs total | `[measure]` | Flink source metrics; shows partition pruning working |

The third row is the one that matters. Anything below 100% means the
"identical reconstruction" claim is false, and the cause has to be found
before the claim is made.

---

## What these numbers are not

At three symbols across three venues, this system handles thousands of
messages per second, not the trillions per day Uber's does. The architecture
is the same shape; the scale is not, and saying otherwise in an interview
invites a question that ends badly.

What the numbers legitimately support is a claim about *engineering*: that
the system was measured, that it was broken on purpose to see what happened,
and that the failure modes are known rather than assumed.
