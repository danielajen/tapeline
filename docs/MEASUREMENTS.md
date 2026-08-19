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

### Flink — measured in CI, 18 August 2026

Run on a GitHub-hosted 16 GB runner, `md.book.v1` seeded with ~8,000
deterministic deltas, parallelism 2, 10-second checkpoint interval.

| Metric | Measured |
|---|---|
| Job state after 10 minutes | **RUNNING** (stable, no restarts) |
| Records read | **7,926** |
| Records written | **7,926** |
| **Checkpoints completed** | **3** |
| **Checkpoints failed** | **0** |
| Checkpoint duration | **avg 89 ms, max 215 ms** |
| Checkpoint state size | **27,751 bytes** |

**This is what makes exactly-once a measured claim rather than a design one.**
Flink's Kafka sink commits its transaction only when a checkpoint completes;
three completing with zero failures means the two-phase commit is working end
to end, and the 27 KB of state is real order-book content being persisted and
restored, not an empty snapshot.

Getting here took **seven** distinct defects — see POSTMORTEM 3 and 4. The
last was the hardest: Kryo does not round-trip Scala collections, and the fix
had to reach the *event* types, not just the state types.

**Still open: `quotes produced = 0`.** Two hypotheses tested, one eliminated.

The first was that the generator produced permanently crossed books — it
randomised the mid per message and deltas only added levels, so a bid from a
high-mid message sat above an ask from a low-mid one and `BookFunction`
correctly refused to emit. Fixing the generator to send periodic snapshots and
deletes on the previous touch **changed the state size from 27,751 to 19,113
bytes**, confirming the deletes take effect — and quotes stayed at zero. So
that was a real bug in the test data, and not this one.

The surviving hypothesis, untested: the emit path is a **processing-time
timer** registered from `processElement`, and it re-arms only when another
element arrives. The seeded 8,000 records drain in seconds, so after the
backlog is consumed no element ever re-arms the timer. That would explain
sustained zero output from a job that is otherwise healthy — and it would be a
real design bug, not a test artifact: a quiet symbol would stop producing
quotes entirely rather than continuing to publish its last book.

Verifying it means either a continuously-fed topic or an event-time timer.
Recorded as unresolved rather than rounded up.

- **Previously: Flink ran in CI and crash-looped.** The workflow now gets all the
  way through cluster startup and job submission — five separate defects fixed
  to get there — and the job reads records and writes 2,220 before failing on
  a Kryo serialization bug in the event types (POSTMORTEM 4). **No checkpoint
  has ever completed, and no quote has ever been produced by Flink.**
  Exactly-once is written and unit-tested, not observed.

- **The workflow itself now runs in CI**, on a 16 GB GitHub-hosted runner, on every commit
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
- **Monolith vs per-topic comparison.** Measured; see below.
  Previously recorded here as: both jobs exist; neither has been run,
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

Measured by `.github/workflows/job-layout.yml`. Both layouts read identical
seeded input on the same cluster: book deltas at 500/s and trades at 50/s for
30 seconds, a ten-to-one ratio, which is what makes the two stages' state
profiles diverge in the first place. A 90-second observation window per layout.

| Metric | Monolithic | Per-topic | Source |
|---|---|---|---|
| Jobs | 1 | 2 | — |
| Stages in one failure domain | **5** | **2** | job graph |
| Checkpoints completed | 3 | 3 book / 3 trades | `counts.completed` |
| Checkpoints failed | 0 | 0 | `counts.failed` |
| Checkpoint duration avg | **113 ms** | **61 ms** | `end_to_end_duration.avg` |
| Checkpoint duration max | **232 ms** | **151 ms** | `end_to_end_duration.max` |
| State persisted per checkpoint | **46,121 B** | **28,186 B** | `state_size.avg` |
| Max sustainable throughput | not measured | not measured | needs dedicated hardware |
| CPU at equal throughput | not measured | not measured | needs dedicated hardware |

The per-topic column takes the **worse** of the two jobs for duration, not the
average: a pipeline is as slow to checkpoint as its slowest independent job,
and averaging would flatter the split layout by hiding the book job behind the
trade job. State size sums, because that is the total being persisted.

**What the numbers say.** The monolith checkpoints roughly twice as slowly and
persists 1.6x the state per checkpoint, at this small scale where nothing is
under stress. That is the mechanism the design doc describes, visible before
it becomes a problem: every stage's state is captured on every checkpoint, so
the interval has to suit the largest one. The blast radius difference needs no
statistics — a failure anywhere in the monolith restarts five stages,
including order book state that had nothing to do with the fault.

**What the numbers do not say.** Two rows are left unmeasured rather than
filled in. Max sustainable throughput and steady-state CPU need sustained load
on dedicated hardware; a 4-vCPU shared runner over 90 seconds cannot produce
an honest number for either. The expectation stated before measuring still
stands and is still untested: the per-topic version should use **more** total
CPU and have **worse** end-to-end latency because of the extra Kafka hop. What
it buys is what the table above does show.

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

## Flink book job, end to end (GitHub Actions, 4 vCPU / 16 GB)

Measured by `.github/workflows/flink-e2e.yml` on every dispatch. Kafka and
Schema Registry in Docker, a real Flink 1.20.1 cluster, 8,000 Avro book
messages seeded through the registry.

| | |
|---|---|
| records into the book operator | 7,930 |
| quotes written to `md.quotes.v1` | 64 |
| checkpoints completed / failed | 3 / 0 |
| checkpoint duration | avg 83 ms, max 194 ms |
| checkpointed state | avg 14.5 KB, max 14.6 KB |

The quote count is far below the record count by design: quotes are emitted on
a timer and only when the top of book changes, so 7,930 deltas across 9 keys
over ~80 seconds coalesce to 64 publishes.

Completed checkpoints are what makes the exactly-once claim measured rather
than asserted. The sink is transactional, and a transaction commits on
checkpoint completion; three clean checkpoints mean the two-phase commit path
actually ran.

Not measured: throughput under sustained load, recovery time from a real
TaskManager failure, or behaviour at production parallelism. This is a
correctness and liveness check, not a benchmark.

## Topic layout: per-type topics vs one monolithic topic

Measured by `.github/workflows/topic-layout.yml`. 40,000 records of each of
three event types, sized representatively (trade 120 B, book delta 900 B,
quote 180 B), written once to per-type topics and once to a single topic
carrying a type header. Equal partition counts on both sides, fixed seed. The
consumer wants trades only.

| | Per-type | Monolith |
|---|---|---|
| Records delivered | 40,000 | 119,998 |
| Records useful | 40,000 | 40,000 |
| Bytes fetched | 5,080,000 | 48,838,906 |
| Elapsed | 16.3 s | 26.2 s |

**Read amplification: 9.61x. Records discarded: 66.7%.**

The byte ratio is far worse than the record ratio, and that is the finding.
Discarding two thirds of the records sounds like a 3x cost; it is 9.6x,
because the type this consumer does not want is also the largest. A consumer
that wants trades pays for everyone else's book deltas, in network transfer
and decompression, before it can discard them.

This is one side of the tradeoff, not a verdict. Per-type topics cost more
partitions to operate and give up total ordering across event types, and
nothing here measures that. What it does establish is that the read
amplification argument is real and larger than a record count suggests.

## gRPC server-streaming

Measured by `.github/workflows/grpc-stream.yml`. 60 concurrent subscribers on
a 4-vCPU runner shared with Kafka, Postgres, Redis, ClickHouse and the server
under test, quotes produced into the topic for the whole run so subscribers
receive pushed updates rather than only the opening snapshot.

| | |
|---|---|
| Peak concurrent streams | 60 |
| Quotes delivered to subscribers | 70,564 |
| Delivery rate | 344 quotes/sec |
| Failed streams | 0 |

This establishes that the fan-out path works: one Kafka consumer per replica,
broadcast in memory to every open stream, delivering to 60 simultaneous
subscribers without dropping one.

**Time to first quote is not measured.** The harness reports a p95 that
reproduces the hold duration to four significant figures across two runs and
did not move when the sleep was sliced into 50 ms pieces, so it is timing the
test rather than the server. The metric is still printed, because the number
is evidence of something, but nothing is asserted on it. Filling this row with
that value would be inventing a measurement.

**Not measured:** behaviour at 500 subscribers, which is what the full
scenario runs. A shared runner at that concurrency would measure the runner.

## Kappa backfill correctness

Measured by `.github/workflows/backfill-proof.yml`. 8,900 trades seeded once,
processed by the live path into `md.bars.v1` and written to Iceberg by the
lakehouse job, then replayed out of Iceberg by `BackfillJob` into a separate
topic. One-second windows.

| | |
|---|---|
| Live bars | 225 |
| Replayed bars | 279 |
| Windows computed by both | **225** |
| Mismatched fields | **0** |

Every window both paths computed agrees on open, high, low, close, volume,
VWAP and trade count. Float fields compare with a relative tolerance of 1e-9,
because VWAP is a sum of products over a sum and the two paths can accumulate
in a different order; ordering may change the last bits of a double and
nothing else. The comparator was checked in both directions — it passes float
noise and replay-only windows, and fails a deliberately altered volume.

The 54 extra replayed bars are expected rather than a discrepancy. The live
job runs against an unbounded source and is cancelled, so its final windows
never close and never emit; the replay is bounded and closes all of them.

This is what BACKFILL.md previously argued structurally — that the two paths
call the same `TradesJob.aggregate`, so no second implementation exists to
drift. The argument was sound and was not a proof: the source and the
watermark strategy do differ, and those decide which record lands in which
window. Now it is measured.

**Scope:** the warehouse is a local Hadoop catalog, not S3. The property under
test is that a bounded replay through the same operators reproduces the same
aggregates; the filesystem behind the catalog is not part of that property.
