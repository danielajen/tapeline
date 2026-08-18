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

## Verified today

These are the only numbers in this repository that are actually measured, and
they come from the test suites rather than from a running system.

| Metric | Value | How |
|---|---|---|
| Go statement coverage | **76.1%** | `make cover` |
| Go tests | **63 passing, race detector clean** | `go test -race ./...` |
| Scala tests | **54 passing** | `mvn test` in `stream/` |
| Java tests | **51 passing** | `mvn test` in `serving/` |
| Stream jar size | **30 MB shaded** | `mvn package` in `stream/` |

---

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
