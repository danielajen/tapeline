# Tapeline

A distributed real-time market data platform. It ingests live order-book and
trade feeds from three crypto exchanges plus on-chain Ethereum logs,
processes them as one exactly-once stream, detects cross-exchange price
divergence, and serves both live gRPC streams and historical queries from a
lakehouse.

Five tiers, three languages, one wire format:

```
Coinbase Advanced Trade ─┐
Kraken WebSocket v2 ─────┼─► Go ingestion ─► Kafka (Avro + Schema Registry)
Binance combined stream ─┤    · reconnect + backoff      │ partitioned by symbol
Ethereum / Base logs ────┘    · sequence-gap detection   │
                              · schema normalization     │
                                                         ▼
                         Flink (Scala) — one job per topic
                          · stateful L2 order books
                          · windowed VWAP / OHLC / imbalance
                          · cross-exchange divergence, rolling z-score
                          · exactly-once via Kafka txns + checkpointing
                                   │                        │
                     ┌─────────────┘                        └──────────┐
                     ▼                                                 ▼
              ClickHouse                                       Iceberg on S3
           (real-time OLAP)                                 (history + replay)
                     │                                                 │
                     ▼                                                 │
       Java / Spring Boot serving tier                                 │
        · gRPC server-streaming live quotes                            │
        · REST gateway · Redis hot cache · Postgres metadata           │
        · HMAC-signed keys, nonce replay protection, token buckets     │
                                                                       │
                     └──── Kappa backfill: the same operators ◄────────┘

Infra: Kubernetes (EKS) · Terraform · GitHub Actions CI
Observability: Prometheus · Grafana · OpenTelemetry
Testing: go test -race · ScalaTest · JUnit · k6 load · chaos experiments
```

---

## Why this is shaped the way it is

The interesting parts of this project are not the technology list. They are
five decisions, each of which cost something:

**One Flink job per topic, not one job for everything.** The monolithic
version is still in the repository — [`MonolithicJob.scala`](stream/src/main/scala/io/tapeline/stream/jobs/MonolithicJob.scala) —
because it is the version this replaced, and the reason it was replaced is
the most useful thing here. Book deltas arrive orders of magnitude more often
than trades and carry state that grows with depth; sharing one job means
sharing one checkpoint interval, one parallelism and one restart. Checkpoints
short enough for the books made the job checkpoint constantly; long enough
for the rest made book checkpoints time out. The split costs an extra Kafka
hop and three deployments, and buys independent tuning, scaling and failure.
→ [`DESIGN_DECISIONS.md#d2`](docs/DESIGN_DECISIONS.md)

**Avro on the wire, Protobuf at the RPC boundary.** Protobuf's compatibility
model is enforced by convention; a registry enforces it server-side, so an
incompatible schema is rejected before a single message is written. The cost
is two serialization formats and a conversion at the boundary.
→ [`DESIGN_DECISIONS.md#d1`](docs/DESIGN_DECISIONS.md)

**Exactly-once, and its actual price.** Kafka transactions plus checkpointing
means end-to-end latency becomes a function of the checkpoint interval, every
consumer must set `read_committed` or the guarantee buys nothing, and two
jobs sharing a transactional id prefix will silently fence each other.
→ [`DESIGN_DECISIONS.md#d5`](docs/DESIGN_DECISIONS.md)

**Pure logic, thin framework wrappers**, in all three languages. Go decoders
are `bytes → events` with no sockets; `OrderBook` and `Divergence` are values
with no Flink types; `SignedRequest` and `HmacSigner` have no servlet in
sight. This is why 191 tests run in about four seconds with no cluster, no
broker and no container — and why the bug in the postmortem was catchable at
all. → [`DESIGN_DECISIONS.md#d8`](docs/DESIGN_DECISIONS.md)

**Ingestion runs exactly one replica, and that is the weakest part.** Two
producers would double-publish everything. The honest consequence is a single
point of failure with restart-shaped recovery. Leader election is in the
roadmap; pretending a `replicas: 1` Deployment is horizontally scalable is
not. → [`DESIGN_DECISIONS.md#d7`](docs/DESIGN_DECISIONS.md)

---

## Quick start

```bash
make up            # Kafka, registry, Redis, Postgres, ClickHouse, MinIO, Flink, Grafana
make submit-jobs   # submit the book, trades and divergence jobs
make smoke         # walk one event from venue socket to served quote
```

| | |
|---|---|
| Grafana | <http://localhost:3000> (anonymous viewer) |
| Prometheus | <http://localhost:9091> |
| Flink UI | <http://localhost:8082> |
| REST | <http://localhost:8080> |
| gRPC | `localhost:9090` (reflection on) |

No broker needed to watch normalization work against live venues:

```bash
make dry-run       # real WebSocket feeds, in-memory sink
```

`make help` lists everything else.

---

## Repository layout

```
ingest/     Go     WebSocket fan-in, normalization, gap detection, Avro → Kafka
stream/     Scala  Flink jobs: order books, windows, divergence, Kappa backfill
serving/    Java   gRPC + REST, Redis cache, HMAC auth, OLAP queries
proto/             The gRPC contract, shared by the serving tier and clients
deploy/            docker-compose, Terraform (VPC/EKS/MSK/S3), K8s, Prometheus, Grafana
loadtest/          k6 scripts for the REST and gRPC paths
chaos/             Failure experiments, each with a hypothesis stated up front
docs/              Design decisions, measurements, backfill proof, postmortem
```

---

## Testing

```bash
make check      # gofmt, vet, all three suites, schema drift
make cover      # Go coverage report
make load       # k6 against REST
make chaos      # kill the broker, a TaskManager, Redis, the network
```

| Suite | Tests | Notes |
|---|---|---|
| Go | 76 | Race detector clean, **76%** statement coverage |
| Scala | 54 | Order books, rolling stats, aggregates, divergence, codecs |
| Java | 51 unit + 10 integration | HMAC, replay, rate limiting, fan-out, Avro framing; Testcontainers against real Kafka + Postgres |

Three tests worth pointing at specifically:

- **`TestSchemaEvolutionBothDirections`** — a v1 consumer reads v2 data and a
  v2 consumer reads v1 data with defaults applied. This is the test behind
  the schema-evolution claim.
- **`TestIncompatibleChangeIsRejected`** — a new required field with no
  default is *refused*. A compatibility check that has never rejected
  anything is one nobody has verified is switched on.
- **`aBadSignatureConsumesNeitherNonceNorTokens`** — an unauthenticated
  caller cannot burn a real tenant's nonce values or rate-limit budget. The
  ordering inside `RequestAuthenticator` is the security property, and this
  pins it.

---

## Measurements

Measured on 17 August 2026 against live exchange feeds:

| | |
|---|---|
| Events published, 120 s run | **48,036** (400/sec, 3 venues, 3 symbols) |
| Decode errors / publish errors | **0 / 0** |
| Source lag, continuous book streams | **29–54 ms** mean |
| Kafka broker killed mid-stream | **0 events lost, 20 s to resume** |
| Serving tier under k6 | **1,751 req/sec**, 350 µs median, 0.05% errors |

**Flink, the serving tier and the backfill proof are still unmeasured**, and
[`MEASUREMENTS.md`](docs/MEASUREMENTS.md) says so explicitly rather than
estimating. The first live run also found five real bugs —
including a silent 59% data loss — written up in
[`POSTMORTEM.md`](docs/POSTMORTEM.md).

---

## Documentation

| | |
|---|---|
| [Design decisions](docs/DESIGN_DECISIONS.md) | Nine decisions, each with its cost and what would change it |
| [Measurements](docs/MEASUREMENTS.md) | Every number, and the command that produces it |
| [Backfill](docs/BACKFILL.md) | How replay reconstructs aggregates exactly, and the proof query |
| [Schema evolution](docs/SCHEMA_EVOLUTION.md) | FULL compatibility, both directions, and what breaks |
| [Postmortem](docs/POSTMORTEM.md) | A real silent-corruption bug, with root cause and corrective actions |
| [API](docs/API.md) | HMAC signing, rate limits, gRPC and REST surfaces |
| [Roadmap](docs/ROADMAP.md) | What is deliberately not built, and why |

---

## Honest scope

At three symbols across three venues this handles thousands of messages per
second — not the trillions per day the systems it is modelled on handle. The
architecture is the same shape; the scale is not.

What the project legitimately demonstrates is engineering practice: the
failure modes were found by breaking things on purpose, the costs of each
design decision are written down, the unmeasured numbers are marked as
unmeasured, and the one real data-corruption bug found during development has
a postmortem rather than a silent fix.

Everything the project does *not* do is in [`ROADMAP.md`](docs/ROADMAP.md).
