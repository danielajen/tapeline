# Design decisions

Each entry states the decision, what it cost, and what would have to change
for it to be wrong. A decision without a cost attached is not a decision, it
is a preference — and in an interview the cost is the part that gets asked
about.

---

## D1. Avro for data in motion, Protobuf only at the RPC boundary

**Decision.** Every Kafka topic carries Avro framed in the Confluent wire
format with a schema registry id. The gRPC serving API uses Protobuf.

**Why not Protobuf everywhere.** It was the first design and it was wrong.
Protobuf's compatibility model is field-number-based and is enforced by
convention: nothing stops a producer deploying a message that consumers
cannot read. Avro plus a registry makes compatibility a *server-side gate* —
an incompatible schema is rejected at registration, before a single message
is written. That difference is why both Uber and Netflix use Avro for
data in motion despite using Protobuf elsewhere.

**Why not Avro everywhere.** Protobuf's generated code and its gRPC
integration are better, and the RPC boundary does not need a registry: the
client and server share a `.proto` file at build time.

**Cost.** Two serialization formats in one system, and a conversion at the
boundary in `serving/.../QuoteSnapshot.java`. Contributors must know which
side of the line they are on.

**What would change it.** A single-language system, or one where the RPC and
event schemas are genuinely the same shape. Neither is true here.

---

## D2. One Flink job per topic

**Decision.** `book`, `trades` and `divergence` are separate jobs with
separate consumer groups, deployments and checkpoints.
`MonolithicJob.scala` — the version this replaced — is still in the
repository.

**How this was arrived at.** The monolith was built first and it worked
until the order books grew. The three stages have incompatible operational
profiles: book deltas arrive one to two orders of magnitude more often than
trades and carry per-key state that grows with depth, while a trade window
holds one small accumulator. Sharing one job means sharing one parallelism,
one memory budget, one checkpoint interval and one restart. Checkpoints short
enough for the book state made the job checkpoint constantly; long enough for
the rest made book checkpoints time out. Raising parallelism for book
throughput over-provisioned two stages that did not need it.

Netflix documented reaching the same conclusion for the same reason. That was
reassuring, not causal — the tuning wall came first and the write-up was
found afterwards.

**Cost.** Three deployments instead of one, three consumer groups to monitor,
and an extra Kafka hop between the book and divergence stages. Latency is
strictly worse than the fused version.

**What would change it.** If book and trade volumes were comparable and state
sizes similar, the operational overhead would buy nothing.

---

## D3. Scala against Flink's Java API

**Decision.** The stream tier is Scala, using `flink-streaming-java`.

**Why.** Flink's own Scala DataStream API was deprecated in 1.18 and removed
in 2.0. Writing Scala against it today would be building on a dead interface.
The Java API from Scala is the supported path.

**Cost.** Explicit `TypeInformation` at every operator boundary, because
Flink's type extraction cannot see through Scala case classes and falls back
to Kryo. Kryo serialization of case classes is measurably slower than a
purpose-built serializer, and it is the first thing to fix if the stream tier
becomes CPU-bound.

**What would change it.** Writing the tier in Java, which would get POJO
serializers for free and lose the pattern matching and immutability that make
`OrderBook` and `Divergence` pleasant to test.

---

## D4. Redis is a cache, not the analytical store

**Decision.** Redis holds the latest quote per (symbol, venue). Range and
window queries go to ClickHouse for recent data and Iceberg for history.
`WindowQueryService` routes between them by age and reports which tier
answered.

**Why.** The naive design serves everything from Redis. It works until
someone asks for a day of one-second bars, at which point the answer is
either a scan of thousands of keys or a giant serialized blob per symbol.
Uber's actual pattern is Kafka → Flink → Pinot → Presto: a hot key-value path
and an analytical path, not one store doing both.

**Cost.** Three stores to operate rather than one, and a routing rule that
can be wrong. The `served_from` field on the response exists so a regression
where queries silently fall through to the slow tier is visible in metrics
rather than only in latency.

**What would change it.** If the only query were "latest price", Redis alone
would be correct and the OLAP tier would be over-engineering.

---

## D5. Exactly-once, and what it actually costs

**Decision.** Kafka transactions plus Flink checkpointing, with
`DeliveryGuarantee.EXACTLY_ONCE` on every sink and `read_committed` on every
consumer.

**What it costs, stated plainly.**

1. **Latency becomes a function of the checkpoint interval.** Records are
   written inside a transaction that commits only when the checkpoint that
   produced them completes. With a 30-second interval, a downstream
   `read_committed` consumer sees output in 30-second steps, not
   continuously. The serving tier's freshness is bounded by this, not by
   processing time.
2. **Every consumer must set `read_committed`.** A consumer that does not
   reads aborted records, and the guarantee buys nothing while still costing
   the latency. This is configured in `ServingConfig.java` and is the single
   easiest thing to get wrong.
3. **Transactional id prefixes must be unique per job.** Two jobs sharing one
   fence each other's transactions, and the symptom is a job that silently
   stops committing rather than any error naming the collision.

**What would change it.** At-least-once plus idempotent downstream writes is
a legitimate alternative and often the right one. It is not right here
because the aggregates are sums: a duplicated trade changes a VWAP, and
unlike a duplicated row it cannot be detected after the fact.

---

## D6. On-chain reorgs drop the log rather than reversing it

**Decision.** `EVM.Decode` drops logs marked `removed: true` instead of
emitting a compensating event.

**Why.** For an accounting system this would be wrong — a reversal must be
recorded. For a market data feed, on-chain flow is a *signal*, not a ledger,
and a reorged-out transfer is a transfer that never happened. Emitting a
negative event would require every downstream aggregate to handle
retractions, for a correction that arrives seconds later and moves a
volume statistic by a rounding error.

**Cost.** Aggregates over on-chain volume are very slightly wrong during a
reorg window and self-correct as the canonical chain advances. The system
cannot be used for settlement or accounting without changing this.

---

## D7. Ingestion runs exactly one replica

**Decision.** `deploy/k8s/10-ingestd.yaml` sets `replicas: 1` with a
`Recreate` strategy.

**Why.** Two replicas would each connect to every venue and publish every
event twice, and Kafka would faithfully store both copies. Deduplicating
downstream is possible but costs keyed state in Flink for a problem that not
running two producers avoids entirely.

**Cost, stated rather than hidden.** Ingestion is a single point of failure
with a restart-shaped recovery — a pod restart is a gap in the data, and the
gap detector will report it on the next connect. This is the weakest part of
the architecture.

**What would change it.** Leader election, or partitioning symbols across
replicas so each owns a disjoint set. Both are in `docs/ROADMAP.md`. Neither
was built because a single replica genuinely handles the current volume, and
building a distributed producer to publish three symbols would be resume-driven
development.

---

## D8. Pure domain logic, thin operators

**Decision.** In every tier, the interesting logic is in code with no
framework types in its signature, and the framework class is a thin wrapper:

| Tier   | Pure                                          | Wrapper                |
|--------|-----------------------------------------------|------------------------|
| Go     | `venue.Decoder` implementations, `gap.Detector` | `venue.Runner`         |
| Scala  | `OrderBook`, `TradeAggregate`, `Divergence`     | `BookFunction`, jobs   |
| Java   | `SignedRequest`, `HmacSigner`                   | `AuthInterceptor`      |

**Why.** It is what makes the test suites possible at all. Order book
correctness, VWAP weighting, divergence thresholds, HMAC canonicalization and
sequence-gap semantics are all tested without a cluster, a broker, a registry
or a container. The suites run in about three seconds combined.

**Cost.** More types and one more indirection than a direct implementation.
`BookSnapshot` exists purely so `OrderBook` can stay a `TreeMap`-based value
while Flink state holds something with no `Ordering` in it.

---

## D9. Owning the Confluent wire format in two languages

**Decision.** Both `ingest/internal/encode` and
`serving/.../ConfluentAvroReader.java` implement the 5-byte framing directly
rather than depending on a vendor serializer.

**Why.** It is forty lines each, it is the interoperability contract between
three languages in this repository, and the Java vendor serializer requires a
non-Maven-Central repository for one class. Owning it also makes the reader
schema explicit at the call site, so schema resolution is visible rather than
implied.

**Cost.** Two implementations of the same five bytes, in two languages, that
must agree. They are pinned by tests on both sides (`avro_test.go`,
`ConfluentAvroReaderTest.java`), and the shared schema files are diffed in CI.

**What would change it.** Needing the parts of the vendor library this does
not reimplement — logical types, or the subject-name strategies other than
`TopicNameStrategy`.
