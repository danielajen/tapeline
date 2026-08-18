# Roadmap — what is deliberately not built

A list of known gaps is more useful than the absence of one. Each entry says
what is missing, why it was left, and what it would take. Anything here is
fair game in an interview, and having thought about it is the point.

## Correctness and availability

**Ingestion runs a single replica.** Two producers would double-publish every
event. Fixing it properly means either leader election (a lease in Postgres
or the Kubernetes API, with the follower hot but silent) or partitioning
symbols across replicas so each owns a disjoint set. The second is better —
it scales as well as failing over — and is roughly a day of work. It was not
done because one replica handles three symbols comfortably, and building a
distributed producer for that volume would be building the resume rather than
the system. See `DESIGN_DECISIONS.md#d7`.

**Kraken book checksums are not verified.** Kraken v2 publishes a CRC32 over
the top ten levels instead of a sequence number. Verifying it requires the
assembled book, which lives in Flink, so the check belongs in `BookFunction`
rather than in the adapter. Until it exists, Kraken book integrity is
unverified — the adapter marks those messages unsequenced, which is honest
but is not the same as checked.

**Order books are not backfilled.** Only trades are. Reconstructing a book
needs every delta from a known snapshot, so replaying an arbitrary range
requires periodically persisting book snapshots to Iceberg alongside the
deltas. Perhaps two days of work, and the missing half of the Kappa story.

**The backfill writes to the live topic.** A consumer sees replayed and live
bars interleaved, distinguishable only by Kafka timestamp. A `lineage` field
on the record, or a separate topic per replay, would fix it.

## Serving

**The lakehouse query path is not implemented.** `WindowQueryService` throws
for ranges older than the OLAP retention rather than falling through to
Iceberg. That is deliberate: a Trino or Spark scan takes seconds to minutes,
which does not belong behind a synchronous RPC a caller is holding a
connection open for. The right shape is an async job with a result handle,
and shipping the synchronous version would mean an endpoint whose p99 is
measured in minutes.

**gRPC request bodies are not signed.** The interceptor signs method,
timestamp and nonce, but uses the empty-body hash — because gRPC messages
arrive after headers, so signing the payload means buffering it before
authenticating, letting an unauthenticated caller make the server allocate.
Signing the body properly needs a streaming-aware scheme. The tradeoff is
documented at the call site rather than hidden.

**Streaming subscribers are pinned to a replica.** In-memory fan-out means a
rolling deploy drops every stream. Clients must reconnect, which they must
handle anyway, and `StreamQuotes` opens with a snapshot to make reconnecting
cheap. A shared subscription registry would fix it and would add a
distributed system to a place that currently has none.

**No TLS or mTLS between services.** Everything inside the VPC is plaintext.
Fine for a portfolio deployment; not fine for anything real. MSK is
provisioned with `TLS_PLAINTEXT`, so the switch is configuration rather than
code.

## Data and schema

**Venue symbol mappings are hard-coded in Go** while `venue_symbols` in
Postgres holds the same data. The table should be the authority, loaded at
startup with a periodic refresh. Right now the table documents the mapping
and does not enforce it.

**Avro schemas are copied into three modules** and kept honest by a CI diff.
A shared artifact — a published schema jar, or a build step generating all
three from one source — would be better. The diff was chosen because coupling
the Go build to the JVM build for three files costs more than it saves. See
`DESIGN_DECISIONS.md#d9`.

**No dead letter queue.** A record that fails to decode is counted and
dropped. Counting makes it alertable; a DLQ would make it recoverable.

## Operations

**No alerting destination.** Rules exist in `deploy/prometheus/alerts.yml`
and fire into nothing. Alertmanager plus a webhook is an afternoon.

**No automated backfill reconciliation.** The correctness comparison in
`BACKFILL.md` is a query someone runs, not a scheduled job that alerts on
divergence.

**No load shedding.** Under overload the serving tier drops updates to slow
subscribers, which protects memory but not CPU. There is no admission control
that refuses new subscriptions when a replica is saturated.

**Secrets are Kubernetes Secrets**, which is base64, not encryption. External
Secrets Operator with AWS Secrets Manager is the standard fix.

## Deliberately out of scope

These are not gaps; they are decisions.

- **No order management or execution.** This is a market data system. Adding
  execution changes the correctness bar from "aggregates are right" to "money
  is not lost", which is a different project.
- **No machine learning.** The divergence detector is a rolling z-score
  because that is what the problem needs. A model here would be decoration.
- **No frontend.** Grafana is the interface. A React dashboard would
  demonstrate nothing this project is about.
- **No microsecond latency work.** The transport is a public WebSocket over
  the internet, with tens of milliseconds of unavoidable jitter. Optimizing
  microseconds behind that is measuring the wrong thing.
