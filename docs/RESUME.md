# Resume material

The bullets from the v2 gap-closing spec, updated to match what the
repository actually contains. **Placeholders in `[measure]` are numbers you
have not measured yet.** Do not fill them with estimates — `MEASUREMENTS.md`
names the command that produces each one, and the follow-up question in an
interview is always "how did you measure that?"

---

## The project block (replaces Jobify)

> **Tapeline** — Distributed real-time market data platform ·
> *Go, Scala, Java, Flink, Kafka, Avro, gRPC, Kubernetes, Terraform, Iceberg*
>
> - Built a **fault-tolerant ingestion service in Go** fanning in WebSocket
>   feeds from 3 crypto exchanges and on-chain Ethereum logs, normalizing
>   heterogeneous payloads into **Avro** schemas managed through a schema
>   registry and publishing to **Kafka** at `[measure]` msg/sec, with
>   per-stream sequence-gap detection, backward-compatible schema evolution,
>   and exponential-backoff reconnect
> - Engineered **stateful Flink stream processors (Scala)** maintaining live
>   L2 order books across `[measure]` trading pairs with **exactly-once
>   semantics** via Kafka transactions and checkpointing; refactored from a
>   monolithic job to per-topic jobs after checkpoint tuning became
>   untenable, cutting checkpoint duration `[measure]`
> - Designed a **gRPC server-streaming API (Java/Spring Boot)** serving live
>   quotes at **`[measure]`ms p99 under `[measure]` req/sec**, backed by a
>   Redis hot cache and a ClickHouse real-time OLAP layer, secured with
>   HMAC-signed keys, nonce replay protection and Redis token-bucket rate
>   limiting
> - Implemented a **Kappa-style backfill path** replaying `[measure]`M+
>   events/day from **Apache Iceberg on S3** through the same processing
>   operators, reconstructing windowed aggregates bit-identically after
>   simulated state loss
> - Deployed on **Kubernetes via Terraform-provisioned AWS**, with
>   Prometheus/Grafana and OpenTelemetry tracing; **chaos-tested broker and
>   pod failure with `[measure]` events lost and `[measure]`s recovery**;
>   **76% statement coverage** in Go plus 105 JVM tests, run under the race
>   detector in GitHub Actions CI

If four bullets is the limit, merge the last two.

### What is safe to claim today

| Claim | Status |
|---|---|
| Go ingestion, Avro, registry, gap detection | **Built and tested** |
| Schema evolution under FULL compatibility | **Built, tested both directions** |
| Flink stateful order books, exactly-once wiring | **Built, domain logic tested** |
| Monolith-to-per-topic refactor | **Both versions in the repo; the comparison is unmeasured** |
| Kappa backfill through shared operators | **Built; the correctness comparison is unrun** |
| gRPC + REST serving, HMAC, rate limiting | **Built and tested** |
| Terraform, Kubernetes, CI | **Written and validated; not applied to a live account** |
| Throughput, latency and recovery numbers | **Not measured** |

Every `[measure]` is a row in that last line. Run the load and chaos suites
before this block goes on a resume.

### One claim to be careful with

"Exactly-once" invites a hard question, and the good answer is the cost, not
the mechanism: latency becomes a function of the checkpoint interval, every
consumer must set `read_committed`, and transactional id prefixes must be
unique per job or the jobs fence each other. That answer is in
`DESIGN_DECISIONS.md#d5` and is worth being able to give from memory.

---

## Skills section

**Cut:** HTML, CSS, JavaScript, C#, Firebase, Flask, Vue.js, standalone JUnit.

> **Languages:** Java, Scala, Go, Python, TypeScript, SQL
>
> **Distributed Systems & Data:** Kafka, Flink, Spark, Avro, Protobuf, gRPC,
> Apache Iceberg, ClickHouse, Azure Synapse, Azure Data Lake Storage
>
> **Infrastructure:** Kubernetes, Docker, Terraform, AWS (S3, EKS, MSK, IAM),
> Azure, GitHub Actions
>
> **Storage & Observability:** PostgreSQL, Redis, Prometheus, Grafana,
> OpenTelemetry
>
> **Testing:** JUnit, Testcontainers, k6, chaos engineering

Six languages, every one load-bearing in this repository. Four category lines
instead of two, because scannability is the whole job of this section.

**Header addition:** `Canadian citizen — TN visa eligible for US roles.`

Note the honest substitution: the spec named **Apache Pinot**; this
implementation uses **ClickHouse**, for the reason given in
`DESIGN_DECISIONS.md#d4` (one container versus five, same JDBC interface,
same architectural point). List what you built. If a Pinot-specific role
comes up, the swap is a URL and the reasoning is already written down.

---

## Interview material this repository actually supports

Ranked by how well the code backs the story.

**1. The monolith-to-per-topic refactor.** `MonolithicJob.scala` is still in
the repository with the reasoning in its class comment, and both versions
run. "I made the same architectural mistake Netflix documented, hit the same
checkpoint tuning wall, and here is the tradeoff I chose" is a
senior-engineer answer — but only once the before-and-after numbers in
`MEASUREMENTS.md` are filled in. Do that first.

**2. The postmortem.** `POSTMORTEM.md` documents a real silent data
corruption bug — every Binance trade side inverted by Go's case-insensitive
JSON fallback — with timeline, root cause, contributing factors and
corrective actions. It is real, it is reproducible with one command, and
almost no new grad has one. Its most quotable line is the contributing
factor: the same bug class had already been fixed elsewhere in the same file
two hours earlier, and fixing the instance rather than the class cost those
two hours.

**3. Schema evolution.** `SCHEMA_EVOLUTION.md` plus a test that proves both
directions plus a test that proves an incompatible change is *rejected*. That
last one matters: a compatibility check that has never rejected anything is a
check nobody has verified is switched on.

**4. Exactly-once and its costs.** See above.

**5. Pure logic, thin operators.** The same split in three languages — Go
decoders versus `Runner`, Scala `OrderBook` versus `BookFunction`, Java
`SignedRequest` versus `AuthInterceptor` — and it is why 168 tests run in
about three seconds with no cluster, broker or container.

---

## Other resume changes from the spec

**Microsoft, bullet 2:**

> Designed and operated a **fault-tolerant distributed ingestion service**
> unifying 5 analytics clusters, cutting end-to-end latency from 3+ weeks to
> <24 hours while computing 20 KPIs daily with zero manual intervention or
> on-call escalation

**Ministry of the Attorney General** — compress to one line, drop "12GB+"
(it reads *down* after "34 trillion events"):

> Engineered a rules-based anomaly detection system with **ETL pipelines**
> validating legal case data, and automated internal approval workflows with
> **event-driven architecture**, eliminating 7 hours of manual processing
> weekly

**321DataPro** — keep the NFC revenue bullet; lead the second with
architecture rather than hardware:

> Built an **event-driven pipeline** processing IoT match telemetry through
> Node.js on AWS Lambda, streaming real-time leaderboard updates to clients
> over **WebSockets**

---

## The thing that beats the project on ROI

From the research, and it has not changed: **referrals**. There is already
one at Point72. Getting one at Databricks or Snowflake before the fall wave
moves the needle more than any technology added here. Build this *and* fix
the funnel — the project is not the binding constraint, volume is.
