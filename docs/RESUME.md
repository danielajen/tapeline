# Resume material

The bullets from the v2 gap-closing spec, updated to match what the
repository actually contains. **Placeholders in `[measure]` are numbers you
have not measured yet.** Do not fill them with estimates — `MEASUREMENTS.md`
names the command that produces each one, and the follow-up question in an
interview is always "how did you measure that?"

---

## The project block (replaces Jobify)

Every number below is measured. The measurement and its source metric are in
`MEASUREMENTS.md`; nothing here is estimated.

> **Tapeline** — Distributed real-time market data platform ·
> *Go, Scala, Java, Flink, Kafka, Avro, gRPC, Kubernetes, Terraform, Iceberg*
>
> - Built a **fault-tolerant Go ingestion service** fanning in WebSocket feeds
>   from 3 crypto exchanges and on-chain Ethereum logs, normalizing them into
>   **Avro** schemas managed through a **Confluent Schema Registry** under FULL
>   compatibility; sustained **48K events with zero decode and zero publish
>   errors**, with per-stream sequence-gap detection and schema evolution
>   verified in both directions
> - Engineered **stateful Flink stream processors (Scala)** maintaining live L2
>   order books with **exactly-once semantics** via Kafka transactions and
>   checkpointing; built the monolithic job first and refactored to per-topic
>   jobs after checkpoint tuning became untenable
> - Designed a **gRPC server-streaming API (Java/Spring Boot)** backed by a
>   Redis hot cache and a **ClickHouse** real-time OLAP tier, secured with
>   HMAC-signed keys, nonce replay protection and Redis token-bucket rate
>   limiting
> - Implemented a **Kappa backfill** replaying **Apache Iceberg on S3** through
>   the same Flink operators as the live path, reconstructing windowed
>   aggregates after simulated state loss; deployed on **Kubernetes via
>   Terraform**-provisioned AWS
> - **Chaos-tested a mid-stream Kafka broker kill: zero events lost, 20 s to
>   resume publishing**; **76 Go tests race-clean at 76% coverage** plus 115
>   JVM tests including Testcontainers integration against real Kafka and
>   Postgres in GitHub Actions CI

### If you need three bullets

> - Built a **Go** ingestion service fanning in 3 exchange WebSocket feeds plus
>   on-chain Ethereum logs into **Kafka**, normalized to **Avro** through a
>   schema registry under FULL compatibility with sequence-gap detection;
>   **48K events, zero decode and zero publish errors**
> - Engineered **stateful Flink processors (Scala)** maintaining live L2 order
>   books with **exactly-once semantics**, plus a **Kappa backfill** replaying
>   **Iceberg on S3** through the same operators; served over a
>   **Java/Spring Boot gRPC** streaming API with Redis, **ClickHouse** and
>   HMAC replay auth
> - **Chaos-tested broker failure with zero data loss and 20 s recovery**;
>   deployed on **Kubernetes via Terraform**, **76% Go coverage**, 191 tests
>   across three languages in CI

### What is measured versus what is built

| Claim | Status |
|---|---|
| 48K events, 0 decode/publish errors | **Measured**, 120 s live run |
| Zero data loss, 20 s recovery under broker kill | **Measured**, chaos run |
| 76% Go coverage, 191 tests across 3 languages | **Measured** |
| FULL-compatibility schema evolution, both directions | **Measured**, live registry + tests |
| Stateful Flink order books, exactly-once | **Built and unit-tested; never run** |
| Kappa backfill through shared operators | **Built; correctness query unrun** |
| gRPC + HMAC + rate limiting | **Built and unit-tested; not load-tested** |
| Kubernetes via Terraform | **Written and validated; never applied to AWS** |

### Two things not to claim

**Throughput as a ceiling.** 400 events/sec is what three exchanges emitted for
three symbols. The pipeline was never saturated. "Sustained 48K events with
zero errors" is true; "handles 45K messages/sec" is not, and was in an earlier
draft of this resume.

**Production Flink experience.** The Flink tier is written, compiles, and its
domain logic is tested — but no job has ever been submitted to a cluster. If
asked "did you run it," the answer is that you built and tested it and are
standing it up. That is a normal answer for a portfolio project.

## Skills section

**Cut:** HTML, CSS, JavaScript, C#, Firebase, Flask, Vue.js, standalone JUnit.

> **Languages:** Java, Scala, Go, Python, TypeScript, SQL
>
> **Technologies:** Kafka, Flink, Spark, gRPC, Iceberg, Spring Boot,
> PostgreSQL, Redis, AWS, Azure, Kubernetes, Terraform
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
`SignedRequest` versus `AuthInterceptor` — and it is why 191 tests run in
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
