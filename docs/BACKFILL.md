# Kappa backfill: replaying history through the live code path

## The problem this solves

Kafka retention is finite — seven days here, configured in `msk.tf`. Three
situations need events older than that:

1. **A window definition changes.** A new aggregate has to be computed over
   history, not just going forward.
2. **A bug is found in an aggregate.** Everything it produced is wrong and has
   to be recomputed.
3. **State is lost.** A savepoint is corrupted, or a job is restarted without
   one.

In each case the events exist — the streaming path wrote them to Iceberg —
but the log no longer holds them.

## The design

```
                 ┌────────────────────────────────────────┐
   Kafka  ──────►│  TradesJob.decode                      │
                 │        │                               │
                 │        ▼                               │
                 │  TradesJob.aggregate  ◄──── the shared │──► md.bars.v1
                 │   (window + VWAP + OHLC)      operators│
                 │        ▲                               │
   Iceberg ─────►│  BackfillJob rows → Trade              │
                 └────────────────────────────────────────┘
```

`BackfillJob` and `TradesJob` both call `TradesJob.aggregate`. Not a copy of
it — the same method. There is no second implementation of the windowing, the
VWAP weighting or the OHLC selection that could drift out of agreement,
because there is no second implementation at all.

That is what makes the claim checkable rather than rhetorical. In
`stream/src/main/scala/io/tapeline/stream/jobs/BackfillJob.scala`:

```scala
// The same operators as the live job. This call is the whole claim.
TradesJob.aggregate(trades, cfg)
```

## What legitimately differs, and why

Only two things, and both are forced:

**The source is bounded.** Iceberg returns a finite scan; Kafka does not. The
job terminates rather than running forever.

**Out-of-orderness tolerance is 24 hours instead of 5 seconds.** A Parquet
scan returns records in *file* order, not event order, and files are read in
parallel. A live watermark would classify most of the input as late and drop
it — producing an empty result that looks like a successful run, which is the
worst possible failure mode.

Everything else — the filter, the key selector, the window assigner, the
aggregate function, the sink — is shared.

There is a third thing that would have differed if it had not been designed
around: `DivergenceFunction` uses **event time**, not wall clock, for its
staleness check. Under replay those are months apart. Wall clock would mark
every replayed quote stale and silently emit nothing. The test
`event time drives freshness, so backfill is not filtered out wholesale`
in `DivergenceSpec.scala` pins that behaviour.

## Running it

```bash
# Write a day of trades to the lakehouse first (this runs continuously).
flink run -d -c io.tapeline.stream.TapelineJob \
  stream/target/tapeline-stream-0.1.0.jar lakehouse

# Then replay a range. Epoch microseconds — the canonical unit everywhere.
make backfill START=1755302400000000 END=1755388800000000
```

## The correctness proof

Reconstruction is not verified by inspection. The procedure:

**1. Capture the streaming output for a window.**

```sql
CREATE TABLE bars_streaming AS
SELECT venue, symbol, window_start_us, open, high, low, close, volume, vwap, trade_count
FROM bars
WHERE window_start_us >= :start AND window_start_us < :end;
```

**2. Delete the state and replay.**

```bash
flink cancel <trades-job-id>          # no savepoint: the state is gone
make backfill START=:start END=:end
```

**3. Compare every field of every bar.**

```sql
SELECT
    s.symbol, s.venue, s.window_start_us,
    s.vwap  AS streamed_vwap,  b.vwap  AS backfilled_vwap,
    s.close AS streamed_close, b.close AS backfilled_close,
    s.trade_count AS streamed_count, b.trade_count AS backfilled_count
FROM bars_streaming s
FULL OUTER JOIN bars b
  ON  s.symbol = b.symbol
  AND s.venue  = b.venue
  AND s.window_start_us = b.window_start_us
WHERE s.symbol IS NULL                    -- a bar the backfill invented
   OR b.symbol IS NULL                    -- a bar the backfill missed
   OR abs(s.vwap - b.vwap) > 1e-9
   OR s.close <> b.close
   OR s.trade_count <> b.trade_count;
```

**An empty result set is the proof.** Any row is a discrepancy that has to be
explained before the claim is made.

## Why the aggregates reconstruct exactly

The property that makes this work is that `TradeAggregate` is a
**commutative, associative fold with an identity**. `TradeAggregateSpec` pins
all three:

```scala
a.merge(b) shouldBe b.merge(a)                    // commutative
a.merge(b).merge(c) shouldBe a.merge(b.merge(c))  // associative
a.merge(TradeAggregate.empty) shouldBe a          // identity
```

Because of those properties, the *order* records arrive in cannot change the
result — only which records land in which window can, and that is determined
by event time, which is carried in the record itself.

The detail that makes this true rather than nearly true: `open` and `close`
are selected by **event time**, not by arrival order.

```scala
open  = if (t.eventTimeUs < firstEventTimeUs) t.price else open,
close = if (t.eventTimeUs >= lastEventTimeUs) t.price else close,
```

Taking the first and last *arrival* would make the bar depend on network
timing, and a replay would produce different — not wrong-looking, just
different — opens and closes. That single choice is what the entire proof
rests on, and it is covered by
`open and close follow event time, not arrival order`.

## Why partitioning matters here

The Iceberg table is partitioned by `symbol`, with file-level statistics on
`event_time_us`. A one-day replay therefore reads roughly one day of files
rather than scanning the table.

Verify it rather than assuming it: the ratio of files scanned to files in the
table appears in the Flink source metrics, and the row for it in
`MEASUREMENTS.md` exists so pruning that has quietly stopped working shows up
as a number instead of as an unexplained slow job.

Day granularity, not hourly: three symbols across three venues at hourly
granularity produce tens of thousands of small files a month, and small files
are the standard way an Iceberg table becomes slow.

## What this does not do

- **Books are not backfilled, only trades.** Reconstructing an order book
  requires every delta from a known snapshot forward, so replaying an
  arbitrary range is not meaningful without also persisting periodic
  snapshots. That is in `ROADMAP.md`.
- **The backfill writes to the same topic as the live job.** A downstream
  consumer sees both, distinguishable only by the Kafka timestamp. A separate
  topic per replay, or a lineage field on the record, would be better.
- **There is no automatic reconciliation.** The comparison above is a query
  someone runs, not a job that runs on a schedule and alerts.
