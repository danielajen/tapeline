-- The real-time OLAP tier.
--
-- This answers "give me the last N minutes of one-second bars for BTC-USD"
-- in single-digit milliseconds. Iceberg answers the same question over years
-- of history in seconds. Both are correct; they are sized for different
-- questions, and WindowQueryService routes between them by age.

CREATE DATABASE IF NOT EXISTS tapeline;

CREATE TABLE IF NOT EXISTS tapeline.bars
(
    venue           LowCardinality(String),
    symbol          LowCardinality(String),
    window_start_us Int64,
    window_end_us   Int64,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    vwap            Float64,
    trade_count     Int64,
    window_start    DateTime64(6) MATERIALIZED toDateTime64(window_start_us / 1000000.0, 6)
)
ENGINE = MergeTree
-- Monthly parts. Daily would multiply part count for no pruning benefit at
-- this retention; the ORDER BY does the real filtering work.
PARTITION BY toYYYYMM(window_start)
-- Symbol first because every query filters on it, then venue, then time.
-- This ordering is what makes a symbol+range scan read a contiguous run
-- rather than skipping through the whole table.
ORDER BY (symbol, venue, window_start_us)
-- Retention. The lakehouse is the system of record; this tier is a cache
-- with an index, and keeping years here would cost far more than it saves.
TTL toDateTime(window_start) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS tapeline.divergence
(
    symbol          LowCardinality(String),
    venue_a         LowCardinality(String),
    venue_b         LowCardinality(String),
    price_a         Float64,
    price_b         Float64,
    divergence_bps  Float64,
    zscore          Float64,
    window_start_us Int64,
    event_time_us   Int64,
    event_time      DateTime64(6) MATERIALIZED toDateTime64(event_time_us / 1000000.0, 6)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time_us)
TTL toDateTime(event_time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- One-minute rollups, maintained incrementally. A dashboard asking for a day
-- of one-minute bars would otherwise scan 86,400 one-second rows per symbol
-- per venue on every refresh.
CREATE MATERIALIZED VIEW IF NOT EXISTS tapeline.bars_1m
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (symbol, venue, minute)
AS SELECT
    symbol,
    venue,
    toStartOfMinute(window_start) AS minute,
    argMinState(open, window_start_us)  AS open_state,
    maxState(high)                      AS high_state,
    minState(low)                       AS low_state,
    argMaxState(close, window_start_us) AS close_state,
    sumState(volume)                    AS volume_state,
    -- VWAP must be re-derived from notional over volume. Averaging the
    -- per-second VWAPs would weight a second with one trade the same as a
    -- second with a thousand, which is a different and wrong number.
    sumState(vwap * volume)             AS notional_state,
    sumState(trade_count)               AS trade_count_state
FROM tapeline.bars
GROUP BY symbol, venue, minute;
