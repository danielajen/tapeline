package io.tapeline.stream.stats

import io.tapeline.stream.model.Events.{Trade, WindowBar}

/** Windowed trade statistics, expressed as a fold.
  *
  * Shaped as an accumulator with `add` and `merge` so a single definition
  * serves both the streaming path (Flink's AggregateFunction) and the
  * backfill path (a plain fold over records replayed from Iceberg). One
  * definition means the replay-correctness proof in docs/BACKFILL.md is
  * actually proving something: both paths run this code, not two
  * implementations that agree today.
  */
final case class TradeAggregate(
    open: Double,
    high: Double,
    low: Double,
    close: Double,
    volume: Double,
    notional: Double,
    count: Long,
    firstEventTimeUs: Long,
    lastEventTimeUs: Long
) {

  def add(t: Trade): TradeAggregate =
    if (count == 0L) {
      TradeAggregate(
        open = t.price, high = t.price, low = t.price, close = t.price,
        volume = t.size, notional = t.notional, count = 1L,
        firstEventTimeUs = t.eventTimeUs, lastEventTimeUs = t.eventTimeUs
      )
    } else {
      TradeAggregate(
        // Open and close follow event time, not arrival order. Within a
        // window Flink makes no ordering promise, so taking the first and
        // last *arrival* would make the bar depend on network timing.
        open = if (t.eventTimeUs < firstEventTimeUs) t.price else open,
        high = math.max(high, t.price),
        low = math.min(low, t.price),
        close = if (t.eventTimeUs >= lastEventTimeUs) t.price else close,
        volume = volume + t.size,
        notional = notional + t.notional,
        count = count + 1L,
        firstEventTimeUs = math.min(firstEventTimeUs, t.eventTimeUs),
        lastEventTimeUs = math.max(lastEventTimeUs, t.eventTimeUs)
      )
    }

  /** Combines two partial aggregates. Required for session merging and for
    * parallel pre-aggregation, and it must be associative and commutative —
    * the property the backfill proof leans on.
    */
  def merge(other: TradeAggregate): TradeAggregate =
    if (count == 0L) other
    else if (other.count == 0L) this
    else
      TradeAggregate(
        open = if (other.firstEventTimeUs < firstEventTimeUs) other.open else open,
        high = math.max(high, other.high),
        low = math.min(low, other.low),
        close = if (other.lastEventTimeUs > lastEventTimeUs) other.close else close,
        volume = volume + other.volume,
        notional = notional + other.notional,
        count = count + other.count,
        firstEventTimeUs = math.min(firstEventTimeUs, other.firstEventTimeUs),
        lastEventTimeUs = math.max(lastEventTimeUs, other.lastEventTimeUs)
      )

  /** Volume-weighted average price. NaN on zero volume, which is honest —
    * a VWAP over no volume does not exist, and emitting 0.0 would silently
    * drag any downstream average toward zero.
    */
  def vwap: Double = if (volume <= 0) Double.NaN else notional / volume

  def toBar(venue: String, symbol: String, windowStartUs: Long, windowEndUs: Long): WindowBar =
    WindowBar(
      venue = venue,
      symbol = symbol,
      windowStartUs = windowStartUs,
      windowEndUs = windowEndUs,
      open = open, high = high, low = low, close = close,
      volume = volume, vwap = vwap, tradeCount = count
    )
}

object TradeAggregate {
  val empty: TradeAggregate = TradeAggregate(
    open = 0.0, high = Double.MinValue, low = Double.MaxValue, close = 0.0,
    volume = 0.0, notional = 0.0, count = 0L,
    firstEventTimeUs = Long.MaxValue, lastEventTimeUs = Long.MinValue
  )

  /** Folds a batch of trades. This is the entry point the backfill path and
    * the tests use.
    */
  def fold(trades: Iterable[Trade]): TradeAggregate =
    trades.foldLeft(empty)(_ add _)
}
