package io.tapeline.stream.functions

import io.tapeline.stream.model.Events.{Trade, WindowBar}
import io.tapeline.stream.stats.TradeAggregate
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/** Incremental aggregation of trades into OHLCV + VWAP bars.
  *
  * Split into an AggregateFunction plus a ProcessWindowFunction on purpose.
  * The AggregateFunction folds each trade as it arrives, so window state is
  * one small accumulator regardless of how many trades land in the window.
  * The alternative — a bare ProcessWindowFunction — buffers every element
  * until the window fires, which on a busy symbol is the difference between
  * kilobytes and hundreds of megabytes of state per checkpoint.
  *
  * The ProcessWindowFunction runs only at trigger time, purely to attach the
  * window bounds and key, which the AggregateFunction cannot see.
  */
class TradeAggregateFunction
    extends AggregateFunction[Trade, TradeAggregate, TradeAggregate] {

  override def createAccumulator(): TradeAggregate = TradeAggregate.empty

  override def add(value: Trade, acc: TradeAggregate): TradeAggregate = acc.add(value)

  override def getResult(acc: TradeAggregate): TradeAggregate = acc

  // Required for session windows and for parallel pre-aggregation. The
  // associativity that makes this safe is pinned by TradeAggregateSpec.
  override def merge(a: TradeAggregate, b: TradeAggregate): TradeAggregate = a.merge(b)
}

/** Attaches window bounds and the key to a finished aggregate. */
class TradeBarWindowFunction
    extends ProcessWindowFunction[TradeAggregate, WindowBar, String, TimeWindow] {

  override def process(
      key: String,
      context: ProcessWindowFunction[TradeAggregate, WindowBar, String, TimeWindow]#Context,
      elements: java.lang.Iterable[TradeAggregate],
      out: Collector[WindowBar]
  ): Unit = {
    val it = elements.iterator()
    if (!it.hasNext) return

    // With an AggregateFunction upstream this iterable holds exactly one
    // pre-folded accumulator.
    val agg = it.next()
    if (agg.count == 0L) return

    val parts = key.split('|')
    val (venue, symbol) = if (parts.length == 2) (parts(0), parts(1)) else ("unknown", key)

    // Flink windows are in milliseconds; the canonical unit here is
    // microseconds, and mixing the two is the single easiest way to produce
    // bars that are silently off by a factor of a thousand.
    out.collect(
      agg.toBar(
        venue = venue,
        symbol = symbol,
        windowStartUs = context.window.getStart * 1000L,
        windowEndUs = context.window.getEnd * 1000L
      )
    )
  }
}

object TradeWindow {

  /** Bars are per venue and symbol: a VWAP blended across three exchanges
    * would describe no market that anyone can trade against.
    */
  def keyOf(t: Trade): String = s"${t.venue}|${t.symbol}"
}
