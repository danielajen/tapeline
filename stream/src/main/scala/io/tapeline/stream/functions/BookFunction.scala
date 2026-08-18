package io.tapeline.stream.functions

import io.tapeline.stream.book.{BookSnapshot, OrderBook}
import io.tapeline.stream.model.Events.{BookDelta, Quote}
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/** Maintains one L2 order book per (venue, symbol) and emits quotes.
  *
  * This is the only stateful operator in the streaming path and it is
  * deliberately thin: the book logic lives in [[OrderBook]], which is pure
  * and covered by OrderBookSpec. What this class owns is the parts that only
  * make sense inside Flink — keyed state, the emit timer, and the metrics.
  *
  * Quotes are emitted on a timer rather than on every delta. A busy symbol
  * produces thousands of book updates a second and the top of book usually
  * does not move; emitting per delta would multiply downstream volume for no
  * added information. The timer coalesces to a configured rate and only fires
  * when the quote actually changed.
  */
class BookFunction(
    maxDepth: Int,
    imbalanceDepth: Int,
    emitIntervalMs: Long
) extends KeyedProcessFunction[String, BookDelta, Quote] {

  @transient private var bookState: ValueState[BookSnapshot] = _
  @transient private var lastEmitted: ValueState[Quote] = _
  @transient private var timerSet: ValueState[java.lang.Boolean] = _

  @transient private var crossedBooks: Counter = _
  @transient private var snapshotsApplied: Counter = _
  @transient private var quotesEmitted: Counter = _
  @transient private var quotesSuppressed: Counter = _

  override def open(ctx: OpenContext): Unit = {
    bookState = getRuntimeContext.getState(
      new ValueStateDescriptor[BookSnapshot]("book", TypeInformation.of(classOf[BookSnapshot]))
    )
    lastEmitted = getRuntimeContext.getState(
      new ValueStateDescriptor[Quote]("last-quote", TypeInformation.of(classOf[Quote]))
    )
    timerSet = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Boolean]("timer-set", TypeInformation.of(classOf[java.lang.Boolean]))
    )

    val group = getRuntimeContext.getMetricGroup.addGroup("tapeline").addGroup("book")
    crossedBooks = group.counter("crossed_books")
    snapshotsApplied = group.counter("snapshots_applied")
    quotesEmitted = group.counter("quotes_emitted")
    quotesSuppressed = group.counter("quotes_suppressed")
  }

  override def processElement(
      delta: BookDelta,
      ctx: KeyedProcessFunction[String, BookDelta, Quote]#Context,
      out: Collector[Quote]
  ): Unit = {

    val current = Option(bookState.value())
      .map(OrderBook.fromSnapshot)
      .getOrElse(OrderBook.empty)

    if (delta.isSnapshot) snapshotsApplied.inc()

    val updated = current(delta).trim(maxDepth)

    // A crossed book means deltas were applied out of order or a resync was
    // missed. It is counted rather than dropped: the metric is what tells you
    // the ingestion tier's gap detection is not keeping up, and hiding it
    // would make that invisible.
    if (updated.isCrossed) crossedBooks.inc()

    bookState.update(updated.toSnapshot)

    // Register a processing-time timer to coalesce emissions. One timer at a
    // time per key, or a busy symbol would queue thousands.
    if (timerSet.value() == null) {
      val fireAt = ctx.timerService().currentProcessingTime() + emitIntervalMs
      ctx.timerService().registerProcessingTimeTimer(fireAt)
      timerSet.update(java.lang.Boolean.TRUE)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, BookDelta, Quote]#OnTimerContext,
      out: Collector[Quote]
  ): Unit = {
    timerSet.clear()

    val snapshot = Option(bookState.value())
    if (snapshot.isEmpty) return

    val book = OrderBook.fromSnapshot(snapshot.get)
    if (book.isCrossed) {
      // Serving a crossed quote would publish a negative spread and an
      // arbitrage that does not exist. Wait for the next delta to fix it.
      quotesSuppressed.inc()
      return
    }

    val venueSymbol = ctx.getCurrentKey.split('|')
    if (venueSymbol.length != 2) return

    book.toQuote(venueSymbol(0), venueSymbol(1), System.currentTimeMillis() * 1000L, imbalanceDepth)
      .filter(_.isValid)
      .foreach { q =>
        val previous = Option(lastEmitted.value())
        // Only emit on change. Republishing an identical quote costs
        // bandwidth downstream and tells a subscriber nothing.
        if (!previous.contains(q.copy(emitTimeUs = previous.map(_.emitTimeUs).getOrElse(0L)))) {
          lastEmitted.update(q)
          quotesEmitted.inc()
          out.collect(q)
        } else {
          quotesSuppressed.inc()
        }
      }
  }
}

object BookFunction {

  /** The keying used by the book job: one book per venue and symbol.
    *
    * Keyed on both because an order book is a property of a venue. Keying on
    * symbol alone would interleave three exchanges' deltas into one book,
    * which is not a book at all.
    */
  def keyOf(d: BookDelta): String = s"${d.venue}|${d.symbol}"
}
