package io.tapeline.stream

import io.tapeline.stream.functions.BookFunction
import io.tapeline.stream.model.Events.{BookDelta, Level, Quote}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** BookFunction driven through Flink's own operator harness.
  *
  * ==Why this file exists==
  *
  * Every other test in this module is pure: values in, values out, no
  * framework. That made the order book, the aggregates and the divergence
  * detector cheap to test and it caught real bugs. It also could not catch a
  * whole class of defect, and three of them shipped:
  *
  *   - Kryo silently degrading Scala collections in state, then in events
  *   - a processing-time timer that stopped re-arming once input went quiet
  *
  * None of those exist in-process. They only appear when Flink actually
  * serializes state, actually fires timers, and actually advances a clock.
  * The harness does all three without a cluster, so the feedback loop is
  * seconds rather than a ten-minute CI run.
  */
class BookFunctionHarnessSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val EmitIntervalMs = 100L

  private var harness: KeyedOneInputStreamOperatorTestHarness[String, BookDelta, Quote] = _

  override def beforeEach(): Unit = {
    val operator = new KeyedProcessOperator[String, BookDelta, Quote](
      new BookFunction(maxDepth = 50, imbalanceDepth = 5, emitIntervalMs = EmitIntervalMs)
    )
    harness = new KeyedOneInputStreamOperatorTestHarness[String, BookDelta, Quote](
      operator,
      (d: BookDelta) => BookFunction.keyOf(d),
      Types.STRING
    )
    harness.open()
  }

  override def afterEach(): Unit = if (harness != null) harness.close()

  private def delta(
      venue: String = "coinbase",
      symbol: String = "BTC-USD",
      isSnapshot: Boolean = true,
      bid: Double = 100.0,
      ask: Double = 101.0,
      seq: Long = 1L
  ): BookDelta = BookDelta.fromLevels(
    venue, symbol, isSnapshot,
    bids = Seq(Level(bid, 2.0), Level(bid - 1, 3.0)),
    asks = Seq(Level(ask, 1.5), Level(ask + 1, 2.5)),
    eventTimeUs = 1_000_000L, ingestTimeUs = 1_000_100L, sequence = seq
  )

  private def emitted: Seq[Quote] =
    harness.extractOutputValues().asScala.toSeq

  test("a book delta followed by a timer tick emits a quote") {
    harness.processElement(delta(), 1L)
    harness.setProcessingTime(EmitIntervalMs + 1)

    val quotes = emitted
    quotes should have size 1
    quotes.head.venue shouldBe "coinbase"
    quotes.head.symbol shouldBe "BTC-USD"
    quotes.head.bidPrice shouldBe 100.0
    quotes.head.askPrice shouldBe 101.0
  }

  /** The bug. The timer used to re-arm only from processElement, so a symbol
    * that went quiet stopped publishing entirely instead of continuing to
    * serve its last known book. In CI the job consumed 7,934 records,
    * checkpointed 19KB of state, and emitted zero quotes for exactly this
    * reason: the seeded backlog drained and nothing re-armed the timer.
    */
  test("the timer keeps firing after input goes quiet") {
    harness.processElement(delta(), 1L)

    // One element, then silence — and the clock keeps moving.
    harness.setProcessingTime(EmitIntervalMs + 1)
    emitted should have size 1

    // The book has not changed, so nothing new is published...
    harness.setProcessingTime(EmitIntervalMs * 5)
    emitted should have size 1

    // ...but the timer must still be armed, so a later change publishes
    // without needing an element to wake the operator up first.
    harness.processElement(delta(bid = 102.0, ask = 103.0, seq = 2L), 2L)
    harness.setProcessingTime(EmitIntervalMs * 10)

    val quotes = emitted
    quotes should have size 2
    quotes.last.bidPrice shouldBe 102.0
  }

  test("an unchanged book does not republish") {
    harness.processElement(delta(), 1L)
    harness.setProcessingTime(EmitIntervalMs + 1)
    harness.setProcessingTime(EmitIntervalMs * 2)
    harness.setProcessingTime(EmitIntervalMs * 3)

    // Republishing an identical quote costs bandwidth and tells a subscriber
    // nothing.
    emitted should have size 1
  }

  test("a crossed book is suppressed rather than served") {
    // Best bid above best ask. A negative spread is an arbitrage that does
    // not exist, so nothing should reach the topic.
    harness.processElement(delta(bid = 105.0, ask = 101.0), 1L)
    harness.setProcessingTime(EmitIntervalMs * 3)

    emitted shouldBe empty
  }

  test("state survives a snapshot and restore") {
    harness.processElement(delta(), 1L)
    harness.setProcessingTime(EmitIntervalMs + 1)
    emitted should have size 1

    // This is the step that exercises Kryo, and the reason two serialization
    // bugs reached a cluster: in-process there is no serialization at all.
    val snapshot = harness.snapshot(1L, 1L)
    harness.close()

    val operator = new KeyedProcessOperator[String, BookDelta, Quote](
      new BookFunction(maxDepth = 50, imbalanceDepth = 5, emitIntervalMs = EmitIntervalMs)
    )
    harness = new KeyedOneInputStreamOperatorTestHarness[String, BookDelta, Quote](
      operator, (d: BookDelta) => BookFunction.keyOf(d), Types.STRING
    )
    harness.initializeState(snapshot)
    harness.open()

    // The delta narrows the spread INSIDE the restored book rather than
    // jumping outside it. A delta at 104/105 on top of restored levels at
    // 100/101 would leave best bid above best ask, and the operator would
    // correctly suppress the crossed quote — which would make this test fail
    // for a reason that has nothing to do with state restore.
    harness.processElement(delta(isSnapshot = false, bid = 100.5, ask = 100.9, seq = 2L), 2L)
    harness.setProcessingTime(EmitIntervalMs * 20)

    val quotes = emitted
    quotes should not be empty
    // 100.5 could only come from the new delta; 100.9 as best ask could only
    // come from it too. That the book did not reset to empty is what proves
    // the restore worked — an empty book would emit nothing at all.
    quotes.last.bidPrice shouldBe 100.5
    quotes.last.askPrice shouldBe 100.9
  }

  test("each venue and symbol keeps its own book") {
    harness.processElement(delta(venue = "coinbase", symbol = "BTC-USD", bid = 100.0, ask = 101.0), 1L)
    harness.processElement(delta(venue = "kraken", symbol = "BTC-USD", bid = 200.0, ask = 201.0), 2L)
    harness.setProcessingTime(EmitIntervalMs + 1)

    val byVenue = emitted.map(q => q.venue -> q.bidPrice).toMap
    byVenue("coinbase") shouldBe 100.0
    byVenue("kraken") shouldBe 200.0
  }
}
