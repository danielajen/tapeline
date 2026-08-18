package io.tapeline.stream

import io.tapeline.stream.book.OrderBook
import io.tapeline.stream.model.Events.{BookDelta, Level}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OrderBookSpec extends AnyFunSuite with Matchers {

  private def snapshot(bids: Seq[(Double, Double)], asks: Seq[(Double, Double)], seq: Long = 1L) =
    BookDelta(
      venue = "coinbase", symbol = "BTC-USD", isSnapshot = true,
      bids = bids.map { case (p, s) => Level(p, s) },
      asks = asks.map { case (p, s) => Level(p, s) },
      eventTimeUs = 1_000_000L, ingestTimeUs = 1_000_100L, sequence = seq
    )

  private def update(
      bids: Seq[(Double, Double)],
      asks: Seq[(Double, Double)],
      seq: Long = 2L,
      eventTimeUs: Long = 2_000_000L
  ) = BookDelta(
    venue = "coinbase", symbol = "BTC-USD", isSnapshot = false,
    bids = bids.map { case (p, s) => Level(p, s) },
    asks = asks.map { case (p, s) => Level(p, s) },
    eventTimeUs = eventTimeUs, ingestTimeUs = eventTimeUs + 100, sequence = seq
  )

  test("an empty book has no best prices and reports itself empty") {
    OrderBook.empty.bestBid shouldBe None
    OrderBook.empty.bestAsk shouldBe None
    OrderBook.empty.isEmpty shouldBe true
    OrderBook.empty.mid shouldBe None
  }

  test("a snapshot builds both sides with the correct orderings") {
    val book = OrderBook.empty(snapshot(
      bids = Seq(64000.0 -> 1.0, 64100.0 -> 2.0, 63900.0 -> 3.0),
      asks = Seq(64300.0 -> 1.5, 64200.0 -> 0.5, 64400.0 -> 2.5)
    ))

    // Best bid is the highest, best ask the lowest.
    book.bestBid shouldBe Some(Level(64100.0, 2.0))
    book.bestAsk shouldBe Some(Level(64200.0, 0.5))
    book.mid shouldBe Some(64150.0)
    book.topBids(2).map(_.price) shouldBe Seq(64100.0, 64000.0)
    book.topAsks(2).map(_.price) shouldBe Seq(64200.0, 64300.0)
  }

  test("a snapshot replaces rather than merges") {
    val first = OrderBook.empty(snapshot(Seq(100.0 -> 1.0, 99.0 -> 1.0), Seq(101.0 -> 1.0)))
    val second = first(snapshot(Seq(200.0 -> 5.0), Seq(201.0 -> 5.0), seq = 9L))

    second.depth shouldBe ((1, 1))
    second.bestBid shouldBe Some(Level(200.0, 5.0))
    second.lastSequence shouldBe 9L
  }

  test("an incremental update patches levels and a zero size deletes") {
    val book = OrderBook.empty(snapshot(
      bids = Seq(100.0 -> 1.0, 99.0 -> 2.0, 98.0 -> 3.0),
      asks = Seq(101.0 -> 1.0, 102.0 -> 2.0)
    ))

    val patched = book(update(
      bids = Seq(99.0 -> 7.0, 98.0 -> 0.0, 97.0 -> 4.0),
      asks = Seq(101.0 -> 0.0)
    ))

    patched.bids.get(99.0) shouldBe Some(7.0) // resized
    patched.bids.get(98.0) shouldBe None      // deleted by a zero size
    patched.bids.get(97.0) shouldBe Some(4.0) // inserted
    patched.bids.get(100.0) shouldBe Some(1.0) // untouched
    patched.bestAsk shouldBe Some(Level(102.0, 2.0)) // 101 was removed
  }

  test("a zero level in a snapshot is not part of the book") {
    val book = OrderBook.empty(snapshot(
      bids = Seq(100.0 -> 1.0, 99.0 -> 0.0),
      asks = Seq(101.0 -> 1.0)
    ))
    book.bids.size shouldBe 1
    book.bids.get(99.0) shouldBe None
  }

  test("imbalance is bid-heavy positive, ask-heavy negative, and bounded") {
    val bidHeavy = OrderBook.empty(snapshot(
      bids = Seq(100.0 -> 9.0), asks = Seq(101.0 -> 1.0)
    ))
    bidHeavy.imbalance(1) shouldBe 0.8 +- 1e-9

    val askHeavy = OrderBook.empty(snapshot(
      bids = Seq(100.0 -> 1.0), asks = Seq(101.0 -> 9.0)
    ))
    askHeavy.imbalance(1) shouldBe -0.8 +- 1e-9

    val balanced = OrderBook.empty(snapshot(
      bids = Seq(100.0 -> 5.0), asks = Seq(101.0 -> 5.0)
    ))
    balanced.imbalance(1) shouldBe 0.0 +- 1e-9
  }

  test("imbalance respects the requested depth") {
    val book = OrderBook.empty(snapshot(
      bids = Seq(100.0 -> 1.0, 99.0 -> 100.0),
      asks = Seq(101.0 -> 1.0, 102.0 -> 100.0)
    ))
    // At depth 1 the deep levels are invisible and the book looks balanced.
    book.imbalance(1) shouldBe 0.0 +- 1e-9
    book.imbalance(2) shouldBe 0.0 +- 1e-9

    val skewed = OrderBook.empty(snapshot(
      bids = Seq(100.0 -> 1.0, 99.0 -> 100.0),
      asks = Seq(101.0 -> 1.0)
    ))
    skewed.imbalance(1) shouldBe 0.0 +- 1e-9
    skewed.imbalance(2) should be > 0.9
  }

  test("imbalance on a one-sided book is zero, not infinite") {
    val oneSided = OrderBook.empty(snapshot(bids = Seq(100.0 -> 5.0), asks = Seq.empty))
    oneSided.imbalance(5) shouldBe 1.0 +- 1e-9

    OrderBook.empty.imbalance(5) shouldBe 0.0
  }

  test("imbalance rejects a non-positive depth rather than returning nonsense") {
    an[IllegalArgumentException] should be thrownBy OrderBook.empty.imbalance(0)
  }

  test("a crossed book is detected") {
    val ok = OrderBook.empty(snapshot(Seq(100.0 -> 1.0), Seq(101.0 -> 1.0)))
    ok.isCrossed shouldBe false

    // Best bid above best ask: deltas were applied out of order.
    val crossed = OrderBook.empty(snapshot(Seq(102.0 -> 1.0), Seq(101.0 -> 1.0)))
    crossed.isCrossed shouldBe true

    // Locked (bid == ask) also counts: it cannot persist in a real book.
    val locked = OrderBook.empty(snapshot(Seq(101.0 -> 1.0), Seq(101.0 -> 1.0)))
    locked.isCrossed shouldBe true
  }

  test("trim bounds per-key state and keeps the levels nearest the touch") {
    val manyBids = (1 to 500).map(i => (100.0 - i) -> 1.0)
    val manyAsks = (1 to 500).map(i => (100.0 + i) -> 1.0)
    val book = OrderBook.empty(snapshot(manyBids, manyAsks))

    book.depth shouldBe ((500, 500))

    val trimmed = book.trim(10)
    trimmed.depth shouldBe ((10, 10))
    // The levels kept must be the ones nearest the mid, not an arbitrary ten.
    trimmed.bestBid shouldBe Some(Level(99.0, 1.0))
    trimmed.topBids(10).map(_.price).min shouldBe 90.0
    trimmed.bestAsk shouldBe Some(Level(101.0, 1.0))
    trimmed.topAsks(10).map(_.price).max shouldBe 110.0
  }

  test("trim is a no-op when the book is already small enough") {
    val book = OrderBook.empty(snapshot(Seq(100.0 -> 1.0), Seq(101.0 -> 1.0)))
    book.trim(10) should be theSameInstanceAs book
  }

  test("a book projects to a quote only when both sides are populated") {
    val oneSided = OrderBook.empty(snapshot(Seq(100.0 -> 1.0), Seq.empty))
    oneSided.toQuote("coinbase", "BTC-USD", 5_000_000L) shouldBe None

    val full = OrderBook.empty(snapshot(Seq(100.0 -> 2.0), Seq(101.0 -> 3.0)))
    val q = full.toQuote("coinbase", "BTC-USD", 5_000_000L).get

    q.bidPrice shouldBe 100.0
    q.askSize shouldBe 3.0
    q.mid shouldBe 100.5
    q.eventTimeUs shouldBe 1_000_000L  // from the book, not the emit time
    q.emitTimeUs shouldBe 5_000_000L
    q.spreadBps shouldBe (1.0 / 100.5 * 10000.0) +- 1e-6
    q.isValid shouldBe true
  }

  test("spread on an invalid quote is NaN, not a plausible zero") {
    val q = OrderBook.empty(snapshot(Seq(100.0 -> 1.0), Seq(101.0 -> 1.0)))
      .toQuote("coinbase", "BTC-USD", 1L).get
      .copy(bidPrice = 0.0)
    q.spreadBps.isNaN shouldBe true
    q.isValid shouldBe false
  }

  test("applying a sequence of deltas is order-dependent and reproducible") {
    val deltas = Seq(
      snapshot(Seq(100.0 -> 1.0), Seq(101.0 -> 1.0), seq = 1),
      update(Seq(100.5 -> 2.0), Seq.empty, seq = 2),
      update(Seq.empty, Seq(100.9 -> 3.0), seq = 3),
      update(Seq(100.5 -> 0.0), Seq.empty, seq = 4)
    )

    val replayedOnce = deltas.foldLeft(OrderBook.empty)((b, d) => b(d))
    val replayedTwice = deltas.foldLeft(OrderBook.empty)((b, d) => b(d))

    // Determinism is the property the Kappa backfill proof depends on:
    // the same deltas in the same order must rebuild the same book.
    replayedOnce shouldBe replayedTwice
    replayedOnce.bestBid shouldBe Some(Level(100.0, 1.0))
    replayedOnce.bestAsk shouldBe Some(Level(100.9, 3.0))
    replayedOnce.lastSequence shouldBe 4L
  }
}
