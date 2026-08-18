package io.tapeline.stream

import io.tapeline.stream.model.Events.Trade
import io.tapeline.stream.stats.{RollingStats, TradeAggregate}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RollingStatsSpec extends AnyFunSuite with Matchers {

  test("mean and stddev over a simple sample") {
    val s = Seq(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0)
      .foldLeft(RollingStats.empty(100))(_ add _)

    s.count shouldBe 8
    s.mean shouldBe 5.0 +- 1e-9
    // Sample (n-1) standard deviation of that sample is sqrt(32/7).
    s.stddev shouldBe math.sqrt(32.0 / 7.0) +- 1e-9
  }

  test("the window evicts the oldest observation once full") {
    val s = (1 to 10).map(_.toDouble).foldLeft(RollingStats.empty(3))(_ add _)

    s.count shouldBe 3
    s.isFull shouldBe true
    // Only 8, 9, 10 remain.
    s.mean shouldBe 9.0 +- 1e-9
  }

  test("eviction keeps the incremental sums honest over many rotations") {
    val capacity = 50
    val rolled = (1 to 5000).map(_.toDouble).foldLeft(RollingStats.empty(capacity))(_ add _)

    val expected = (4951 to 5000).map(_.toDouble)
    rolled.mean shouldBe (expected.sum / capacity) +- 1e-6

    // Recomputed from scratch, this is what the incremental sums must match.
    val fresh = expected.foldLeft(RollingStats.empty(capacity))(_ add _)
    rolled.mean shouldBe fresh.mean +- 1e-6
    rolled.stddev shouldBe fresh.stddev +- 1e-6
  }

  test("fewer than two observations gives NaN rather than a fake zero") {
    RollingStats.empty(10).mean.isNaN shouldBe true
    RollingStats.empty(10).stddev.isNaN shouldBe true
    RollingStats.empty(10).add(5.0).stddev.isNaN shouldBe true
  }

  test("a constant series has zero deviation and no infinite z-scores") {
    val s = Seq.fill(20)(3.0).foldLeft(RollingStats.empty(50))(_ add _)
    s.stddev shouldBe 0.0
    // The guard: a perfectly stable baseline must not make every reading an
    // infinite-sigma alert.
    s.zscore(99.0) shouldBe 0.0
    s.zscore(3.0) shouldBe 0.0
  }

  test("z-score measures deviations from the rolling mean") {
    val s = Seq(10.0, 12.0, 14.0, 16.0, 18.0).foldLeft(RollingStats.empty(10))(_ add _)
    s.mean shouldBe 14.0 +- 1e-9
    s.zscore(14.0) shouldBe 0.0 +- 1e-9
    s.zscore(14.0 + s.stddev) shouldBe 1.0 +- 1e-9
    s.zscore(14.0 - 2 * s.stddev) shouldBe -2.0 +- 1e-9
  }

  test("NaN and infinite observations are ignored, not propagated") {
    val s = RollingStats.empty(10).add(1.0).add(Double.NaN).add(Double.PositiveInfinity).add(3.0)
    s.count shouldBe 2
    s.mean shouldBe 2.0 +- 1e-9
  }

  test("warmth gates alerting on a cold baseline") {
    val cold = (1 to 5).map(_.toDouble).foldLeft(RollingStats.empty(100))(_ add _)
    cold.isWarm(30) shouldBe false

    val warm = (1 to 40).map(_.toDouble).foldLeft(RollingStats.empty(100))(_ add _)
    warm.isWarm(30) shouldBe true
  }

  test("a capacity below two is rejected") {
    an[IllegalArgumentException] should be thrownBy RollingStats.empty(1)
  }
}

class TradeAggregateSpec extends AnyFunSuite with Matchers {

  private def trade(price: Double, size: Double, eventTimeUs: Long, id: String = "t") =
    Trade("coinbase", "BTC-USD", id, price, size, "BUY", eventTimeUs, eventTimeUs + 50, 1L)

  test("an empty aggregate reports no volume and a NaN vwap") {
    TradeAggregate.empty.count shouldBe 0L
    TradeAggregate.empty.vwap.isNaN shouldBe true
  }

  test("vwap is volume-weighted, not a plain mean") {
    val agg = TradeAggregate.fold(Seq(
      trade(100.0, 1.0, 1000),
      trade(200.0, 9.0, 2000)
    ))
    // The arithmetic mean would be 150. Weighting by size gives 190.
    agg.vwap shouldBe 190.0 +- 1e-9
    agg.volume shouldBe 10.0
    agg.count shouldBe 2L
  }

  test("open and close follow event time, not arrival order") {
    // Deliberately out of order, which is what a real stream delivers.
    val agg = TradeAggregate.fold(Seq(
      trade(150.0, 1.0, 5000),
      trade(100.0, 1.0, 1000), // earliest by event time
      trade(200.0, 1.0, 9000), // latest by event time
      trade(175.0, 1.0, 7000)
    ))

    agg.open shouldBe 100.0
    agg.close shouldBe 200.0
    agg.high shouldBe 200.0
    agg.low shouldBe 100.0
  }

  test("merge is associative and commutative") {
    val a = TradeAggregate.fold(Seq(trade(100.0, 1.0, 1000), trade(110.0, 2.0, 2000)))
    val b = TradeAggregate.fold(Seq(trade(90.0, 3.0, 500), trade(120.0, 1.0, 3000)))
    val c = TradeAggregate.fold(Seq(trade(105.0, 4.0, 1500)))

    // Commutative.
    a.merge(b) shouldBe b.merge(a)
    // Associative.
    a.merge(b).merge(c) shouldBe a.merge(b.merge(c))

    // And equal to folding everything in one pass — the property that lets
    // pre-aggregation happen in parallel without changing the answer.
    val all = TradeAggregate.fold(Seq(
      trade(100.0, 1.0, 1000), trade(110.0, 2.0, 2000),
      trade(90.0, 3.0, 500), trade(120.0, 1.0, 3000),
      trade(105.0, 4.0, 1500)
    ))
    a.merge(b).merge(c) shouldBe all
  }

  test("merging with an empty aggregate is the identity") {
    val a = TradeAggregate.fold(Seq(trade(100.0, 1.0, 1000)))
    a.merge(TradeAggregate.empty) shouldBe a
    TradeAggregate.empty.merge(a) shouldBe a
  }

  test("a zero-volume aggregate does not report a vwap of zero") {
    val agg = TradeAggregate.fold(Seq(trade(100.0, 0.0, 1000)))
    agg.count shouldBe 1L
    // Emitting 0.0 here would drag any downstream average toward zero.
    agg.vwap.isNaN shouldBe true
  }

  test("a bar carries the window bounds it was asked for") {
    val agg = TradeAggregate.fold(Seq(trade(100.0, 2.0, 1_500_000), trade(104.0, 2.0, 1_800_000)))
    val bar = agg.toBar("kraken", "ETH-USD", 1_000_000L, 2_000_000L)

    bar.venue shouldBe "kraken"
    bar.symbol shouldBe "ETH-USD"
    bar.windowStartUs shouldBe 1_000_000L
    bar.windowEndUs shouldBe 2_000_000L
    bar.open shouldBe 100.0
    bar.close shouldBe 104.0
    bar.vwap shouldBe 102.0 +- 1e-9
    bar.tradeCount shouldBe 2L
  }

  test("replaying the same trades reproduces the same bar") {
    val trades = (1 to 200).map(i => trade(100.0 + (i % 17), 0.1 * (i % 5 + 1), 1000L * i, s"t$i"))

    val streamed = trades.foldLeft(TradeAggregate.empty)(_ add _)
    // The backfill path folds the same records read back from Iceberg.
    val backfilled = TradeAggregate.fold(trades.reverse)

    streamed.vwap shouldBe backfilled.vwap +- 1e-12
    streamed.open shouldBe backfilled.open
    streamed.close shouldBe backfilled.close
    streamed.high shouldBe backfilled.high
    streamed.low shouldBe backfilled.low
    streamed.count shouldBe backfilled.count
  }
}
