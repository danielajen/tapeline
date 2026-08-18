package io.tapeline.stream

import io.tapeline.stream.divergence.Divergence
import io.tapeline.stream.model.Events.Quote
import io.tapeline.stream.stats.RollingStats
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DivergenceSpec extends AnyFunSuite with Matchers {

  private def quote(venue: String, mid: Double, eventTimeUs: Long, spread: Double = 1.0) =
    Quote(
      venue = venue, symbol = "BTC-USD",
      bidPrice = mid - spread / 2, bidSize = 1.0,
      askPrice = mid + spread / 2, askSize = 1.0,
      imbalance = 0.0, eventTimeUs = eventTimeUs, emitTimeUs = eventTimeUs
    )

  private val cfg = Divergence.Config()

  /** Feeds a stable baseline so the detector is warm before the test case. */
  private def warmBaselines(
      basePrice: Double,
      jitterBps: Double,
      n: Int,
      cfg: Divergence.Config = cfg
  ): Divergence.Baselines =
    (1 to n).foldLeft(Divergence.Baselines.empty) { (bl, i) =>
      val drift = basePrice * (jitterBps / 10000.0) * (if (i % 2 == 0) 1 else -1)
      Divergence.evaluate(
        "BTC-USD",
        Map(
          "binance" -> quote("binance", basePrice, 1_000_000L * i),
          "coinbase" -> quote("coinbase", basePrice + drift, 1_000_000L * i)
        ),
        bl, 1_000_000L * i, cfg
      ).baselines
    }

  test("divergence is symmetric under venue swap") {
    val a = quote("coinbase", 64000.0, 1000)
    val b = quote("binance", 64064.0, 1000)

    val ab = Divergence.divergenceBps(a, b)
    val ba = Divergence.divergenceBps(b, a)

    ab shouldBe -ba +- 1e-9
    // 64 on ~64032 average is almost exactly 10 bps.
    ab shouldBe 9.995 +- 0.01
  }

  test("pairs are canonicalized so one baseline serves both orderings") {
    Divergence.canonicalPair("coinbase", "binance") shouldBe ("binance", "coinbase")
    Divergence.canonicalPair("binance", "coinbase") shouldBe ("binance", "coinbase")
  }

  test("fewer than two fresh venues produces nothing") {
    val single = Map("coinbase" -> quote("coinbase", 64000.0, 1_000_000L))
    Divergence.evaluate("BTC-USD", single, Divergence.Baselines.empty, 1_000_000L, cfg)
      .events shouldBe empty

    Divergence.evaluate("BTC-USD", Map.empty, Divergence.Baselines.empty, 1L, cfg)
      .events shouldBe empty
  }

  test("a cold baseline never fires, however large the divergence") {
    // 500 bps apart on the very first observation. Alerting here would be
    // alerting on a sample size of one.
    val quotes = Map(
      "coinbase" -> quote("coinbase", 64000.0, 1_000_000L),
      "binance" -> quote("binance", 67200.0, 1_000_000L)
    )
    val r = Divergence.evaluate("BTC-USD", quotes, Divergence.Baselines.empty, 1_000_000L, cfg)

    r.events shouldBe empty
    // The observation is still folded into the baseline.
    r.baselines.get(("binance", "coinbase")).map(_.count) shouldBe Some(1)
  }

  test("a genuine outlier against a warm, tight baseline fires") {
    val warm = warmBaselines(basePrice = 64000.0, jitterBps = 1.0, n = 60)
    val now = 100_000_000L

    val quotes = Map(
      "binance" -> quote("binance", 64000.0, now),
      "coinbase" -> quote("coinbase", 64000.0 * 1.002, now) // ~20 bps out
    )
    val r = Divergence.evaluate("BTC-USD", quotes, warm, now, cfg)

    r.events should have size 1
    val ev = r.events.head
    ev.symbol shouldBe "BTC-USD"
    math.abs(ev.divergenceBps) should be > cfg.minDivergenceBps
    math.abs(ev.zscore) should be > cfg.minZscore
    ev.eventTimeUs shouldBe now
  }

  test("a divergence inside the normal band does not fire") {
    // Baseline jitters by +/-10 bps, so a 10 bps reading is unremarkable
    // even though it clears the absolute 5 bps threshold.
    val warm = warmBaselines(basePrice = 64000.0, jitterBps = 10.0, n = 60)
    val now = 100_000_000L

    val quotes = Map(
      "binance" -> quote("binance", 64000.0, now),
      "coinbase" -> quote("coinbase", 64000.0 * 1.001, now)
    )
    Divergence.evaluate("BTC-USD", quotes, warm, now, cfg).events shouldBe empty
  }

  test("the absolute threshold suppresses statistically odd but tiny moves") {
    // A baseline this tight makes a 1 bps move a huge z-score. It is still
    // not tradeable, and alerting on it would bury the real signals.
    val warm = warmBaselines(basePrice = 64000.0, jitterBps = 0.001, n = 60)
    val now = 100_000_000L

    val quotes = Map(
      "binance" -> quote("binance", 64000.0, now),
      "coinbase" -> quote("coinbase", 64000.0 * 1.0001, now) // ~1 bps
    )
    val r = Divergence.evaluate("BTC-USD", quotes, warm, now, cfg)
    r.events shouldBe empty
  }

  test("a stale venue is excluded rather than reported as divergent") {
    val warm = warmBaselines(basePrice = 64000.0, jitterBps = 1.0, n = 60)
    val now = 100_000_000L

    // Coinbase stopped publishing an hour ago and the market moved. Without
    // the freshness gate this is a permanent, growing false alert.
    val quotes = Map(
      "binance" -> quote("binance", 64000.0, now),
      "coinbase" -> quote("coinbase", 61000.0, now - 3_600_000_000L)
    )
    Divergence.evaluate("BTC-USD", quotes, warm, now, cfg).events shouldBe empty
  }

  test("an invalid or crossed quote is excluded") {
    val warm = warmBaselines(basePrice = 64000.0, jitterBps = 1.0, n = 60)
    val now = 100_000_000L

    val crossed = quote("coinbase", 64000.0, now).copy(bidPrice = 65000.0, askPrice = 63000.0)
    crossed.isValid shouldBe false

    val quotes = Map("binance" -> quote("binance", 64000.0, now), "coinbase" -> crossed)
    Divergence.evaluate("BTC-USD", quotes, warm, now, cfg).events shouldBe empty
  }

  test("three venues produce three pairwise comparisons") {
    val cold = Divergence.Baselines.empty
    val now = 1_000_000L
    val quotes = Map(
      "coinbase" -> quote("coinbase", 64000.0, now),
      "binance" -> quote("binance", 64010.0, now),
      "kraken" -> quote("kraken", 64020.0, now)
    )
    val r = Divergence.evaluate("BTC-USD", quotes, cold, now, cfg)
    r.baselines.byPair.keySet shouldBe Set(
      ("binance", "coinbase"), ("coinbase", "kraken"), ("binance", "kraken")
    )
  }

  test("event time drives freshness, so backfill is not filtered out wholesale") {
    val warm = warmBaselines(basePrice = 64000.0, jitterBps = 1.0, n = 60)

    // Quotes from six months ago, evaluated at their own event time. Using
    // wall clock here would drop every replayed record and the backfill would
    // silently emit nothing.
    val historicalNow = 1_700_000_000_000_000L
    val quotes = Map(
      "binance" -> quote("binance", 64000.0, historicalNow),
      "coinbase" -> quote("coinbase", 64000.0 * 1.002, historicalNow)
    )
    Divergence.evaluate("BTC-USD", quotes, warm, historicalNow, cfg).events should have size 1
  }

  test("the z-score is taken against the baseline as it stood before this observation") {
    // If the outlier were folded into the baseline before the z-score was
    // computed, it would drag the mean toward itself and suppress its own
    // alert — quietly, and more so the smaller the window.
    val cfgFast = Divergence.Config(
      minObservations = 2, baselineWindow = 100, minZscore = 1.0, minDivergenceBps = 1.0
    )
    val prior = Seq(1.0, 2.0, 3.0, 2.0, 1.0, 2.0, 3.0, 2.0)
      .foldLeft(RollingStats.empty(100))(_ add _)
    val bl = Divergence.Baselines(Map(("a", "b") -> prior))

    val r = Divergence.evaluate(
      "BTC-USD",
      Map("a" -> quote("a", 100.0, 9000L), "b" -> quote("b", 100.2, 9000L)), // ~20 bps
      bl, 9000L, cfgFast
    )

    r.events should have size 1
    val ev = r.events.head
    // Exactly the pre-observation baseline, not one that has already seen it.
    ev.zscore shouldBe prior.zscore(ev.divergenceBps) +- 1e-9
    // The observation is still recorded for next time.
    r.baselines.get(("a", "b")).map(_.count) shouldBe Some(prior.count + 1)
  }
}
