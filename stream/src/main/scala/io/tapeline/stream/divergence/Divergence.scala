package io.tapeline.stream.divergence

import io.tapeline.stream.model.Events.{DivergenceEvent, Quote}
import io.tapeline.stream.stats.RollingStats

/** Cross-exchange price divergence detection.
  *
  * The whole detector is a pure function from "the latest quote per venue for
  * one symbol" to "the venue pairs that are unusually far apart". Keeping it
  * pure is what allows the interesting cases — a stale venue, a crossed book,
  * a cold baseline — to be tested as a table rather than by running a job.
  */
object Divergence {

  /** Configuration for one detector instance. */
  final case class Config(
      minDivergenceBps: Double = 5.0,
      minZscore: Double = 3.0,
      baselineWindow: Int = 600,
      minObservations: Int = 30,
      /** A quote older than this is not evidence of anything. Comparing
        * against a venue that stopped publishing produces a growing
        * "divergence" that is really just staleness, and it is the most
        * common false positive in a detector like this.
        */
      maxQuoteAgeUs: Long = 5_000_000L
  )

  /** The per-venue-pair rolling baselines for one symbol. */
  final case class Baselines(byPair: Map[(String, String), RollingStats]) {

    def observe(pair: (String, String), bps: Double, window: Int): Baselines = {
      val stats = byPair.getOrElse(pair, RollingStats.empty(window))
      Baselines(byPair.updated(pair, stats.add(bps)))
    }

    def get(pair: (String, String)): Option[RollingStats] = byPair.get(pair)
  }

  object Baselines {
    val empty: Baselines = Baselines(Map.empty)
  }

  /** Signed divergence of `b` relative to `a`, in basis points of their
    * average mid. Using the average as the denominator rather than one side's
    * price keeps the measure symmetric: swapping the venues flips the sign
    * and nothing else.
    */
  def divergenceBps(a: Quote, b: Quote): Double = {
    val midA = a.mid
    val midB = b.mid
    val reference = (midA + midB) / 2.0
    if (reference <= 0) Double.NaN else (midB - midA) / reference * 10000.0
  }

  /** Orders a venue pair so (coinbase, binance) and (binance, coinbase) share
    * one baseline instead of maintaining two half-populated ones.
    */
  def canonicalPair(v1: String, v2: String): (String, String) =
    if (v1 <= v2) (v1, v2) else (v2, v1)

  /** The result of examining one snapshot of the market. */
  final case class Result(
      events: Seq[DivergenceEvent],
      baselines: Baselines
  )

  /** Evaluates every venue pair for one symbol.
    *
    * `nowUs` is event time, not wall clock — under backfill the two differ by
    * months, and using wall clock here would make every replayed quote look
    * stale and silently produce zero events.
    */
  def evaluate(
      symbol: String,
      quotes: Map[String, Quote],
      baselines: Baselines,
      nowUs: Long,
      cfg: Config
  ): Result = {

    val fresh = quotes.values
      .filter(q => q.isValid && (nowUs - q.eventTimeUs) <= cfg.maxQuoteAgeUs)
      .toSeq
      .sortBy(_.venue)

    if (fresh.size < 2) return Result(Seq.empty, baselines)

    val pairs = for {
      i <- fresh.indices
      j <- (i + 1) until fresh.size
    } yield (fresh(i), fresh(j))

    pairs.foldLeft(Result(Seq.empty, baselines)) { case (acc, (a, b)) =>
      val bps = divergenceBps(a, b)
      if (bps.isNaN) acc
      else {
        val pair = canonicalPair(a.venue, b.venue)
        val prior = acc.baselines.get(pair)

        // The baseline is read *before* this observation is folded in.
        // Including the current value would drag the mean toward the outlier
        // and suppress exactly the alert being tested for.
        val z = prior.map(_.zscore(bps)).getOrElse(Double.NaN)
        val warm = prior.exists(_.isWarm(cfg.minObservations))

        val updated = acc.baselines.observe(pair, bps, cfg.baselineWindow)

        val fires =
          warm &&
            math.abs(bps) >= cfg.minDivergenceBps &&
            !z.isNaN &&
            math.abs(z) >= cfg.minZscore

        if (!fires) acc.copy(baselines = updated)
        else {
          val ev = DivergenceEvent(
            symbol = symbol,
            venueA = a.venue,
            venueB = b.venue,
            priceA = a.mid,
            priceB = b.mid,
            divergenceBps = bps,
            zscore = z,
            windowStartUs = math.min(a.eventTimeUs, b.eventTimeUs),
            eventTimeUs = math.max(a.eventTimeUs, b.eventTimeUs)
          )
          Result(acc.events :+ ev, updated)
        }
      }
    }
  }
}
