package io.tapeline.stream.stats

/** A fixed-capacity rolling mean and standard deviation.
  *
  * The divergence detector needs a baseline: "BTC-USD is 8 bps apart between
  * Coinbase and Binance" is meaningless without knowing whether that pair
  * normally sits at 2 bps or at 12. A rolling z-score supplies that, and it
  * is what turns a threshold alert into a statistical one.
  *
  * Implementation notes worth defending in review:
  *
  *   - Sums are maintained incrementally rather than recomputed, so an update
  *     is O(1) and not O(window).
  *   - Variance uses the sum-of-squares form, which is numerically poor for
  *     large means and tiny variances. At the magnitudes here (basis points,
  *     order 1-100) it is fine, and the alternative — keeping a Welford
  *     accumulator that supports *eviction* — is materially more code. The
  *     guard below clamps the negative variance that catastrophic
  *     cancellation would otherwise produce.
  *   - Immutable, because it lives inside Flink keyed state. See the comment
  *     on OrderBook for why that matters under checkpointing.
  */
final case class RollingStats(
    capacity: Int,
    values: Vector[Double],
    sum: Double,
    sumSq: Double
) {
  require(capacity > 1, s"capacity must be at least 2, got $capacity")

  def count: Int = values.size
  def isFull: Boolean = values.size >= capacity

  /** Adds an observation, evicting the oldest once at capacity. */
  def add(x: Double): RollingStats =
    if (x.isNaN || x.isInfinite) this
    else if (values.size < capacity) {
      copy(values = values :+ x, sum = sum + x, sumSq = sumSq + x * x)
    } else {
      val evicted = values.head
      copy(
        values = values.tail :+ x,
        sum = sum - evicted + x,
        sumSq = sumSq - evicted * evicted + x * x
      )
    }

  def mean: Double = if (values.isEmpty) Double.NaN else sum / values.size

  /** Sample standard deviation. NaN below two observations. */
  def stddev: Double = {
    val n = values.size
    if (n < 2) Double.NaN
    else {
      val variance = (sumSq - (sum * sum) / n) / (n - 1)
      // Catastrophic cancellation can drive this slightly below zero when
      // every observation is identical.
      if (variance <= 0) 0.0 else math.sqrt(variance)
    }
  }

  /** How many standard deviations `x` sits from the rolling mean.
    *
    * Returns 0.0 rather than infinity when the deviation is zero: a
    * perfectly stable baseline should not make every observation an
    * infinite-sigma alert.
    */
  def zscore(x: Double): Double = {
    val sd = stddev
    if (sd.isNaN) Double.NaN
    else if (sd == 0.0) 0.0
    else (x - mean) / sd
  }

  /** True once there are enough observations for the z-score to mean
    * anything. Alerting before this is alerting on noise.
    */
  def isWarm(minObservations: Int): Boolean = values.size >= minObservations
}

object RollingStats {
  def empty(capacity: Int): RollingStats =
    RollingStats(capacity, Vector.empty, 0.0, 0.0)
}
