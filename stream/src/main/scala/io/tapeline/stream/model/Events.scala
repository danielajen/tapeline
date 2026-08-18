package io.tapeline.stream.model

/** The canonical event model, mirroring the Avro schemas under
  * `ingest/schemas/avro`.
  *
  * These are plain case classes with no Flink or Avro types in their
  * signatures. That is deliberate and it is the same split the Go tier uses:
  * everything interesting about this system — order book maintenance,
  * windowed statistics, divergence detection — is a pure function over these
  * values, and can therefore be tested without a cluster, a broker, or a
  * registry. The Flink operators in `io.tapeline.stream.jobs` are thin
  * wrappers that do nothing but call into this package.
  */
object Events {

  /** One price level. A size of zero is a deletion, not an empty level. */
  final case class Level(price: Double, size: Double)

  final case class Trade(
      venue: String,
      symbol: String,
      tradeId: String,
      price: Double,
      size: Double,
      side: String,
      eventTimeUs: Long,
      ingestTimeUs: Long,
      sequence: Long
  ) {
    def notional: Double = price * size
    def isBuy: Boolean = side == "BUY"
  }

  /** An L2 update.
    *
    * ==Why parallel arrays and not Seq[Level]==
    *
    * This held `bids: Seq[Level]` and it crash-looped every Flink job that
    * touched it. Flink serializes records crossing an operator boundary with
    * Kryo, and Kryo does not round-trip Scala's immutable collections: a
    * `Seq` written by the map operator arrived at the keyed process function
    * as something whose `foldLeft` threw `NoSuchElementException: head of
    * empty list`.
    *
    * The rule this encodes is broader than the one first written down, which
    * only covered types placed in `ValueState`: **no Scala collection may
    * cross a Flink boundary, state or network.** Events are serialized too.
    *
    * Unit tests cannot catch this. In-process there is no serialization at
    * all — only a real cluster, across a real operator boundary, exercises
    * Kryo. See docs/POSTMORTEM.md#postmortem-4.
    */
  final case class BookDelta(
      venue: String,
      symbol: String,
      isSnapshot: Boolean,
      bidPrices: Array[Double],
      bidSizes: Array[Double],
      askPrices: Array[Double],
      askSizes: Array[Double],
      eventTimeUs: Long,
      ingestTimeUs: Long,
      sequence: Long
  ) {
    def bids: Vector[Level] = BookDelta.levels(bidPrices, bidSizes)
    def asks: Vector[Level] = BookDelta.levels(askPrices, askSizes)
  }

  object BookDelta {
    private def levels(prices: Array[Double], sizes: Array[Double]): Vector[Level] = {
      val n = math.min(prices.length, sizes.length)
      val b = Vector.newBuilder[Level]
      var i = 0
      while (i < n) { b += Level(prices(i), sizes(i)); i += 1 }
      b.result()
    }

    /** Builds from level sequences, for tests and decoders. */
    def fromLevels(
        venue: String, symbol: String, isSnapshot: Boolean,
        bids: Seq[Level], asks: Seq[Level],
        eventTimeUs: Long, ingestTimeUs: Long, sequence: Long
    ): BookDelta = BookDelta(
      venue, symbol, isSnapshot,
      bids.map(_.price).toArray, bids.map(_.size).toArray,
      asks.map(_.price).toArray, asks.map(_.size).toArray,
      eventTimeUs, ingestTimeUs, sequence
    )
  }

  final case class ChainTransfer(
      chain: String,
      token: String,
      symbol: String,
      fromAddr: String,
      toAddr: String,
      amountRaw: String,
      decimals: Int,
      blockNumber: Long,
      logIndex: Int,
      txHash: String,
      eventTimeUs: Long,
      ingestTimeUs: Long
  )

  /** Top of book plus the derived statistics the serving tier publishes. */
  final case class Quote(
      venue: String,
      symbol: String,
      bidPrice: Double,
      bidSize: Double,
      askPrice: Double,
      askSize: Double,
      imbalance: Double,
      eventTimeUs: Long,
      emitTimeUs: Long
  ) {
    def mid: Double = (bidPrice + askPrice) / 2.0

    /** Spread in basis points of the mid. Returns NaN on an empty or crossed
      * book rather than a plausible-looking zero — a zero spread is a real,
      * meaningful value and must not be confused with "no data".
      */
    def spreadBps: Double = {
      val m = mid
      if (m <= 0 || bidPrice <= 0 || askPrice <= 0) Double.NaN
      else (askPrice - bidPrice) / m * 10000.0
    }

    def isValid: Boolean =
      bidPrice > 0 && askPrice > 0 && askPrice >= bidPrice
  }

  /** A completed time window of trade activity for one venue and symbol. */
  final case class WindowBar(
      venue: String,
      symbol: String,
      windowStartUs: Long,
      windowEndUs: Long,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Double,
      vwap: Double,
      tradeCount: Long
  )

  /** A cross-exchange price divergence beyond the configured threshold. */
  final case class DivergenceEvent(
      symbol: String,
      venueA: String,
      venueB: String,
      priceA: Double,
      priceB: Double,
      divergenceBps: Double,
      zscore: Double,
      windowStartUs: Long,
      eventTimeUs: Long
  )
}
