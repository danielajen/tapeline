package io.tapeline.stream.book

import io.tapeline.stream.model.Events.{BookDelta, Level, Quote}

import scala.collection.immutable.TreeMap

/** An immutable L2 order book for one (venue, symbol).
  *
  * Immutable, and stored whole in Flink keyed state. The alternative — a
  * mutable book mutated in place — is faster per update and wrong under
  * checkpointing, because Flink may snapshot state concurrently with
  * processing and a half-applied mutation would be persisted. Persisting a
  * torn book is the kind of bug that only appears after a restore, which is
  * to say during an incident.
  *
  * Bids are held in descending price order and asks ascending, so the best
  * price on each side is `head` on both.
  */
final case class OrderBook(
    bids: TreeMap[Double, Double],
    asks: TreeMap[Double, Double],
    lastSequence: Long,
    lastEventTimeUs: Long
) {

  def bestBid: Option[Level] = bids.headOption.map { case (p, s) => Level(p, s) }
  def bestAsk: Option[Level] = asks.headOption.map { case (p, s) => Level(p, s) }

  def isEmpty: Boolean = bids.isEmpty || asks.isEmpty

  /** A crossed book (best bid >= best ask) means the deltas were applied out
    * of order or a resync was missed. It is a correctness signal, not a
    * market condition, and the job counts it.
    */
  def isCrossed: Boolean = (for {
    b <- bids.headOption
    a <- asks.headOption
  } yield b._1 >= a._1).getOrElse(false)

  def mid: Option[Double] = for {
    b <- bids.headOption
    a <- asks.headOption
  } yield (b._1 + a._1) / 2.0

  /** Order book imbalance over the top `depth` levels, in [-1, 1].
    *
    * Positive means bid-heavy. This is the standard microstructure
    * definition: (bidQty - askQty) / (bidQty + askQty). Returns 0.0 when
    * either side is empty, since the ratio is undefined there.
    */
  def imbalance(depth: Int): Double = {
    require(depth > 0, s"depth must be positive, got $depth")
    val bidQty = bids.take(depth).values.sum
    val askQty = asks.take(depth).values.sum
    val total = bidQty + askQty
    if (total <= 0) 0.0 else (bidQty - askQty) / total
  }

  def topBids(n: Int): Seq[Level] = bids.take(n).toSeq.map { case (p, s) => Level(p, s) }
  def topAsks(n: Int): Seq[Level] = asks.take(n).toSeq.map { case (p, s) => Level(p, s) }

  /** Applies one delta, returning a new book.
    *
    * A snapshot replaces both sides. An incremental update patches them, with
    * a zero size removing the level.
    */
  def apply(delta: BookDelta): OrderBook =
    if (delta.isSnapshot) {
      OrderBook(
        bids = OrderBook.buildSide(delta.bids, OrderBook.BidOrdering),
        asks = OrderBook.buildSide(delta.asks, OrderBook.AskOrdering),
        lastSequence = delta.sequence,
        lastEventTimeUs = delta.eventTimeUs
      )
    } else {
      copy(
        bids = OrderBook.patchSide(bids, delta.bids),
        asks = OrderBook.patchSide(asks, delta.asks),
        lastSequence = delta.sequence,
        lastEventTimeUs = delta.eventTimeUs
      )
    }

  /** Drops levels beyond `maxDepth` on each side.
    *
    * Unbounded books are the standard way a stateful streaming job runs out
    * of memory: venues send deltas for levels far from the touch that are
    * never cleared, and the state grows without limit across a long-running
    * job. Trimming bounds per-key state at a known cost — quotes and
    * imbalance only ever read the top few levels anyway.
    */
  def trim(maxDepth: Int): OrderBook = {
    require(maxDepth > 0, s"maxDepth must be positive, got $maxDepth")
    if (bids.size <= maxDepth && asks.size <= maxDepth) this
    else copy(bids = bids.take(maxDepth), asks = asks.take(maxDepth))
  }

  /** Projects the book into a serveable quote, if both sides are populated. */
  def toQuote(venue: String, symbol: String, emitTimeUs: Long, imbalanceDepth: Int = 5): Option[Quote] =
    for {
      b <- bestBid
      a <- bestAsk
    } yield Quote(
      venue = venue,
      symbol = symbol,
      bidPrice = b.price,
      bidSize = b.size,
      askPrice = a.price,
      askSize = a.size,
      imbalance = imbalance(imbalanceDepth),
      eventTimeUs = lastEventTimeUs,
      emitTimeUs = emitTimeUs
    )

  def depth: (Int, Int) = (bids.size, asks.size)

  /** Projects the book into the flat form that goes into Flink state.
    *
    * The book itself is never checkpointed directly. A `TreeMap` carries its
    * `Ordering` as a field, so persisting one would serialize a Scala
    * standard-library function object into durable state — and state written
    * by one Scala version would then have to be readable by the next. The
    * flat form contains only doubles and longs, so savepoint compatibility
    * depends on this project's own types and nothing else.
    *
    * Both directions are O(n) in the retained depth, which `trim` bounds.
    */
  def toSnapshot: BookSnapshot = {
    val b = bids.toArray
    val a = asks.toArray
    BookSnapshot(
      bidPrices = b.map(_._1), bidSizes = b.map(_._2),
      askPrices = a.map(_._1), askSizes = a.map(_._2),
      lastSequence = lastSequence,
      lastEventTimeUs = lastEventTimeUs
    )
  }
}

/** The checkpoint-safe representation of an [[OrderBook]]. Levels are held in
  * book order: bids descending, asks ascending.
  *
  * ==Why parallel arrays of primitives, and not a Seq of Level==
  *
  * This held `Vector[Level]` and it was wrong in a way no unit test could see.
  * Flink serializes state it cannot analyse with Kryo, and Kryo does not
  * round-trip Scala's immutable collections: a `Vector` written into state came
  * back as a `List`. The next `foldLeft` over it then failed with
  * `NoSuchElementException: head of empty list`, and the job crash-looped the
  * first time it was ever submitted to a real cluster.
  *
  * The round trip is exercised in-process by BookSnapshotSpec, which passed
  * throughout — because in-process there is no serialization at all. Only a
  * real checkpoint exercises Kryo.
  *
  * Arrays of primitives are the fix: they have a well-defined Kryo
  * representation, they are what the state backend stores efficiently anyway,
  * and they remove Scala's collection library from the checkpoint format
  * entirely — so savepoint compatibility no longer depends on it.
  */
final case class BookSnapshot(
    bidPrices: Array[Double],
    bidSizes: Array[Double],
    askPrices: Array[Double],
    askSizes: Array[Double],
    lastSequence: Long,
    lastEventTimeUs: Long
) {
  def bids: Vector[Level] =
    bidPrices.iterator.zip(bidSizes.iterator).map { case (p, s) => Level(p, s) }.toVector
  def asks: Vector[Level] =
    askPrices.iterator.zip(askSizes.iterator).map { case (p, s) => Level(p, s) }.toVector
}

object BookSnapshot {
  val empty: BookSnapshot =
    BookSnapshot(Array.empty, Array.empty, Array.empty, Array.empty, -1L, 0L)
}

object OrderBook {

  /** Bids sort high to low, so `head` is the best bid. */
  val BidOrdering: Ordering[Double] = Ordering.Double.TotalOrdering.reverse

  /** Asks sort low to high, so `head` is the best ask. */
  val AskOrdering: Ordering[Double] = Ordering.Double.TotalOrdering

  val empty: OrderBook = OrderBook(
    bids = TreeMap.empty[Double, Double](BidOrdering),
    asks = TreeMap.empty[Double, Double](AskOrdering),
    lastSequence = -1L,
    lastEventTimeUs = 0L
  )

  /** Rebuilds a book from its checkpointed flat form. */
  def fromSnapshot(s: BookSnapshot): OrderBook = OrderBook(
    bids = buildSideArrays(s.bidPrices, s.bidSizes, BidOrdering),
    asks = buildSideArrays(s.askPrices, s.askSizes, AskOrdering),
    lastSequence = s.lastSequence,
    lastEventTimeUs = s.lastEventTimeUs
  )

  private def buildSideArrays(
      prices: Array[Double],
      sizes: Array[Double],
      ord: Ordering[Double]
  ): TreeMap[Double, Double] = {
    var acc = TreeMap.empty[Double, Double](ord)
    var i = 0
    while (i < prices.length && i < sizes.length) {
      if (sizes(i) > 0) acc = acc.updated(prices(i), sizes(i))
      i += 1
    }
    acc
  }

  private def buildSide(levels: Seq[Level], ord: Ordering[Double]): TreeMap[Double, Double] =
    levels.foldLeft(TreeMap.empty[Double, Double](ord)) { (acc, lvl) =>
      // A snapshot can legitimately contain a zero level; it simply is not
      // part of the book.
      if (lvl.size > 0) acc.updated(lvl.price, lvl.size) else acc
    }

  private def patchSide(
      side: TreeMap[Double, Double],
      updates: Seq[Level]
  ): TreeMap[Double, Double] =
    updates.foldLeft(side) { (acc, lvl) =>
      if (lvl.size > 0) acc.updated(lvl.price, lvl.size) else acc - lvl.price
    }
}
