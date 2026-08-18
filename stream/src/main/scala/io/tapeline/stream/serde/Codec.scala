package io.tapeline.stream.serde

import io.tapeline.stream.model.Events._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.io.Source
import scala.jdk.CollectionConverters._

/** Avro schemas and the conversions between GenericRecord and the case
  * classes in [[io.tapeline.stream.model.Events]].
  *
  * The reader schemas for the ingestion topics are byte-identical copies of
  * `ingest/schemas/avro`, and CI fails the build if they drift (see
  * `.github/workflows/ci.yml`). Copying rather than sharing is a deliberate
  * trade: a shared artifact would couple the Go build to the JVM build for
  * the sake of three files, while a diff check costs one CI step and makes
  * the divergence impossible to miss.
  */
object Schemas {

  private def load(resource: String): Schema = {
    val stream = Option(getClass.getResourceAsStream(s"/avro/$resource"))
      .getOrElse(throw new IllegalStateException(s"missing Avro resource /avro/$resource"))
    try new Schema.Parser().parse(Source.fromInputStream(stream, "UTF-8").mkString)
    finally stream.close()
  }

  // Inputs, read from the topics the Go tier writes.
  lazy val trade: Schema = load("trade.v1.avsc")
  lazy val bookDelta: Schema = load("book_delta.v1.avsc")
  lazy val chainTransfer: Schema = load("chain_transfer.v1.avsc")

  // Outputs, written by the stream tier.
  lazy val quote: Schema = load("quote.v1.avsc")
  lazy val divergence: Schema = load("divergence.v1.avsc")
  lazy val windowBar: Schema = load("window_bar.v1.avsc")
}

/** GenericRecord conversions.
  *
  * Every accessor goes through the typed helpers below rather than casting
  * inline. Avro hands back `Utf8` for strings, not `String`, and a bare
  * `.asInstanceOf[String]` compiles, passes a unit test that constructs its
  * own records, and then fails only against real registry-decoded data.
  */
object Codec {

  private def str(r: GenericRecord, field: String): String =
    r.get(field) match {
      case null => ""
      case v    => v.toString
    }

  private def dbl(r: GenericRecord, field: String): Double =
    r.get(field) match {
      case null                => 0.0
      case v: java.lang.Double => v.doubleValue()
      case v: java.lang.Number => v.doubleValue()
      case other =>
        throw new IllegalArgumentException(s"field $field is not numeric: ${other.getClass}")
    }

  private def lng(r: GenericRecord, field: String): Long =
    r.get(field) match {
      case null                => 0L
      case v: java.lang.Number => v.longValue()
      case other =>
        throw new IllegalArgumentException(s"field $field is not numeric: ${other.getClass}")
    }

  private def int(r: GenericRecord, field: String): Int =
    r.get(field) match {
      case null                => 0
      case v: java.lang.Number => v.intValue()
      case other =>
        throw new IllegalArgumentException(s"field $field is not numeric: ${other.getClass}")
    }

  private def bool(r: GenericRecord, field: String): Boolean =
    r.get(field) match {
      case null                 => false
      case v: java.lang.Boolean => v.booleanValue()
      case other =>
        throw new IllegalArgumentException(s"field $field is not boolean: ${other.getClass}")
    }

  private def levels(r: GenericRecord, field: String): Seq[Level] =
    r.get(field) match {
      case null => Seq.empty
      case coll: java.util.Collection[_] =>
        coll.asScala.toSeq.map {
          case lvl: GenericRecord => Level(dbl(lvl, "price"), dbl(lvl, "size"))
          case other =>
            throw new IllegalArgumentException(s"level is not a record: ${other.getClass}")
        }
      case other =>
        throw new IllegalArgumentException(s"field $field is not an array: ${other.getClass}")
    }

  // --- decode ---------------------------------------------------------------

  def toTrade(r: GenericRecord): Trade = Trade(
    venue = str(r, "venue"),
    symbol = str(r, "symbol"),
    tradeId = str(r, "trade_id"),
    price = dbl(r, "price"),
    size = dbl(r, "size"),
    side = str(r, "side"),
    eventTimeUs = lng(r, "event_time_us"),
    ingestTimeUs = lng(r, "ingest_time_us"),
    sequence = lng(r, "sequence")
  )

  def toBookDelta(r: GenericRecord): BookDelta = BookDelta.fromLevels(
    venue = str(r, "venue"),
    symbol = str(r, "symbol"),
    isSnapshot = bool(r, "is_snapshot"),
    bids = levels(r, "bids"),
    asks = levels(r, "asks"),
    eventTimeUs = lng(r, "event_time_us"),
    ingestTimeUs = lng(r, "ingest_time_us"),
    sequence = lng(r, "sequence")
  )

  /** Reads back a quote the stream tier itself wrote. Needed because the
    * divergence job consumes the quote topic rather than the raw book topic;
    * `mid` and `spread_bps` are recomputed from the sides rather than read,
    * so a stale derived column in the topic cannot skew a detection.
    */
  def toQuote(r: GenericRecord): Quote = Quote(
    venue = str(r, "venue"),
    symbol = str(r, "symbol"),
    bidPrice = dbl(r, "bid_price"),
    bidSize = dbl(r, "bid_size"),
    askPrice = dbl(r, "ask_price"),
    askSize = dbl(r, "ask_size"),
    imbalance = dbl(r, "imbalance"),
    eventTimeUs = lng(r, "event_time_us"),
    emitTimeUs = lng(r, "emit_time_us")
  )

  def toChainTransfer(r: GenericRecord): ChainTransfer = ChainTransfer(
    chain = str(r, "chain"),
    token = str(r, "token"),
    symbol = str(r, "symbol"),
    fromAddr = str(r, "from_addr"),
    toAddr = str(r, "to_addr"),
    amountRaw = str(r, "amount_raw"),
    decimals = int(r, "decimals"),
    blockNumber = lng(r, "block_number"),
    logIndex = int(r, "log_index"),
    txHash = str(r, "tx_hash"),
    eventTimeUs = lng(r, "event_time_us"),
    ingestTimeUs = lng(r, "ingest_time_us")
  )

  // --- encode ---------------------------------------------------------------

  def fromQuote(q: Quote): GenericRecord = {
    val r = new GenericData.Record(Schemas.quote)
    r.put("venue", q.venue)
    r.put("symbol", q.symbol)
    r.put("bid_price", q.bidPrice)
    r.put("bid_size", q.bidSize)
    r.put("ask_price", q.askPrice)
    r.put("ask_size", q.askSize)
    r.put("mid", q.mid)
    // NaN is a legal double and Avro carries it fine, but a NaN in a
    // downstream OLAP store poisons every aggregate that touches it. The
    // book job filters invalid quotes before this point; this is the belt.
    r.put("spread_bps", if (q.spreadBps.isNaN) 0.0 else q.spreadBps)
    r.put("imbalance", q.imbalance)
    r.put("event_time_us", q.eventTimeUs)
    r.put("emit_time_us", q.emitTimeUs)
    r
  }

  def fromDivergence(d: DivergenceEvent): GenericRecord = {
    val r = new GenericData.Record(Schemas.divergence)
    r.put("symbol", d.symbol)
    r.put("venue_a", d.venueA)
    r.put("venue_b", d.venueB)
    r.put("price_a", d.priceA)
    r.put("price_b", d.priceB)
    r.put("divergence_bps", d.divergenceBps)
    r.put("zscore", d.zscore)
    r.put("window_start_us", d.windowStartUs)
    r.put("event_time_us", d.eventTimeUs)
    r
  }

  def fromWindowBar(b: WindowBar): GenericRecord = {
    val r = new GenericData.Record(Schemas.windowBar)
    r.put("venue", b.venue)
    r.put("symbol", b.symbol)
    r.put("window_start_us", b.windowStartUs)
    r.put("window_end_us", b.windowEndUs)
    r.put("open", b.open)
    r.put("high", b.high)
    r.put("low", b.low)
    r.put("close", b.close)
    r.put("volume", b.volume)
    r.put("vwap", if (b.vwap.isNaN) 0.0 else b.vwap)
    r.put("trade_count", b.tradeCount)
    r
  }
}
