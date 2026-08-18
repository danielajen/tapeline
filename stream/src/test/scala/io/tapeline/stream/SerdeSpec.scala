package io.tapeline.stream

import io.tapeline.stream.book.{BookSnapshot, OrderBook}
import io.tapeline.stream.model.Events.{BookDelta, DivergenceEvent, Level, Quote, Trade, WindowBar}
import io.tapeline.stream.serde.{Codec, Schemas}
import org.apache.avro.generic.GenericData
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream

class BookSnapshotSpec extends AnyFunSuite with Matchers {

  test("a book survives the round trip through its checkpointed form") {
    val delta = BookDelta(
      venue = "coinbase", symbol = "BTC-USD", isSnapshot = true,
      bids = Seq(Level(100.0, 1.0), Level(99.5, 2.0), Level(99.0, 3.0)),
      asks = Seq(Level(100.5, 1.5), Level(101.0, 2.5)),
      eventTimeUs = 1_700_000_000_000_000L, ingestTimeUs = 1_700_000_000_000_100L,
      sequence = 4242L
    )
    val original = OrderBook.empty(delta)

    val restored = OrderBook.fromSnapshot(original.toSnapshot)

    restored shouldBe original
    restored.bestBid shouldBe original.bestBid
    restored.bestAsk shouldBe original.bestAsk
    restored.lastSequence shouldBe 4242L
    restored.imbalance(3) shouldBe original.imbalance(3) +- 1e-12
  }

  test("the checkpointed form preserves book ordering") {
    val book = OrderBook.empty(BookDelta(
      "kraken", "ETH-USD", isSnapshot = true,
      bids = Seq(Level(10.0, 1), Level(30.0, 1), Level(20.0, 1)),
      asks = Seq(Level(60.0, 1), Level(40.0, 1), Level(50.0, 1)),
      eventTimeUs = 1L, ingestTimeUs = 2L, sequence = 1L
    ))

    val snap = book.toSnapshot
    // Bids descend, asks ascend — the flat form is not an arbitrary dump.
    snap.bidPrices.toVector shouldBe Vector(30.0, 20.0, 10.0)
    snap.askPrices.toVector shouldBe Vector(40.0, 50.0, 60.0)

    OrderBook.fromSnapshot(snap).bestBid.map(_.price) shouldBe Some(30.0)
  }

  test("an empty snapshot restores to an empty book") {
    OrderBook.fromSnapshot(BookSnapshot.empty) shouldBe OrderBook.empty
  }

  test("the checkpointed form holds only primitives, not Scala collections") {
    // Kryo does not round-trip Scala's immutable collections. A Vector written
    // into Flink state came back as a List and the job crash-looped on its
    // first real checkpoint. Arrays of primitives have a well-defined Kryo
    // representation; this pins the shape so the regression cannot return.
    val snap = OrderBook.empty(BookDelta(
      "coinbase", "BTC-USD", isSnapshot = true,
      bids = Seq(Level(100.0, 1.0)), asks = Seq(Level(101.0, 2.0)),
      eventTimeUs = 1L, ingestTimeUs = 2L, sequence = 5L
    )).toSnapshot

    snap.bidPrices.getClass shouldBe classOf[Array[Double]]
    snap.askSizes.getClass shouldBe classOf[Array[Double]]
    OrderBook.fromSnapshot(snap).bestAsk.map(_.size) shouldBe Some(2.0)
  }
}

class CodecSpec extends AnyFunSuite with Matchers {

  /** Serializes and deserializes through real Avro binary, which is the only
    * way to catch the Utf8-versus-String trap: a record built in memory holds
    * java.lang.String, but one decoded off the wire holds org.apache.avro
    * .util.Utf8, and a cast that works on the first fails on the second.
    */
  private def throughAvro(record: GenericRecord): GenericRecord = {
    val schema = record.getSchema
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
    encoder.flush()

    val decoder = DecoderFactory.get().binaryDecoder(out.toByteArray, null)
    new GenericDatumReader[GenericRecord](schema).read(null, decoder)
  }

  test("a trade decodes from wire-form Avro") {
    val r = new GenericData.Record(Schemas.trade)
    r.put("venue", "coinbase")
    r.put("symbol", "BTC-USD")
    r.put("trade_id", "778899")
    r.put("price", 64231.17)
    r.put("size", 0.0125)
    r.put("side", "BUY")
    r.put("event_time_us", 1_755_400_000_123_456L)
    r.put("ingest_time_us", 1_755_400_000_223_456L)
    r.put("sequence", 42L)

    val t = Codec.toTrade(throughAvro(r))

    t shouldBe Trade(
      "coinbase", "BTC-USD", "778899", 64231.17, 0.0125, "BUY",
      1_755_400_000_123_456L, 1_755_400_000_223_456L, 42L
    )
    // Explicitly: a Utf8 that leaked through would not compare equal here.
    t.venue.getClass shouldBe classOf[String]
  }

  test("a book delta decodes with both sides and their levels") {
    val r = new GenericData.Record(Schemas.bookDelta)
    r.put("venue", "binance")
    r.put("symbol", "ETH-USD")
    r.put("is_snapshot", false)

    val levelSchema = Schemas.bookDelta.getField("bids").schema().getElementType
    def level(price: Double, size: Double) = {
      val l = new GenericData.Record(levelSchema)
      l.put("price", price); l.put("size", size); l
    }
    r.put("bids", java.util.Arrays.asList(level(3100.5, 2.0), level(3100.0, 0.0)))
    r.put("asks", java.util.Arrays.asList(level(3101.0, 1.25)))
    r.put("event_time_us", 5L)
    r.put("ingest_time_us", 6L)
    r.put("sequence", 7L)

    val d = Codec.toBookDelta(throughAvro(r))

    d.venue shouldBe "binance"
    d.isSnapshot shouldBe false
    d.bids shouldBe Seq(Level(3100.5, 2.0), Level(3100.0, 0.0))
    d.asks shouldBe Seq(Level(3101.0, 1.25))
    d.sequence shouldBe 7L
  }

  test("a quote round-trips through Avro with derived columns recomputed") {
    val q = Quote(
      venue = "kraken", symbol = "BTC-USD",
      bidPrice = 64000.0, bidSize = 1.0, askPrice = 64008.0, askSize = 2.0,
      imbalance = -0.25, eventTimeUs = 111L, emitTimeUs = 222L
    )

    val decoded = Codec.toQuote(throughAvro(Codec.fromQuote(q)))

    decoded shouldBe q
    // mid and spread are derived on read, so a stale column in the topic
    // cannot skew a downstream detection.
    decoded.mid shouldBe 64004.0
    decoded.spreadBps shouldBe q.spreadBps +- 1e-9
  }

  test("NaN derived values are neutralised before they reach the topic") {
    // A NaN in an OLAP store poisons every aggregate that touches it.
    val bad = Quote("kraken", "BTC-USD", 0.0, 0.0, 0.0, 0.0, 0.0, 1L, 2L)
    bad.spreadBps.isNaN shouldBe true

    val record = Codec.fromQuote(bad)
    record.get("spread_bps").asInstanceOf[java.lang.Double].isNaN shouldBe false
  }

  test("a divergence event round-trips") {
    val d = DivergenceEvent(
      "BTC-USD", "binance", "coinbase", 64000.0, 64064.0, 10.0, 4.2, 100L, 200L
    )
    val record = throughAvro(Codec.fromDivergence(d))

    record.get("symbol").toString shouldBe "BTC-USD"
    record.get("divergence_bps") shouldBe 10.0
    record.get("zscore") shouldBe 4.2
    record.get("event_time_us") shouldBe 200L
  }

  test("a window bar round-trips, including a NaN vwap guard") {
    val bar = WindowBar("coinbase", "BTC-USD", 1000L, 2000L, 100, 110, 90, 105, 0.0, Double.NaN, 0L)
    val record = throughAvro(Codec.fromWindowBar(bar))

    record.get("vwap").asInstanceOf[java.lang.Double].isNaN shouldBe false
    record.get("window_start_us") shouldBe 1000L
    record.get("trade_count") shouldBe 0L
  }

  test("every shipped schema parses and the input schemas match the ingest tier") {
    Seq(Schemas.trade, Schemas.bookDelta, Schemas.chainTransfer,
        Schemas.quote, Schemas.divergence, Schemas.windowBar)
      .foreach(_.getFullName should startWith("io.tapeline.md."))

    // Field-for-field agreement with ingest/schemas/avro/trade.v1.avsc. CI
    // additionally diffs the files; this catches a bad edit at test time.
    Schemas.trade.getFields.size() shouldBe 9
    Schemas.bookDelta.getField("is_snapshot") should not be null
  }
}
