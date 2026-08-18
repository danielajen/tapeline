package io.tapeline.stream.functions

import io.tapeline.stream.model.Events.{BookDelta, Quote, Trade}
import io.tapeline.stream.serde.Codec
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.java.functions.KeySelector

/** Named top-level operator classes.
  *
  * These exist as named classes rather than anonymous inner classes or
  * lambdas for one reason: an anonymous class defined inside a Scala object
  * captures a reference to that object, and Flink serializes every operator
  * to ship it to the TaskManagers. The failure is a NotSerializableException
  * at submit time naming a class you did not write, which is a genuinely
  * confusing hour the first time it happens. Top-level classes with no
  * captured scope cannot do that.
  */
final class ToTradeMapper extends MapFunction[GenericRecord, Trade] {
  override def map(r: GenericRecord): Trade = Codec.toTrade(r)
}

final class ToBookDeltaMapper extends MapFunction[GenericRecord, BookDelta] {
  override def map(r: GenericRecord): BookDelta = Codec.toBookDelta(r)
}

final class QuoteMapper extends MapFunction[GenericRecord, Quote] {
  override def map(r: GenericRecord): Quote = Codec.toQuote(r)
}

final class BookKeySelector extends KeySelector[BookDelta, String] {
  override def getKey(d: BookDelta): String = BookFunction.keyOf(d)
}

final class TradeKeySelector extends KeySelector[Trade, String] {
  override def getKey(t: Trade): String = TradeWindow.keyOf(t)
}

/** Divergence is keyed by symbol alone so every venue meets on one key. */
final class QuoteSymbolKeySelector extends KeySelector[Quote, String] {
  override def getKey(q: Quote): String = q.symbol
}

final class TradeTimestampAssigner extends SerializableTimestampAssigner[Trade] {
  // Flink works in milliseconds; the canonical unit here is microseconds.
  override def extractTimestamp(t: Trade, recordTimestamp: Long): Long = t.eventTimeUs / 1000L
}

final class BookDeltaTimestampAssigner extends SerializableTimestampAssigner[BookDelta] {
  override def extractTimestamp(d: BookDelta, recordTimestamp: Long): Long = d.eventTimeUs / 1000L
}

final class QuoteTimestampAssigner extends SerializableTimestampAssigner[Quote] {
  override def extractTimestamp(q: Quote, recordTimestamp: Long): Long = q.eventTimeUs / 1000L
}

/** Drops records whose venue timestamp is missing or implausible.
  *
  * A zero event time becomes epoch 1970 once it reaches the watermark
  * generator, which pins the watermark at the beginning of time and stalls
  * every event-time window in the job indefinitely. One malformed record can
  * therefore stop all output, so it is cheaper to drop it here.
  */
final class ValidTradeFilter extends FilterFunction[Trade] {
  override def filter(t: Trade): Boolean =
    t.eventTimeUs > 0 && t.price > 0 && t.size >= 0 && t.symbol.nonEmpty
}

final class ValidBookDeltaFilter extends FilterFunction[BookDelta] {
  override def filter(d: BookDelta): Boolean =
    d.eventTimeUs > 0 && d.symbol.nonEmpty && (d.bids.nonEmpty || d.asks.nonEmpty)
}
