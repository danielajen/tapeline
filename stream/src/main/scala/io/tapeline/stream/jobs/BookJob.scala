package io.tapeline.stream.jobs

import java.time.Duration

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.functions._
import io.tapeline.stream.model.Events.{BookDelta, Quote}
import io.tapeline.stream.pipeline.{Connectors, FlinkEnv}
import io.tapeline.stream.serde.{Codec, Schemas}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/** One job, one topic: `md.book.v1` in, `md.quotes.v1` out.
  *
  * The per-topic split is the central architectural decision in this tier and
  * it was arrived at the hard way — see docs/DESIGN_DECISIONS.md#d2 and
  * [[MonolithicJob]], which is the version this replaced and is kept in the
  * repository on purpose. Book traffic is one to two orders of magnitude
  * heavier than trade traffic and its state is unbounded where trade state is
  * a small accumulator; no single set of parallelism, memory and checkpoint
  * settings serves both well.
  */
object BookJob {

  val Name = "book"

  def build(env: StreamExecutionEnvironment, cfg: StreamConfig): DataStream[Quote] = {
    val source = Connectors.avroSource(
      cfg = cfg,
      topic = cfg.topicBook,
      groupId = cfg.groupId(Name),
      readerSchema = Schemas.bookDelta
    )

    val raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), s"kafka:${cfg.topicBook}")

    // Watermarks are assigned after decoding rather than on the source.
    // Assigning at the source would give per-split watermarks, which handles
    // an idle partition more precisely; doing it here keeps the timestamp
    // extractor working on a typed record instead of on GenericRecord field
    // lookups. With one partition per symbol and continuously active
    // symbols, the difference does not bite — and `withIdleness` covers the
    // case where it would.
    val deltas: DataStream[BookDelta] = raw
      .map(new ToBookDeltaMapper, TypeInformation.of(classOf[BookDelta]))
      .name("decode-book-delta")
      .filter(new ValidBookDeltaFilter)
      .name("drop-invalid-deltas")
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[BookDelta](Duration.ofMillis(cfg.maxOutOfOrdernessMs))
          .withTimestampAssigner(new BookDeltaTimestampAssigner)
          // A venue that goes quiet must not hold the watermark back for
          // every other venue in the job.
          .withIdleness(Duration.ofMillis(cfg.idlenessMs))
      )
      .name("watermarks")

    deltas
      .keyBy(new BookKeySelector)
      .process(
        new BookFunction(cfg.bookMaxDepth, cfg.imbalanceDepth, cfg.quoteEmitIntervalMs),
        TypeInformation.of(classOf[Quote])
      )
      .name("order-book")
  }

  def run(cfg: StreamConfig): Unit = {
    val env = FlinkEnv.create(cfg)
    val quotes = build(env, cfg)

    quotes
      .sinkTo(
        Connectors.avroSink[Quote](
          cfg = cfg,
          topic = cfg.topicQuotes,
          transactionalIdPrefix = cfg.transactionalId(Name),
          schemaJson = Schemas.quote.toString,
          // Keyed by symbol, matching the ingestion tier, so a downstream
          // consumer keyed on symbol sees every venue in order.
          keyOf = (q: Quote) => q.symbol,
          toRecord = (q: Quote) => Codec.fromQuote(q),
          eventTimeUsOf = (q: Quote) => q.eventTimeUs
        )
      )
      .name("kafka:quotes")

    env.execute("tapeline-book")
  }
}
