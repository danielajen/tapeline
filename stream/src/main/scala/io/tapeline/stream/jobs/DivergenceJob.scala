package io.tapeline.stream.jobs

import java.time.Duration

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.functions._
import io.tapeline.stream.model.Events.{DivergenceEvent, Quote}
import io.tapeline.stream.pipeline.{Connectors, FlinkEnv}
import io.tapeline.stream.serde.{Codec, Schemas}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/** One job, one topic: `md.quotes.v1` in, `md.divergence.v1` out.
  *
  * Reads the quote stream that [[BookJob]] produces rather than the raw book
  * topic. Chaining through Kafka instead of fusing the two jobs costs a
  * serialization hop and buys three things: the jobs scale and restart
  * independently, the quote stream is reusable by the serving tier and the
  * OLAP loader, and a bad deploy of the divergence logic cannot take order
  * book maintenance down with it.
  */
object DivergenceJob {

  val Name = "divergence"

  def build(env: StreamExecutionEnvironment, cfg: StreamConfig): DataStream[DivergenceEvent] = {
    val source = Connectors.avroSource(
      cfg = cfg,
      topic = cfg.topicQuotes,
      groupId = cfg.groupId(Name),
      readerSchema = Schemas.quote
    )

    val quotes: DataStream[Quote] = env
      .fromSource(source, WatermarkStrategy.noWatermarks(), s"kafka:${cfg.topicQuotes}")
      .map(new QuoteMapper, TypeInformation.of(classOf[Quote]))
      .name("decode-quote")
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[Quote](Duration.ofMillis(cfg.maxOutOfOrdernessMs))
          .withTimestampAssigner(new QuoteTimestampAssigner)
          .withIdleness(Duration.ofMillis(cfg.idlenessMs))
      )
      .name("watermarks")

    quotes
      .keyBy(new QuoteSymbolKeySelector)
      .process(
        new DivergenceFunction(cfg.divergence),
        TypeInformation.of(classOf[DivergenceEvent])
      )
      .name("divergence-detector")
  }

  def run(cfg: StreamConfig): Unit = {
    val env = FlinkEnv.create(cfg)

    build(env, cfg)
      .sinkTo(
        Connectors.avroSink[DivergenceEvent](
          cfg = cfg,
          topic = cfg.topicDivergence,
          transactionalIdPrefix = cfg.transactionalId(Name),
          schemaJson = Schemas.divergence.toString,
          keyOf = (d: DivergenceEvent) => d.symbol,
          toRecord = (d: DivergenceEvent) => Codec.fromDivergence(d),
          eventTimeUsOf = (d: DivergenceEvent) => d.eventTimeUs
        )
      )
      .name("kafka:divergence")

    env.execute("tapeline-divergence")
  }
}
