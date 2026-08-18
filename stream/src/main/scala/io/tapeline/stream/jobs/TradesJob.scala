package io.tapeline.stream.jobs

import java.time.Duration

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.functions._
import io.tapeline.stream.model.Events.{Trade, WindowBar}
import io.tapeline.stream.pipeline.{Connectors, FlinkEnv}
import io.tapeline.stream.serde.{Codec, Schemas}
import io.tapeline.stream.stats.TradeAggregate
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/** One job, one topic: `md.trades.v1` in, `md.bars.v1` out.
  *
  * Windowed OHLCV and VWAP per venue and symbol. Small, bounded state and
  * comparatively light traffic — which is exactly why it is a separate job
  * from [[BookJob]] rather than sharing one.
  */
object TradesJob {

  val Name = "trades"

  /** Decodes and timestamps the trade stream. Shared with [[MonolithicJob]]
    * and with the backfill path so all three consume identical records.
    */
  def decode(
      env: StreamExecutionEnvironment,
      cfg: StreamConfig,
      groupId: String
  ): DataStream[Trade] = {
    val source = Connectors.avroSource(
      cfg = cfg,
      topic = cfg.topicTrades,
      groupId = groupId,
      readerSchema = Schemas.trade
    )

    env
      .fromSource(source, WatermarkStrategy.noWatermarks(), s"kafka:${cfg.topicTrades}")
      .map(new ToTradeMapper, TypeInformation.of(classOf[Trade]))
      .name("decode-trade")
      .filter(new ValidTradeFilter)
      .name("drop-invalid-trades")
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[Trade](Duration.ofMillis(cfg.maxOutOfOrdernessMs))
          .withTimestampAssigner(new TradeTimestampAssigner)
          .withIdleness(Duration.ofMillis(cfg.idlenessMs))
      )
      .name("watermarks")
  }

  /** The windowing stage, factored out so the streaming path and the Kappa
    * backfill path provably run the same operators rather than two
    * implementations that happen to agree. This is what makes the replay
    * comparison in docs/BACKFILL.md meaningful.
    */
  def aggregate(trades: DataStream[Trade], cfg: StreamConfig): DataStream[WindowBar] =
    trades
      .keyBy(new TradeKeySelector)
      .window(TumblingEventTimeWindows.of(Time.seconds(cfg.windowSeconds)))
      .aggregate(
        new TradeAggregateFunction,
        new TradeBarWindowFunction,
        TypeInformation.of(classOf[TradeAggregate]),
        TypeInformation.of(classOf[TradeAggregate]),
        TypeInformation.of(classOf[WindowBar])
      )
      .name(s"bars-${cfg.windowSeconds}s")

  def build(env: StreamExecutionEnvironment, cfg: StreamConfig): DataStream[WindowBar] =
    aggregate(decode(env, cfg, cfg.groupId(Name)), cfg)

  def run(cfg: StreamConfig): Unit = {
    val env = FlinkEnv.create(cfg)

    build(env, cfg)
      .sinkTo(
        Connectors.avroSink[WindowBar](
          cfg = cfg,
          topic = cfg.topicBars,
          transactionalIdPrefix = cfg.transactionalId(Name),
          schemaJson = Schemas.windowBar.toString,
          keyOf = (b: WindowBar) => b.symbol,
          toRecord = (b: WindowBar) => Codec.fromWindowBar(b),
          eventTimeUsOf = (b: WindowBar) => b.windowEndUs
        )
      )
      .name("kafka:bars")

    env.execute("tapeline-trades")
  }
}
