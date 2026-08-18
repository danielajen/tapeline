package io.tapeline.stream.jobs

import java.time.Duration

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.functions.{TradeTimestampAssigner, ValidTradeFilter}
import io.tapeline.stream.lakehouse.{Lakehouse, RowToTradeMapper}
import io.tapeline.stream.model.Events.{Trade, WindowBar}
import io.tapeline.stream.pipeline.{Connectors, FlinkEnv}
import io.tapeline.stream.serde.{Codec, Schemas}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row
import io.tapeline.stream.table.TableEnv

/** Kappa-style backfill: the same processing code, a different source.
  *
  * ==What "the same code path" actually means here==
  *
  * The claim is easy to make and easy to fake. What makes it true in this
  * repository is one line: this job calls `TradesJob.aggregate`, the exact
  * method the live streaming job calls. There is no second implementation of
  * the windowing, the VWAP, or the OHLC selection to drift out of agreement.
  * Only the source and the watermark strategy differ, and they differ because
  * they must:
  *
  *   - The source is Iceberg rather than Kafka, so it is bounded.
  *   - Out-of-orderness tolerance is larger, because a Parquet scan returns
  *     records in file order, not event order — files are read in parallel
  *     and a strict live watermark would drop most of the input as late.
  *
  * ==Why this exists==
  *
  * Kafka retention is finite. When a window definition changes, a bug is
  * found in an aggregate, or a day of state is lost, the events needed to
  * recompute are no longer in the log — but they are in Iceberg, because the
  * streaming path wrote them there. Replaying from object storage through the
  * same operators reconstructs the aggregates exactly.
  *
  * The proof and the measured comparison are in docs/BACKFILL.md.
  */
object BackfillJob {

  val Name = "backfill"

  /** Out-of-orderness allowance for replay. Deliberately much larger than the
    * live setting: file order is not event order.
    */
  val ReplayOutOfOrdernessMs: Long = 24L * 60 * 60 * 1000

  def build(
      env: StreamExecutionEnvironment,
      tEnv: StreamTableEnvironment,
      cfg: StreamConfig
  ): DataStream[WindowBar] = {

    Lakehouse.createCatalog(tEnv, cfg)

    // Predicate pushdown does the work here. Iceberg prunes by partition
    // (symbol) and by file-level column statistics on event_time_us, so a
    // one-day replay reads one day of files rather than scanning the table.
    val table = tEnv.sqlQuery(
      s"""SELECT venue, symbol, trade_id, price, size, side,
         |       event_time_us, ingest_time_us, sequence
         |FROM ${Lakehouse.qualified(cfg, Lakehouse.TradesTable)}
         |WHERE event_time_us >= ${cfg.backfillStartUs}
         |  AND event_time_us <  ${cfg.backfillEndUs}""".stripMargin
    )

    val rows: DataStream[Row] = tEnv.toDataStream(table)

    val trades: DataStream[Trade] = rows
      .map(new RowToTradeMapper, TypeInformation.of(classOf[Trade]))
      .name("decode-iceberg-trade")
      .filter(new ValidTradeFilter)
      .name("drop-invalid-trades")
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[Trade](Duration.ofMillis(ReplayOutOfOrdernessMs))
          .withTimestampAssigner(new TradeTimestampAssigner)
      )
      .name("replay-watermarks")

    // The same operators as the live job. This call is the whole claim.
    TradesJob.aggregate(trades, cfg)
  }

  def run(cfg: StreamConfig): Unit = {
    require(
      cfg.isBackfill,
      "backfill needs TAPELINE_BACKFILL_START_US and TAPELINE_BACKFILL_END_US"
    )

    val env = FlinkEnv.create(cfg)
    val tEnv = TableEnv.create(env)

    build(env, tEnv, cfg)
      .sinkTo(
        Connectors.avroSink[WindowBar](
          cfg = cfg,
          topic = cfg.topicBars,
          // A distinct transactional id prefix. Sharing one with the live
          // trades job would make the two fence each other's Kafka
          // transactions, and a backfill would silently stop live output.
          transactionalIdPrefix = cfg.transactionalId(Name),
          schemaJson = Schemas.windowBar.toString,
          keyOf = (b: WindowBar) => b.symbol,
          toRecord = (b: WindowBar) => Codec.fromWindowBar(b),
          eventTimeUsOf = (b: WindowBar) => b.windowEndUs
        )
      )
      .name("kafka:bars-backfill")

    env.execute(s"tapeline-backfill-${cfg.backfillStartUs}-${cfg.backfillEndUs}")
  }
}
