package io.tapeline.stream.jobs

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.lakehouse.{Lakehouse, TradeToRowMapper}
import io.tapeline.stream.pipeline.FlinkEnv
import io.tapeline.stream.table.TableEnv
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.types.Row

/** One job, one topic: `md.trades.v1` in, Iceberg out.
  *
  * The lakehouse writer is a separate job from [[TradesJob]] for the same
  * reason the others are separate: its failure mode is different. An S3
  * commit that stalls should not stop live bar production, and a backpressure
  * spike from object storage should not propagate into the low-latency path.
  *
  * Iceberg commits once per Flink checkpoint, so the checkpoint interval is
  * also the data-freshness interval for the lakehouse and the lower bound on
  * file count. Thirty seconds is the compromise: fresh enough that a backfill
  * can start within a minute of the events it needs, slow enough that a day
  * of three symbols across three venues stays in the low thousands of files.
  */
object LakehouseJob {

  val Name = "lakehouse"

  def run(cfg: StreamConfig): Unit = {
    val env = FlinkEnv.create(cfg)
    val tEnv = TableEnv.create(env)

    Lakehouse.createCatalog(tEnv, cfg)
    Lakehouse.createTradesTable(tEnv, cfg)

    val rows: DataStream[Row] = TradesJob
      .decode(env, cfg, cfg.groupId(Name))
      .map(new TradeToRowMapper, Lakehouse.tradeRowType)
      .name("to-lakehouse-row")

    tEnv.createTemporaryView("trades_stream", tEnv.fromDataStream(rows))

    // Column order must match the DDL; see Lakehouse.tradeRowType.
    val result = tEnv.executeSql(
      s"""INSERT INTO ${Lakehouse.qualified(cfg, Lakehouse.TradesTable)}
         |SELECT * FROM trades_stream""".stripMargin
    )

    // executeSql submits the job itself, so there is no env.execute here.
    // Calling both would submit two jobs, the second of which has an empty
    // graph and fails with a message that explains none of this.
    result.await()
  }
}
