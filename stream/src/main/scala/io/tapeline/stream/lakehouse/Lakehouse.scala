package io.tapeline.stream.lakehouse

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.model.Events.Trade
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row

/** Iceberg catalog and table definitions, plus the Row conversions the
  * lakehouse boundary needs.
  *
  * ==Partitioning==
  *
  * Trades are partitioned by `days(event_time)` and `symbol`. That choice is
  * the difference between a backfill that reads one day of files and one that
  * scans the whole table:
  *
  *   - Day granularity, not hour: hourly partitions on three symbols across
  *     three venues produce tens of thousands of small files a month, and
  *     small files are the standard way an Iceberg table becomes slow.
  *   - Symbol second, because every query this system serves filters on it.
  *   - Not partitioned by venue: it has three values, so it prunes almost
  *     nothing while tripling the partition count.
  */
object Lakehouse {

  val TradesTable = "trades"
  val BarsTable = "bars"

  /** Registers the Iceberg catalog. The Hadoop catalog is used because it
    * needs nothing but object storage — no Hive metastore, no Glue, no extra
    * cost line. A production deployment would use Glue or REST, which is a
    * one-line change here and no change anywhere else.
    */
  def createCatalog(tEnv: StreamTableEnvironment, cfg: StreamConfig): Unit = {
    tEnv.executeSql(
      s"""CREATE CATALOG ${cfg.icebergCatalog} WITH (
         |  'type' = 'iceberg',
         |  'catalog-type' = 'hadoop',
         |  'warehouse' = '${cfg.icebergWarehouse}',
         |  'property-version' = '1'
         |)""".stripMargin
    )
    tEnv.executeSql(s"CREATE DATABASE IF NOT EXISTS ${cfg.icebergCatalog}.${cfg.icebergDatabase}")
  }

  def createTradesTable(tEnv: StreamTableEnvironment, cfg: StreamConfig): Unit =
    tEnv.executeSql(
      s"""CREATE TABLE IF NOT EXISTS ${qualified(cfg, TradesTable)} (
         |  venue          STRING NOT NULL,
         |  symbol         STRING NOT NULL,
         |  trade_id       STRING,
         |  price          DOUBLE,
         |  size           DOUBLE,
         |  side           STRING,
         |  event_time_us  BIGINT,
         |  ingest_time_us BIGINT,
         |  sequence       BIGINT,
         |  event_time     TIMESTAMP(6)
         |) PARTITIONED BY (symbol)
         |WITH (
         |  'format-version' = '2',
         |  'write.format.default' = 'parquet',
         |  'write.parquet.compression-codec' = 'zstd',
         |  -- Target file size. The default 128MB produces too few, too large
         |  -- files for a stream committing every checkpoint; 64MB keeps
         |  -- commits cheap without drifting into small-file territory.
         |  'write.target-file-size-bytes' = '67108864',
         |  -- Expire old snapshots so metadata does not grow without bound.
         |  'history.expire.max-snapshot-age-ms' = '604800000'
         |)""".stripMargin
    )

  def qualified(cfg: StreamConfig, table: String): String =
    s"${cfg.icebergCatalog}.${cfg.icebergDatabase}.$table"

  /** The Row schema for trades, matching the table column order exactly.
    * Column order is load-bearing for `INSERT INTO ... SELECT *`, so the
    * mapper below and the DDL above must be changed together.
    */
  val tradeRowType: TypeInformation[Row] = Types.ROW_NAMED(
    Array(
      "venue", "symbol", "trade_id", "price", "size", "side",
      "event_time_us", "ingest_time_us", "sequence", "event_time"
    ),
    Types.STRING, Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.STRING,
    Types.LONG, Types.LONG, Types.LONG, Types.LOCAL_DATE_TIME
  )
}

/** Converts a canonical trade into the lakehouse Row shape. */
final class TradeToRowMapper extends MapFunction[Trade, Row] {
  override def map(t: Trade): Row = {
    val r = Row.withNames()
    r.setField("venue", t.venue)
    r.setField("symbol", t.symbol)
    r.setField("trade_id", t.tradeId)
    r.setField("price", java.lang.Double.valueOf(t.price))
    r.setField("size", java.lang.Double.valueOf(t.size))
    r.setField("side", t.side)
    r.setField("event_time_us", java.lang.Long.valueOf(t.eventTimeUs))
    r.setField("ingest_time_us", java.lang.Long.valueOf(t.ingestTimeUs))
    r.setField("sequence", java.lang.Long.valueOf(t.sequence))
    // A real timestamp column alongside the microsecond long. The long is
    // the source of truth; the timestamp exists so Iceberg can partition and
    // so a human can run a query without dividing by a million.
    r.setField(
      "event_time",
      java.time.Instant.ofEpochMilli(t.eventTimeUs / 1000L)
        .atZone(java.time.ZoneOffset.UTC).toLocalDateTime
    )
    r
  }
}

/** Reads a lakehouse Row back into a canonical trade.
  *
  * This is the other half of the Kappa loop: records written by the streaming
  * path come back through here and into the very same window operators.
  */
final class RowToTradeMapper extends MapFunction[Row, Trade] {
  private def str(r: Row, name: String): String =
    Option(r.getField(name)).map(_.toString).getOrElse("")

  private def dbl(r: Row, name: String): Double =
    Option(r.getField(name)).collect { case n: java.lang.Number => n.doubleValue() }.getOrElse(0.0)

  private def lng(r: Row, name: String): Long =
    Option(r.getField(name)).collect { case n: java.lang.Number => n.longValue() }.getOrElse(0L)

  override def map(r: Row): Trade = Trade(
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
}
