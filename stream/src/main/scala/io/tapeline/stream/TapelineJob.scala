package io.tapeline.stream

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.jobs._

/** Entry point for every Tapeline Flink job.
  *
  * One jar, one main class, a job name argument. The alternative — a jar per
  * job — means three artifacts to build, version and promote for code that is
  * ninety percent shared, and makes it possible to run mismatched versions of
  * jobs that exchange records through Kafka.
  *
  * {{{
  *   flink run -c io.tapeline.stream.TapelineJob tapeline-stream.jar book
  *   flink run -c io.tapeline.stream.TapelineJob tapeline-stream.jar trades
  *   flink run -c io.tapeline.stream.TapelineJob tapeline-stream.jar divergence
  *   flink run -c io.tapeline.stream.TapelineJob tapeline-stream.jar lakehouse
  *   flink run -c io.tapeline.stream.TapelineJob tapeline-stream.jar backfill
  *   flink run -c io.tapeline.stream.TapelineJob tapeline-stream.jar monolith
  * }}}
  */
object TapelineJob {

  private val Usage =
    s"""Usage: TapelineJob <job>
       |
       |Jobs:
       |  ${BookJob.Name}        md.book.v1    -> md.quotes.v1      (stateful L2 books)
       |  ${TradesJob.Name}      md.trades.v1  -> md.bars.v1        (windowed OHLCV + VWAP)
       |  ${DivergenceJob.Name}  md.quotes.v1  -> md.divergence.v1  (cross-exchange divergence)
       |  ${LakehouseJob.Name}   md.trades.v1  -> Iceberg           (lakehouse writer)
       |  ${BackfillJob.Name}     Iceberg      -> md.bars.v1        (Kappa replay)
       |  ${MonolithicJob.Name}       all topics in one job (superseded; see the class docs)
       |
       |Configuration is read from TAPELINE_* environment variables.
       |""".stripMargin

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      System.err.println(Usage)
      sys.exit(2)
    }

    val cfg = StreamConfig.fromEnv()

    args(0) match {
      case BookJob.Name       => BookJob.run(cfg)
      case TradesJob.Name     => TradesJob.run(cfg)
      case DivergenceJob.Name => DivergenceJob.run(cfg)
      case LakehouseJob.Name  => LakehouseJob.run(cfg)
      case BackfillJob.Name   => BackfillJob.run(cfg)
      case MonolithicJob.Name => MonolithicJob.run(cfg)
      case unknown =>
        System.err.println(s"unknown job: $unknown\n\n$Usage")
        sys.exit(2)
    }
  }
}
