package io.tapeline.stream.config

import io.tapeline.stream.divergence.Divergence

/** Configuration for every Tapeline Flink job.
  *
  * Read from environment variables with the same `TAPELINE_` prefix the Go
  * tier uses, so one ConfigMap configures both tiers and the topic names can
  * only be wrong in one place.
  */
final case class StreamConfig(
    kafkaBrokers: String,
    schemaRegistryUrl: String,
    groupIdPrefix: String,
    transactionalIdPrefix: String,
    topicTrades: String,
    topicBook: String,
    topicChain: String,
    topicQuotes: String,
    topicDivergence: String,
    topicBars: String,
    checkpointIntervalMs: Long,
    checkpointTimeoutMs: Long,
    minPauseBetweenCheckpointsMs: Long,
    maxOutOfOrdernessMs: Long,
    idlenessMs: Long,
    windowSeconds: Int,
    bookMaxDepth: Int,
    imbalanceDepth: Int,
    quoteEmitIntervalMs: Long,
    parallelism: Int,
    icebergWarehouse: String,
    icebergCatalog: String,
    icebergDatabase: String,
    divergence: Divergence.Config,
    /** Backfill mode replays from Iceberg instead of Kafka. See BackfillJob. */
    backfillStartUs: Long,
    backfillEndUs: Long
) {

  def groupId(job: String): String = s"$groupIdPrefix.$job"
  def transactionalId(job: String): String = s"$transactionalIdPrefix.$job"

  /** True when the job should replay from Iceberg rather than read Kafka. */
  def isBackfill: Boolean = backfillStartUs > 0 && backfillEndUs > backfillStartUs

  /** Rejects settings that would otherwise fail late and obscurely.
    *
    * The transactional id prefix is the one worth being strict about: two
    * jobs sharing a prefix fence each other's Kafka transactions, and the
    * symptom is one job quietly failing to commit rather than any error that
    * names the collision.
    */
  def validated(): StreamConfig = {
    require(kafkaBrokers.nonEmpty, "TAPELINE_KAFKA_BROKERS must be set")
    require(schemaRegistryUrl.nonEmpty, "TAPELINE_SCHEMA_REGISTRY_URL must be set")
    require(transactionalIdPrefix.nonEmpty, "TAPELINE_TXN_ID_PREFIX must be set")
    require(windowSeconds > 0, s"window seconds must be positive, got $windowSeconds")
    require(bookMaxDepth > 0, s"book depth must be positive, got $bookMaxDepth")
    require(
      imbalanceDepth <= bookMaxDepth,
      s"imbalance depth $imbalanceDepth exceeds retained book depth $bookMaxDepth"
    )
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(
      checkpointTimeoutMs > checkpointIntervalMs,
      "checkpoint timeout must exceed the interval, or every checkpoint races the next"
    )
    require(
      backfillEndUs == 0L || backfillEndUs > backfillStartUs,
      s"backfill end ($backfillEndUs) must be after start ($backfillStartUs)"
    )
    this
  }
}

object StreamConfig {

  private def env(key: String, default: String): String =
    sys.env.get(key).filter(_.nonEmpty).getOrElse(default)

  private def envLong(key: String, default: Long): Long =
    sys.env.get(key).filter(_.nonEmpty) match {
      case None => default
      case Some(v) =>
        v.toLongOption.getOrElse(
          throw new IllegalArgumentException(s"$key=$v is not a number")
        )
    }

  private def envInt(key: String, default: Int): Int = envLong(key, default.toLong).toInt

  private def envDouble(key: String, default: Double): Double =
    sys.env.get(key).filter(_.nonEmpty) match {
      case None => default
      case Some(v) =>
        v.toDoubleOption.getOrElse(
          throw new IllegalArgumentException(s"$key=$v is not a number")
        )
    }

  def fromEnv(): StreamConfig = {
    val cfg = StreamConfig(
      kafkaBrokers = env("TAPELINE_KAFKA_BROKERS", "localhost:9092"),
      schemaRegistryUrl = env("TAPELINE_SCHEMA_REGISTRY_URL", "http://localhost:8081"),
      groupIdPrefix = env("TAPELINE_GROUP_ID_PREFIX", "tapeline"),
      transactionalIdPrefix = env("TAPELINE_TXN_ID_PREFIX", "tapeline-txn"),
      topicTrades = env("TAPELINE_TOPIC_TRADES", "md.trades.v1"),
      topicBook = env("TAPELINE_TOPIC_BOOK", "md.book.v1"),
      topicChain = env("TAPELINE_TOPIC_CHAIN", "md.chain.v1"),
      topicQuotes = env("TAPELINE_TOPIC_QUOTES", "md.quotes.v1"),
      topicDivergence = env("TAPELINE_TOPIC_DIVERGENCE", "md.divergence.v1"),
      topicBars = env("TAPELINE_TOPIC_BARS", "md.bars.v1"),
      checkpointIntervalMs = envLong("TAPELINE_CHECKPOINT_INTERVAL_MS", 30_000L),
      checkpointTimeoutMs = envLong("TAPELINE_CHECKPOINT_TIMEOUT_MS", 120_000L),
      minPauseBetweenCheckpointsMs = envLong("TAPELINE_CHECKPOINT_MIN_PAUSE_MS", 5_000L),
      maxOutOfOrdernessMs = envLong("TAPELINE_MAX_OUT_OF_ORDERNESS_MS", 5_000L),
      idlenessMs = envLong("TAPELINE_IDLENESS_MS", 60_000L),
      windowSeconds = envInt("TAPELINE_WINDOW_SECONDS", 1),
      bookMaxDepth = envInt("TAPELINE_BOOK_MAX_DEPTH", 50),
      imbalanceDepth = envInt("TAPELINE_IMBALANCE_DEPTH", 5),
      quoteEmitIntervalMs = envLong("TAPELINE_QUOTE_EMIT_INTERVAL_MS", 100L),
      parallelism = envInt("TAPELINE_PARALLELISM", 2),
      icebergWarehouse = env("TAPELINE_ICEBERG_WAREHOUSE", "s3://tapeline-lakehouse/warehouse"),
      icebergCatalog = env("TAPELINE_ICEBERG_CATALOG", "tapeline"),
      icebergDatabase = env("TAPELINE_ICEBERG_DATABASE", "market_data"),
      divergence = Divergence.Config(
        minDivergenceBps = envDouble("TAPELINE_DIVERGENCE_MIN_BPS", 5.0),
        minZscore = envDouble("TAPELINE_DIVERGENCE_MIN_ZSCORE", 3.0),
        baselineWindow = envInt("TAPELINE_DIVERGENCE_BASELINE_WINDOW", 600),
        minObservations = envInt("TAPELINE_DIVERGENCE_MIN_OBSERVATIONS", 30),
        maxQuoteAgeUs = envLong("TAPELINE_DIVERGENCE_MAX_QUOTE_AGE_US", 5_000_000L)
      ),
      backfillStartUs = envLong("TAPELINE_BACKFILL_START_US", 0L),
      backfillEndUs = envLong("TAPELINE_BACKFILL_END_US", 0L)
    )
    cfg.validated()
  }
}
