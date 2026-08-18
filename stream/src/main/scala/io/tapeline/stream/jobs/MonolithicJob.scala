package io.tapeline.stream.jobs

import io.tapeline.stream.config.StreamConfig
import io.tapeline.stream.functions._
import io.tapeline.stream.model.Events.{DivergenceEvent, Quote, WindowBar}
import io.tapeline.stream.pipeline.{Connectors, FlinkEnv}
import io.tapeline.stream.serde.{Codec, Schemas}
import org.apache.flink.api.common.typeinfo.TypeInformation

/** The version this architecture replaced. Kept deliberately.
  *
  * ==Why this file is still here==
  *
  * One job consuming every topic — book, trades, and the divergence stage
  * fused inline — is the obvious first design, and it is what Tapeline ran
  * first. It failed for a specific and instructive reason.
  *
  * The three stages have incompatible operational profiles:
  *
  *   - Book deltas arrive one to two orders of magnitude more often than
  *     trades, and carry per-key state that grows with book depth.
  *   - Trade windows hold a single small accumulator per key.
  *   - Divergence holds a rolling baseline per venue pair.
  *
  * Inside one job they share a parallelism, a memory budget, a checkpoint
  * interval, and a restart. Tuning checkpoints short enough for the book
  * state made the whole job checkpoint constantly; long enough for the rest
  * made book checkpoints time out. Raising parallelism for book throughput
  * over-provisioned two stages that did not need it. And a single failure in
  * any stage restarted all three, so a divergence bug cost order book state.
  *
  * Splitting into one job per topic costs more operational surface — three
  * deployments, three consumer groups, three sets of dashboards — and buys
  * the ability to tune, scale, and fail each independently. Netflix
  * documented reaching the same conclusion for the same reason, which was
  * reassuring but not the reason: the checkpoint tuning wall came first, and
  * the write-up came after.
  *
  * The measured before-and-after is in docs/MEASUREMENTS.md. This job stays
  * runnable so the comparison can be re-run rather than merely asserted.
  */
object MonolithicJob {

  val Name = "monolith"

  def run(cfg: StreamConfig): Unit = {
    val env = FlinkEnv.create(cfg)

    // Stage 1: books to quotes.
    val quotes: org.apache.flink.streaming.api.datastream.DataStream[Quote] =
      BookJob.build(env, cfg)

    // Stage 2: trades to bars, in the same job.
    val bars: org.apache.flink.streaming.api.datastream.DataStream[WindowBar] =
      TradesJob.aggregate(TradesJob.decode(env, cfg, cfg.groupId(Name)), cfg)

    // Stage 3: divergence fused directly onto the in-memory quote stream
    // rather than reading it back from Kafka. Lower latency, and the reason
    // a divergence failure used to take order book state with it.
    val divergences: org.apache.flink.streaming.api.datastream.DataStream[DivergenceEvent] =
      quotes
        .keyBy(new QuoteSymbolKeySelector)
        .process(
          new DivergenceFunction(cfg.divergence),
          TypeInformation.of(classOf[DivergenceEvent])
        )
        .name("divergence-detector-inline")

    quotes
      .sinkTo(
        Connectors.avroSink[Quote](
          cfg, cfg.topicQuotes, cfg.transactionalId(s"$Name-quotes"),
          Schemas.quote.toString,
          (q: Quote) => q.symbol, (q: Quote) => Codec.fromQuote(q), (q: Quote) => q.eventTimeUs
        )
      )
      .name("kafka:quotes")

    bars
      .sinkTo(
        Connectors.avroSink[WindowBar](
          cfg, cfg.topicBars, cfg.transactionalId(s"$Name-bars"),
          Schemas.windowBar.toString,
          (b: WindowBar) => b.symbol, (b: WindowBar) => Codec.fromWindowBar(b),
          (b: WindowBar) => b.windowEndUs
        )
      )
      .name("kafka:bars")

    divergences
      .sinkTo(
        Connectors.avroSink[DivergenceEvent](
          cfg, cfg.topicDivergence, cfg.transactionalId(s"$Name-divergence"),
          Schemas.divergence.toString,
          (d: DivergenceEvent) => d.symbol, (d: DivergenceEvent) => Codec.fromDivergence(d),
          (d: DivergenceEvent) => d.eventTimeUs
        )
      )
      .name("kafka:divergence")

    env.execute("tapeline-monolith")
  }
}
