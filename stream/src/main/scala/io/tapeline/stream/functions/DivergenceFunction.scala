package io.tapeline.stream.functions

import io.tapeline.stream.divergence.Divergence
import io.tapeline.stream.model.Events.{DivergenceEvent, Quote}
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.jdk.CollectionConverters._

/** Cross-exchange divergence detection, keyed by symbol.
  *
  * Keyed by symbol alone — not by venue — because the whole point is to
  * compare venues against each other, so every venue's quote for a symbol
  * must land on the same key. That makes symbol the partitioning unit for
  * this operator and is why the ingestion tier partitions Kafka by symbol.
  *
  * As with the book function, the logic itself lives in the pure
  * [[Divergence]] object and this class only supplies state and metrics.
  */
class DivergenceFunction(cfg: Divergence.Config)
    extends KeyedProcessFunction[String, Quote, DivergenceEvent] {

  @transient private var latestByVenue: MapState[String, Quote] = _
  @transient private var baselines: ValueState[Divergence.Baselines] = _

  @transient private var evaluations: Counter = _
  @transient private var alertsEmitted: Counter = _

  override def open(ctx: OpenContext): Unit = {
    latestByVenue = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Quote](
        "latest-by-venue", Types.STRING, TypeInformation.of(classOf[Quote])
      )
    )
    baselines = getRuntimeContext.getState(
      new ValueStateDescriptor[Divergence.Baselines](
        "baselines", TypeInformation.of(classOf[Divergence.Baselines])
      )
    )

    val group = getRuntimeContext.getMetricGroup.addGroup("tapeline").addGroup("divergence")
    evaluations = group.counter("evaluations")
    alertsEmitted = group.counter("alerts_emitted")
  }

  override def processElement(
      quote: Quote,
      ctx: KeyedProcessFunction[String, Quote, DivergenceEvent]#Context,
      out: Collector[DivergenceEvent]
  ): Unit = {

    // A late quote must not overwrite a newer one. Streams from three venues
    // are merged upstream and arrival order across them is not event order.
    val existing = Option(latestByVenue.get(quote.venue))
    if (existing.exists(_.eventTimeUs > quote.eventTimeUs)) return

    latestByVenue.put(quote.venue, quote)

    val quotes = latestByVenue.entries().asScala
      .map(e => e.getKey -> e.getValue)
      .toMap

    val current = Option(baselines.value()).getOrElse(Divergence.Baselines.empty)

    // Event time, not wall clock: under backfill the two are months apart and
    // wall clock would mark every replayed quote stale.
    val result = Divergence.evaluate(
      symbol = ctx.getCurrentKey,
      quotes = quotes,
      baselines = current,
      nowUs = quote.eventTimeUs,
      cfg = cfg
    )

    evaluations.inc()
    baselines.update(result.baselines)

    result.events.foreach { ev =>
      alertsEmitted.inc()
      out.collect(ev)
    }
  }
}
