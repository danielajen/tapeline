package io.tapeline.serving.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The consumer half of the distributed trace.
 *
 * <p>The Go ingestion tier writes W3C Trace Context into Kafka record headers
 * rather than into the Avro payload, precisely so that a consumer in a
 * different language can pick it up without the schema knowing anything about
 * observability. This class is the other end of that contract: it extracts the
 * `traceparent` header off a consumed record and continues the same trace.
 *
 * <p>The result is one trace spanning a venue WebSocket read in Go, a Kafka
 * hop, and a Redis write in Java — which is the only way to answer "why was
 * this quote stale" without correlating timestamps across three log streams by
 * hand.
 *
 * <p>Tracing is off unless {@code TAPELINE_OTLP_ENDPOINT} is set. That is a
 * supported configuration, not a degraded one: the unit tests and the local
 * dry-run have no collector and must still work.
 */
@Configuration
public class Tracing {

    public static final String SERVICE_NAME = "tapeline-serving";

    /** Reads Kafka record headers for the propagator. */
    private static final TextMapGetter<ConsumerRecord<?, ?>> KAFKA_GETTER =
            new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(ConsumerRecord<?, ?> record) {
                    List<String> keys = new ArrayList<>();
                    for (Header h : record.headers()) {
                        keys.add(h.key());
                    }
                    return keys;
                }

                @Override
                public String get(ConsumerRecord<?, ?> record, String key) {
                    if (record == null) {
                        return null;
                    }
                    Header header = record.headers().lastHeader(key);
                    return header == null
                            ? null
                            : new String(header.value(), StandardCharsets.UTF_8);
                }
            };

    @Bean
    public OpenTelemetry openTelemetry(
            @Value("${tapeline.otlp.endpoint:}") String endpoint,
            @Value("${tapeline.otlp.sample-ratio:0.05}") double sampleRatio) {

        if (endpoint == null || endpoint.isBlank()) {
            // No collector configured. A no-op propagates nothing and costs
            // nothing, and every call site below stays unconditional.
            return OpenTelemetry.noop();
        }

        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.of(
                        AttributeKey.stringKey("service.name"), SERVICE_NAME)));

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                // ParentBased so a trace sampled by the Go tier stays sampled
                // here. Sampling independently at each hop yields traces with
                // holes, which are worse than no trace at all.
                .setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(sampleRatio)))
                .addSpanProcessor(BatchSpanProcessor.builder(
                                OtlpGrpcSpanExporter.builder()
                                        .setEndpoint("http://" + endpoint)
                                        .setTimeout(Duration.ofSeconds(5))
                                        .build())
                        .setScheduleDelay(Duration.ofSeconds(5))
                        .build())
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();
    }

    @Bean
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer(SERVICE_NAME);
    }

    /**
     * Continues the trace a Kafka record carries, or starts a fresh root when
     * the record has no trace headers — which is what a record written before
     * tracing existed looks like.
     */
    public static Context extract(OpenTelemetry otel, ConsumerRecord<?, ?> record) {
        return otel.getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), record, KAFKA_GETTER);
    }
}
