package io.tapeline.serving.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/**
 * The Java half of the cross-language trace contract.
 *
 * <p>The Go tier's {@code tracing_test.go} proves a span context survives
 * injection into Kafka headers. This proves the same bytes are readable on the
 * other side by a different runtime — the half that makes the distributed
 * tracing claim true rather than half-true.
 */
class TracingTest {

    /** A real traceparent, in exactly the form the Go tier writes. */
    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String PARENT_SPAN_ID = "00f067aa0ba902b7";

    private static OpenTelemetry sdk() {
        return OpenTelemetrySdk.builder()
                .setTracerProvider(
                        SdkTracerProvider.builder().setSampler(Sampler.alwaysOn()).build())
                .setPropagators(
                        ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    private static ConsumerRecord<String, byte[]> record(String traceparent) {
        ConsumerRecord<String, byte[]> r =
                new ConsumerRecord<>("md.quotes.v1", 0, 42L, "BTC-USD", new byte[] {1});
        if (traceparent != null) {
            r.headers().add("traceparent", traceparent.getBytes(StandardCharsets.UTF_8));
        }
        r.headers().add("venue", "coinbase".getBytes(StandardCharsets.UTF_8));
        return r;
    }

    @Test
    void aTraceStartedInGoContinuesInJava() {
        Context extracted = Tracing.extract(sdk(), record(TRACEPARENT));
        Span span = Span.fromContext(extracted);

        assertThat(span.getSpanContext().isValid()).isTrue();
        assertThat(span.getSpanContext().getTraceId())
                .as("the trace id must survive the Kafka hop unchanged")
                .isEqualTo(TRACE_ID);
        assertThat(span.getSpanContext().getSpanId())
                .as("the Go span becomes this span's parent")
                .isEqualTo(PARENT_SPAN_ID);
        assertThat(span.getSpanContext().isRemote()).isTrue();
        assertThat(span.getSpanContext().isSampled())
                .as("sampling must propagate, or traces arrive with holes")
                .isTrue();
    }

    @Test
    void aRecordWithNoTraceHeadersStartsAFreshRoot() {
        // A record written before tracing existed must not crash the consumer.
        assertThat(Span.fromContext(Tracing.extract(sdk(), record(null)))
                        .getSpanContext().isValid())
                .isFalse();
    }

    @Test
    void aMalformedTraceparentIsIgnoredRatherThanFatal() {
        assertThat(Span.fromContext(Tracing.extract(sdk(), record("not-a-traceparent")))
                        .getSpanContext().isValid())
                .isFalse();
    }

    @Test
    void noCollectorConfiguredYieldsANoOpRatherThanAFailure() {
        // Tracing off is a supported configuration, not a degraded one.
        OpenTelemetry noop = new Tracing().openTelemetry("", 0.05);
        assertThat(noop).isNotNull();
        assertThat(Span.fromContext(Tracing.extract(noop, record(TRACEPARENT)))
                        .getSpanContext().isValid())
                .isFalse();
    }
}
