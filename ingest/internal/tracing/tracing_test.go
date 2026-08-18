package tracing

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// withTracing installs a real SDK provider so spans have valid, recorded
// contexts. The no-op provider produces invalid span contexts, and a test
// against it would pass while proving nothing.
func withTracing(t *testing.T) trace.Tracer {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp.Tracer("test")
}

// The property the whole distributed-tracing claim rests on: a span started
// in the ingestion tier must be reconstructable on the other side of Kafka,
// with the same trace id, by a different process in a different language.
// This test proves the Go half; ConfluentAvroReaderTest's Java counterpart
// consumes the same header names.
func TestTraceContextSurvivesTheKafkaHop(t *testing.T) {
	tracer := withTracing(t)

	producerCtx, span := tracer.Start(context.Background(), "ingest.frame")
	original := span.SpanContext()
	headers := InjectKafka(producerCtx)
	span.End()

	if len(headers) == 0 {
		t.Fatal("no trace headers were produced for a sampled span")
	}
	if _, ok := headers["traceparent"]; !ok {
		t.Fatalf("no W3C traceparent header; got keys %v", keysOf(headers))
	}

	// A fresh context, as a separate process would have.
	consumerCtx := ExtractKafka(context.Background(), headers)
	extracted := trace.SpanContextFromContext(consumerCtx)

	if !extracted.IsValid() {
		t.Fatal("extracted span context is invalid")
	}
	if extracted.TraceID() != original.TraceID() {
		t.Errorf("trace id changed across the hop: %s -> %s",
			original.TraceID(), extracted.TraceID())
	}
	if extracted.SpanID() != original.SpanID() {
		t.Errorf("parent span id lost: %s -> %s", original.SpanID(), extracted.SpanID())
	}
	if !extracted.IsRemote() {
		t.Error("extracted context is not marked remote; it came from another process")
	}
	// Sampling must carry across, or traces arrive with holes in them.
	if extracted.IsSampled() != original.IsSampled() {
		t.Error("sampling decision did not propagate")
	}
}

// An untraced publish must add no header bytes at all. At market data volumes
// a few dozen bytes per record on every record is real bandwidth.
func TestUntracedPublishAddsNoHeaders(t *testing.T) {
	withTracing(t)

	if headers := InjectKafka(context.Background()); headers != nil {
		t.Errorf("headers produced for an untraced context: %v", headers)
	}
}

func TestExtractIgnoresAbsentAndGarbageHeaders(t *testing.T) {
	withTracing(t)

	// A record written before tracing existed carries no headers at all.
	ctx := ExtractKafka(context.Background(), nil)
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("a valid span context was conjured from no headers")
	}

	// A malformed header must not panic or produce a bogus context — a
	// consumer that crashes on a bad header is worse than one that ignores it.
	ctx = ExtractKafka(context.Background(), map[string]string{"traceparent": "not-a-traceparent"})
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("a garbage traceparent produced a valid span context")
	}
}

func TestInitWithNoEndpointIsANoOpNotAnError(t *testing.T) {
	// Tracing off is a supported configuration: the dry-run mode has no
	// collector and must still start.
	shutdown, err := Init(context.Background(), Config{Endpoint: ""})
	if err != nil {
		t.Fatalf("Init with no endpoint: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown: %v", err)
	}
	if InjectKafka(context.Background()) != nil {
		t.Error("the no-op provider produced trace headers")
	}
}

func TestVenueAttributes(t *testing.T) {
	attrs := VenueAttributes("coinbase", "BTC-USD", "trade")
	if len(attrs) != 3 {
		t.Fatalf("got %d attributes, want 3", len(attrs))
	}
	for _, a := range attrs {
		if !strings.HasPrefix(string(a.Key), "tapeline.") {
			t.Errorf("attribute %q is not namespaced", a.Key)
		}
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
