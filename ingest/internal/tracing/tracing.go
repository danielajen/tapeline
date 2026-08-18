// Package tracing wires OpenTelemetry into the ingestion tier.
//
// The point of tracing here is not the ingestion tier on its own — that is
// one process and its logs are legible. The point is that a trace started
// when a venue frame arrives survives the hop through Kafka and is picked up
// again by the serving tier, so "why was this quote stale" can be answered
// across three processes and two languages in one view instead of by
// correlating timestamps by hand.
//
// Propagation is W3C Trace Context carried in Kafka record headers. That
// choice matters: the alternative — embedding trace ids in the Avro payload —
// would put an observability concern into the data schema, and every schema
// evolution would then have to reason about it.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// ServiceName is the resource name this tier reports as.
const ServiceName = "tapeline-ingestd"

// Config controls the exporter.
type Config struct {
	// Endpoint is the OTLP gRPC collector address. Empty disables tracing
	// entirely and installs a no-op tracer.
	Endpoint string
	// SampleRatio in [0,1]. Market data is high-volume: tracing every event
	// would cost more than the pipeline it observes, and a head sample of a
	// few percent answers the same questions.
	SampleRatio float64
	Version     string
}

// Shutdown flushes buffered spans. Always call it — an exporter that is not
// shut down drops whatever is still in its queue, which is reliably the
// spans from the incident you care about.
type Shutdown func(context.Context) error

// Init installs the global tracer provider and propagator.
func Init(ctx context.Context, cfg Config) (Shutdown, error) {
	// Tracing off is a supported configuration, not a degraded one. The local
	// dry-run mode has no collector and should not need one.
	if cfg.Endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(ServiceName),
			semconv.ServiceVersion(cfg.Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	ratio := cfg.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 0.01
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxQueueSize(4096),
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		// ParentBased so a sampled trace stays sampled end to end. Sampling
		// independently at each hop produces traces with holes in them, which
		// are worse than no trace at all.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns the ingestion tier's tracer.
func Tracer() trace.Tracer { return otel.Tracer(ServiceName) }

// KafkaHeaderCarrier adapts a string map to the OTel propagator interface so
// trace context can ride in Kafka record headers.
type KafkaHeaderCarrier map[string]string

func (c KafkaHeaderCarrier) Get(key string) string { return c[key] }
func (c KafkaHeaderCarrier) Set(key, value string) { c[key] = value }
func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectKafka serializes the span context in ctx into headers a Kafka record
// can carry. Returns nil when nothing is being traced, so an untraced
// publish adds no header bytes at all.
func InjectKafka(ctx context.Context) map[string]string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}
	carrier := KafkaHeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

// ExtractKafka rebuilds a span context from Kafka headers. The serving tier
// does the equivalent on the Java side.
func ExtractKafka(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, KafkaHeaderCarrier(headers))
}

// VenueAttributes are the attributes every ingestion span carries.
func VenueAttributes(venue, symbol, kind string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("tapeline.venue", venue),
		attribute.String("tapeline.symbol", symbol),
		attribute.String("tapeline.kind", kind),
	}
}
