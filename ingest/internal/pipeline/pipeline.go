// Package pipeline is the seam between the venue adapters and Kafka.
//
// It does four things, in this order, for every event: check sequence
// continuity, drop duplicates, encode to Avro with a registry schema id, and
// publish batched with the symbol as the partition key. Everything else —
// dialing, decoding, order books — lives on one side of it or the other.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tapeline/ingest/internal/encode"
	"github.com/tapeline/ingest/internal/gap"
	"github.com/tapeline/ingest/internal/metrics"
	"github.com/tapeline/ingest/internal/model"
	"github.com/tapeline/ingest/internal/schema"
	"github.com/tapeline/ingest/internal/sink"
	"github.com/tapeline/ingest/internal/tracing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Pipeline consumes canonical events and publishes framed Kafka records.
type Pipeline struct {
	In       <-chan model.Event
	Producer sink.Producer
	Detector *gap.Detector
	Encoders map[model.Kind]*encode.Encoder
	Topics   map[model.Kind]string
	Log      *slog.Logger

	// BatchSize and FlushInterval bound how long an event waits before it is
	// written. The interval matters more than the size: at three symbols on a
	// quiet market a size-only trigger would hold events indefinitely.
	BatchSize     int
	FlushInterval time.Duration

	// OnResync is called when a gap or regression makes local state
	// untrustworthy. The default wiring resets that venue's sequence state
	// and lets the next snapshot re-establish it.
	OnResync func(venue string, key gap.Key)

	// DropOnGap controls whether the gapped event itself is published.
	// Default false: publish it, and let the stream tier decide. A gap means
	// data is missing, not that the data in hand is wrong.
	DropOnGap bool

	// MaxPendingEvents bounds how many un-published events are retained while
	// Kafka is unreachable. Beyond it the oldest are dropped and counted, which
	// is a visible, alertable loss rather than an out-of-memory kill.
	MaxPendingEvents int

	// ConnectionScopedVenues names venues whose sequence numbers count the
	// connection, not the (symbol, channel) stream.
	//
	// Coinbase Advanced Trade is the case that forced this. It stamps one
	// monotonic `sequence_num` on every envelope across every product and
	// channel on the socket. Keying the detector on (venue, symbol, channel)
	// splits that single counter across six streams, and each one then sees a
	// number that jumps by roughly six per message — reported as a gap.
	//
	// This was not theoretical: the first live run against Coinbase produced
	// 1,915 gaps and 11,161 phantom "missed" messages in 45 seconds, while
	// Kraken and Binance produced none. Both figures were entirely artificial.
	ConnectionScopedVenues map[string]bool
}

// Stats is a snapshot of what the pipeline has done, used by tests and by
// the shutdown log line.
type Stats struct {
	Received   int64
	Published  int64
	Duplicates int64
	Gaps       int64
	Missed     int64
	EncodeErrs int64
	// PublishFailures counts flush attempts that failed. Non-zero with
	// Dropped at zero means the outage was ridden out without loss.
	PublishFailures int64
	// Dropped counts events discarded because the retry buffer was full.
	Dropped int64
}

// Run pumps events until the input channel closes or ctx is cancelled.
func (p *Pipeline) Run(ctx context.Context) (Stats, error) {
	if p.Log == nil {
		p.Log = slog.Default()
	}
	if p.Detector == nil {
		p.Detector = gap.New()
	}
	if p.BatchSize <= 0 {
		p.BatchSize = 500
	}
	if p.FlushInterval <= 0 {
		p.FlushInterval = 10 * time.Millisecond
	}
	if p.MaxPendingEvents <= 0 {
		p.MaxPendingEvents = 100_000
	}

	var stats Stats
	batch := make([]sink.Message, 0, p.BatchSize)
	ticker := time.NewTicker(p.FlushInterval)
	defer ticker.Stop()

	// flush never propagates a publish failure.
	//
	// It used to. A single failed write returned an error from Run, which
	// exited the process — so a broker restart that Kafka itself recovers from
	// in seconds took the ingestion tier down permanently, and the first chaos
	// run ended with the service dead and the claim of surviving broker
	// failure disproved.
	//
	// A publish failure is transient by default: the broker comes back, and
	// the batch is still in memory. The batch is therefore retained and
	// retried on the next tick. What is NOT unbounded is the retention — past
	// MaxPendingEvents the oldest are dropped and counted, because an
	// out-of-memory kill loses everything silently whereas a counted drop is
	// something an alert can fire on.
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := p.Producer.Publish(ctx, batch...); err != nil {
			stats.PublishFailures++
			p.Log.Warn("publish failed, retaining batch for retry",
				"buffered", len(batch), "err", err)

			if len(batch) > p.MaxPendingEvents {
				dropped := len(batch) - p.MaxPendingEvents
				batch = append(batch[:0], batch[dropped:]...)
				stats.Dropped += int64(dropped)
				metrics.EventsDropped.Add(float64(dropped))
				p.Log.Error("retry buffer full, dropped oldest events",
					"dropped", dropped, "retained", len(batch))
			}
			metrics.PipelineQueueDepth.WithLabelValues("batch").Set(float64(len(batch)))
			return
		}

		stats.Published += int64(len(batch))
		batch = batch[:0]
		metrics.PipelineQueueDepth.WithLabelValues("batch").Set(0)
	}

	for {
		select {
		case <-ctx.Done():
			// Best effort final flush on a bounded context so shutdown does
			// not silently drop what is already encoded.
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := p.Producer.Publish(flushCtx, batch...); err != nil && len(batch) > 0 {
				p.Log.Error("final flush failed", "buffered", len(batch), "err", err)
			} else {
				stats.Published += int64(len(batch))
			}
			cancel()
			return stats, nil

		case ev, ok := <-p.In:
			if !ok {
				flush()
				return stats, nil
			}

			stats.Received++
			metrics.EventsReceived.WithLabelValues(ev.Venue, string(ev.Kind)).Inc()

			if drop := p.checkSequence(ev, &stats); drop {
				continue
			}

			msg, err := p.frame(ctx, ev)
			if err != nil {
				stats.EncodeErrs++
				metrics.DecodeErrors.WithLabelValues(ev.Venue).Inc()
				p.Log.Error("encode failed", "venue", ev.Venue, "kind", ev.Kind, "err", err)
				continue
			}

			batch = append(batch, msg)
			metrics.PipelineQueueDepth.WithLabelValues("batch").Set(float64(len(batch)))

			if len(batch) >= p.BatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// checkSequence runs gap detection and reports whether to drop the event.
func (p *Pipeline) checkSequence(ev model.Event, stats *Stats) (drop bool) {
	key := p.sequenceKey(ev)
	res := p.Detector.Observe(key, ev.Sequence)

	switch res.Status {
	case gap.StatusDuplicate:
		stats.Duplicates++
		metrics.DuplicatesDropped.WithLabelValues(ev.Venue, key.Channel).Inc()
		// Duplicates are always dropped. Publishing them would push the
		// dedupe problem into the stream tier, where it costs state.
		return true

	case gap.StatusGap, gap.StatusRegression:
		stats.Gaps++
		stats.Missed += res.Missing
		metrics.SequenceGaps.WithLabelValues(ev.Venue, key.Channel).Inc()
		if res.Missing > 0 {
			metrics.SequenceMessagesMissed.WithLabelValues(ev.Venue, key.Channel).
				Add(float64(res.Missing))
		}
		p.Log.Warn("sequence discontinuity",
			"venue", ev.Venue, "symbol", ev.Symbol, "channel", key.Channel,
			"status", res.Status.String(), "expected", res.Expected, "got", res.Got,
			"missing", res.Missing)
		if p.OnResync != nil {
			p.OnResync(ev.Venue, key)
		}
		return p.DropOnGap

	default:
		return false
	}
}

// frame encodes an event and wraps it as a Kafka message.
//
// A span is started per event rather than per batch. Per batch would be
// cheaper, but a batch spans several symbols and venues, so its duration
// would not attribute to anything you could act on. Sampling — one percent
// by default — is what keeps the per-event cost acceptable.
func (p *Pipeline) frame(ctx context.Context, ev model.Event) (sink.Message, error) {
	ctx, span := tracing.Tracer().Start(ctx, "ingest.frame",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(tracing.VenueAttributes(ev.Venue, ev.Symbol, string(ev.Kind))...),
	)
	defer span.End()

	fail := func(err error) (sink.Message, error) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return sink.Message{}, err
	}

	enc, ok := p.Encoders[ev.Kind]
	if !ok {
		return fail(fmt.Errorf("no encoder registered for kind %q", ev.Kind))
	}
	topic, ok := p.Topics[ev.Kind]
	if !ok {
		return fail(fmt.Errorf("no topic configured for kind %q", ev.Kind))
	}

	payload := ev.Payload()
	if payload == nil {
		return fail(fmt.Errorf("event of kind %q carries no payload", ev.Kind))
	}

	value, err := enc.Encode(payload)
	if err != nil {
		return fail(err)
	}

	eventTimeUS, ingestTimeUS := timestamps(ev)
	metrics.ObserveSourceLag(ev.Venue, string(ev.Kind), eventTimeUS, ingestTimeUS)

	span.SetAttributes(
		attribute.String("messaging.destination.name", topic),
		attribute.Int("messaging.message.body.size", len(value)),
		attribute.Int("tapeline.schema_id", enc.SchemaID()),
		attribute.Int64("tapeline.source_lag_us", ingestTimeUS-eventTimeUS),
	)

	return sink.Message{
		Topic: topic,
		// Partition by symbol so per-symbol ordering survives into Flink.
		Key:    ev.Symbol,
		Value:  value,
		TimeUS: eventTimeUS,
		Venue:  ev.Venue,
		Kind:   string(ev.Kind),
		// W3C Trace Context in Kafka headers, not in the Avro payload:
		// observability must not become part of the data schema, or every
		// schema evolution has to reason about it.
		Trace: tracing.InjectKafka(ctx),
	}, nil
}

func timestamps(ev model.Event) (eventTimeUS, ingestTimeUS int64) {
	switch ev.Kind {
	case model.KindTrade:
		return ev.Trade.EventTimeUS, ev.Trade.IngestTimeUS
	case model.KindBookDelta:
		return ev.Book.EventTimeUS, ev.Book.IngestTimeUS
	case model.KindChainTransfer:
		return ev.Chain.EventTimeUS, ev.Chain.IngestTimeUS
	}
	return 0, 0
}

// sequenceKey chooses the granularity gap detection runs at.
//
// The default is per (venue, symbol, channel), which is what a venue that
// sequences each product stream independently requires. Venues listed in
// ConnectionScopedVenues collapse to a single key, because that is the only
// thing their counter actually describes.
func (p *Pipeline) sequenceKey(ev model.Event) gap.Key {
	if p.ConnectionScopedVenues[ev.Venue] {
		return gap.Key{Venue: ev.Venue, Symbol: "", Channel: "connection"}
	}
	return gap.Key{Venue: ev.Venue, Symbol: ev.Symbol, Channel: channelFor(ev.Kind)}
}

func channelFor(k model.Kind) string {
	switch k {
	case model.KindTrade:
		return "trades"
	case model.KindBookDelta:
		return "book"
	case model.KindChainTransfer:
		return "chain"
	}
	return "unknown"
}

// RegisterSchemas registers each kind's schema under its topic subject and
// returns ready encoders.
//
// This runs at startup and fails fast. A producer that cannot register its
// schema must not start: if it published anyway it would write records no
// consumer can resolve, and the damage outlives the process.
func RegisterSchemas(
	ctx context.Context,
	reg *schema.Client,
	topics map[model.Kind]string,
	schemasByKind map[model.Kind]string,
) (map[model.Kind]*encode.Encoder, error) {
	encoders := make(map[model.Kind]*encode.Encoder, len(topics))

	for kind, topic := range topics {
		schemaJSON, ok := schemasByKind[kind]
		if !ok {
			return nil, fmt.Errorf("no schema for kind %q", kind)
		}
		subject := schema.SubjectForTopic(topic)

		// Compatibility is checked before registering so an incompatible
		// change fails with a message naming the subject rather than a
		// registry 409 the caller has to decode.
		ok, err := reg.CheckCompatibility(ctx, subject, schemaJSON)
		if err != nil && !errors.Is(err, schema.ErrNotFound) {
			return nil, fmt.Errorf("compatibility check for %s: %w", subject, err)
		}
		if err == nil && !ok {
			return nil, fmt.Errorf(
				"schema for %s is incompatible with the registered latest version; "+
					"evolve it with defaults or bump the topic version", subject)
		}

		id, err := reg.Register(ctx, subject, schemaJSON)
		if err != nil {
			return nil, err
		}
		metrics.SchemaRegistrations.WithLabelValues(subject).Inc()

		enc, err := encode.NewEncoder(id, schemaJSON)
		if err != nil {
			return nil, fmt.Errorf("encoder for %s: %w", subject, err)
		}
		encoders[kind] = enc
	}

	return encoders, nil
}
