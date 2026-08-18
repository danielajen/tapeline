package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tapeline/ingest/internal/encode"
	"github.com/tapeline/ingest/internal/gap"
	"github.com/tapeline/ingest/internal/model"
	"github.com/tapeline/ingest/internal/sink"
	avroschema "github.com/tapeline/ingest/schemas"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func testEncoders(t *testing.T) map[model.Kind]*encode.Encoder {
	t.Helper()
	out := make(map[model.Kind]*encode.Encoder)
	for kind, schemaJSON := range avroschema.Current() {
		enc, err := encode.NewEncoder(int(len(kind)), schemaJSON)
		if err != nil {
			t.Fatalf("encoder for %s: %v", kind, err)
		}
		out[kind] = enc
	}
	return out
}

func testTopics() map[model.Kind]string {
	return map[model.Kind]string{
		model.KindTrade:         "md.trades.v1",
		model.KindBookDelta:     "md.book.v1",
		model.KindChainTransfer: "md.chain.v1",
	}
}

func tradeEvent(venue, symbol string, seq int64) model.Event {
	return model.Event{
		Kind: model.KindTrade, Venue: venue, Symbol: symbol, Sequence: seq,
		Trade: &model.Trade{
			Venue: venue, Symbol: symbol, TradeID: "t", Price: 100, Size: 1,
			Side: model.SideBuy, EventTimeUS: 1_000_000, IngestTimeUS: 1_050_000,
			Sequence: seq,
		},
	}
}

// runPipeline feeds events, closes the input, and returns the outcome.
func runPipeline(t *testing.T, p *Pipeline, in chan model.Event, events []model.Event) Stats {
	t.Helper()
	p.In = in
	done := make(chan Stats, 1)
	errCh := make(chan error, 1)
	go func() {
		s, err := p.Run(context.Background())
		errCh <- err
		done <- s
	}()

	for _, ev := range events {
		in <- ev
	}
	close(in)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("pipeline: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not finish")
	}
	return <-done
}

func TestPublishesFramedRecordsKeyedBySymbol(t *testing.T) {
	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 10, FlushInterval: 5 * time.Millisecond,
	}

	stats := runPipeline(t, p, make(chan model.Event, 8), []model.Event{
		tradeEvent("coinbase", "BTC-USD", 1),
		tradeEvent("coinbase", "BTC-USD", 2),
		tradeEvent("coinbase", "ETH-USD", 1),
	})

	if stats.Received != 3 || stats.Published != 3 {
		t.Fatalf("stats = %+v, want 3 received and 3 published", stats)
	}

	msgs := prod.Messages()
	if len(msgs) != 3 {
		t.Fatalf("published %d messages, want 3", len(msgs))
	}
	for i, m := range msgs {
		if m.Topic != "md.trades.v1" {
			t.Errorf("message %d topic = %q", i, m.Topic)
		}
		// The partition key is the symbol. If this ever becomes the venue or
		// a random key, per-symbol ordering breaks in Flink.
		if m.Key != m.Venue && m.Key == "" {
			t.Errorf("message %d has an empty key", i)
		}
		if m.Value[0] != encode.MagicByte {
			t.Errorf("message %d is not Confluent-framed: first byte 0x%02x", i, m.Value[0])
		}
		if m.Kind != string(model.KindTrade) {
			t.Errorf("message %d kind header = %q", i, m.Kind)
		}
	}
	if msgs[0].Key != "BTC-USD" || msgs[2].Key != "ETH-USD" {
		t.Errorf("partition keys = %q, %q; want the symbols", msgs[0].Key, msgs[2].Key)
	}
}

func TestDuplicatesAreDroppedBeforeKafka(t *testing.T) {
	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 10, FlushInterval: 5 * time.Millisecond,
	}

	stats := runPipeline(t, p, make(chan model.Event, 8), []model.Event{
		tradeEvent("kraken", "BTC-USD", 1),
		tradeEvent("kraken", "BTC-USD", 2),
		tradeEvent("kraken", "BTC-USD", 2), // retransmit
		tradeEvent("kraken", "BTC-USD", 3),
	})

	if stats.Duplicates != 1 {
		t.Errorf("duplicates = %d, want 1", stats.Duplicates)
	}
	// Deduping here rather than in Flink is the point: it costs a map lookup
	// upstream instead of keyed state downstream.
	if got := prod.Len(); got != 3 {
		t.Errorf("published %d messages, want 3 (the duplicate must not reach Kafka)", got)
	}
}

func TestGapIsRecordedAndTheEventStillPublishes(t *testing.T) {
	prod := sink.NewMemoryProducer()
	var resyncs int

	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 10, FlushInterval: 5 * time.Millisecond,
		OnResync: func(string, gap.Key) { resyncs++ },
	}

	stats := runPipeline(t, p, make(chan model.Event, 8), []model.Event{
		tradeEvent("binance", "BTC-USD", 10),
		tradeEvent("binance", "BTC-USD", 15), // 4 missing
	})

	if stats.Gaps != 1 {
		t.Errorf("gaps = %d, want 1", stats.Gaps)
	}
	if stats.Missed != 4 {
		t.Errorf("missed = %d, want 4", stats.Missed)
	}
	if resyncs != 1 {
		t.Errorf("resync callback fired %d times, want 1", resyncs)
	}
	// A gap means data is missing, not that the message in hand is wrong.
	// Dropping it would discard good data on top of the data already lost.
	if got := prod.Len(); got != 2 {
		t.Errorf("published %d, want 2 — the gapped event is still valid data", got)
	}
}

func TestDropOnGapSuppressesTheEvent(t *testing.T) {
	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 10, FlushInterval: 5 * time.Millisecond,
		DropOnGap: true,
	}

	runPipeline(t, p, make(chan model.Event, 8), []model.Event{
		tradeEvent("binance", "BTC-USD", 10),
		tradeEvent("binance", "BTC-USD", 15),
	})

	if got := prod.Len(); got != 1 {
		t.Errorf("published %d, want 1 with DropOnGap", got)
	}
}

func TestUnsequencedVenuesPublishEverything(t *testing.T) {
	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 10, FlushInterval: 5 * time.Millisecond,
	}

	events := make([]model.Event, 0, 5)
	for i := 0; i < 5; i++ {
		events = append(events, model.Event{
			Kind: model.KindBookDelta, Venue: "kraken", Symbol: "ETH-USD",
			Sequence: model.NoSequence,
			Book: &model.BookDelta{
				Venue: "kraken", Symbol: "ETH-USD",
				Bids: []model.Level{{Price: 3100, Size: 1}}, Asks: nil,
				EventTimeUS: 1, IngestTimeUS: 2, Sequence: model.NoSequence,
			},
		})
	}

	stats := runPipeline(t, p, make(chan model.Event, 8), events)
	if stats.Gaps != 0 || stats.Duplicates != 0 {
		t.Errorf("unsequenced stream produced gaps=%d duplicates=%d, want 0/0",
			stats.Gaps, stats.Duplicates)
	}
	if prod.Len() != 5 {
		t.Errorf("published %d, want 5", prod.Len())
	}
}

func TestFlushIntervalReleasesPartialBatches(t *testing.T) {
	prod := sink.NewMemoryProducer()
	in := make(chan model.Event, 4)
	p := &Pipeline{
		In:       in,
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		// A batch size far above what the test sends: only the ticker can
		// release these. On a quiet market this is the path that runs.
		BatchSize: 10_000, FlushInterval: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = p.Run(ctx) }()

	in <- tradeEvent("coinbase", "BTC-USD", 1)

	deadline := time.After(2 * time.Second)
	for prod.Len() == 0 {
		select {
		case <-deadline:
			t.Fatal("partial batch never flushed; events would sit until the buffer filled")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// A publish failure must NOT stop the pipeline.
//
// It used to: a single failed write returned an error from Run and the process
// exited. The first live chaos run killed the Kafka broker and ended with the
// ingestion tier dead, disproving the "survives broker failure" claim outright.
// A broker restart is transient; the batch is still in memory and the right
// response is to retry it, not to quit.
func TestPublishFailureIsRetriedNotFatal(t *testing.T) {
	prod := sink.NewMemoryProducer()
	prod.SetFailure(errors.New("broker unavailable"))

	in := make(chan model.Event, 8)
	p := &Pipeline{
		In:       in,
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 1, FlushInterval: 5 * time.Millisecond,
	}

	done := make(chan Stats, 1)
	go func() {
		st, err := p.Run(context.Background())
		if err != nil {
			t.Errorf("Run returned %v; a transient publish failure must not be fatal", err)
		}
		done <- st
	}()

	in <- tradeEvent("coinbase", "BTC-USD", 1)
	time.Sleep(60 * time.Millisecond)

	// The broker comes back.
	prod.SetFailure(nil)
	in <- tradeEvent("coinbase", "BTC-USD", 2)
	close(in)

	select {
	case st := <-done:
		if st.PublishFailures == 0 {
			t.Error("publish failures were not counted")
		}
		if st.Dropped != 0 {
			t.Errorf("dropped %d events; the buffer was nowhere near full", st.Dropped)
		}
		// Both events must survive the outage — the retained batch is retried.
		if prod.Len() != 2 {
			t.Errorf("published %d, want 2 — events buffered during the outage were lost", prod.Len())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not finish")
	}
}

// Retention is bounded. An outage longer than the buffer drops the oldest and
// counts them, because an OOM kill loses everything silently whereas a counted
// drop is something an alert can fire on.
func TestRetryBufferIsBoundedAndDropsAreCounted(t *testing.T) {
	prod := sink.NewMemoryProducer()
	prod.SetFailure(errors.New("broker down"))

	in := make(chan model.Event, 64)
	p := &Pipeline{
		In:       in,
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 1, FlushInterval: 2 * time.Millisecond,
		MaxPendingEvents: 5,
	}

	done := make(chan Stats, 1)
	go func() {
		st, _ := p.Run(context.Background())
		done <- st
	}()

	for i := int64(1); i <= 40; i++ {
		in <- tradeEvent("coinbase", "BTC-USD", i)
	}
	time.Sleep(200 * time.Millisecond)
	close(in)

	select {
	case st := <-done:
		if st.Dropped == 0 {
			t.Error("no drops recorded despite a full buffer")
		}
		if st.Dropped >= 40 {
			t.Errorf("dropped %d of 40; the buffer retained nothing", st.Dropped)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not finish")
	}
}

func TestMissingEncoderIsCountedNotFatal(t *testing.T) {
	prod := sink.NewMemoryProducer()
	encoders := testEncoders(t)
	delete(encoders, model.KindBookDelta)

	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: encoders, Topics: testTopics(),
		BatchSize: 10, FlushInterval: 5 * time.Millisecond,
	}

	stats := runPipeline(t, p, make(chan model.Event, 8), []model.Event{
		tradeEvent("coinbase", "BTC-USD", 1),
		{
			Kind: model.KindBookDelta, Venue: "coinbase", Symbol: "BTC-USD", Sequence: 2,
			Book: &model.BookDelta{Venue: "coinbase", Symbol: "BTC-USD", EventTimeUS: 1, IngestTimeUS: 2},
		},
	})

	if stats.EncodeErrs != 1 {
		t.Errorf("encode errors = %d, want 1", stats.EncodeErrs)
	}
	// One bad kind must not stop the trades from flowing.
	if prod.Len() != 1 {
		t.Errorf("published %d, want 1", prod.Len())
	}
}

// The pipeline must attach W3C trace context to the Kafka record when the
// publish is being traced, and attach nothing when it is not. Header bytes on
// every record are real bandwidth at market data volumes.
func TestTraceContextIsAttachedToPublishedRecords(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	prod := sink.NewMemoryProducer()
	in := make(chan model.Event, 4)
	p := &Pipeline{
		In:       in,
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 1, FlushInterval: time.Hour,
	}

	ctx, span := tp.Tracer("test").Start(context.Background(), "publish-batch")
	go func() { _, _ = p.Run(ctx) }()

	in <- tradeEvent("coinbase", "BTC-USD", 1)

	deadline := time.After(3 * time.Second)
	for prod.Len() == 0 {
		select {
		case <-deadline:
			t.Fatal("nothing was published")
		case <-time.After(5 * time.Millisecond):
		}
	}
	span.End()

	msg := prod.Messages()[0]
	if msg.Trace == nil {
		t.Fatal("no trace headers on a traced publish")
	}
	if _, ok := msg.Trace["traceparent"]; !ok {
		t.Errorf("no W3C traceparent header; got %v", msg.Trace)
	}
}

func TestNoTraceHeadersWhenTracingIsDisabled(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 10, FlushInterval: 5 * time.Millisecond,
	}

	runPipeline(t, p, make(chan model.Event, 8), []model.Event{
		tradeEvent("coinbase", "BTC-USD", 1),
	})

	if trace := prod.Messages()[0].Trace; trace != nil {
		t.Errorf("trace headers added with tracing disabled: %v", trace)
	}
}

// Coinbase Advanced Trade stamps one monotonic sequence_num on every envelope
// across every product and channel on the socket. Keying gap detection on
// (venue, symbol, channel) splits that single counter across every stream, and
// each one sees a number that jumps — reported as a gap that never happened.
//
// The first live run produced 1,915 phantom gaps in 45 seconds from Coinbase
// while Kraken and Binance produced none. This pins the fix.
func TestConnectionScopedVenuesDoNotReportPhantomGaps(t *testing.T) {
	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 20, FlushInterval: 5 * time.Millisecond,
		ConnectionScopedVenues: map[string]bool{"coinbase": true},
	}

	// One connection-level counter interleaved across three symbols, exactly
	// as Coinbase delivers it. Per-stream keying would see 1,4,7 on BTC and
	// call every step a gap.
	events := []model.Event{
		tradeEvent("coinbase", "BTC-USD", 1),
		tradeEvent("coinbase", "ETH-USD", 2),
		tradeEvent("coinbase", "SOL-USD", 3),
		tradeEvent("coinbase", "BTC-USD", 4),
		tradeEvent("coinbase", "ETH-USD", 5),
		tradeEvent("coinbase", "SOL-USD", 6),
	}

	stats := runPipeline(t, p, make(chan model.Event, 16), events)

	if stats.Gaps != 0 {
		t.Errorf("gaps = %d, want 0 — the connection counter is contiguous", stats.Gaps)
	}
	if stats.Missed != 0 {
		t.Errorf("missed = %d, want 0", stats.Missed)
	}
	if prod.Len() != 6 {
		t.Errorf("published %d, want 6", prod.Len())
	}
}

// A genuine drop on a connection-scoped venue must still be caught.
func TestConnectionScopedVenuesStillDetectRealGaps(t *testing.T) {
	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 20, FlushInterval: 5 * time.Millisecond,
		ConnectionScopedVenues: map[string]bool{"coinbase": true},
	}

	stats := runPipeline(t, p, make(chan model.Event, 16), []model.Event{
		tradeEvent("coinbase", "BTC-USD", 1),
		tradeEvent("coinbase", "ETH-USD", 2),
		tradeEvent("coinbase", "BTC-USD", 9), // 3..8 genuinely lost
	})

	if stats.Gaps != 1 {
		t.Errorf("gaps = %d, want 1", stats.Gaps)
	}
	if stats.Missed != 6 {
		t.Errorf("missed = %d, want 6", stats.Missed)
	}
}

// Per-stream venues must be unaffected by the fix.
func TestPerStreamVenuesKeepIndependentSequences(t *testing.T) {
	prod := sink.NewMemoryProducer()
	p := &Pipeline{
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 20, FlushInterval: 5 * time.Millisecond,
		ConnectionScopedVenues: map[string]bool{"coinbase": true},
	}

	// Kraken sequences per symbol, so these are three independent streams
	// each starting at 1 — not a triple-duplicate.
	stats := runPipeline(t, p, make(chan model.Event, 16), []model.Event{
		tradeEvent("kraken", "BTC-USD", 1),
		tradeEvent("kraken", "ETH-USD", 1),
		tradeEvent("kraken", "SOL-USD", 1),
		tradeEvent("kraken", "BTC-USD", 2),
	})

	if stats.Gaps != 0 || stats.Duplicates != 0 {
		t.Errorf("gaps=%d duplicates=%d, want 0/0", stats.Gaps, stats.Duplicates)
	}
	if prod.Len() != 4 {
		t.Errorf("published %d, want 4", prod.Len())
	}
}
