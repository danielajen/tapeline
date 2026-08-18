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

func TestPublishFailureSurfaces(t *testing.T) {
	prod := sink.NewMemoryProducer()
	prod.FailWith = errors.New("broker unavailable")

	in := make(chan model.Event, 4)
	p := &Pipeline{
		In:       in,
		Producer: prod, Detector: gap.New(),
		Encoders: testEncoders(t), Topics: testTopics(),
		BatchSize: 1, FlushInterval: time.Hour,
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Run(context.Background())
		errCh <- err
	}()
	in <- tradeEvent("coinbase", "BTC-USD", 1)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("a failed publish returned nil; data loss would be silent")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not report the publish failure")
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
