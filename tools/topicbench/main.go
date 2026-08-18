// topicbench measures what a topic-per-event-type layout costs and buys
// against a single monolithic topic.
//
//	topicbench -brokers localhost:9092 -per-type 60000
//
// The design decision it tests is recorded in docs/DESIGN_DECISIONS.md: this
// system publishes trades, book deltas and quotes to separate topics rather
// than to one stream carrying a type tag. That is a real tradeoff, not a free
// win, and it was made without evidence. This measures both sides of it.
//
// What is actually being measured is read amplification. A consumer that
// wants one event type must, on a monolithic topic, fetch every other type
// across the network and discard it after decoding enough to know it is
// unwanted. On per-type topics the broker never sends it. The interesting
// number is therefore bytes fetched per useful record, not raw throughput.
//
// Both sides read the same payloads, produced once, so the comparison is not
// confounded by different data.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// The three event types this system carries. Sizes are representative rather
// than exact: a book delta is much larger than a trade, which is the whole
// reason read amplification is uneven across consumers.
var kinds = []struct {
	name  string
	topic string
	bytes int
}{
	{"trade", "bench.trades", 120},
	{"book", "bench.book", 900},
	{"quote", "bench.quotes", 180},
}

const monolithTopic = "bench.all"

// A consumer wanting only trades must still receive book deltas and quotes
// from the monolith. This is the header it would filter on.
const typeHeader = "event_type"

func main() {
	brokers := flag.String("brokers", "localhost:9092", "kafka brokers")
	perType := flag.Int("per-type", 60000, "records produced per event type")
	target := flag.String("target", "trade", "the event type the consumer wants")
	flag.Parse()

	ctx := context.Background()

	if err := produce(ctx, *brokers, *perType); err != nil {
		log.Fatalf("produce: %v", err)
	}

	// Per-type: the broker sends only what was asked for.
	var wanted string
	for _, k := range kinds {
		if k.name == *target {
			wanted = k.topic
		}
	}
	if wanted == "" {
		log.Fatalf("unknown target %q", *target)
	}

	perTypeResult, err := consume(ctx, *brokers, wanted, "", *perType)
	if err != nil {
		log.Fatalf("consume per-type: %v", err)
	}

	// Monolith: everything arrives, the consumer discards what it does not
	// want. Reading all three types' worth of records to find one type's.
	monolithResult, err := consume(ctx, *brokers, monolithTopic, *target, *perType)
	if err != nil {
		log.Fatalf("consume monolith: %v", err)
	}

	report(*target, perTypeResult, monolithResult)
}

type result struct {
	delivered int   // records the broker sent us
	useful    int   // records we actually wanted
	bytesRead int64 // payload bytes that crossed the network
	elapsed   time.Duration
}

func produce(ctx context.Context, brokers string, perType int) error {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchSize:    500,
	}
	defer w.Close()

	rng := rand.New(rand.NewSource(42)) // fixed seed: the run must be repeatable
	symbols := []string{"BTC-USD", "ETH-USD", "SOL-USD"}

	batch := make([]kafka.Message, 0, 1000)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := w.WriteMessages(ctx, batch...)
		batch = batch[:0]
		return err
	}

	for i := 0; i < perType; i++ {
		for _, k := range kinds {
			payload := make([]byte, k.bytes)
			rng.Read(payload)
			key := []byte(symbols[i%len(symbols)])
			hdr := []kafka.Header{{Key: typeHeader, Value: []byte(k.name)}}

			// The same payload goes to both layouts, so the only difference
			// measured is the layout itself.
			batch = append(batch,
				kafka.Message{Topic: k.topic, Key: key, Value: payload, Headers: hdr},
				kafka.Message{Topic: monolithTopic, Key: key, Value: payload, Headers: hdr},
			)
			if len(batch) >= 1000 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// consume reads until it has seen `expectUseful` records of the wanted type.
// filter is empty for the per-type read, because there is nothing to filter.
func consume(ctx context.Context, brokers, topic, filter string, expectUseful int) (result, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		Topic:    topic,
		GroupID:  fmt.Sprintf("topicbench-%s-%d", topic, time.Now().UnixNano()),
		MinBytes: 1 << 10,
		MaxBytes: 10 << 20,
	})
	defer r.Close()

	var res result
	start := time.Now()
	deadline, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	for res.useful < expectUseful {
		m, err := r.ReadMessage(deadline)
		if err != nil {
			return res, err
		}
		res.delivered++
		// Bytes that crossed the network, whether or not they were wanted.
		res.bytesRead += int64(len(m.Value) + len(m.Key))

		if filter == "" {
			res.useful++
			continue
		}
		// The discard path. Cheap here, but it is not free: the record was
		// fetched, decompressed and handed to the application first.
		for _, h := range m.Headers {
			if h.Key == typeHeader && string(h.Value) == filter {
				res.useful++
			}
		}
	}
	res.elapsed = time.Since(start)
	return res, nil
}

func report(target string, perType, monolith result) {
	amp := float64(monolith.bytesRead) / float64(perType.bytesRead)
	waste := float64(monolith.delivered-monolith.useful) / float64(monolith.delivered) * 100

	fmt.Printf(`
Topic layout: per-type vs monolith
==================================
consumer wants            %s only

                          per-type      monolith
records delivered         %-13d %d
records useful            %-13d %d
bytes fetched             %-13d %d
elapsed                   %-13s %s

read amplification        %.2fx
records discarded         %.1f%%
`,
		target,
		perType.delivered, monolith.delivered,
		perType.useful, monolith.useful,
		perType.bytesRead, monolith.bytesRead,
		perType.elapsed.Round(time.Millisecond), monolith.elapsed.Round(time.Millisecond),
		amp, waste)

	if f, err := os.Create("topicbench-results.txt"); err == nil {
		defer f.Close()
		fmt.Fprintf(f, "target=%s\nper_type_bytes=%d\nmonolith_bytes=%d\n",
			target, perType.bytesRead, monolith.bytesRead)
		fmt.Fprintf(f, "per_type_delivered=%d\nmonolith_delivered=%d\n",
			perType.delivered, monolith.delivered)
		fmt.Fprintf(f, "per_type_ms=%d\nmonolith_ms=%d\n",
			perType.elapsed.Milliseconds(), monolith.elapsed.Milliseconds())
		fmt.Fprintf(f, "read_amplification=%.2f\ndiscarded_pct=%.1f\n", amp, waste)
	}
}
