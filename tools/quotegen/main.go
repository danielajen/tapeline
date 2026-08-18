// Command quotegen publishes synthetic Avro quotes to the quote topic.
//
// It exists so the serving tier can be load-tested independently of Flink.
// The p99 claim is about the serving path — Kafka consume, Redis cache, HMAC
// auth, gRPC/REST response — and none of that depends on which process wrote
// the quotes. Feeding the topic directly isolates the measurement to the tier
// being measured, and lets the rate be controlled rather than left to whatever
// three exchanges happen to be doing.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/segmentio/kafka-go"
)

type quote struct {
	Venue       string  `avro:"venue"`
	Symbol      string  `avro:"symbol"`
	BidPrice    float64 `avro:"bid_price"`
	BidSize     float64 `avro:"bid_size"`
	AskPrice    float64 `avro:"ask_price"`
	AskSize     float64 `avro:"ask_size"`
	Mid         float64 `avro:"mid"`
	SpreadBps   float64 `avro:"spread_bps"`
	Imbalance   float64 `avro:"imbalance"`
	EventTimeUS int64   `avro:"event_time_us"`
	EmitTimeUS  int64   `avro:"emit_time_us"`
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "kafka brokers")
	topic := flag.String("topic", "md.quotes.v1", "quote topic")
	schemaID := flag.Int("schema-id", 0, "registry schema id for the quote subject")
	rate := flag.Int("rate", 500, "quotes per second")
	dur := flag.Duration("duration", 60*time.Second, "how long to run")
	schemaPath := flag.String("schema", "stream/src/main/resources/avro/quote.v1.avsc", "path to the quote Avro schema")
	flag.Parse()

	if *schemaID == 0 {
		log.Fatal("-schema-id is required; register the quote schema first")
	}

	schemaJSON, err := os.ReadFile(*schemaPath)
	if err != nil {
		log.Fatalf("reading quote schema: %v", err)
	}
	schema, err := avro.Parse(string(schemaJSON))
	if err != nil {
		log.Fatalf("parsing quote schema: %v", err)
	}

	header := make([]byte, 5)
	header[0] = 0x00
	binary.BigEndian.PutUint32(header[1:], uint32(*schemaID))

	w := &kafka.Writer{
		Addr:         kafka.TCP(*brokers),
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchSize:    200,
		BatchTimeout: 5 * time.Millisecond,
	}
	defer w.Close()

	symbols := []string{"BTC-USD", "ETH-USD", "SOL-USD"}
	venues := []string{"coinbase", "kraken", "binance"}
	base := map[string]float64{"BTC-USD": 64000, "ETH-USD": 3100, "SOL-USD": 140}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	deadline := time.After(*dur)

	var sent int
	batch := make([]kafka.Message, 0, 200)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("sent %d quotes\n", sent)
			return
		case <-deadline:
			if len(batch) > 0 {
				_ = w.WriteMessages(context.Background(), batch...)
			}
			fmt.Printf("sent %d quotes in %s\n", sent, *dur)
			return
		case <-ticker.C:
			sym := symbols[rand.Intn(len(symbols))]
			ven := venues[rand.Intn(len(venues))]
			mid := base[sym] * (1 + (rand.Float64()-0.5)*0.001)
			spread := mid * 0.0001
			now := time.Now().UnixMicro()

			q := quote{
				Venue: ven, Symbol: sym,
				BidPrice: mid - spread/2, BidSize: 1 + rand.Float64(),
				AskPrice: mid + spread/2, AskSize: 1 + rand.Float64(),
				Mid: mid, SpreadBps: spread / mid * 10000,
				Imbalance:   (rand.Float64() - 0.5) * 2,
				EventTimeUS: now, EmitTimeUS: now,
			}
			payload, err := avro.Marshal(schema, &q)
			if err != nil {
				log.Fatalf("marshal: %v", err)
			}
			batch = append(batch, kafka.Message{
				Key:   []byte(sym),
				Value: append(append([]byte{}, header...), payload...),
			})
			sent++
			if len(batch) >= 200 {
				if err := w.WriteMessages(context.Background(), batch...); err != nil {
					log.Printf("write: %v", err)
				}
				batch = batch[:0]
			}
		}
	}
}
