// Package sink publishes framed events to Kafka.
//
// Partitioning is by symbol, not by venue and not round-robin. Ordering in
// Kafka is per-partition, and the Flink tier maintains order-book state keyed
// by symbol — so every message for a symbol must land on one partition or the
// book rebuilds out of order. This one line of configuration is load-bearing
// for correctness downstream.
package sink

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/tapeline/ingest/internal/metrics"
)

// Message is one framed record destined for a topic.
type Message struct {
	Topic string
	// Key is the partition key. Always the canonical symbol.
	Key    string
	Value  []byte
	TimeUS int64
	// Headers carry the venue and event kind so consumers can route without
	// deserializing the payload.
	Venue string
	Kind  string
}

// Producer is the publish interface, small enough that the pipeline tests
// substitute a memory implementation without a broker.
type Producer interface {
	Publish(ctx context.Context, msgs ...Message) error
	Close() error
}

// KafkaProducer wraps a single kafka-go Writer in per-message-topic mode.
type KafkaProducer struct {
	w *kafka.Writer
}

// Config configures the writer.
type Config struct {
	Brokers []string
	// RequiredAcks defaults to all in-sync replicas. Anything weaker makes
	// the "zero data loss under broker failure" claim untrue.
	RequiredAcks kafka.RequiredAcks
	BatchSize    int
	BatchBytes   int64
	BatchTimeout time.Duration
	Compression  kafka.Compression
	Async        bool
}

// DefaultConfig returns durable, moderately batched settings.
func DefaultConfig(brokers []string) Config {
	return Config{
		Brokers:      brokers,
		RequiredAcks: kafka.RequireAll,
		BatchSize:    500,
		BatchBytes:   1 << 20,
		BatchTimeout: 10 * time.Millisecond,
		Compression:  kafka.Lz4,
		Async:        false,
	}
}

// NewKafkaProducer builds a producer from cfg.
func NewKafkaProducer(cfg Config) *KafkaProducer {
	return &KafkaProducer{
		w: &kafka.Writer{
			Addr: kafka.TCP(cfg.Brokers...),
			// Topic is intentionally unset: each Message carries its own, so
			// one writer and one connection pool serves every topic.
			Balancer:     &kafka.Hash{},
			RequiredAcks: cfg.RequiredAcks,
			BatchSize:    cfg.BatchSize,
			BatchBytes:   cfg.BatchBytes,
			BatchTimeout: cfg.BatchTimeout,
			Compression:  cfg.Compression,
			Async:        cfg.Async,
			MaxAttempts:  10,
			// AllowAutoTopicCreation is false on purpose. Topics are created
			// by Terraform with an explicit partition count; letting a
			// producer create them yields the broker default and silently
			// caps parallelism downstream.
			AllowAutoTopicCreation: false,
		},
	}
}

// Publish writes msgs, recording latency and outcome per topic.
func (p *KafkaProducer) Publish(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}

	kms := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		kms = append(kms, kafka.Message{
			Topic: m.Topic,
			Key:   []byte(m.Key),
			Value: m.Value,
			Time:  time.UnixMicro(m.TimeUS),
			Headers: []kafka.Header{
				{Key: "venue", Value: []byte(m.Venue)},
				{Key: "kind", Value: []byte(m.Kind)},
			},
		})
	}

	start := time.Now()
	err := p.w.WriteMessages(ctx, kms...)
	elapsed := time.Since(start).Seconds()

	for _, m := range msgs {
		metrics.PublishLatencySeconds.WithLabelValues(m.Topic).Observe(elapsed)
		if err != nil {
			metrics.PublishErrors.WithLabelValues(m.Venue, m.Topic).Inc()
		} else {
			metrics.EventsPublished.WithLabelValues(m.Venue, m.Topic).Inc()
		}
	}
	return err
}

// Close flushes and closes the writer.
func (p *KafkaProducer) Close() error { return p.w.Close() }

// MemoryProducer records messages in memory. Used by tests and by the
// --dry-run mode, which is how you validate normalization against live venue
// feeds without standing up a broker.
type MemoryProducer struct {
	mu       sync.Mutex
	messages []Message
	closed   bool
	// FailWith, when set, makes Publish fail. Used to exercise the retry path.
	FailWith error
}

// NewMemoryProducer returns an empty in-memory producer.
func NewMemoryProducer() *MemoryProducer { return &MemoryProducer{} }

// Publish appends to the in-memory log.
func (m *MemoryProducer) Publish(_ context.Context, msgs ...Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("sink: producer closed")
	}
	if m.FailWith != nil {
		return m.FailWith
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

// Messages returns a copy of everything published so far.
func (m *MemoryProducer) Messages() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Message, len(m.messages))
	copy(out, m.messages)
	return out
}

// Len returns the number of published messages.
func (m *MemoryProducer) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

// Close marks the producer closed.
func (m *MemoryProducer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
