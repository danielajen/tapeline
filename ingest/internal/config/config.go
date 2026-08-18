// Package config loads ingestd's settings from the environment.
//
// Environment only, no config file. The service runs in Kubernetes where a
// ConfigMap is already the deployment-time file; adding a second file format
// on top of it would just be two places to look when something is wrong.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full ingestd configuration.
type Config struct {
	// Venues to run. Any of: coinbase, kraken, binance.
	Venues []string
	// Symbols in canonical BASE-QUOTE form, e.g. BTC-USD.
	Symbols []string

	KafkaBrokers      []string
	SchemaRegistryURL string

	TopicTrades string
	TopicBook   string
	TopicChain  string

	MetricsAddr string
	LogLevel    string

	// DryRun swaps Kafka for an in-memory sink. This is how you validate
	// venue normalization against live feeds without a broker, and it is the
	// first thing to reach for when a venue changes its schema.
	DryRun bool

	// BufferSize is the fan-in channel depth. Large enough to absorb a book
	// snapshot burst, small enough that backpressure reaches the sockets
	// before memory does.
	BufferSize int
	// BatchSize and FlushInterval control Kafka batching.
	BatchSize     int
	FlushInterval time.Duration

	// KrakenDepth is the L2 depth Kraken should stream.
	KrakenDepth int
	// BinanceDepthMS is the Binance diff-depth interval: 100 or 1000.
	BinanceDepthMS int

	OnchainEnabled bool
	OnchainChain   string
	OnchainWSURL   string
}

// Defaults returns the configuration used when nothing is set.
func Defaults() Config {
	return Config{
		Venues:            []string{"coinbase", "kraken", "binance"},
		Symbols:           []string{"BTC-USD", "ETH-USD", "SOL-USD"},
		KafkaBrokers:      []string{"localhost:9092"},
		SchemaRegistryURL: "http://localhost:8081",
		TopicTrades:       "md.trades.v1",
		TopicBook:         "md.book.v1",
		TopicChain:        "md.chain.v1",
		MetricsAddr:       ":9101",
		LogLevel:          "info",
		BufferSize:        16384,
		BatchSize:         500,
		FlushInterval:     10 * time.Millisecond,
		KrakenDepth:       10,
		BinanceDepthMS:    100,
		OnchainChain:      "ethereum",
	}
}

// Load reads the environment over the defaults and validates the result.
func Load() (Config, error) {
	c := Defaults()

	c.Venues = envList("TAPELINE_VENUES", c.Venues)
	c.Symbols = envList("TAPELINE_SYMBOLS", c.Symbols)
	c.KafkaBrokers = envList("TAPELINE_KAFKA_BROKERS", c.KafkaBrokers)
	c.SchemaRegistryURL = envStr("TAPELINE_SCHEMA_REGISTRY_URL", c.SchemaRegistryURL)
	c.TopicTrades = envStr("TAPELINE_TOPIC_TRADES", c.TopicTrades)
	c.TopicBook = envStr("TAPELINE_TOPIC_BOOK", c.TopicBook)
	c.TopicChain = envStr("TAPELINE_TOPIC_CHAIN", c.TopicChain)
	c.MetricsAddr = envStr("TAPELINE_METRICS_ADDR", c.MetricsAddr)
	c.LogLevel = envStr("TAPELINE_LOG_LEVEL", c.LogLevel)
	c.DryRun = envBool("TAPELINE_DRY_RUN", c.DryRun)

	var err error
	if c.BufferSize, err = envInt("TAPELINE_BUFFER_SIZE", c.BufferSize); err != nil {
		return c, err
	}
	if c.BatchSize, err = envInt("TAPELINE_BATCH_SIZE", c.BatchSize); err != nil {
		return c, err
	}
	if c.FlushInterval, err = envDuration("TAPELINE_FLUSH_INTERVAL", c.FlushInterval); err != nil {
		return c, err
	}
	if c.KrakenDepth, err = envInt("TAPELINE_KRAKEN_DEPTH", c.KrakenDepth); err != nil {
		return c, err
	}
	if c.BinanceDepthMS, err = envInt("TAPELINE_BINANCE_DEPTH_MS", c.BinanceDepthMS); err != nil {
		return c, err
	}

	c.OnchainEnabled = envBool("TAPELINE_ONCHAIN_ENABLED", c.OnchainEnabled)
	c.OnchainChain = envStr("TAPELINE_ONCHAIN_CHAIN", c.OnchainChain)
	c.OnchainWSURL = envStr("TAPELINE_ONCHAIN_WS_URL", c.OnchainWSURL)

	return c, c.Validate()
}

// Validate rejects configurations that would fail later and less clearly.
func (c Config) Validate() error {
	if len(c.Symbols) == 0 {
		return fmt.Errorf("config: no symbols configured")
	}
	for _, s := range c.Symbols {
		if !strings.Contains(s, "-") {
			return fmt.Errorf("config: symbol %q must be canonical BASE-QUOTE, e.g. BTC-USD", s)
		}
	}
	if len(c.Venues) == 0 && !c.OnchainEnabled {
		return fmt.Errorf("config: no venues and no on-chain source enabled")
	}
	for _, v := range c.Venues {
		switch v {
		case "coinbase", "kraken", "binance":
		default:
			return fmt.Errorf("config: unknown venue %q", v)
		}
	}
	if !c.DryRun && len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("config: TAPELINE_KAFKA_BROKERS is required unless TAPELINE_DRY_RUN=true")
	}
	if c.OnchainEnabled && c.OnchainWSURL == "" {
		return fmt.Errorf("config: TAPELINE_ONCHAIN_WS_URL is required when on-chain ingestion is enabled")
	}
	if c.BufferSize < 1 {
		return fmt.Errorf("config: buffer size must be positive, got %d", c.BufferSize)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("config: batch size must be positive, got %d", c.BatchSize)
	}
	if c.BinanceDepthMS != 100 && c.BinanceDepthMS != 1000 {
		return fmt.Errorf("config: binance depth interval must be 100 or 1000 ms, got %d", c.BinanceDepthMS)
	}
	return nil
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envList(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("config: %s=%q is not an integer: %w", key, v, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("config: %s=%q is not a duration: %w", key, v, err)
	}
	return d, nil
}
