package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultsAreValid(t *testing.T) {
	if err := Defaults().Validate(); err != nil {
		t.Fatalf("the shipped defaults do not validate: %v", err)
	}
}

func TestLoadReadsEnvironment(t *testing.T) {
	t.Setenv("TAPELINE_VENUES", "coinbase, binance")
	t.Setenv("TAPELINE_SYMBOLS", "BTC-USD,ETH-USD")
	t.Setenv("TAPELINE_KAFKA_BROKERS", "b1:9092,b2:9092")
	t.Setenv("TAPELINE_BATCH_SIZE", "42")
	t.Setenv("TAPELINE_FLUSH_INTERVAL", "250ms")
	t.Setenv("TAPELINE_DRY_RUN", "true")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Whitespace around a comma is the most common way this gets typed into
	// a ConfigMap.
	if len(c.Venues) != 2 || c.Venues[1] != "binance" {
		t.Errorf("venues = %v", c.Venues)
	}
	if len(c.KafkaBrokers) != 2 {
		t.Errorf("brokers = %v", c.KafkaBrokers)
	}
	if c.BatchSize != 42 {
		t.Errorf("batch size = %d, want 42", c.BatchSize)
	}
	if c.FlushInterval != 250*time.Millisecond {
		t.Errorf("flush interval = %v", c.FlushInterval)
	}
	if !c.DryRun {
		t.Error("dry run not picked up")
	}
	// Anything unset must keep its default.
	if c.TopicTrades != "md.trades.v1" {
		t.Errorf("topic = %q, want the default", c.TopicTrades)
	}
}

func TestValidationRejectsBadConfigs(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantSub string
	}{
		{
			name:    "non-canonical symbol",
			mutate:  func(c *Config) { c.Symbols = []string{"BTCUSD"} },
			wantSub: "canonical BASE-QUOTE",
		},
		{
			name:    "unknown venue",
			mutate:  func(c *Config) { c.Venues = []string{"ftx"} },
			wantSub: "unknown venue",
		},
		{
			name:    "no sources at all",
			mutate:  func(c *Config) { c.Venues = nil; c.OnchainEnabled = false },
			wantSub: "no venues",
		},
		{
			name:    "no brokers outside dry run",
			mutate:  func(c *Config) { c.KafkaBrokers = nil; c.DryRun = false },
			wantSub: "TAPELINE_KAFKA_BROKERS is required",
		},
		{
			name:    "on-chain without an endpoint",
			mutate:  func(c *Config) { c.OnchainEnabled = true; c.OnchainWSURL = "" },
			wantSub: "TAPELINE_ONCHAIN_WS_URL is required",
		},
		{
			name:    "bad binance depth interval",
			mutate:  func(c *Config) { c.BinanceDepthMS = 250 },
			wantSub: "must be 100 or 1000",
		},
		{
			name:    "zero buffer",
			mutate:  func(c *Config) { c.BufferSize = 0 },
			wantSub: "buffer size must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Defaults()
			tc.mutate(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestDryRunNeedsNoBrokers(t *testing.T) {
	c := Defaults()
	c.KafkaBrokers = nil
	c.DryRun = true
	if err := c.Validate(); err != nil {
		t.Errorf("dry run should not require brokers: %v", err)
	}
}

func TestBadNumericEnvIsAnErrorNotADefault(t *testing.T) {
	t.Setenv("TAPELINE_BATCH_SIZE", "lots")
	// Silently falling back to a default would make a typo in a ConfigMap
	// invisible until someone wondered why throughput looked wrong.
	if _, err := Load(); err == nil {
		t.Fatal("a non-numeric batch size was accepted")
	}
}
