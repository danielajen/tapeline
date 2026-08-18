// Command ingestd is Tapeline's ingestion tier.
//
// It fans in WebSocket market data from several exchanges plus EVM logs,
// normalizes every venue's dialect into one canonical schema, checks
// sequence continuity, and publishes Avro-framed records to Kafka
// partitioned by symbol.
//
// Run it against live public feeds with no broker at all:
//
//	TAPELINE_DRY_RUN=true go run ./cmd/ingestd
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tapeline/ingest/internal/config"
	"github.com/tapeline/ingest/internal/encode"
	"github.com/tapeline/ingest/internal/gap"
	"github.com/tapeline/ingest/internal/metrics"
	"github.com/tapeline/ingest/internal/model"
	"github.com/tapeline/ingest/internal/onchain"
	"github.com/tapeline/ingest/internal/pipeline"
	"github.com/tapeline/ingest/internal/schema"
	"github.com/tapeline/ingest/internal/sink"
	"github.com/tapeline/ingest/internal/venue"
	avroschema "github.com/tapeline/ingest/schemas"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ingestd exited with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	// SIGINT/SIGTERM cancel the root context; every goroutine below hangs
	// off it, so shutdown is one cancel and a WaitGroup rather than a
	// shutdown channel per component.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("starting ingestd",
		"venues", cfg.Venues, "symbols", cfg.Symbols,
		"dry_run", cfg.DryRun, "onchain", cfg.OnchainEnabled)

	topics := map[model.Kind]string{
		model.KindTrade:         cfg.TopicTrades,
		model.KindBookDelta:     cfg.TopicBook,
		model.KindChainTransfer: cfg.TopicChain,
	}

	encoders, err := buildEncoders(ctx, cfg, topics, log)
	if err != nil {
		return err
	}

	producer, err := buildProducer(cfg, log)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := producer.Close(); cerr != nil {
			log.Error("closing producer", "err", cerr)
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := metrics.Serve(ctx, cfg.MetricsAddr); err != nil {
			log.Error("metrics server stopped", "err", err)
		}
	}()
	log.Info("metrics listening", "addr", cfg.MetricsAddr)

	events := make(chan model.Event, cfg.BufferSize)
	detector := gap.New()

	decoders, resetters := buildDecoders(cfg)
	if len(decoders) == 0 {
		return errors.New("no ingestion sources configured")
	}

	for _, d := range decoders {
		d := d
		runner := &venue.Runner{
			Decoder:     d,
			Symbols:     cfg.Symbols,
			Dialer:      venue.GorillaDialer{HandshakeTimeout: 15 * time.Second},
			Out:         events,
			Backoff:     venue.DefaultBackoff(),
			ReadTimeout: 45 * time.Second,
			Log:         log,
			OnConnect: func(name string) {
				// Sequence numbers restart on a new session, so the detector
				// must forget this venue or every message after a reconnect
				// looks like a regression.
				detector.ResetVenue(name)
				if reset, ok := resetters[name]; ok {
					reset()
				}
				metrics.ConnectionUp.WithLabelValues(name).Set(1)
			},
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer metrics.ConnectionUp.WithLabelValues(d.Name()).Set(0)
			if err := runner.Run(ctx); err != nil {
				log.Error("venue runner stopped", "venue", d.Name(), "err", err)
			}
		}()
	}

	p := &pipeline.Pipeline{
		In:            events,
		Producer:      producer,
		Detector:      detector,
		Encoders:      encoders,
		Topics:        topics,
		Log:           log,
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
		OnResync: func(v string, key gap.Key) {
			// Forget the stream so the next snapshot re-establishes the
			// baseline instead of gap-alerting forever.
			detector.Reset(key)
		},
	}

	stats, err := p.Run(ctx)
	log.Info("pipeline stopped",
		"received", stats.Received, "published", stats.Published,
		"duplicates", stats.Duplicates, "gaps", stats.Gaps,
		"messages_missed", stats.Missed, "encode_errors", stats.EncodeErrs)

	stop()
	wg.Wait()

	if cfg.DryRun {
		if mem, ok := producer.(*sink.MemoryProducer); ok {
			log.Info("dry run complete", "messages_buffered", mem.Len())
		}
	}
	return err
}

// buildDecoders instantiates the configured sources and returns per-venue
// reset hooks for the ones that carry state across a reconnect.
func buildDecoders(cfg config.Config) ([]venue.Decoder, map[string]func()) {
	var decoders []venue.Decoder
	resetters := make(map[string]func())

	for _, v := range cfg.Venues {
		switch v {
		case "coinbase":
			decoders = append(decoders, venue.NewCoinbase(cfg.Symbols))
		case "kraken":
			decoders = append(decoders, venue.NewKraken(cfg.Symbols, cfg.KrakenDepth))
		case "binance":
			b := venue.NewBinance(cfg.Symbols, cfg.BinanceDepthMS)
			decoders = append(decoders, b)
			resetters[b.Name()] = b.ResetBooks
		}
	}

	if cfg.OnchainEnabled {
		decoders = append(decoders, onchain.NewEVM(cfg.OnchainChain, cfg.OnchainWSURL, nil, nil))
	}

	return decoders, resetters
}

// buildEncoders registers schemas with the registry, or builds local
// encoders in dry-run mode where no registry is reachable.
func buildEncoders(
	ctx context.Context,
	cfg config.Config,
	topics map[model.Kind]string,
	log *slog.Logger,
) (map[model.Kind]*encode.Encoder, error) {
	schemasByKind := avroschema.Current()

	if cfg.DryRun {
		// Schema id 0 is not a real registry id. It is only ever written in
		// dry-run mode, where nothing consumes the output.
		encoders := make(map[model.Kind]*encode.Encoder, len(schemasByKind))
		for kind, s := range schemasByKind {
			enc, err := encode.NewEncoder(0, s)
			if err != nil {
				return nil, fmt.Errorf("dry-run encoder for %s: %w", kind, err)
			}
			encoders[kind] = enc
		}
		log.Warn("dry run: schemas not registered, framing with placeholder id 0")
		return encoders, nil
	}

	reg := schema.New(cfg.SchemaRegistryURL)

	// FULL compatibility, not the registry default of BACKWARD. Backward
	// alone allows a change that new producers can write but old consumers
	// cannot read; during a rolling deploy both halves are live at once, so
	// only FULL is actually safe here.
	for _, topic := range topics {
		subject := schema.SubjectForTopic(topic)
		if err := reg.SetCompatibility(ctx, subject, schema.CompatFull); err != nil {
			log.Warn("could not set compatibility level",
				"subject", subject, "err", err)
		}
	}

	regCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	encoders, err := pipeline.RegisterSchemas(regCtx, reg, topics, schemasByKind)
	if err != nil {
		return nil, fmt.Errorf("registering schemas: %w", err)
	}
	for kind, enc := range encoders {
		log.Info("schema registered", "kind", kind, "schema_id", enc.SchemaID())
	}
	return encoders, nil
}

func buildProducer(cfg config.Config, log *slog.Logger) (sink.Producer, error) {
	if cfg.DryRun {
		log.Warn("dry run: publishing to an in-memory sink, not Kafka")
		return sink.NewMemoryProducer(), nil
	}
	kcfg := sink.DefaultConfig(cfg.KafkaBrokers)
	kcfg.BatchSize = cfg.BatchSize
	kcfg.BatchTimeout = cfg.FlushInterval
	return sink.NewKafkaProducer(kcfg), nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
