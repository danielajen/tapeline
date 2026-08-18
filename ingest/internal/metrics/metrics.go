// Package metrics owns every Prometheus series the ingestion tier exports.
//
// They are declared in one place so the Grafana dashboard in
// deploy/grafana can be written against a known set of names, and so the
// load-test run in week 9 has something to read. If a metric is not here,
// no dashboard panel can depend on it.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "tapeline"

var (
	// EventsReceived counts decoded canonical events by venue and kind.
	EventsReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "events_received_total",
		Help: "Canonical events decoded from venue feeds.",
	}, []string{"venue", "kind"})

	// EventsPublished counts events successfully written to Kafka.
	EventsPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "events_published_total",
		Help: "Events acknowledged by Kafka.",
	}, []string{"venue", "topic"})

	// PublishErrors counts Kafka write failures.
	PublishErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "publish_errors_total",
		Help: "Kafka write failures.",
	}, []string{"venue", "topic"})

	// DecodeErrors counts venue frames that could not be decoded. A sustained
	// non-zero rate here almost always means the venue changed its schema.
	DecodeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "decode_errors_total",
		Help: "Venue frames that failed to decode.",
	}, []string{"venue"})

	// SequenceGaps counts detected gaps. Each one means a resync.
	SequenceGaps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "sequence_gaps_total",
		Help: "Detected sequence gaps, by venue and channel.",
	}, []string{"venue", "channel"})

	// SequenceMessagesMissed counts how many messages the gaps represent.
	// Gaps alone undercount: one gap can be one message or ten thousand.
	SequenceMessagesMissed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "sequence_messages_missed_total",
		Help: "Messages implied missing by detected gaps.",
	}, []string{"venue", "channel"})

	// DuplicatesDropped counts suppressed retransmits.
	DuplicatesDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "duplicates_dropped_total",
		Help: "Duplicate or out-of-order frames suppressed.",
	}, []string{"venue", "channel"})

	// Reconnects counts WebSocket reconnect attempts.
	Reconnects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "reconnects_total",
		Help: "WebSocket reconnect attempts.",
	}, []string{"venue", "reason"})

	// ConnectionUp is 1 while a venue socket is connected.
	ConnectionUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "connection_up",
		Help: "1 when the venue WebSocket is connected, 0 otherwise.",
	}, []string{"venue"})

	// SourceLagSeconds is venue event time minus local receipt time. This is
	// the number that answers "how stale is the data", and it is the one
	// panel to watch during chaos testing.
	SourceLagSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "source_lag_seconds",
		Help:    "Local receipt time minus venue event time.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"venue", "kind"})

	// PublishLatencySeconds measures the Kafka write call itself.
	PublishLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "publish_latency_seconds",
		Help:    "Time spent in the Kafka write call.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1},
	}, []string{"topic"})

	// PipelineQueueDepth is the fan-in channel depth. Sustained growth here
	// means Kafka is the bottleneck and backpressure is reaching the sockets.
	PipelineQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "pipeline_queue_depth",
		Help: "Buffered events awaiting publish.",
	}, []string{"stage"})

	// SchemaRegistrations counts schema registry registrations by subject.
	SchemaRegistrations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "ingest", Name: "schema_registrations_total",
		Help: "Schemas registered with the schema registry.",
	}, []string{"subject"})
)

var collectors = []prometheus.Collector{
	EventsReceived, EventsPublished, PublishErrors, DecodeErrors,
	SequenceGaps, SequenceMessagesMissed, DuplicatesDropped,
	Reconnects, ConnectionUp, SourceLagSeconds, PublishLatencySeconds,
	PipelineQueueDepth, SchemaRegistrations,
}

// Registry is the process registry. It is separate from the default
// registry so tests can build a fresh one without global state leaking
// between them.
var Registry = prometheus.NewRegistry()

func init() {
	for _, c := range collectors {
		Registry.MustRegister(c)
	}
}

// ObserveSourceLag records receipt-minus-event lag, ignoring events whose
// venue timestamp is missing or in the future (clock skew would otherwise
// pollute the histogram with negative-adjacent values).
func ObserveSourceLag(venue, kind string, eventTimeUS, ingestTimeUS int64) {
	if eventTimeUS <= 0 || ingestTimeUS < eventTimeUS {
		return
	}
	SourceLagSeconds.WithLabelValues(venue, kind).
		Observe(float64(ingestTimeUS-eventTimeUS) / 1e6)
}

// Serve runs the /metrics endpoint until ctx is cancelled.
func Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}
