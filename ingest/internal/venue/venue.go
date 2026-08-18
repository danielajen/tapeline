// Package venue contains one adapter per exchange plus the connection
// machinery they share.
//
// The split is deliberate: Decoder is pure (bytes in, canonical events out)
// so every venue's message handling is unit-testable against captured
// payloads with no network and no broker, while Runner owns all the messy
// stateful parts — dialing, subscribing, backoff, read deadlines, resync.
// Venue schemas change without notice; pure decoders are what make that a
// one-file fix with a regression test instead of an outage.
package venue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/tapeline/ingest/internal/model"
)

// Channel names used for gap-detector keys and metric labels.
const (
	ChannelTrades = "trades"
	ChannelBook   = "book"
)

// Decoder is the pure part of a venue adapter.
type Decoder interface {
	// Name is the canonical venue id used in events, metrics and topic keys.
	Name() string
	// URL is the WebSocket endpoint.
	URL() string
	// Subscriptions returns the messages to send after connecting, in order.
	Subscriptions(symbols []string) []any
	// Decode turns one raw frame into zero or more canonical events. A frame
	// that is a heartbeat, ack, or otherwise uninteresting returns (nil, nil);
	// only genuinely malformed input returns an error.
	Decode(raw []byte, now time.Time) ([]model.Event, error)
}

// Conn is the subset of a WebSocket the Runner uses, so tests can drive the
// reconnect logic with a scripted fake.
type Conn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteJSON(v any) error
	SetReadDeadline(t time.Time) error
	Close() error
}

// Dialer opens a Conn.
type Dialer interface {
	Dial(ctx context.Context, url string) (Conn, error)
}

// GorillaDialer is the production Dialer.
type GorillaDialer struct {
	// HandshakeTimeout bounds the TLS + HTTP upgrade.
	HandshakeTimeout time.Duration
}

// Dial opens a real WebSocket.
func (g GorillaDialer) Dial(ctx context.Context, url string) (Conn, error) {
	d := websocket.Dialer{
		HandshakeTimeout: g.HandshakeTimeout,
		Proxy:            http.ProxyFromEnvironment,
		// Venue book snapshots are large; the default 4KB buffers cause
		// avoidable syscalls on every frame.
		ReadBufferSize:  64 * 1024,
		WriteBufferSize: 16 * 1024,
	}
	if d.HandshakeTimeout == 0 {
		d.HandshakeTimeout = 15 * time.Second
	}
	c, resp, err := d.DialContext(ctx, url, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial %s: %w (http %d)", url, err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial %s: %w", url, err)
	}
	c.SetReadLimit(8 << 20)
	return &gorillaConn{c}, nil
}

type gorillaConn struct{ c *websocket.Conn }

func (g *gorillaConn) ReadMessage() (int, []byte, error) { return g.c.ReadMessage() }
func (g *gorillaConn) WriteJSON(v any) error             { return g.c.WriteJSON(v) }
func (g *gorillaConn) SetReadDeadline(t time.Time) error { return g.c.SetReadDeadline(t) }
func (g *gorillaConn) Close() error                      { return g.c.Close() }

// Backoff configures reconnect pacing.
type Backoff struct {
	Base   time.Duration
	Max    time.Duration
	Factor float64
	// Jitter is the fraction of the delay randomized, in [0,1]. Without it,
	// all three venue clients reconnect in lockstep after a network blip and
	// hammer the exchanges simultaneously.
	Jitter float64
}

// DefaultBackoff is a half-second base doubling to thirty seconds.
func DefaultBackoff() Backoff {
	return Backoff{Base: 500 * time.Millisecond, Max: 30 * time.Second, Factor: 2, Jitter: 0.3}
}

// Delay returns the wait before attempt n (zero-based).
func (b Backoff) Delay(attempt int, rnd *rand.Rand) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := float64(b.Base) * math.Pow(b.Factor, float64(attempt))
	if d > float64(b.Max) || math.IsInf(d, 1) {
		d = float64(b.Max)
	}
	if b.Jitter > 0 {
		span := d * b.Jitter
		d = d - span/2 + span*rnd.Float64()
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// Runner drives one venue connection for the process lifetime.
type Runner struct {
	Decoder Decoder
	Symbols []string
	Dialer  Dialer
	Out     chan<- model.Event
	Backoff Backoff
	// ReadTimeout fails a connection that has gone silent. Venues send
	// heartbeats; a socket that is open but delivering nothing is the
	// failure mode that silently stalls a pipeline, and only a read deadline
	// catches it.
	ReadTimeout time.Duration
	Log         *slog.Logger

	// OnConnect fires after a successful subscribe. The pipeline uses it to
	// reset gap-detector state, since sequence numbers restart on resync.
	OnConnect func(venue string)
	// MaxAttempts bounds reconnects; zero means retry forever.
	MaxAttempts int

	rnd *rand.Rand
}

// Run connects, subscribes and pumps events until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	if r.Log == nil {
		r.Log = slog.Default()
	}
	if r.Backoff.Base == 0 {
		r.Backoff = DefaultBackoff()
	}
	if r.ReadTimeout == 0 {
		r.ReadTimeout = 45 * time.Second
	}
	if r.rnd == nil {
		r.rnd = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // jitter, not crypto
	}

	name := r.Decoder.Name()
	attempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		err := r.session(ctx)
		switch {
		case err == nil, errors.Is(err, context.Canceled):
			return nil
		}

		attempt++
		if r.MaxAttempts > 0 && attempt >= r.MaxAttempts {
			return fmt.Errorf("venue %s: giving up after %d attempts: %w", name, attempt, err)
		}

		delay := r.Backoff.Delay(attempt-1, r.rnd)
		r.Log.Warn("venue disconnected, backing off",
			"venue", name, "attempt", attempt, "delay", delay, "err", err)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// session runs one connection to exhaustion.
func (r *Runner) session(ctx context.Context) error {
	name := r.Decoder.Name()

	conn, err := r.Dialer.Dial(ctx, r.Decoder.URL())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	for _, sub := range r.Decoder.Subscriptions(r.Symbols) {
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe %s: %w", name, err)
		}
	}

	if r.OnConnect != nil {
		r.OnConnect(name)
	}
	r.Log.Info("venue connected", "venue", name, "symbols", r.Symbols)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := conn.SetReadDeadline(time.Now().Add(r.ReadTimeout)); err != nil {
			return fmt.Errorf("set read deadline %s: %w", name, err)
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}

		events, err := r.Decoder.Decode(raw, time.Now())
		if err != nil {
			// A single bad frame is not a reason to drop the connection.
			// A sustained rate of them is, and that is what the
			// decode_errors_total alert is for.
			r.Log.Warn("decode failed", "venue", name, "err", err)
			continue
		}

		for _, ev := range events {
			select {
			case r.Out <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// --- shared parsing helpers -------------------------------------------------

// parseFloat parses a venue's stringified number. Exchanges send decimals as
// strings precisely so clients do not round them in JSON parsing; we parse
// once, here, and only here.
func parseFloat(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

// mustFloat parses and swallows the error, returning 0. Used only where the
// venue guarantees numeric content and a zero is harmless.
func mustFloat(s string) float64 {
	f, _ := parseFloat(s)
	return f
}

// parseRFC3339Micros parses an ISO-8601 timestamp to epoch microseconds,
// returning fallback when the field is absent or unparseable.
func parseRFC3339Micros(s string, fallback time.Time) int64 {
	if s == "" {
		return fallback.UnixMicro()
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return fallback.UnixMicro()
	}
	return t.UnixMicro()
}

// canonicalPair normalizes a pair to BASE-QUOTE uppercase.
func canonicalPair(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "/", "-")
	s = strings.ReplaceAll(s, "_", "-")
	return s
}
