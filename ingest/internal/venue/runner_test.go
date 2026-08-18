package venue

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/tapeline/ingest/internal/model"
)

// scriptedConn replays a fixed set of frames and then fails, which is how a
// dropped WebSocket actually behaves.
type scriptedConn struct {
	mu       sync.Mutex
	frames   [][]byte
	idx      int
	failWith error
	written  []any
	closed   bool
}

func (c *scriptedConn) ReadMessage() (int, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.idx < len(c.frames) {
		f := c.frames[c.idx]
		c.idx++
		return 1, f, nil
	}
	if c.failWith != nil {
		return 0, nil, c.failWith
	}
	return 0, nil, io.EOF
}

func (c *scriptedConn) WriteJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.written = append(c.written, v)
	return nil
}

func (c *scriptedConn) SetReadDeadline(time.Time) error { return nil }

func (c *scriptedConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// countingDialer hands out a fresh scriptedConn per dial and records how many
// times it was asked.
type countingDialer struct {
	mu     sync.Mutex
	dials  int
	frames [][]byte
	// failDialsBefore makes the first N dials fail outright, exercising the
	// "cannot even connect" branch rather than the "connected then dropped"
	// one.
	failDialsBefore int
	conns           []*scriptedConn
}

func (d *countingDialer) Dial(_ context.Context, _ string) (Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dials++
	if d.dials <= d.failDialsBefore {
		return nil, errors.New("connection refused")
	}
	c := &scriptedConn{frames: d.frames, failWith: errors.New("unexpected EOF")}
	d.conns = append(d.conns, c)
	return c, nil
}

func (d *countingDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials
}

func fastBackoff() Backoff {
	return Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond, Factor: 2, Jitter: 0}
}

func TestRunnerReconnectsAfterDrop(t *testing.T) {
	frame := []byte(`{"channel":"trade","type":"update","data":[
	  {"symbol":"BTC/USD","side":"buy","qty":1,"price":100,"trade_id":1,"timestamp":"2026-08-17T12:00:00.000000Z"}]}`)

	dialer := &countingDialer{frames: [][]byte{frame}}
	out := make(chan model.Event, 64)

	var connects int
	var mu sync.Mutex

	r := &Runner{
		Decoder:     NewKraken([]string{"BTC-USD"}, 10),
		Symbols:     []string{"BTC-USD"},
		Dialer:      dialer,
		Out:         out,
		Backoff:     fastBackoff(),
		ReadTimeout: time.Second,
		OnConnect: func(string) {
			mu.Lock()
			connects++
			mu.Unlock()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Each session yields one event, then the socket drops and the runner
	// must come back. Three events means it reconnected at least twice.
	for i := 0; i < 3; i++ {
		select {
		case ev := <-out:
			if ev.Symbol != "BTC-USD" {
				t.Fatalf("event %d: symbol = %q", i, ev.Symbol)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d (dials so far: %d)", i, dialer.count())
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v, want nil on context cancel", err)
	}

	if dialer.count() < 3 {
		t.Errorf("dialed %d times, want at least 3", dialer.count())
	}
	mu.Lock()
	defer mu.Unlock()
	if connects < 3 {
		t.Errorf("OnConnect fired %d times, want at least 3 — sequence state would go stale", connects)
	}
}

func TestRunnerSendsSubscriptionsOnEverySession(t *testing.T) {
	dialer := &countingDialer{frames: nil}
	out := make(chan model.Event, 8)

	r := &Runner{
		Decoder:     NewKraken([]string{"BTC-USD"}, 10),
		Symbols:     []string{"BTC-USD"},
		Dialer:      dialer,
		Out:         out,
		Backoff:     fastBackoff(),
		ReadTimeout: time.Second,
		MaxAttempts: 3,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)

	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if len(dialer.conns) == 0 {
		t.Fatal("no connections were made")
	}
	for i, c := range dialer.conns {
		c.mu.Lock()
		n := len(c.written)
		closed := c.closed
		c.mu.Unlock()
		// Kraken subscribes to trade and book: two frames, every session.
		if n != 2 {
			t.Errorf("session %d sent %d subscribe frames, want 2", i, n)
		}
		if !closed {
			t.Errorf("session %d leaked its connection", i)
		}
	}
}

func TestRunnerGivesUpAfterMaxAttempts(t *testing.T) {
	dialer := &countingDialer{failDialsBefore: 1000}

	r := &Runner{
		Decoder:     NewKraken([]string{"BTC-USD"}, 10),
		Dialer:      dialer,
		Out:         make(chan model.Event, 1),
		Backoff:     fastBackoff(),
		MaxAttempts: 4,
	}

	err := r.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil, want an error after exhausting attempts")
	}
	if dialer.count() != 4 {
		t.Errorf("dialed %d times, want exactly 4", dialer.count())
	}
}

// A bad frame must not take the connection down with it. Venues ship
// malformed messages; dropping the socket for each one turns a cosmetic
// problem into an outage.
func TestRunnerSurvivesUndecodableFrames(t *testing.T) {
	good := []byte(`{"channel":"trade","type":"update","data":[
	  {"symbol":"BTC/USD","side":"buy","qty":1,"price":100,"trade_id":1,"timestamp":"2026-08-17T12:00:00.000000Z"}]}`)
	bad := []byte(`{"channel":"trade","type":"update","data":"this should be an array"}`)

	dialer := &countingDialer{frames: [][]byte{bad, good, bad, good}}
	out := make(chan model.Event, 16)

	r := &Runner{
		Decoder:     NewKraken([]string{"BTC-USD"}, 10),
		Dialer:      dialer,
		Out:         out,
		Backoff:     fastBackoff(),
		ReadTimeout: time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// Both good frames from the first session should arrive despite the two
	// bad ones bracketing them.
	for i := 0; i < 2; i++ {
		select {
		case <-out:
		case <-time.After(2 * time.Second):
			t.Fatalf("bad frame killed the session before good frame %d", i)
		}
	}
	if dialer.count() != 1 {
		t.Errorf("dialed %d times; a decode error should not reconnect", dialer.count())
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: time.Second, Factor: 2, Jitter: 0}
	rnd := rand.New(rand.NewSource(1))

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second, // capped
		time.Second,
	}
	for i, w := range want {
		if got := b.Delay(i, rnd); got != w {
			t.Errorf("Delay(%d) = %v, want %v", i, got, w)
		}
	}

	// A huge attempt count must not overflow into a negative or absurd delay.
	if got := b.Delay(10_000, rnd); got != time.Second {
		t.Errorf("Delay(10000) = %v, want the cap %v", got, time.Second)
	}
}

func TestBackoffJitterStaysInBand(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Second, Factor: 2, Jitter: 0.5}
	rnd := rand.New(rand.NewSource(42))

	// Jitter of 0.5 spreads the delay across +/-25% of the nominal value.
	lo, hi := 750*time.Millisecond, 1250*time.Millisecond
	var sawSpread bool
	prev := b.Delay(3, rnd)
	for i := 0; i < 200; i++ {
		d := b.Delay(3, rnd)
		if d < lo || d > hi {
			t.Fatalf("jittered delay %v outside [%v, %v]", d, lo, hi)
		}
		if d != prev {
			sawSpread = true
		}
		prev = d
	}
	if !sawSpread {
		t.Error("jitter produced identical delays; venues would reconnect in lockstep")
	}
}
