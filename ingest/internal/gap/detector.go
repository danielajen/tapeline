// Package gap tracks per-stream sequence continuity.
//
// Every venue that publishes sequence numbers publishes them per (venue,
// symbol, channel) — never globally — so the detector keys on that triple.
// A gap means the WebSocket dropped frames and the local book is now wrong;
// the correct response is to resubscribe and take a fresh snapshot, not to
// keep applying deltas to a book you know is stale.
package gap

import (
	"fmt"
	"sync"
)

// Status is the verdict for one observed sequence number.
type Status int

const (
	// StatusOK means the sequence advanced by exactly one.
	StatusOK Status = iota
	// StatusFirst means this is the first sequence seen for the stream.
	StatusFirst
	// StatusGap means sequences were skipped; the book must be resynced.
	StatusGap
	// StatusDuplicate means the sequence was already seen (replay or retransmit).
	StatusDuplicate
	// StatusRegression means the sequence went backwards by more than a
	// duplicate would explain — usually a venue-side counter reset.
	StatusRegression
	// StatusUnsequenced means the venue supplies no sequence for this stream.
	StatusUnsequenced
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusFirst:
		return "first"
	case StatusGap:
		return "gap"
	case StatusDuplicate:
		return "duplicate"
	case StatusRegression:
		return "regression"
	case StatusUnsequenced:
		return "unsequenced"
	}
	return "unknown"
}

// Key identifies one sequenced stream.
type Key struct {
	Venue   string
	Symbol  string
	Channel string
}

func (k Key) String() string { return fmt.Sprintf("%s/%s/%s", k.Venue, k.Symbol, k.Channel) }

// Result is the verdict plus enough detail to log or alert on.
type Result struct {
	Key      Key
	Status   Status
	Expected int64
	Got      int64
	// Missing is how many sequence numbers were skipped. Non-zero only for
	// StatusGap.
	Missing int64
}

// NeedsResync reports whether the local view of this stream can no longer be
// trusted.
func (r Result) NeedsResync() bool {
	return r.Status == StatusGap || r.Status == StatusRegression
}

// Detector is safe for concurrent use across venue goroutines.
type Detector struct {
	mu   sync.Mutex
	last map[Key]int64

	// regressionTolerance is how far backwards a sequence may go before it is
	// treated as a counter reset rather than a duplicate. Venues retransmit a
	// handful of recent frames after a reconnect; they do not retransmit
	// thousands.
	regressionTolerance int64
}

// New returns a Detector with the default regression tolerance.
func New() *Detector { return NewWithTolerance(1000) }

// NewWithTolerance lets tests pin the tolerance.
func NewWithTolerance(tolerance int64) *Detector {
	return &Detector{last: make(map[Key]int64), regressionTolerance: tolerance}
}

// Observe records a sequence number and returns the verdict.
func (d *Detector) Observe(k Key, seq int64) Result {
	if seq < 0 {
		return Result{Key: k, Status: StatusUnsequenced, Got: seq}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	last, seen := d.last[k]
	if !seen {
		d.last[k] = seq
		return Result{Key: k, Status: StatusFirst, Expected: seq, Got: seq}
	}

	expected := last + 1
	switch {
	case seq == expected:
		d.last[k] = seq
		return Result{Key: k, Status: StatusOK, Expected: expected, Got: seq}

	case seq > expected:
		d.last[k] = seq
		return Result{
			Key: k, Status: StatusGap,
			Expected: expected, Got: seq,
			Missing: seq - expected,
		}

	case last-seq <= d.regressionTolerance:
		// Already seen. Do not move `last` backwards — a retransmit must not
		// re-open a gap we already closed.
		return Result{Key: k, Status: StatusDuplicate, Expected: expected, Got: seq}

	default:
		// A large backwards jump is a venue counter reset. Adopt the new
		// baseline, because refusing to would gap-alert on every message from
		// here on.
		d.last[k] = seq
		return Result{Key: k, Status: StatusRegression, Expected: expected, Got: seq}
	}
}

// Reset forgets a stream, which is what you do after resubscribing and
// requesting a fresh snapshot.
func (d *Detector) Reset(k Key) {
	d.mu.Lock()
	delete(d.last, k)
	d.mu.Unlock()
}

// ResetVenue forgets every stream for a venue, used on reconnect.
func (d *Detector) ResetVenue(venue string) {
	d.mu.Lock()
	for k := range d.last {
		if k.Venue == venue {
			delete(d.last, k)
		}
	}
	d.mu.Unlock()
}

// Last returns the last sequence recorded for a stream.
func (d *Detector) Last(k Key) (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.last[k]
	return v, ok
}
