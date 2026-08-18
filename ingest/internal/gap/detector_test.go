package gap

import (
	"sync"
	"testing"
)

func key() Key { return Key{Venue: "coinbase", Symbol: "BTC-USD", Channel: "book"} }

func TestObserveSequenceLifecycle(t *testing.T) {
	tests := []struct {
		name        string
		seqs        []int64
		wantStatus  []Status
		wantMissing []int64
	}{
		{
			name:        "contiguous run",
			seqs:        []int64{10, 11, 12, 13},
			wantStatus:  []Status{StatusFirst, StatusOK, StatusOK, StatusOK},
			wantMissing: []int64{0, 0, 0, 0},
		},
		{
			name:        "single dropped message",
			seqs:        []int64{1, 3},
			wantStatus:  []Status{StatusFirst, StatusGap},
			wantMissing: []int64{0, 1},
		},
		{
			name:        "large gap reports magnitude",
			seqs:        []int64{100, 5000},
			wantStatus:  []Status{StatusFirst, StatusGap},
			wantMissing: []int64{0, 4899},
		},
		{
			name:        "retransmit is a duplicate, not a gap",
			seqs:        []int64{5, 6, 6, 7},
			wantStatus:  []Status{StatusFirst, StatusOK, StatusDuplicate, StatusOK},
			wantMissing: []int64{0, 0, 0, 0},
		},
		{
			name:        "venue counter reset adopts the new baseline",
			seqs:        []int64{900_000, 900_001, 5, 6},
			wantStatus:  []Status{StatusFirst, StatusOK, StatusRegression, StatusOK},
			wantMissing: []int64{0, 0, 0, 0},
		},
		{
			name:        "unsequenced venues never gap",
			seqs:        []int64{-1, -1, -1},
			wantStatus:  []Status{StatusUnsequenced, StatusUnsequenced, StatusUnsequenced},
			wantMissing: []int64{0, 0, 0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New()
			for i, seq := range tc.seqs {
				got := d.Observe(key(), seq)
				if got.Status != tc.wantStatus[i] {
					t.Fatalf("seq %d (index %d): status = %v, want %v",
						seq, i, got.Status, tc.wantStatus[i])
				}
				if got.Missing != tc.wantMissing[i] {
					t.Fatalf("seq %d (index %d): missing = %d, want %d",
						seq, i, got.Missing, tc.wantMissing[i])
				}
			}
		})
	}
}

// A duplicate must not rewind the watermark. If it did, the next in-order
// message would be reported as a gap that never happened — and a spurious
// gap triggers a book resync, which is expensive.
func TestDuplicateDoesNotRewindWatermark(t *testing.T) {
	d := New()
	d.Observe(key(), 10)
	d.Observe(key(), 11)
	d.Observe(key(), 12)

	if got := d.Observe(key(), 11); got.Status != StatusDuplicate {
		t.Fatalf("replayed 11: status = %v, want duplicate", got.Status)
	}
	if got := d.Observe(key(), 13); got.Status != StatusOK {
		t.Fatalf("13 after replay: status = %v, want ok (watermark was rewound)", got.Status)
	}
}

func TestStreamsAreIndependent(t *testing.T) {
	d := New()
	btc := Key{Venue: "coinbase", Symbol: "BTC-USD", Channel: "book"}
	eth := Key{Venue: "coinbase", Symbol: "ETH-USD", Channel: "book"}
	trades := Key{Venue: "coinbase", Symbol: "BTC-USD", Channel: "trades"}

	d.Observe(btc, 100)
	d.Observe(eth, 1)
	d.Observe(trades, 77)

	if got := d.Observe(btc, 101); got.Status != StatusOK {
		t.Fatalf("BTC book: %v, want ok", got.Status)
	}
	if got := d.Observe(eth, 2); got.Status != StatusOK {
		t.Fatalf("ETH book: %v, want ok", got.Status)
	}
	if got := d.Observe(trades, 78); got.Status != StatusOK {
		t.Fatalf("BTC trades: %v, want ok", got.Status)
	}
}

func TestResetVenueClearsOnlyThatVenue(t *testing.T) {
	d := New()
	cb := Key{Venue: "coinbase", Symbol: "BTC-USD", Channel: "book"}
	kr := Key{Venue: "kraken", Symbol: "BTC-USD", Channel: "book"}

	d.Observe(cb, 500)
	d.Observe(kr, 900)

	d.ResetVenue("coinbase")

	if _, ok := d.Last(cb); ok {
		t.Fatal("coinbase stream survived ResetVenue")
	}
	if last, ok := d.Last(kr); !ok || last != 900 {
		t.Fatalf("kraken stream was cleared: last=%d ok=%v", last, ok)
	}
	// After a reset the next observation is a fresh baseline, not a gap.
	if got := d.Observe(cb, 1); got.Status != StatusFirst {
		t.Fatalf("post-reset status = %v, want first", got.Status)
	}
}

func TestNeedsResync(t *testing.T) {
	for _, tc := range []struct {
		status Status
		want   bool
	}{
		{StatusOK, false},
		{StatusFirst, false},
		{StatusDuplicate, false},
		{StatusUnsequenced, false},
		{StatusGap, true},
		{StatusRegression, true},
	} {
		if got := (Result{Status: tc.status}).NeedsResync(); got != tc.want {
			t.Errorf("%v.NeedsResync() = %v, want %v", tc.status, got, tc.want)
		}
	}
}

// The detector is shared by one goroutine per venue, so the race detector
// needs something to chew on.
func TestConcurrentObserveIsRaceFree(t *testing.T) {
	d := New()
	var wg sync.WaitGroup
	for v := 0; v < 8; v++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			k := Key{Venue: "v", Symbol: string(rune('A' + v)), Channel: "book"}
			for i := int64(0); i < 500; i++ {
				d.Observe(k, i)
			}
		}(v)
	}
	wg.Wait()
}
