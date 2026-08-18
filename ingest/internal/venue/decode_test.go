package venue

import (
	"testing"
	"time"

	"github.com/tapeline/ingest/internal/model"
)

// The payloads below are shaped exactly like the ones the venues send. They
// are the regression suite for the part of this system most likely to break
// without warning: an exchange changing its message format.

var testNow = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

func TestCoinbaseDecodeTrades(t *testing.T) {
	raw := []byte(`{
	  "channel": "market_trades",
	  "client_id": "",
	  "timestamp": "2026-08-17T12:00:00.123456Z",
	  "sequence_num": 41,
	  "events": [{
	    "type": "update",
	    "trades": [
	      {"trade_id":"778899","product_id":"BTC-USD","price":"64231.17","size":"0.0125","side":"BUY","time":"2026-08-17T11:59:59.900000Z"},
	      {"trade_id":"778900","product_id":"ETH-USD","price":"3102.44","size":"1.5","side":"SELL","time":"2026-08-17T11:59:59.950000Z"}
	    ]
	  }]
	}`)

	c := NewCoinbase([]string{"BTC-USD", "ETH-USD"})
	events, err := c.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}

	first := events[0]
	if first.Kind != model.KindTrade || first.Venue != VenueCoinbase || first.Symbol != "BTC-USD" {
		t.Errorf("unexpected envelope: %+v", first)
	}
	if first.Sequence != 41 {
		t.Errorf("sequence = %d, want 41 (from the envelope, not the trade)", first.Sequence)
	}
	tr := first.Trade
	if tr.Price != 64231.17 || tr.Size != 0.0125 || tr.Side != model.SideBuy {
		t.Errorf("trade fields wrong: %+v", tr)
	}
	if tr.TradeID != "778899" {
		t.Errorf("trade id = %q, want 778899", tr.TradeID)
	}
	wantEventUS := time.Date(2026, 8, 17, 11, 59, 59, 900_000_000, time.UTC).UnixMicro()
	if tr.EventTimeUS != wantEventUS {
		t.Errorf("event time = %d, want %d", tr.EventTimeUS, wantEventUS)
	}
	if tr.IngestTimeUS != testNow.UnixMicro() {
		t.Errorf("ingest time = %d, want %d", tr.IngestTimeUS, testNow.UnixMicro())
	}
	if events[1].Trade.Side != model.SideSell {
		t.Errorf("second trade side = %q, want SELL", events[1].Trade.Side)
	}
}

func TestCoinbaseDecodeL2SplitsSides(t *testing.T) {
	raw := []byte(`{
	  "channel": "l2_data",
	  "timestamp": "2026-08-17T12:00:00.000000Z",
	  "sequence_num": 12,
	  "events": [{
	    "type": "snapshot",
	    "product_id": "BTC-USD",
	    "updates": [
	      {"side":"bid","event_time":"2026-08-17T11:59:59.000000Z","price_level":"64230.00","new_quantity":"1.5"},
	      {"side":"offer","event_time":"2026-08-17T11:59:59.000000Z","price_level":"64232.00","new_quantity":"2.0"},
	      {"side":"bid","event_time":"2026-08-17T11:59:59.000000Z","price_level":"64229.00","new_quantity":"0"}
	    ]
	  }]
	}`)

	c := NewCoinbase([]string{"BTC-USD"})
	events, err := c.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}

	b := events[0].Book
	if !b.IsSnapshot {
		t.Error("snapshot flag not set for a snapshot event")
	}
	if len(b.Bids) != 2 || len(b.Asks) != 1 {
		t.Fatalf("side split wrong: %d bids, %d asks", len(b.Bids), len(b.Asks))
	}
	// "offer" must land on the ask side, not fall through to a default.
	if b.Asks[0].Price != 64232.00 {
		t.Errorf("ask price = %v, want 64232", b.Asks[0].Price)
	}
	// A zero quantity is a deletion and must survive as a zero, not be dropped.
	if b.Bids[1].Size != 0 {
		t.Errorf("level deletion lost: %+v", b.Bids[1])
	}
}

func TestCoinbaseIgnoresControlFramesAndSurfacesErrors(t *testing.T) {
	c := NewCoinbase([]string{"BTC-USD"})

	events, err := c.Decode([]byte(`{"channel":"subscriptions","events":[]}`), testNow)
	if err != nil || len(events) != 0 {
		t.Errorf("subscription ack: events=%d err=%v, want 0, nil", len(events), err)
	}

	if _, err := c.Decode([]byte(`{"type":"error","message":"authentication failure"}`), testNow); err == nil {
		t.Error("error frame decoded without error")
	}
}

func TestKrakenDecodeTrades(t *testing.T) {
	raw := []byte(`{
	  "channel":"trade",
	  "type":"update",
	  "data":[
	    {"symbol":"BTC/USD","side":"buy","qty":0.001,"price":64100.5,"ord_type":"market","trade_id":9001,"timestamp":"2026-08-17T11:59:58.500000Z"},
	    {"symbol":"BTC/USD","side":"sell","qty":0.25,"price":64099.0,"ord_type":"limit","trade_id":9002,"timestamp":"2026-08-17T11:59:58.600000Z"}
	  ]}`)

	k := NewKraken([]string{"BTC-USD"}, 10)
	events, err := k.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}

	// BTC/USD must come back as the canonical BTC-USD, or nothing downstream
	// can join Kraken to the other two venues.
	if events[0].Symbol != "BTC-USD" {
		t.Errorf("symbol = %q, want BTC-USD", events[0].Symbol)
	}
	if events[0].Sequence != 9001 {
		t.Errorf("sequence = %d, want the trade_id 9001", events[0].Sequence)
	}
	if events[0].Trade.TradeID != "9001" || events[0].Trade.Side != model.SideBuy {
		t.Errorf("trade wrong: %+v", events[0].Trade)
	}
	if events[1].Trade.Side != model.SideSell || events[1].Trade.Price != 64099.0 {
		t.Errorf("second trade wrong: %+v", events[1].Trade)
	}
}

func TestKrakenDecodeBookIsUnsequenced(t *testing.T) {
	raw := []byte(`{
	  "channel":"book","type":"update",
	  "data":[{
	    "symbol":"ETH/USD",
	    "bids":[{"price":3100.1,"qty":5.0}],
	    "asks":[{"price":3100.9,"qty":0.0}],
	    "checksum":2131876818,
	    "timestamp":"2026-08-17T11:59:59.100000Z"
	  }]}`)

	k := NewKraken([]string{"ETH-USD"}, 10)
	events, err := k.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}

	// Kraken v2 books carry a CRC checksum, not a sequence. Claiming a
	// sequence of 0 here would make the gap detector fire forever.
	if events[0].Sequence != model.NoSequence {
		t.Errorf("sequence = %d, want NoSequence (%d)", events[0].Sequence, model.NoSequence)
	}
	b := events[0].Book
	if b.Symbol != "ETH-USD" || len(b.Bids) != 1 || len(b.Asks) != 1 {
		t.Errorf("book decode wrong: %+v", b)
	}
	if b.IsSnapshot {
		t.Error("type=update decoded as a snapshot")
	}
}

func TestKrakenSubscriptionFailureIsAnError(t *testing.T) {
	k := NewKraken([]string{"BTC-USD"}, 10)
	raw := []byte(`{"method":"subscribe","success":false,"error":"Currency pair not supported BTC/XYZ"}`)
	if _, err := k.Decode(raw, testNow); err == nil {
		t.Error("failed subscription ack decoded without error")
	}
}

func TestKrakenSymbolMapping(t *testing.T) {
	for in, want := range map[string]string{
		"BTC-USD": "BTC/USD",
		"eth-usd": "ETH/USD",
		"SOL-USD": "SOL/USD",
	} {
		if got := KrakenSymbol(in); got != want {
			t.Errorf("KrakenSymbol(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBinanceSymbolMapping(t *testing.T) {
	// Binance lists no USD spot pairs, so canonical USD maps to USDT.
	for in, want := range map[string]string{
		"BTC-USD":  "BTCUSDT",
		"ETH-USD":  "ETHUSDT",
		"SOL-USDC": "SOLUSDC",
		"ETH-BTC":  "ETHBTC",
	} {
		if got := BinanceSymbol(in); got != want {
			t.Errorf("BinanceSymbol(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBinanceDecodeTradeAggressorSide(t *testing.T) {
	b := NewBinance([]string{"BTC-USD"}, 100)

	// m=true means the buyer was the resting maker, so the aggressor sold.
	makerBuy := []byte(`{"stream":"btcusdt@trade","data":{
	  "e":"trade","E":1755432000000,"s":"BTCUSDT","t":51234,
	  "p":"64150.10","q":"0.004","T":1755431999900,"m":true,"M":true}}`)

	events, err := b.Decode(makerBuy, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Trade.Side != model.SideSell {
		t.Errorf("m=true side = %q, want SELL (buyer was the maker)", events[0].Trade.Side)
	}
	if events[0].Symbol != "BTC-USD" {
		t.Errorf("symbol = %q, want BTC-USD", events[0].Symbol)
	}
	if events[0].Trade.EventTimeUS != 1755431999900*1000 {
		t.Errorf("event time = %d, want trade time in micros", events[0].Trade.EventTimeUS)
	}

	takerBuy := []byte(`{"stream":"btcusdt@trade","data":{
	  "e":"trade","E":1755432000000,"s":"BTCUSDT","t":51235,
	  "p":"64150.20","q":"0.004","T":1755431999950,"m":false,"M":true}}`)
	events, err = b.Decode(takerBuy, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if events[0].Trade.Side != model.SideBuy {
		t.Errorf("m=false side = %q, want BUY", events[0].Trade.Side)
	}
}

// Binance depth updates carry an [U, u] range and the contiguity rule is
// U == previous u + 1. This test pins the translation into the dense
// sequence the shared gap detector understands.
func TestBinanceDepthDenseSequence(t *testing.T) {
	b := NewBinance([]string{"BTC-USD"}, 100)

	depth := func(first, final int64) []byte {
		return []byte(`{"stream":"btcusdt@depth","data":{
		  "e":"depthUpdate","E":1755432000000,"s":"BTCUSDT",
		  "U":` + itoa(first) + `,"u":` + itoa(final) + `,
		  "b":[["64150.00","1.5"]],"a":[["64151.00","0.0"]]}}`)
	}

	decodeSeq := func(t *testing.T, raw []byte) int64 {
		t.Helper()
		events, err := b.Decode(raw, testNow)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("got %d events, want 1", len(events))
		}
		return events[0].Sequence
	}

	// First update establishes the baseline.
	if got := decodeSeq(t, depth(100, 110)); got != 0 {
		t.Fatalf("first dense sequence = %d, want 0", got)
	}
	// Contiguous: U == previous u + 1. Advances by exactly one.
	if got := decodeSeq(t, depth(111, 130)); got != 1 {
		t.Fatalf("contiguous dense sequence = %d, want 1", got)
	}
	if got := decodeSeq(t, depth(131, 131)); got != 2 {
		t.Fatalf("contiguous dense sequence = %d, want 2", got)
	}
	// A break: ids 132..140 never arrived, so 9 are missing and the counter
	// jumps by 10 (the 9 missing plus this one).
	if got := decodeSeq(t, depth(141, 150)); got != 12 {
		t.Fatalf("post-gap dense sequence = %d, want 12", got)
	}
	// An overlapping retransmit must not advance the counter.
	if got := decodeSeq(t, depth(145, 150)); got != 12 {
		t.Fatalf("retransmit dense sequence = %d, want 12 (unchanged)", got)
	}

	// After a reconnect Binance restarts its ids, so the cursor is dropped.
	b.ResetBooks()
	if got := decodeSeq(t, depth(9, 9)); got != 0 {
		t.Fatalf("post-reset dense sequence = %d, want 0", got)
	}
}

func TestBinanceDepthLevelsAndDeletions(t *testing.T) {
	b := NewBinance([]string{"BTC-USD"}, 100)
	raw := []byte(`{"stream":"btcusdt@depth","data":{
	  "e":"depthUpdate","E":1755432000000,"s":"BTCUSDT","U":1,"u":2,
	  "b":[["64150.00","1.5"],["64149.00","0"]],
	  "a":[["64151.00","2.25"]]}}`)

	events, err := b.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	book := events[0].Book
	if len(book.Bids) != 2 || len(book.Asks) != 1 {
		t.Fatalf("levels wrong: %d bids %d asks", len(book.Bids), len(book.Asks))
	}
	if book.Bids[1].Size != 0 {
		t.Errorf("deletion level lost: %+v", book.Bids[1])
	}
	if book.IsSnapshot {
		t.Error("diff-depth stream marked as a snapshot; it is always incremental")
	}
}

func TestBinanceMalformedLevelIsAnError(t *testing.T) {
	b := NewBinance([]string{"BTC-USD"}, 100)
	raw := []byte(`{"stream":"btcusdt@depth","data":{
	  "e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[["64150.00"]],"a":[]}}`)
	if _, err := b.Decode(raw, testNow); err == nil {
		t.Error("a one-element price level decoded without error")
	}
}

func TestBinanceURLCarriesSubscription(t *testing.T) {
	b := NewBinance([]string{"BTC-USD", "ETH-USD"}, 100)
	url := b.URL()
	for _, want := range []string{"btcusdt@trade", "btcusdt@depth@100ms", "ethusdt@trade"} {
		if !contains(url, want) {
			t.Errorf("URL %q missing stream %q", url, want)
		}
	}
	if subs := b.Subscriptions([]string{"BTC-USD"}); len(subs) != 0 {
		t.Errorf("Binance sent %d subscribe frames; the URL carries them", len(subs))
	}
}

func TestUnknownVenueSymbolFallsBackToKnownQuoteSplit(t *testing.T) {
	// A symbol that was never subscribed still has to resolve, because
	// Binance will happily send you a stream you did not ask for after a
	// combined-stream reconnect.
	b := NewBinance(nil, 100)
	raw := []byte(`{"stream":"solusdt@trade","data":{
	  "e":"trade","E":1,"s":"SOLUSDT","t":1,"p":"140.5","q":"3","T":1,"m":false}}`)
	events, err := b.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if events[0].Symbol != "SOL-USD" {
		t.Errorf("symbol = %q, want SOL-USD", events[0].Symbol)
	}
}

func TestNormalizeSide(t *testing.T) {
	for in, want := range map[string]string{
		"buy": model.SideBuy, "BUY": model.SideBuy, "b": model.SideBuy, "bid": model.SideBuy,
		"sell": model.SideSell, "SELL": model.SideSell, "offer": model.SideSell, "ask": model.SideSell,
		"": model.SideUnknown, "weird": model.SideUnknown,
	} {
		if got := model.NormalizeSide(in); got != want {
			t.Errorf("NormalizeSide(%q) = %q, want %q", in, got, want)
		}
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// A Coinbase frame can carry several trades. The sequence number belongs to
// the frame, so stamping it on every trade makes all but the first look like
// duplicates — and the pipeline drops them.
//
// A live run published 641 of 1,575 Coinbase trades before this was fixed:
// 59% real data loss, counted as "duplicates suppressed", which is the shape
// of bug that never trips an alert.
func TestCoinbaseMultiTradeFrameDoesNotLoseTrades(t *testing.T) {
	raw := []byte(`{
	  "channel": "market_trades",
	  "timestamp": "2026-08-17T12:00:00.123456Z",
	  "sequence_num": 77,
	  "events": [{
	    "type": "update",
	    "trades": [
	      {"trade_id":"1","product_id":"BTC-USD","price":"64000.00","size":"0.1","side":"BUY","time":"2026-08-17T11:59:59.900000Z"},
	      {"trade_id":"2","product_id":"BTC-USD","price":"64001.00","size":"0.2","side":"SELL","time":"2026-08-17T11:59:59.910000Z"},
	      {"trade_id":"3","product_id":"BTC-USD","price":"64002.00","size":"0.3","side":"BUY","time":"2026-08-17T11:59:59.920000Z"}
	    ]
	  }]
	}`)

	events, err := NewCoinbase([]string{"BTC-USD"}).Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want all 3 trades", len(events))
	}

	// Only the first carries the frame sequence; the rest are unsequenced so
	// the gap detector cannot mistake them for repeats.
	if events[0].Sequence != 77 {
		t.Errorf("first trade sequence = %d, want the frame's 77", events[0].Sequence)
	}
	for i, ev := range events[1:] {
		if ev.Sequence != model.NoSequence {
			t.Errorf("trade %d sequence = %d, want NoSequence", i+1, ev.Sequence)
		}
	}

	// Every trade must survive intact — identity comes from trade_id.
	for i, want := range []string{"1", "2", "3"} {
		if events[i].Trade.TradeID != want {
			t.Errorf("trade %d id = %q, want %q", i, events[i].Trade.TradeID, want)
		}
	}
	if events[2].Trade.Price != 64002.00 {
		t.Errorf("last trade price = %v, want 64002", events[2].Trade.Price)
	}
}

// The same hazard on the level2 path: one frame can carry updates for
// several products.
func TestCoinbaseMultiProductL2FrameDoesNotLoseUpdates(t *testing.T) {
	raw := []byte(`{
	  "channel": "l2_data",
	  "timestamp": "2026-08-17T12:00:00.000000Z",
	  "sequence_num": 88,
	  "events": [
	    {"type":"update","product_id":"BTC-USD","updates":[
	      {"side":"bid","event_time":"2026-08-17T11:59:59.000000Z","price_level":"64000.00","new_quantity":"1.5"}]},
	    {"type":"update","product_id":"ETH-USD","updates":[
	      {"side":"offer","event_time":"2026-08-17T11:59:59.000000Z","price_level":"3100.00","new_quantity":"2.0"}]}
	  ]
	}`)

	events, err := NewCoinbase([]string{"BTC-USD", "ETH-USD"}).Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 product updates", len(events))
	}
	if events[0].Sequence != 88 {
		t.Errorf("first update sequence = %d, want 88", events[0].Sequence)
	}
	if events[1].Sequence != model.NoSequence {
		t.Errorf("second update sequence = %d, want NoSequence", events[1].Sequence)
	}
	if events[1].Symbol != "ETH-USD" {
		t.Errorf("second update symbol = %q, want ETH-USD", events[1].Symbol)
	}
}
