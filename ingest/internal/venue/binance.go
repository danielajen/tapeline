package venue

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tapeline/ingest/internal/model"
)

// VenueBinance is the canonical venue id.
const VenueBinance = "binance"

// binanceQuoteAliases maps a canonical quote currency to the one Binance
// actually lists. Binance has no USD spot pairs; USDT is the standing proxy.
// Keeping the substitution here — rather than making callers write BTC-USDT —
// means the canonical symbol space stays uniform across venues, which is what
// lets the divergence detector compare like with like.
var binanceQuoteAliases = map[string]string{
	"USD": "USDT",
}

// binanceKnownQuotes is the fallback split order for reversing a venue
// symbol we never subscribed to. Longest first, so USDT wins over USD.
var binanceKnownQuotes = []string{"USDT", "USDC", "TUSD", "FDUSD", "BUSD", "USD", "BTC", "ETH", "BNB"}

// Binance adapts the Binance combined-stream market data WebSocket.
//
// Unlike the other two adapters this one carries state, and the reason is
// specific: Binance depth updates carry a [U, u] update-id *range*, and the
// documented contiguity rule is `U == previous u + 1`. Update ids advance by
// the number of levels changed, so feeding u straight into a +1 gap detector
// would report a gap on essentially every message. The adapter therefore
// translates Binance's range semantics into a dense sequence: one step per
// in-order update, and a jump equal to the number of missing ids when the
// rule breaks — so sequence_messages_missed_total carries real magnitude
// rather than a constant 1.
type Binance struct {
	mu      sync.Mutex
	toCanon map[string]string // BTCUSDT -> BTC-USD
	symbols []string
	depthMS int
	bookSeq map[string]*binanceBookCursor
}

type binanceBookCursor struct {
	lastFinalUpdateID int64
	dense             int64
	initialized       bool
}

// NewBinance builds the adapter. depthMS is the depth update interval in
// milliseconds; Binance accepts 100 or 1000, and 0 selects 100.
func NewBinance(symbols []string, depthMS int) *Binance {
	if depthMS != 1000 {
		depthMS = 100
	}
	b := &Binance{
		toCanon: make(map[string]string),
		depthMS: depthMS,
		bookSeq: make(map[string]*binanceBookCursor),
	}
	b.setSymbols(symbols)
	return b
}

func (b *Binance) setSymbols(symbols []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.symbols = append([]string(nil), symbols...)
	for _, s := range symbols {
		b.toCanon[BinanceSymbol(s)] = canonicalPair(s)
	}
}

// Name implements Decoder.
func (b *Binance) Name() string { return VenueBinance }

// BinanceSymbol converts a canonical pair to Binance's concatenated form.
func BinanceSymbol(canonical string) string {
	parts := strings.SplitN(canonicalPair(canonical), "-", 2)
	if len(parts) != 2 {
		return strings.ReplaceAll(canonicalPair(canonical), "-", "")
	}
	quote := parts[1]
	if alias, ok := binanceQuoteAliases[quote]; ok {
		quote = alias
	}
	return parts[0] + quote
}

// canonFor reverses a Binance symbol, falling back to a known-quote split
// for symbols that arrive without a subscription entry.
func (b *Binance) canonFor(venueSymbol string) string {
	up := strings.ToUpper(venueSymbol)

	b.mu.Lock()
	canon, ok := b.toCanon[up]
	b.mu.Unlock()
	if ok {
		return canon
	}

	for _, q := range binanceKnownQuotes {
		if strings.HasSuffix(up, q) && len(up) > len(q) {
			base := up[:len(up)-len(q)]
			// Undo the USD->USDT substitution so the canonical space stays
			// consistent with the other venues.
			if q == "USDT" {
				q = "USD"
			}
			return base + "-" + q
		}
	}
	return up
}

// URL implements Decoder. Binance takes the stream list in the URL rather
// than in a subscribe frame, so there is nothing to send after connecting.
func (b *Binance) URL() string {
	b.mu.Lock()
	symbols := append([]string(nil), b.symbols...)
	depthMS := b.depthMS
	b.mu.Unlock()

	streams := make([]string, 0, len(symbols)*2)
	for _, s := range symbols {
		v := strings.ToLower(BinanceSymbol(s))
		streams = append(streams, v+"@trade", fmt.Sprintf("%s@depth@%dms", v, depthMS))
	}
	return "wss://stream.binance.com:9443/stream?streams=" + strings.Join(streams, "/")
}

// Subscriptions implements Decoder and returns nothing: the URL carries the
// subscription.
func (b *Binance) Subscriptions(symbols []string) []any {
	if len(symbols) > 0 {
		b.setSymbols(symbols)
	}
	return nil
}

type binanceEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
	// Present on error frames.
	Error *struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	} `json:"error"`
}

type binanceTrade struct {
	EventType   string `json:"e"`
	EventTimeMS int64  `json:"E"`
	Symbol      string `json:"s"`
	TradeID     int64  `json:"t"`
	Price       string `json:"p"`
	Quantity    string `json:"q"`
	TradeTimeMS int64  `json:"T"`
	// BuyerIsMaker is Binance's "m". If the buyer was resting, the seller
	// was the aggressor.
	BuyerIsMaker bool `json:"m"`
	// Ignore is Binance's deprecated "M" flag. It is decoded into a field of
	// its own for one reason: encoding/json matches keys case-insensitively
	// when no exact match exists, so without this field "M" binds to
	// BuyerIsMaker and silently inverts the aggressor side of every trade.
	Ignore bool `json:"M"`
}

type binanceDepth struct {
	EventType     string     `json:"e"`
	EventTimeMS   int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

// Decode implements Decoder.
func (b *Binance) Decode(raw []byte, now time.Time) ([]model.Event, error) {
	var env binanceEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("binance envelope: %w", err)
	}
	if env.Error != nil {
		return nil, fmt.Errorf("binance error %d: %s", env.Error.Code, env.Error.Msg)
	}
	if len(env.Data) == 0 {
		// Combined-stream acks arrive as {"result":null,"id":1}.
		return nil, nil
	}

	// Both payload shapes carry "e"; peek at it rather than switching on the
	// stream name, which is absent when a raw (non-combined) stream is used.
	//
	// The peek goes through a map rather than a one-field struct on purpose.
	// encoding/json falls back to case-insensitive field matching, and
	// Binance sends both "e" (event type, a string) and "E" (event time, a
	// number) in the same object — so a struct with only a `json:"e"` tag
	// binds to whichever arrives last and fails with a type error. Map keys
	// are matched exactly.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(env.Data, &probe); err != nil {
		return nil, fmt.Errorf("binance data probe: %w", err)
	}
	var eventType string
	if rawType, ok := probe["e"]; ok {
		if err := json.Unmarshal(rawType, &eventType); err != nil {
			return nil, fmt.Errorf("binance event type: %w", err)
		}
	}

	switch eventType {
	case "trade":
		return b.decodeTrade(env.Data, now)
	case "depthUpdate":
		return b.decodeDepth(env.Data, now)
	default:
		return nil, nil
	}
}

func (b *Binance) decodeTrade(data []byte, now time.Time) ([]model.Event, error) {
	var t binanceTrade
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("binance trade: %w", err)
	}

	price, err := parseFloat(t.Price)
	if err != nil {
		return nil, fmt.Errorf("binance trade price %q: %w", t.Price, err)
	}
	qty, err := parseFloat(t.Quantity)
	if err != nil {
		return nil, fmt.Errorf("binance trade qty %q: %w", t.Quantity, err)
	}

	// `m` is "buyer is the market maker". If the buyer was resting, the
	// aggressor was the seller.
	side := model.SideBuy
	if t.BuyerIsMaker {
		side = model.SideSell
	}

	sym := b.canonFor(t.Symbol)
	eventTimeUS := t.TradeTimeMS * 1000
	if t.TradeTimeMS == 0 {
		eventTimeUS = t.EventTimeMS * 1000
	}

	return []model.Event{{
		Kind:     model.KindTrade,
		Venue:    VenueBinance,
		Symbol:   sym,
		Sequence: t.TradeID,
		Trade: &model.Trade{
			Venue:        VenueBinance,
			Symbol:       sym,
			TradeID:      strconv.FormatInt(t.TradeID, 10),
			Price:        price,
			Size:         qty,
			Side:         side,
			EventTimeUS:  eventTimeUS,
			IngestTimeUS: now.UnixMicro(),
			Sequence:     t.TradeID,
		},
	}}, nil
}

func (b *Binance) decodeDepth(data []byte, now time.Time) ([]model.Event, error) {
	var d binanceDepth
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("binance depth: %w", err)
	}

	sym := b.canonFor(d.Symbol)
	delta := &model.BookDelta{
		Venue:        VenueBinance,
		Symbol:       sym,
		IsSnapshot:   false, // diff-depth stream is always incremental
		Bids:         make([]model.Level, 0, len(d.Bids)),
		Asks:         make([]model.Level, 0, len(d.Asks)),
		EventTimeUS:  d.EventTimeMS * 1000,
		IngestTimeUS: now.UnixMicro(),
		Sequence:     b.denseSequence(sym, d.FirstUpdateID, d.FinalUpdateID),
	}
	for _, lvl := range d.Bids {
		if len(lvl) < 2 {
			return nil, fmt.Errorf("binance depth: malformed bid level %v", lvl)
		}
		delta.Bids = append(delta.Bids, model.Level{Price: mustFloat(lvl[0]), Size: mustFloat(lvl[1])})
	}
	for _, lvl := range d.Asks {
		if len(lvl) < 2 {
			return nil, fmt.Errorf("binance depth: malformed ask level %v", lvl)
		}
		delta.Asks = append(delta.Asks, model.Level{Price: mustFloat(lvl[0]), Size: mustFloat(lvl[1])})
	}

	return []model.Event{{
		Kind:     model.KindBookDelta,
		Venue:    VenueBinance,
		Symbol:   sym,
		Sequence: delta.Sequence,
		Book:     delta,
	}}, nil
}

// denseSequence translates Binance's [U, u] update-id ranges into a dense
// per-symbol counter. See the type comment for why.
func (b *Binance) denseSequence(symbol string, firstUpdateID, finalUpdateID int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	cur, ok := b.bookSeq[symbol]
	if !ok {
		cur = &binanceBookCursor{}
		b.bookSeq[symbol] = cur
	}

	if !cur.initialized {
		cur.initialized = true
		cur.dense = 0
		cur.lastFinalUpdateID = finalUpdateID
		return cur.dense
	}

	switch {
	case firstUpdateID == cur.lastFinalUpdateID+1:
		cur.dense++
	case firstUpdateID <= cur.lastFinalUpdateID:
		// Overlap: a retransmit. Do not advance — the detector will call it
		// a duplicate and the book stays intact.
		cur.lastFinalUpdateID = max64(cur.lastFinalUpdateID, finalUpdateID)
		return cur.dense
	default:
		missing := firstUpdateID - cur.lastFinalUpdateID - 1
		if missing < 1 {
			missing = 1
		}
		cur.dense += missing + 1
	}

	cur.lastFinalUpdateID = finalUpdateID
	return cur.dense
}

// ResetBooks drops depth cursors, called on reconnect because Binance
// restarts update ids for a new stream session.
func (b *Binance) ResetBooks() {
	b.mu.Lock()
	b.bookSeq = make(map[string]*binanceBookCursor)
	b.mu.Unlock()
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
