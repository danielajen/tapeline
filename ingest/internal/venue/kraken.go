package venue

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tapeline/ingest/internal/model"
)

// VenueKraken is the canonical venue id.
const VenueKraken = "kraken"

// Kraken adapts the Kraken WebSocket v2 API.
//
// Two things differ from the other venues and both are handled here rather
// than leaking upstream:
//
//  1. Kraken writes pairs as BTC/USD, so the adapter translates on the way
//     out and back on the way in.
//  2. Kraken v2 book updates carry a CRC32 checksum instead of a sequence
//     number. Checksum verification requires the full local book, which lives
//     in the Flink tier, so book messages are marked unsequenced here and
//     integrity is enforced downstream where the state actually is. Trades do
//     carry a monotonic per-symbol trade_id, which is used as the sequence.
type Kraken struct {
	symbols []string
	depth   int
}

// NewKraken builds the adapter. depth of 0 uses Kraken's default of 10.
func NewKraken(symbols []string, depth int) *Kraken {
	if depth <= 0 {
		depth = 10
	}
	return &Kraken{symbols: append([]string(nil), symbols...), depth: depth}
}

// Name implements Decoder.
func (k *Kraken) Name() string { return VenueKraken }

// URL implements Decoder.
func (k *Kraken) URL() string { return "wss://ws.kraken.com/v2" }

// KrakenSymbol converts a canonical pair to Kraken's slash form.
func KrakenSymbol(canonical string) string {
	return strings.ReplaceAll(canonicalPair(canonical), "-", "/")
}

type krakenSubscribe struct {
	Method string          `json:"method"`
	Params krakenSubParams `json:"params"`
	ReqID  int64           `json:"req_id,omitempty"`
}

type krakenSubParams struct {
	Channel  string   `json:"channel"`
	Symbol   []string `json:"symbol"`
	Depth    int      `json:"depth,omitempty"`
	Snapshot *bool    `json:"snapshot,omitempty"`
}

// Subscriptions implements Decoder.
func (k *Kraken) Subscriptions(symbols []string) []any {
	if len(symbols) > 0 {
		k.symbols = append([]string(nil), symbols...)
	}
	pairs := make([]string, 0, len(k.symbols))
	for _, s := range k.symbols {
		pairs = append(pairs, KrakenSymbol(s))
	}
	snapshot := true
	return []any{
		krakenSubscribe{Method: "subscribe", ReqID: 1,
			Params: krakenSubParams{Channel: "trade", Symbol: pairs, Snapshot: &snapshot}},
		krakenSubscribe{Method: "subscribe", ReqID: 2,
			Params: krakenSubParams{Channel: "book", Symbol: pairs, Depth: k.depth, Snapshot: &snapshot}},
	}
}

type krakenEnvelope struct {
	Channel string          `json:"channel"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
	Method  string          `json:"method"`
	Success *bool           `json:"success"`
	Error   string          `json:"error"`
}

type krakenTrade struct {
	Symbol    string  `json:"symbol"`
	Side      string  `json:"side"`
	Price     float64 `json:"price"`
	Qty       float64 `json:"qty"`
	OrdType   string  `json:"ord_type"`
	TradeID   int64   `json:"trade_id"`
	Timestamp string  `json:"timestamp"`
}

type krakenBookLevel struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

type krakenBook struct {
	Symbol    string            `json:"symbol"`
	Bids      []krakenBookLevel `json:"bids"`
	Asks      []krakenBookLevel `json:"asks"`
	Checksum  uint32            `json:"checksum"`
	Timestamp string            `json:"timestamp"`
}

// Decode implements Decoder.
func (k *Kraken) Decode(raw []byte, now time.Time) ([]model.Event, error) {
	var env krakenEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("kraken envelope: %w", err)
	}

	// Subscription acks and the connection status frame.
	if env.Method != "" {
		if env.Success != nil && !*env.Success {
			return nil, fmt.Errorf("kraken %s failed: %s", env.Method, env.Error)
		}
		return nil, nil
	}

	switch env.Channel {
	case "trade":
		return k.decodeTrades(env, now)
	case "book":
		return k.decodeBook(env, now)
	default:
		// status, heartbeat, pong
		return nil, nil
	}
}

func (k *Kraken) decodeTrades(env krakenEnvelope, now time.Time) ([]model.Event, error) {
	var trades []krakenTrade
	if err := json.Unmarshal(env.Data, &trades); err != nil {
		return nil, fmt.Errorf("kraken trade data: %w", err)
	}

	ingest := now.UnixMicro()
	out := make([]model.Event, 0, len(trades))
	for _, t := range trades {
		sym := canonicalPair(t.Symbol)
		out = append(out, model.Event{
			Kind:     model.KindTrade,
			Venue:    VenueKraken,
			Symbol:   sym,
			Sequence: t.TradeID,
			Trade: &model.Trade{
				Venue:        VenueKraken,
				Symbol:       sym,
				TradeID:      strconv.FormatInt(t.TradeID, 10),
				Price:        t.Price,
				Size:         t.Qty,
				Side:         model.NormalizeSide(t.Side),
				EventTimeUS:  parseRFC3339Micros(t.Timestamp, now),
				IngestTimeUS: ingest,
				Sequence:     t.TradeID,
			},
		})
	}
	return out, nil
}

func (k *Kraken) decodeBook(env krakenEnvelope, now time.Time) ([]model.Event, error) {
	var books []krakenBook
	if err := json.Unmarshal(env.Data, &books); err != nil {
		return nil, fmt.Errorf("kraken book data: %w", err)
	}

	ingest := now.UnixMicro()
	isSnapshot := env.Type == "snapshot"
	out := make([]model.Event, 0, len(books))
	for _, b := range books {
		sym := canonicalPair(b.Symbol)
		delta := &model.BookDelta{
			Venue:        VenueKraken,
			Symbol:       sym,
			IsSnapshot:   isSnapshot,
			Bids:         make([]model.Level, 0, len(b.Bids)),
			Asks:         make([]model.Level, 0, len(b.Asks)),
			EventTimeUS:  parseRFC3339Micros(b.Timestamp, now),
			IngestTimeUS: ingest,
			Sequence:     model.NoSequence,
		}
		for _, l := range b.Bids {
			delta.Bids = append(delta.Bids, model.Level{Price: l.Price, Size: l.Qty})
		}
		for _, l := range b.Asks {
			delta.Asks = append(delta.Asks, model.Level{Price: l.Price, Size: l.Qty})
		}
		out = append(out, model.Event{
			Kind:     model.KindBookDelta,
			Venue:    VenueKraken,
			Symbol:   sym,
			Sequence: model.NoSequence,
			Book:     delta,
		})
	}
	return out, nil
}
