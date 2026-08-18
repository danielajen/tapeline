package venue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tapeline/ingest/internal/model"
)

// VenueCoinbase is the canonical venue id.
const VenueCoinbase = "coinbase"

// Coinbase adapts the Coinbase Advanced Trade market data WebSocket.
//
// Product ids are already in BASE-QUOTE form, so canonical symbols pass
// through unchanged — Coinbase is the venue the canonical format was chosen
// to match.
//
// Sequencing: Advanced Trade puts a per-connection `sequence_num` on every
// envelope, not a per-product one. That makes it a connection-level integrity
// check rather than a per-book one: a gap means frames were dropped
// somewhere, but not which product's book is now wrong. The pipeline
// therefore resyncs the whole venue on a Coinbase gap.
type Coinbase struct {
	symbols []string
}

// NewCoinbase builds the adapter for the given canonical symbols.
func NewCoinbase(symbols []string) *Coinbase {
	return &Coinbase{symbols: append([]string(nil), symbols...)}
}

// Name implements Decoder.
func (c *Coinbase) Name() string { return VenueCoinbase }

// URL implements Decoder.
func (c *Coinbase) URL() string { return "wss://advanced-trade-ws.coinbase.com" }

type coinbaseSubscribe struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channel    string   `json:"channel"`
}

// Subscriptions implements Decoder. market_trades and level2 are public
// channels and need no authentication; the authenticated channels (user,
// futures_balance_summary) are deliberately out of scope.
func (c *Coinbase) Subscriptions(symbols []string) []any {
	if len(symbols) > 0 {
		c.symbols = append([]string(nil), symbols...)
	}
	products := make([]string, 0, len(c.symbols))
	for _, s := range c.symbols {
		products = append(products, canonicalPair(s))
	}
	return []any{
		coinbaseSubscribe{Type: "subscribe", ProductIDs: products, Channel: "market_trades"},
		coinbaseSubscribe{Type: "subscribe", ProductIDs: products, Channel: "level2"},
	}
}

type coinbaseEnvelope struct {
	Channel     string          `json:"channel"`
	Timestamp   string          `json:"timestamp"`
	SequenceNum int64           `json:"sequence_num"`
	Events      json.RawMessage `json:"events"`
	Type        string          `json:"type"`
	Message     string          `json:"message"`
}

type coinbaseTradeEvent struct {
	Type   string `json:"type"`
	Trades []struct {
		TradeID   string `json:"trade_id"`
		ProductID string `json:"product_id"`
		Price     string `json:"price"`
		Size      string `json:"size"`
		Side      string `json:"side"`
		Time      string `json:"time"`
	} `json:"trades"`
}

type coinbaseL2Event struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Updates   []struct {
		Side        string `json:"side"`
		EventTime   string `json:"event_time"`
		PriceLevel  string `json:"price_level"`
		NewQuantity string `json:"new_quantity"`
	} `json:"updates"`
}

// Decode implements Decoder.
func (c *Coinbase) Decode(raw []byte, now time.Time) ([]model.Event, error) {
	var env coinbaseEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("coinbase envelope: %w", err)
	}

	switch env.Channel {
	case "market_trades":
		return c.decodeTrades(env, now)
	case "l2_data":
		return c.decodeL2(env, now)
	case "subscriptions", "heartbeats", "":
		// Acks and heartbeats. An error-typed envelope is worth surfacing;
		// everything else here is routine.
		if env.Type == "error" {
			return nil, fmt.Errorf("coinbase error frame: %s", env.Message)
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func (c *Coinbase) decodeTrades(env coinbaseEnvelope, now time.Time) ([]model.Event, error) {
	var events []coinbaseTradeEvent
	if err := json.Unmarshal(env.Events, &events); err != nil {
		return nil, fmt.Errorf("coinbase market_trades: %w", err)
	}

	ingest := now.UnixMicro()
	out := make([]model.Event, 0, 8)
	for _, ev := range events {
		for _, t := range ev.Trades {
			sym := canonicalPair(t.ProductID)
			price, err := parseFloat(t.Price)
			if err != nil {
				return nil, fmt.Errorf("coinbase trade price %q: %w", t.Price, err)
			}
			size, err := parseFloat(t.Size)
			if err != nil {
				return nil, fmt.Errorf("coinbase trade size %q: %w", t.Size, err)
			}
			out = append(out, model.Event{
				Kind:     model.KindTrade,
				Venue:    VenueCoinbase,
				Symbol:   sym,
				Sequence: env.SequenceNum,
				Trade: &model.Trade{
					Venue:        VenueCoinbase,
					Symbol:       sym,
					TradeID:      t.TradeID,
					Price:        price,
					Size:         size,
					Side:         model.NormalizeSide(t.Side),
					EventTimeUS:  parseRFC3339Micros(t.Time, now),
					IngestTimeUS: ingest,
					Sequence:     env.SequenceNum,
				},
			})
		}
	}
	return out, nil
}

func (c *Coinbase) decodeL2(env coinbaseEnvelope, now time.Time) ([]model.Event, error) {
	var events []coinbaseL2Event
	if err := json.Unmarshal(env.Events, &events); err != nil {
		return nil, fmt.Errorf("coinbase l2_data: %w", err)
	}

	ingest := now.UnixMicro()
	out := make([]model.Event, 0, len(events))
	for _, ev := range events {
		if len(ev.Updates) == 0 {
			continue
		}
		sym := canonicalPair(ev.ProductID)
		delta := &model.BookDelta{
			Venue:        VenueCoinbase,
			Symbol:       sym,
			IsSnapshot:   ev.Type == "snapshot",
			EventTimeUS:  parseRFC3339Micros(ev.Updates[0].EventTime, now),
			IngestTimeUS: ingest,
			Sequence:     env.SequenceNum,
		}
		for _, u := range ev.Updates {
			lvl := model.Level{Price: mustFloat(u.PriceLevel), Size: mustFloat(u.NewQuantity)}
			// Coinbase spells the ask side "offer".
			if model.NormalizeSide(u.Side) == model.SideBuy {
				delta.Bids = append(delta.Bids, lvl)
			} else {
				delta.Asks = append(delta.Asks, lvl)
			}
		}
		out = append(out, model.Event{
			Kind:     model.KindBookDelta,
			Venue:    VenueCoinbase,
			Symbol:   sym,
			Sequence: env.SequenceNum,
			Book:     delta,
		})
	}
	return out, nil
}
