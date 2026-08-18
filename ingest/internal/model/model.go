// Package model holds the canonical, venue-independent market data types.
//
// Everything upstream of this package speaks a venue's own wire format;
// everything downstream speaks these types. The avro struct tags are the
// contract with schemas/avro/*.avsc — if you change a field name here you
// must evolve the schema, not just rename the field.
package model

import "time"

// Kind discriminates the canonical event types.
type Kind string

const (
	KindTrade         Kind = "trade"
	KindBookDelta     Kind = "book_delta"
	KindChainTransfer Kind = "chain_transfer"
)

// Aggressor sides. Venues spell these a dozen ways; normalization collapses
// them to exactly these three values.
const (
	SideBuy     = "BUY"
	SideSell    = "SELL"
	SideUnknown = "UNKNOWN"
)

// NoSequence marks an event from a venue that publishes no sequence number.
// The gap detector treats it as unsequenced rather than as sequence zero,
// which would otherwise look like a permanent gap.
const NoSequence int64 = -1

// Level is one price level of an L2 book. Size zero removes the level.
type Level struct {
	Price float64 `avro:"price" json:"price"`
	Size  float64 `avro:"size" json:"size"`
}

// Trade is a single execution, normalized. Mirrors schemas/avro/trade.v1.avsc.
type Trade struct {
	Venue        string  `avro:"venue" json:"venue"`
	Symbol       string  `avro:"symbol" json:"symbol"`
	TradeID      string  `avro:"trade_id" json:"trade_id"`
	Price        float64 `avro:"price" json:"price"`
	Size         float64 `avro:"size" json:"size"`
	Side         string  `avro:"side" json:"side"`
	EventTimeUS  int64   `avro:"event_time_us" json:"event_time_us"`
	IngestTimeUS int64   `avro:"ingest_time_us" json:"ingest_time_us"`
	Sequence     int64   `avro:"sequence" json:"sequence"`
}

// TradeV2 mirrors schemas/avro/trade.v2.avsc. It exists so the schema
// evolution test can decode v1 bytes with a v2 reader and vice versa.
type TradeV2 struct {
	Venue        string  `avro:"venue" json:"venue"`
	Symbol       string  `avro:"symbol" json:"symbol"`
	TradeID      string  `avro:"trade_id" json:"trade_id"`
	Price        float64 `avro:"price" json:"price"`
	Size         float64 `avro:"size" json:"size"`
	Side         string  `avro:"side" json:"side"`
	EventTimeUS  int64   `avro:"event_time_us" json:"event_time_us"`
	IngestTimeUS int64   `avro:"ingest_time_us" json:"ingest_time_us"`
	Sequence     int64   `avro:"sequence" json:"sequence"`
	MakerOrderID *string `avro:"maker_order_id" json:"maker_order_id"`
	Liquidity    string  `avro:"liquidity" json:"liquidity"`
}

// BookDelta is an L2 update. IsSnapshot replaces the book; otherwise it patches.
type BookDelta struct {
	Venue        string  `avro:"venue" json:"venue"`
	Symbol       string  `avro:"symbol" json:"symbol"`
	IsSnapshot   bool    `avro:"is_snapshot" json:"is_snapshot"`
	Bids         []Level `avro:"bids" json:"bids"`
	Asks         []Level `avro:"asks" json:"asks"`
	EventTimeUS  int64   `avro:"event_time_us" json:"event_time_us"`
	IngestTimeUS int64   `avro:"ingest_time_us" json:"ingest_time_us"`
	Sequence     int64   `avro:"sequence" json:"sequence"`
}

// ChainTransfer is a decoded ERC-20 Transfer log.
//
// AmountRaw stays a decimal string on purpose: uint256 does not fit in a
// float64 and silently losing precision on token amounts is the kind of bug
// that only shows up in production on a whale transfer.
type ChainTransfer struct {
	Chain        string `avro:"chain" json:"chain"`
	Token        string `avro:"token" json:"token"`
	Symbol       string `avro:"symbol" json:"symbol"`
	FromAddr     string `avro:"from_addr" json:"from_addr"`
	ToAddr       string `avro:"to_addr" json:"to_addr"`
	AmountRaw    string `avro:"amount_raw" json:"amount_raw"`
	Decimals     int32  `avro:"decimals" json:"decimals"`
	BlockNumber  int64  `avro:"block_number" json:"block_number"`
	LogIndex     int32  `avro:"log_index" json:"log_index"`
	TxHash       string `avro:"tx_hash" json:"tx_hash"`
	EventTimeUS  int64  `avro:"event_time_us" json:"event_time_us"`
	IngestTimeUS int64  `avro:"ingest_time_us" json:"ingest_time_us"`
}

// Event is the tagged union that moves through the pipeline. Exactly one of
// the pointer fields is non-nil, matching Kind.
type Event struct {
	Kind     Kind
	Venue    string
	Symbol   string
	Sequence int64

	Trade *Trade
	Book  *BookDelta
	Chain *ChainTransfer
}

// Payload returns the concrete record for Avro encoding.
func (e Event) Payload() any {
	switch e.Kind {
	case KindTrade:
		return e.Trade
	case KindBookDelta:
		return e.Book
	case KindChainTransfer:
		return e.Chain
	}
	return nil
}

// Micros converts a time to epoch microseconds, the project's single
// timestamp unit. Milliseconds lose ordering inside a busy book update;
// nanoseconds are noise given WebSocket transport.
func Micros(t time.Time) int64 { return t.UnixMicro() }

// NormalizeSide collapses the many venue spellings of aggressor side.
func NormalizeSide(s string) string {
	switch s {
	case "b", "B", "buy", "BUY", "Buy", "bid", "BID":
		return SideBuy
	case "s", "S", "sell", "SELL", "Sell", "ask", "ASK", "offer", "OFFER":
		return SideSell
	default:
		return SideUnknown
	}
}
