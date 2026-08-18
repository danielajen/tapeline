package encode

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/tapeline/ingest/internal/model"
	avroschema "github.com/tapeline/ingest/schemas"
)

func mustEncoder(t *testing.T, id int, schemaJSON string) *Encoder {
	t.Helper()
	e, err := NewEncoder(id, schemaJSON)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	return e
}

func sampleTrade() *model.Trade {
	return &model.Trade{
		Venue:        "coinbase",
		Symbol:       "BTC-USD",
		TradeID:      "778899",
		Price:        64231.17,
		Size:         0.0125,
		Side:         model.SideBuy,
		EventTimeUS:  1_755_400_000_123_456,
		IngestTimeUS: 1_755_400_000_223_456,
		Sequence:     42,
	}
}

func TestWireFormatFraming(t *testing.T) {
	const schemaID = 1234
	enc := mustEncoder(t, schemaID, avroschema.TradeV1)

	framed, err := enc.Encode(sampleTrade())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if len(framed) <= HeaderLen {
		t.Fatalf("framed payload is %d bytes, expected header plus body", len(framed))
	}
	if framed[0] != MagicByte {
		t.Errorf("magic byte = 0x%02x, want 0x00", framed[0])
	}
	if got := binary.BigEndian.Uint32(framed[1:5]); got != schemaID {
		t.Errorf("schema id = %d, want %d", got, schemaID)
	}

	gotID, payload, err := SplitHeader(framed)
	if err != nil {
		t.Fatalf("SplitHeader: %v", err)
	}
	if gotID != schemaID {
		t.Errorf("SplitHeader id = %d, want %d", gotID, schemaID)
	}
	if len(payload) != len(framed)-HeaderLen {
		t.Errorf("payload length = %d, want %d", len(payload), len(framed)-HeaderLen)
	}
}

func TestSplitHeaderRejectsBadInput(t *testing.T) {
	if _, _, err := SplitHeader([]byte{0x00, 0x01}); !errors.Is(err, ErrShortPayload) {
		t.Errorf("short payload: err = %v, want ErrShortPayload", err)
	}
	// 0x01 is what a Protobuf-framed or plain-JSON payload looks like here.
	// Catching it at the boundary is the difference between a clear error and
	// a nonsense Avro decode.
	if _, _, err := SplitHeader([]byte{0x01, 0, 0, 0, 1, 0xAA}); !errors.Is(err, ErrBadMagic) {
		t.Errorf("bad magic: err = %v, want ErrBadMagic", err)
	}
}

func TestRoundTrip(t *testing.T) {
	enc := mustEncoder(t, 7, avroschema.TradeV1)
	in := sampleTrade()

	framed, err := enc.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var out model.Trade
	if err := Decode(enc.Schema(), framed, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out != *in {
		t.Errorf("round trip mismatch:\n got %+v\nwant %+v", out, *in)
	}
}

// The evolution test. This is the claim the resume bullet makes, so it is
// the claim with a test behind it.
//
// v2 adds maker_order_id (nullable, default null) and liquidity (default
// "UNKNOWN"). Both directions must work, because during a rolling deploy
// both versions are producing and consuming at the same time:
//
//	forward  — a v1 consumer reads v2 data and ignores the new fields
//	backward — a v2 consumer reads v1 data and fills the declared defaults
func TestSchemaEvolutionBothDirections(t *testing.T) {
	v1Schema, err := avro.Parse(avroschema.TradeV1)
	if err != nil {
		t.Fatalf("parse v1: %v", err)
	}
	v2Schema, err := avro.Parse(avroschema.TradeV2)
	if err != nil {
		t.Fatalf("parse v2: %v", err)
	}

	t.Run("registry-level compatibility both ways", func(t *testing.T) {
		if err := Compatible(v2Schema, v1Schema); err != nil {
			t.Errorf("v2 reader cannot read v1 writer: %v", err)
		}
		if err := Compatible(v1Schema, v2Schema); err != nil {
			t.Errorf("v1 reader cannot read v2 writer: %v", err)
		}
	})

	t.Run("old consumer reads new data", func(t *testing.T) {
		makerID := "order-abc-123"
		v2Enc := mustEncoder(t, 2, avroschema.TradeV2)
		src := sampleTrade()
		framed, err := v2Enc.Encode(&model.TradeV2{
			Venue: src.Venue, Symbol: src.Symbol, TradeID: src.TradeID,
			Price: src.Price, Size: src.Size, Side: src.Side,
			EventTimeUS: src.EventTimeUS, IngestTimeUS: src.IngestTimeUS,
			Sequence: src.Sequence,
			// The fields the v1 consumer has never heard of.
			MakerOrderID: &makerID, Liquidity: "TAKER",
		})
		if err != nil {
			t.Fatalf("encode v2: %v", err)
		}

		var out model.Trade
		if err := DecodeResolved(v1Schema, v2Schema, framed, &out); err != nil {
			t.Fatalf("v1 reader on v2 data: %v", err)
		}
		if out != *src {
			t.Errorf("v1 view of v2 record differs:\n got %+v\nwant %+v", out, *src)
		}
	})

	t.Run("new consumer reads old data and applies defaults", func(t *testing.T) {
		v1Enc := mustEncoder(t, 1, avroschema.TradeV1)
		src := sampleTrade()
		framed, err := v1Enc.Encode(src)
		if err != nil {
			t.Fatalf("encode v1: %v", err)
		}

		var out model.TradeV2
		if err := DecodeResolved(v2Schema, v1Schema, framed, &out); err != nil {
			t.Fatalf("v2 reader on v1 data: %v", err)
		}
		if out.Symbol != src.Symbol || out.Price != src.Price || out.Sequence != src.Sequence {
			t.Errorf("carried fields differ: got %+v want %+v", out, *src)
		}
		if out.MakerOrderID != nil {
			t.Errorf("maker_order_id = %v, want nil from the schema default", *out.MakerOrderID)
		}
		if out.Liquidity != "UNKNOWN" {
			t.Errorf("liquidity = %q, want the declared default %q", out.Liquidity, "UNKNOWN")
		}
	})
}

// An incompatible change must be caught here rather than by a consumer at
// runtime: adding a required field with no default breaks every old writer.
func TestIncompatibleChangeIsRejected(t *testing.T) {
	const breaking = `{
	  "type":"record","name":"Trade","namespace":"io.tapeline.md",
	  "fields":[
	    {"name":"venue","type":"string"},
	    {"name":"symbol","type":"string"},
	    {"name":"trade_id","type":"string"},
	    {"name":"price","type":"double"},
	    {"name":"size","type":"double"},
	    {"name":"side","type":"string"},
	    {"name":"event_time_us","type":"long"},
	    {"name":"ingest_time_us","type":"long"},
	    {"name":"sequence","type":"long"},
	    {"name":"settlement_venue","type":"string"}
	  ]}`

	v1Schema, err := avro.Parse(avroschema.TradeV1)
	if err != nil {
		t.Fatalf("parse v1: %v", err)
	}
	breakingSchema, err := avro.Parse(breaking)
	if err != nil {
		t.Fatalf("parse breaking: %v", err)
	}

	if err := Compatible(breakingSchema, v1Schema); err == nil {
		t.Fatal("a new required field with no default was accepted; it must not be")
	}
}

func TestAllShippedSchemasParse(t *testing.T) {
	for name, s := range map[string]string{
		"trade.v1":          avroschema.TradeV1,
		"trade.v2":          avroschema.TradeV2,
		"book_delta.v1":     avroschema.BookDeltaV1,
		"chain_transfer.v1": avroschema.ChainTransferV1,
	} {
		if _, err := avro.Parse(s); err != nil {
			t.Errorf("%s does not parse: %v", name, err)
		}
	}
}

func TestBookAndChainRoundTrip(t *testing.T) {
	bookEnc := mustEncoder(t, 10, avroschema.BookDeltaV1)
	book := &model.BookDelta{
		Venue: "kraken", Symbol: "ETH-USD", IsSnapshot: true,
		Bids:        []model.Level{{Price: 3100.5, Size: 2}, {Price: 3100.0, Size: 0}},
		Asks:        []model.Level{{Price: 3101.0, Size: 1.25}},
		EventTimeUS: 1, IngestTimeUS: 2, Sequence: model.NoSequence,
	}
	framed, err := bookEnc.Encode(book)
	if err != nil {
		t.Fatalf("encode book: %v", err)
	}
	var gotBook model.BookDelta
	if err := Decode(bookEnc.Schema(), framed, &gotBook); err != nil {
		t.Fatalf("decode book: %v", err)
	}
	if len(gotBook.Bids) != 2 || gotBook.Bids[1].Size != 0 || !gotBook.IsSnapshot {
		t.Errorf("book round trip lost data: %+v", gotBook)
	}

	chainEnc := mustEncoder(t, 11, avroschema.ChainTransferV1)
	// A uint256 far beyond float64's integer precision. If this survives, the
	// decimal-string decision is doing its job.
	const huge = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	transfer := &model.ChainTransfer{
		Chain: "ethereum", Token: "0xa0b8", Symbol: "USDC",
		FromAddr: "0x1", ToAddr: "0x2", AmountRaw: huge, Decimals: 6,
		BlockNumber: 20_000_000, LogIndex: 7, TxHash: "0xdead",
		EventTimeUS: 3, IngestTimeUS: 4,
	}
	framed, err = chainEnc.Encode(transfer)
	if err != nil {
		t.Fatalf("encode transfer: %v", err)
	}
	var gotTransfer model.ChainTransfer
	if err := Decode(chainEnc.Schema(), framed, &gotTransfer); err != nil {
		t.Fatalf("decode transfer: %v", err)
	}
	if gotTransfer.AmountRaw != huge {
		t.Errorf("uint256 amount lost precision:\n got %s\nwant %s", gotTransfer.AmountRaw, huge)
	}
}
