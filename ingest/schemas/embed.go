// Package avroschema embeds the canonical Avro schemas.
//
// These .avsc files are the single source of truth for the wire format.
// They are embedded rather than read from disk so a container image cannot
// drift from the schemas it was built with, and so the CI compatibility
// check in .github/workflows/ci.yml validates the exact bytes that ship.
//
// The Flink and serving tiers do not read these files: they resolve schemas
// by id from the registry at runtime. That asymmetry is intentional — the
// producer decides the schema, consumers discover it.
package avroschema

import (
	_ "embed"

	"github.com/tapeline/ingest/internal/model"
)

//go:embed avro/trade.v1.avsc
var TradeV1 string

//go:embed avro/trade.v2.avsc
var TradeV2 string

//go:embed avro/book_delta.v1.avsc
var BookDeltaV1 string

//go:embed avro/chain_transfer.v1.avsc
var ChainTransferV1 string

// Current maps each event kind to the schema version ingestd writes today.
//
// Flipping TradeV1 to TradeV2 here is the entire production-side change for
// the schema evolution demo; consumers keep working because the registry
// enforces FULL compatibility for the subject. See docs/SCHEMA_EVOLUTION.md.
func Current() map[model.Kind]string {
	return map[model.Kind]string{
		model.KindTrade:         TradeV1,
		model.KindBookDelta:     BookDeltaV1,
		model.KindChainTransfer: ChainTransferV1,
	}
}
