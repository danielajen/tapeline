// Package encode implements the Confluent Avro wire format.
//
//	byte 0      magic byte, always 0x00
//	bytes 1..4  schema id, big-endian int32
//	bytes 5..   Avro binary payload, no embedded schema
//
// The reason this format exists is that Avro binary is not self-describing.
// Shipping the schema with every message would multiply the payload; shipping
// a four-byte id and resolving it against the registry costs one lookup per
// distinct id per consumer process. Every Kafka message this project produces
// is framed this way, which is what lets the Flink tier and the serving tier
// deserialize without hard-coding schemas at build time.
package encode

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// MagicByte prefixes every Confluent-framed payload.
const MagicByte byte = 0x00

// HeaderLen is the magic byte plus the four-byte schema id.
const HeaderLen = 5

var (
	// ErrShortPayload means the buffer cannot even hold a header.
	ErrShortPayload = errors.New("encode: payload shorter than wire header")
	// ErrBadMagic means byte 0 was not 0x00, so this is not Confluent-framed.
	ErrBadMagic = errors.New("encode: bad magic byte")
)

// Encoder frames records for one schema id.
type Encoder struct {
	schemaID int
	schema   avro.Schema
	header   [HeaderLen]byte
}

// NewEncoder parses schemaJSON and binds it to a registry-assigned id.
func NewEncoder(schemaID int, schemaJSON string) (*Encoder, error) {
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}
	e := &Encoder{schemaID: schemaID, schema: s}
	e.header[0] = MagicByte
	binary.BigEndian.PutUint32(e.header[1:], uint32(schemaID)) //nolint:gosec // ids are non-negative
	return e, nil
}

// SchemaID returns the registry id this encoder frames with.
func (e *Encoder) SchemaID() int { return e.schemaID }

// Schema returns the parsed writer schema.
func (e *Encoder) Schema() avro.Schema { return e.schema }

// Encode serializes v and prefixes the wire header.
func (e *Encoder) Encode(v any) ([]byte, error) {
	payload, err := avro.Marshal(e.schema, v)
	if err != nil {
		return nil, fmt.Errorf("avro marshal (schema id %d): %w", e.schemaID, err)
	}
	out := make([]byte, 0, HeaderLen+len(payload))
	out = append(out, e.header[:]...)
	out = append(out, payload...)
	return out, nil
}

// SplitHeader validates the framing and returns the schema id and the Avro
// payload. The payload aliases the input; copy it if you intend to retain it.
func SplitHeader(b []byte) (schemaID int, payload []byte, err error) {
	if len(b) < HeaderLen {
		return 0, nil, ErrShortPayload
	}
	if b[0] != MagicByte {
		return 0, nil, fmt.Errorf("%w: got 0x%02x", ErrBadMagic, b[0])
	}
	return int(binary.BigEndian.Uint32(b[1:HeaderLen])), b[HeaderLen:], nil
}

// Decode reads a framed payload using writerSchema. Use DecodeResolved when
// the reader's schema differs from the writer's.
func Decode(writerSchema avro.Schema, b []byte, out any) error {
	_, payload, err := SplitHeader(b)
	if err != nil {
		return err
	}
	return avro.Unmarshal(writerSchema, payload, out)
}

// DecodeResolved reads a framed payload written with writerSchema into a
// value shaped like readerSchema, applying Avro schema resolution: fields
// present only in the reader take their declared defaults, fields present
// only in the writer are skipped.
//
// This is the mechanism that makes a rolling deploy safe. A consumer pinned
// to v1 keeps working against v2 producers, which is exactly what the
// evolution test in avro_test.go proves.
func DecodeResolved(readerSchema, writerSchema avro.Schema, b []byte, out any) error {
	_, payload, err := SplitHeader(b)
	if err != nil {
		return err
	}
	resolved, err := avro.NewSchemaCompatibility().Resolve(readerSchema, writerSchema)
	if err != nil {
		return fmt.Errorf("resolve reader against writer schema: %w", err)
	}
	return avro.Unmarshal(resolved, payload, out)
}

// Compatible reports whether a reader schema can read data written with a
// writer schema.
func Compatible(readerSchema, writerSchema avro.Schema) error {
	return avro.NewSchemaCompatibility().Compatible(readerSchema, writerSchema)
}
