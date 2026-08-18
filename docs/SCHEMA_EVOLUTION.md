# Schema evolution

"I ran a schema registry" is a claim about configuration. "I evolved a schema
under FULL compatibility and proved old consumers kept working" is a claim
about understanding the failure mode. This document is the second one.

## The failure this prevents

Producers and consumers are deployed separately. During any rolling deploy —
which is every deploy — both versions of a schema are live at once:

```
  t0   producers v1        consumers v1
  t1   producers v1+v2     consumers v1        ← new producers, old consumers
  t2   producers v2        consumers v1+v2     ← old data, new consumers
  t3   producers v2        consumers v2
```

At `t1`, a v1 consumer must read v2 data. At `t2`, a v2 consumer must read v1
data. Both directions have to work, which is exactly what **FULL**
compatibility means — and it is why this project overrides the registry's
`BACKWARD` default.

`BACKWARD` alone guarantees only the `t2` direction. A change that satisfies
it can still break every consumer at `t1`, and because `t1` is a window
minutes long, the breakage looks intermittent.

Set in three places, deliberately, so no single misconfiguration silently
weakens it:

- `deploy/docker-compose.yml`: `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: full`
- `ingest/cmd/ingestd/main.go`: `reg.SetCompatibility(ctx, subject, schema.CompatFull)`
- CI: an incompatible schema fails the build before it can be registered

## The change

`trade.v1` → `trade.v2` adds two fields:

```json
{ "name": "maker_order_id", "type": ["null", "string"], "default": null },
{ "name": "liquidity",      "type": "string",           "default": "UNKNOWN" }
```

Both carry defaults. That is the whole trick, and it is what makes the change
FULL-compatible rather than merely backward-compatible:

- **A v1 reader on v2 data** skips fields it does not know about.
- **A v2 reader on v1 data** fills `maker_order_id` with `null` and
  `liquidity` with `"UNKNOWN"` from the declared defaults.

## The proof

`ingest/internal/encode/avro_test.go`, `TestSchemaEvolutionBothDirections`.
It runs on every commit:

```go
t.Run("old consumer reads new data", func(t *testing.T) {
    framed, _ := v2Enc.Encode(&model.TradeV2{ /* ... */
        MakerOrderID: &makerID, Liquidity: "TAKER",
    })
    var out model.Trade                                  // v1 shape
    DecodeResolved(v1Schema, v2Schema, framed, &out)
    if out != *src { t.Errorf(...) }                     // identical to the v1 original
})

t.Run("new consumer reads old data and applies defaults", func(t *testing.T) {
    framed, _ := v1Enc.Encode(src)
    var out model.TradeV2                                // v2 shape
    DecodeResolved(v2Schema, v1Schema, framed, &out)
    if out.MakerOrderID != nil        { t.Errorf(...) }  // default applied
    if out.Liquidity != "UNKNOWN"     { t.Errorf(...) }  // default applied
})
```

There is a third test, and it is the one worth pointing at:

```go
func TestIncompatibleChangeIsRejected(t *testing.T) {
    // Adds a required field with no default.
    if err := Compatible(breakingSchema, v1Schema); err == nil {
        t.Fatal("a new required field with no default was accepted; it must not be")
    }
}
```

A compatibility check that has never rejected anything is a compatibility
check nobody has verified is switched on.

The consumer side is covered independently in
`serving/.../ConfluentAvroReaderTest.java`
(`aProducerCanAddAFieldWithoutBreakingThisService`), because the Java tier
resolves schemas through its own reader and would not be protected by the Go
tests.

## Deploying an evolution

The production-side change is one line in `ingest/schemas/embed.go`:

```go
func Current() map[model.Kind]string {
    return map[model.Kind]string{
        model.KindTrade: TradeV1,   // → TradeV2
        // ...
    }
}
```

The sequence:

1. **Add the schema file** with defaults on every new field.
2. **CI checks compatibility.** `RegisterSchemas` calls
   `CheckCompatibility` before registering, so an incompatible schema fails
   with a message naming the subject rather than a bare registry 409.
3. **Deploy consumers first.** They can already read v1; deploying them first
   means the `t1` window never exists. This is not required by FULL
   compatibility — it is belt and braces, and it costs nothing.
4. **Deploy the producer.** It registers v2, gets a new schema id, and starts
   framing with it.
5. **Old consumers keep working**, resolving v2 writes against their v1
   reader schema, until they are redeployed on their own schedule.

No coordinated deploy. No downtime. No consumer redeployed on the producer's
timetable.

## Changes that are not compatible

For completeness, because knowing what breaks is the useful half:

| Change | Compatible? | Why |
|---|---|---|
| Add a field with a default | Yes | Readers fill the default |
| Add a field with no default | **No** | Old data has nothing to supply it |
| Remove a field with a default | Yes | New readers use the default |
| Remove a field with no default | **No** | Old readers require it |
| Rename a field | **No** | Add-plus-remove; use an alias instead |
| Widen `int` to `long` | Yes | Avro promotes |
| Narrow `long` to `int` | **No** | Values would not fit |
| Change `string` to `int` | **No** | No promotion path |
| Reorder fields | Yes | Avro resolves by name, not position |

A rename is the one that catches people, because it looks harmless in a diff.
Use an `aliases` entry.

## The wire format underneath

None of this works without the framing:

```
byte 0      magic byte, always 0x00
bytes 1..4  schema id, big-endian int32
bytes 5..   Avro binary, no embedded schema
```

Avro binary is not self-describing — you cannot decode it without knowing the
writer's schema. Embedding the schema in every message would multiply the
payload; the four-byte id costs one registry lookup per distinct id per
consumer process, cached forever afterwards because registry ids are
immutable.

The five bytes are implemented independently in Go
(`ingest/internal/encode/avro.go`) and Java
(`serving/.../ConfluentAvroReader.java`), with tests on both sides that pin
the format, including the rejection of a payload whose first byte is not
`0x00`. That check is what turns "Protobuf sent to an Avro topic" into a
clear error instead of a nonsensical decode.
