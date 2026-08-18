# Postmortem 1: every Binance trade was recorded on the wrong side

**Status:** resolved
**Severity:** SEV-2 — silent data corruption, one of three venues
**Detected:** 17 August 2026, by a unit test, before any deployment
**Author:** Daniel Ajenifuja

---

## Summary

For the entire development history of the Binance adapter, every trade
ingested from Binance had its aggressor side inverted: buys were recorded as
sells and sells as buys. The data was well-formed, the pipeline was healthy,
no metric moved, and no error was logged. It was caught by a unit test written
specifically to assert the `m` flag semantics, several hours after the code
that broke it was written.

The cause was a documented behaviour of Go's `encoding/json`: when no field
tag matches a key exactly, the decoder falls back to a **case-insensitive**
match. Binance sends both `"m"` (buyer-is-maker, a boolean) and `"M"` (a
deprecated flag, also a boolean) in the same object. The struct declared only
`json:"m"`, so `"M"` bound to the same field and overwrote it.

---

## Impact

Nothing reached production, because there is no production. Had it, the blast
radius would have been:

- **Every Binance trade side inverted.** Roughly a third of trade volume.
- **Any side-dependent aggregate wrong.** Buy/sell volume imbalance, order
  flow imbalance, and any signal built on aggressor direction.
- **VWAP, OHLC and volume unaffected.** They do not read `side`, which is
  precisely why nothing looked wrong.
- **Coinbase and Kraken unaffected.** Different adapters, different bug.

The most important line in this section is the third one. The corruption was
confined to a field that nothing downstream aggregated yet, so every
dashboard, every alert and every existing test would have stayed green
indefinitely.

---

## Timeline

| Time (UTC) | Event |
|---|---|
| 15:52 | Binance adapter written. `binanceTrade` declares `BuyerIsMaker bool \`json:"m"\``. `"M"` is not declared, because it is documented as "ignore". |
| 15:58 | Adapter compiles. `go vet` is clean. |
| 16:03 | A separate case-insensitivity bug is hit in the same file — the event-type probe struct — and fixed by switching that probe to an exact map lookup. **The same class of bug in `binanceTrade` is not noticed.** |
| 16:05 | `TestBinanceDecodeTradeAggressorSide` runs. `m=true` correctly yields `SELL`. `m=false` yields `SELL` as well. Test fails. |
| 16:07 | Root cause identified: `"M": true` in the test payload is binding to `BuyerIsMaker` case-insensitively and overwriting `m=false`. |
| 16:09 | Fix: declare `Ignore bool \`json:"M"\`` so `"M"` has an exact match of its own. |
| 16:10 | Test passes. Full suite green under `-race`. |

---

## Root cause

`encoding/json` matches object keys to struct fields with an exact tag match
first, then falls back to a case-insensitive comparison. The fallback exists
so that Go structs can consume APIs with inconsistent casing, and it is
documented behaviour rather than a defect.

The Binance trade payload:

```json
{"e":"trade","E":1755432000000,"s":"BTCUSDT","t":51234,
 "p":"64150.10","q":"0.004","T":1755431999900,"m":false,"M":true}
```

The struct as written:

```go
type binanceTrade struct {
    // ...
    BuyerIsMaker bool `json:"m"`   // "m" matches exactly
                                    // "M" has no exact match anywhere,
                                    // so it ALSO binds here — and wins,
                                    // because it is decoded later.
}
```

`"M"` is documented by Binance as "ignore", and it is very nearly always
`true`. So the field it hijacked was effectively pinned to `true`, meaning
every trade was read as buyer-is-maker, meaning every aggressor side came out
`SELL`.

The fix declares a field for it:

```go
// Ignore is Binance's deprecated "M" flag. It is decoded into a field of
// its own for one reason: encoding/json matches keys case-insensitively
// when no exact match exists, so without this field "M" binds to
// BuyerIsMaker and silently inverts the aggressor side of every trade.
Ignore bool `json:"M"`
```

---

## Contributing factors

**1. The venue's field naming.** `m` and `M` as distinct fields in one object
is an unusual choice, and it is exactly the input Go's fallback is worst at.

**2. The same bug had already appeared in the same file, and the fix did not
generalise.** At 16:03 the identical failure mode was hit in the event-type
probe and fixed *locally* — by switching that one struct to a map lookup —
without asking where else in the file the same hazard existed. Fixing the
instance rather than the class cost two hours.

**3. Nothing observable changed.** No metric, no log line, no error. The
corruption was in a field with no downstream consumer yet, which is the
condition under which silent data bugs survive longest.

**4. The bug was in pure decoding logic** — which is also why it was caught.
See below.

---

## What went right

**The pure-decoder architecture made this catchable at all.** Venue adapters
are split into a pure `Decode(bytes, time) → []Event` function and a stateful
`Runner` that owns sockets and reconnects. Because `Decode` touches no
network and no broker, the test that caught this is eleven lines of a table
and runs in microseconds. If side normalization had lived inside the
WebSocket read loop, catching it would have required a live Binance
connection and someone looking at the output — which is to say it would not
have been caught.

**The test asserted semantics, not shape.** The test that failed does not
check that the payload parses; it checks that `m=true` means the *seller* was
the aggressor. A test asserting only that a `Trade` came back with a non-empty
side would have passed.

---

## Corrective actions

| Action | Status |
|---|---|
| Declare `Ignore bool \`json:"M"\`` in `binanceTrade` | Done |
| Convert the event-type probe to an exact map lookup | Done |
| Assert both directions of the `m` flag in `decode_test.go` | Done |
| Audit Coinbase and Kraken structs for case-colliding keys | Done — none found; both venues use distinct, lowercase names |
| Document the hazard at both sites so the next reader does not undo it | Done |
| A lint rule that flags structs whose JSON tags differ only by case | **Not done** — see below |

The last one is the only real gap. There is no `go vet` check for this, and a
custom analyzer is more machinery than one bug justifies today. The mitigation
is the comment at both call sites explaining precisely why the seemingly
useless `Ignore` field must not be deleted — because "unused field, removing
it" is exactly how this bug comes back.

---

## What this changes going forward

**Decoder tests assert meaning, not shape.** Every venue decoder test now
pins a semantic property — which side is the aggressor, that a zero size is a
deletion, that a Kraken book has no sequence — rather than that parsing
succeeded.

**A fix for an instance triggers a search for the class.** The two-hour gap
here was entirely caused by fixing one occurrence and moving on.

**Silent corruption gets priority over loud failure.** A crash is a bug that
reports itself. This one would have run for months, and the only thing
standing between it and a wrong number in a dashboard was a test somebody
chose to write.

---

## Appendix: reproducing it

```bash
cd ingest
git stash        # if the fix is applied
go test ./internal/venue/ -run TestBinanceDecodeTradeAggressorSide -v
```

Before the fix:

```
--- FAIL: TestBinanceDecodeTradeAggressorSide
    decode_test.go:256: m=false side = "SELL", want BUY
```


---

# Postmortem 2: three bugs the first live run found in ten minutes

**Status:** all three fixed, all three now covered by regression tests
**Detected:** 17 August 2026, first time the system was ever run end to end
**Severity:** SEV-1 (silent 59% data loss), SEV-2 (false alerting), SEV-1 (total outage on a transient fault)

## Summary

Every unit test passed. 168 of them, race-detector clean. The Go tier built,
the Scala tier built, the shaded jar built. Then the system was pointed at
live exchange feeds for the first time and produced three distinct bugs inside
ten minutes — none of which any unit test could have caught, because all three
were about the *interaction* between components and the real world.

## Bug 1 — 1,915 phantom sequence gaps in 45 seconds

**Symptom.** Coinbase reported 1,637 book gaps and 278 trade gaps and 11,161
"missed" messages. Kraken and Binance reported zero.

**Cause.** Coinbase Advanced Trade stamps one monotonic `sequence_num` on every
envelope across every product and channel on the socket. The gap detector keyed
on `(venue, symbol, channel)`, splitting that single counter across six streams.
Each stream then saw a number advancing by roughly six per message and called
every step a gap.

The adapter's own doc comment said the sequence was per-connection. The
detector was keyed per-stream anyway. **The comment and the code disagreed, and
the tests asserted the code.**

**Fix.** `Pipeline.ConnectionScopedVenues` — venues whose counter describes the
connection collapse to a single detector key.

**Result.** 1,915 gaps in 45 s → 2 gaps in 90 s.

## Bug 2 — 59% of Coinbase trades silently discarded

**Symptom.** Found only because bug 1's fix made the numbers legible: Coinbase
received 1,575 trades and published 641. The missing 934 were counted as
"duplicates suppressed."

**Cause.** A Coinbase frame can carry several trades, and the decoder stamped
the *envelope's* sequence number on every one. The gap detector saw the same
sequence repeatedly and did exactly what it was built to do: dropped them as
retransmits.

**Why this is the worst of the three.** It presented as a feature working
correctly. The duplicate counter went up, which is what that counter is *for*.
No error, no gap, no alert — real market data deleted, filed under a metric
that means "working as intended."

**Fix.** The sequence belongs to the frame: the first event carries it, the
rest are marked unsequenced. Trade identity comes from `trade_id`, which was
always unique.

## Bug 3 — the process exited on the first Kafka write failure

**Symptom.** The chaos experiment killed the broker. The hypothesis was
"producer retries rather than dropping; throughput recovers within 60 s." What
actually happened:

```
"pipeline stopped" received:53471 published:53470
ERROR "ingestd exited with error"
  err="publish on tick: dial tcp [::1]:9092: connection refused"
```

The service died. A 30-second broker restart became a permanent outage.

**Cause.** `flush()` returned its error, `Run` propagated it, `main` exited.
Written that way deliberately — the reasoning at the time was that a silent
publish failure is data loss, so failing loudly is correct. That reasoning was
right about the failure mode and wrong about the response.

**Fix.** A publish failure retains the batch and retries on the next tick.
Retention is bounded by `MaxPendingEvents`; past it the oldest are dropped and
counted in `events_dropped_total`, because an OOM kill loses everything
silently whereas a counted drop is alertable.

**Result, re-run:** process survived, 20 s to resume publishing after restart,
**zero events dropped**.

## What this says

**Unit tests cannot find integration bugs, and 168 passing tests bought false
confidence.** Every one of these lived in the seam between a component and
reality — a venue's actual sequencing semantics, a frame carrying more than one
event, a broker that goes away.

**Two of the three were silent.** Bugs 1 and 2 produced no error and no alert.
Bug 2 actively reported itself as correct behaviour. The chaos experiment
caught bug 3 only because it stated a hypothesis first and then checked it,
rather than observing the run and describing what happened.

**The claim was false before it was tested.** "Zero data loss under broker
failure" was in the design documents before any broker had ever been killed.
It is true now. It was not true when written, and nothing except running it
would have revealed that.
