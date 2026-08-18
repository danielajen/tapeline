# API

Two surfaces over the same data: gRPC for programmatic clients, REST for
browsers and for `curl`. Both authenticate identically.

## Authentication

HMAC-SHA256 over a canonical request string. This is how Coinbase's own API
authentication works, and it is the scheme this project deliberately mirrors.

### The canonical string

```
METHOD \n PATH \n TIMESTAMP \n NONCE \n sha256_hex(BODY)
```

Every field is bound into the signature, and the newline delimiter is
load-bearing:

| Field | Why it is signed |
|---|---|
| `METHOD` | So a GET signature cannot be replayed against a DELETE |
| `PATH` | So a signature for one symbol cannot be used on another |
| `TIMESTAMP` | Bounds how long a captured request stays usable |
| `NONCE` | Makes a captured request usable exactly zero more times |
| `sha256(BODY)` | The hash, not the body, so signing does not buffer the payload twice |

Without the delimiter, `("/ab", "c")` and `("/a", "bc")` would produce
identical signed bytes — a boundary-shifting attack, pinned by
`fieldBoundariesCannotBeShifted` in `SignedRequestTest.java`.

An empty body hashes the empty string:
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

### Headers

| REST header | gRPC metadata key |
|---|---|
| `X-Tapeline-Key` | `x-tapeline-key` |
| `X-Tapeline-Signature` | `x-tapeline-signature` |
| `X-Tapeline-Timestamp` | `x-tapeline-timestamp` |
| `X-Tapeline-Nonce` | `x-tapeline-nonce` |

For gRPC the canonical string uses `POST` and `/<full.Method/Name>` as the
path, and the empty-body hash — see `ROADMAP.md` for why bodies are not
signed there.

### Signing, in shell

```bash
KEY_ID="tk_local_dev"
SECRET="local-development-secret-not-for-production"
PATH_="/api/v1/quotes/BTC-USD"
TS=$(date +%s)
NONCE=$(uuidgen)
BODY_HASH=$(printf '' | openssl dgst -sha256 -hex | awk '{print $NF}')

SIG=$(printf 'GET\n%s\n%s\n%s\n%s' "$PATH_" "$TS" "$NONCE" "$BODY_HASH" \
      | openssl dgst -sha256 -hmac "$SECRET" -binary | base64)

curl -s "http://localhost:8080${PATH_}" \
  -H "X-Tapeline-Key: ${KEY_ID}" \
  -H "X-Tapeline-Signature: ${SIG}" \
  -H "X-Tapeline-Timestamp: ${TS}" \
  -H "X-Tapeline-Nonce: ${NONCE}"
```

### What a client must get right

- **Clock skew under 30 seconds.** Outside the window the request is
  rejected, in both directions — a future-dated request is refused too, or it
  could be stockpiled and replayed later.
- **A fresh nonce per request.** Reuse is rejected. The nonce is namespaced by
  API key, so one tenant cannot burn another's values.
- **Back off on `429` / `RESOURCE_EXHAUSTED`.** The `retryAfterSeconds` is
  computed from the actual token deficit, not a fixed guess.

### Errors

Every authentication failure returns the same status with the same message.
Distinguishing "unknown key" from "bad signature" would be an enumeration
oracle; the specific reason is in the server log only.

| Condition | REST | gRPC |
|---|---|---|
| Any authentication failure | `401` | `UNAUTHENTICATED` |
| Rate limited | `429` | `RESOURCE_EXHAUSTED` |
| Unknown symbol | `404` | `NOT_FOUND` |
| Bad argument | `400` | `INVALID_ARGUMENT` |

## Rate limiting

A token bucket per API key, evaluated atomically inside Redis. Capacity and
refill rate are per-key columns in `api_keys`.

A token bucket rather than a fixed window, because a fixed window admits a
double-rate burst across its boundary: spend the full quota at the end of one
window and again at the start of the next. Costs differ by operation — a
streaming subscription is charged ten tokens against a point read's one,
because it consumes server resources for as long as it lives.

Under a Redis outage the limiter **fails open**. A rate limiter is a
protection, not a dependency, and taking the API down to enforce a quota
inverts the priority. `RateLimiterFailingOpen` alerts precisely because that
failure is otherwise silent.

## gRPC

```protobuf
service MarketData {
  rpc StreamQuotes(StreamQuotesRequest) returns (stream Quote);
  rpc GetQuote(GetQuoteRequest) returns (Quote);
  rpc StreamDivergence(StreamDivergenceRequest) returns (stream DivergenceEvent);
  rpc QueryWindows(QueryWindowsRequest) returns (QueryWindowsResponse);
}
```

### `StreamQuotes`

Opens with one snapshot per requested symbol, then pushes on change,
coalesced to `max_updates_hz` (default 10, maximum 100). At most 64 symbols
per stream.

Two behaviours worth knowing as a client:

- **Updates are dropped, not queued, for a subscriber that cannot keep up.**
  The next quote supersedes the last, so a slow consumer gets the freshest
  price rather than a growing backlog of stale ones. gRPC would otherwise
  buffer without bound and the server would run out of heap because of one
  client.
- **The stream ends on deploy.** Subscribers are pinned to a replica.
  Reconnect; the opening snapshot makes it cheap.

```bash
# Reflection is enabled in the compose profile, so no proto file is needed.
grpcurl -plaintext \
  -H "x-tapeline-key: tk_local_dev" \
  -H "x-tapeline-signature: ${SIG}" \
  -H "x-tapeline-timestamp: ${TS}" \
  -H "x-tapeline-nonce: ${NONCE}" \
  -d '{"symbols":["BTC-USD"],"maxUpdatesHz":5}' \
  localhost:9090 tapeline.v1.MarketData/StreamQuotes
```

### `QueryWindows`

Historical bars. The response carries `served_from`, either `"olap"` or
`"lakehouse"` — not decoration: without it, a latency regression caused by
queries silently falling through to the slow tier is invisible in metrics.

Ranges older than the OLAP retention currently return `UNIMPLEMENTED`. See
`ROADMAP.md`.

## REST

| Endpoint | Returns |
|---|---|
| `GET /api/v1/quotes/{symbol}` | The freshest valid quote across venues |
| `GET /api/v1/quotes/{symbol}?venue=coinbase` | That venue's latest quote |
| `GET /api/v1/quotes/{symbol}/venues` | Every venue's quote, one round trip |
| `GET /actuator/health` | Liveness and readiness |
| `GET /actuator/prometheus` | Metrics |

Read-only and unary by design. Streaming over HTTP would mean SSE or
WebSockets — a second streaming implementation to keep correct, for no gain
over the gRPC one.

`/quotes/{symbol}/venues` is the view that makes a divergence alert
interpretable, which is why it exists as its own endpoint rather than as a
query parameter.
