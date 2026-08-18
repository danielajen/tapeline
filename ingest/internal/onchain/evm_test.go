package onchain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tapeline/ingest/internal/model"
)

var testNow = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

// A real USDC Transfer log shape: topic0 is the Transfer signature, topics
// 1 and 2 are the left-padded from/to addresses, and data is the uint256
// amount.
const usdcTransferLog = `{
  "jsonrpc":"2.0",
  "method":"eth_subscription",
  "params":{
    "subscription":"0xcd0c3e8af590364c09d0fa6a1210faf5",
    "result":{
      "address":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
      "topics":[
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266",
        "0x00000000000000000000000070997970c51812dc3a010c7d01b50e0d17dc79c8"
      ],
      "data":"0x00000000000000000000000000000000000000000000000000000002540be400",
      "blockNumber":"0x1312d00",
      "transactionHash":"0xAABBCC",
      "logIndex":"0x2a",
      "removed":false
    }
  }
}`

func TestDecodeERC20Transfer(t *testing.T) {
	e := NewEVM("ethereum", "wss://example.invalid", nil, nil)

	events, err := e.Decode([]byte(usdcTransferLog), testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}

	ev := events[0]
	if ev.Kind != model.KindChainTransfer || ev.Venue != "ethereum" {
		t.Errorf("envelope wrong: %+v", ev)
	}

	x := ev.Chain
	if x.Symbol != "USDC" || x.Decimals != 6 {
		t.Errorf("token resolution wrong: symbol=%q decimals=%d", x.Symbol, x.Decimals)
	}
	// Addresses come out of 32-byte topics as checksummed-length hex.
	if x.FromAddr != "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266" {
		t.Errorf("from = %q", x.FromAddr)
	}
	if x.ToAddr != "0x70997970c51812dc3a010c7d01b50e0d17dc79c8" {
		t.Errorf("to = %q", x.ToAddr)
	}
	// 0x2540be400 = 10,000,000,000 raw units = 10,000 USDC at 6 decimals.
	if x.AmountRaw != "10000000000" {
		t.Errorf("amount = %q, want 10000000000", x.AmountRaw)
	}
	if x.BlockNumber != 20_000_000 {
		t.Errorf("block = %d, want 20000000", x.BlockNumber)
	}
	if x.LogIndex != 42 {
		t.Errorf("log index = %d, want 42 (0x2a)", x.LogIndex)
	}
	// The contract address must be lowercased so the token map lookup is
	// not case-dependent.
	if x.Token != "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" {
		t.Errorf("token address not normalized: %q", x.Token)
	}

	// blockNumber*10000 + logIndex gives the gap detector something monotonic.
	if ev.Sequence != 20_000_000*10000+42 {
		t.Errorf("sequence = %d", ev.Sequence)
	}
}

func TestUint256AmountsSurvive(t *testing.T) {
	e := NewEVM("ethereum", "wss://example.invalid", nil, nil)

	// max uint256. A float64 would render this as 1.157920892373162e+77.
	var log map[string]any
	if err := json.Unmarshal([]byte(usdcTransferLog), &log); err != nil {
		t.Fatal(err)
	}
	params := log["params"].(map[string]any)
	result := params["result"].(map[string]any)
	result["data"] = "0x" +
		"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	raw, _ := json.Marshal(log)

	events, err := e.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	const want = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	if got := events[0].Chain.AmountRaw; got != want {
		t.Errorf("amount = %s\nwant   = %s", got, want)
	}
}

func TestRemovedLogsAreDropped(t *testing.T) {
	e := NewEVM("ethereum", "wss://example.invalid", nil, nil)

	var log map[string]any
	if err := json.Unmarshal([]byte(usdcTransferLog), &log); err != nil {
		t.Fatal(err)
	}
	log["params"].(map[string]any)["result"].(map[string]any)["removed"] = true
	raw, _ := json.Marshal(log)

	events, err := e.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("a reorged-out log produced %d events, want 0", len(events))
	}
}

func TestUnknownTokenFallsBackToItsAddress(t *testing.T) {
	e := NewEVM("base", "wss://example.invalid", nil, nil)

	var log map[string]any
	if err := json.Unmarshal([]byte(usdcTransferLog), &log); err != nil {
		t.Fatal(err)
	}
	const unknown = "0x1111111111111111111111111111111111111111"
	log["params"].(map[string]any)["result"].(map[string]any)["address"] = unknown
	raw, _ := json.Marshal(log)

	events, err := e.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if events[0].Chain.Symbol != unknown {
		t.Errorf("symbol = %q, want the raw address", events[0].Chain.Symbol)
	}
	if events[0].Chain.Decimals != 0 {
		t.Errorf("decimals = %d, want 0 for an unknown token", events[0].Chain.Decimals)
	}
}

func TestNonTransferLogsAreIgnored(t *testing.T) {
	e := NewEVM("ethereum", "wss://example.invalid", nil, nil)

	// An Approval log: right shape, wrong topic0.
	raw := []byte(`{"method":"eth_subscription","params":{"subscription":"0x1","result":{
	  "address":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
	  "topics":["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
	            "0x0000000000000000000000000000000000000000000000000000000000000001",
	            "0x0000000000000000000000000000000000000000000000000000000000000002"],
	  "data":"0x01","blockNumber":"0x1","transactionHash":"0x1","logIndex":"0x0"}}}`)

	events, err := e.Decode(raw, testNow)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("an Approval log produced %d events, want 0", len(events))
	}
}

func TestSubscriptionAckAndRPCErrors(t *testing.T) {
	e := NewEVM("ethereum", "wss://example.invalid", nil, nil)

	// The eth_subscribe reply, which carries no method field.
	events, err := e.Decode([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xabc"}`), testNow)
	if err != nil || len(events) != 0 {
		t.Errorf("subscribe ack: events=%d err=%v", len(events), err)
	}

	if _, err := e.Decode([]byte(`{"error":{"code":-32600,"message":"invalid request"}}`), testNow); err == nil {
		t.Error("an RPC error decoded without error")
	}
}

func TestSubscriptionFiltersByConfiguredAddresses(t *testing.T) {
	e := NewEVM("ethereum", "wss://example.invalid", map[string]Token{
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {Symbol: "USDC", Decimals: 6},
	}, nil)

	subs := e.Subscriptions(nil)
	if len(subs) != 1 {
		t.Fatalf("got %d subscriptions, want 1", len(subs))
	}

	raw, err := json.Marshal(subs[0])
	if err != nil {
		t.Fatal(err)
	}
	var req struct {
		Method string `json:"method"`
		Params []any  `json:"params"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	if req.Method != "eth_subscribe" || len(req.Params) != 2 || req.Params[0] != "logs" {
		t.Fatalf("subscription is not an eth_subscribe logs call: %s", raw)
	}

	filter := req.Params[1].(map[string]any)
	addrs := filter["address"].([]any)
	// Without an address filter this is a mainnet firehose, which is both a
	// cost problem and a rate-limit problem.
	if len(addrs) != 1 || addrs[0] != "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" {
		t.Errorf("address filter = %v", addrs)
	}
	topics := filter["topics"].([]any)
	if len(topics) != 1 || topics[0] != TransferTopic0 {
		t.Errorf("topic filter = %v, want just the Transfer signature", topics)
	}
}

func TestTopicToAddressHandlesShortInput(t *testing.T) {
	if got := topicToAddress("0x1234"); got != "0x1234" {
		t.Errorf("short topic = %q, want it returned unchanged", got)
	}
}
