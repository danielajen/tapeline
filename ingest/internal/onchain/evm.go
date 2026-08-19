// Package onchain ingests EVM logs and decodes ERC-20 Transfer events.
//
// This is the fourth ingestion source and it deliberately reuses the same
// Runner, the same Kafka path and the same Avro framing as the exchange
// feeds. That reuse is the interesting part: an exchange trade and an
// on-chain transfer are both "a thing moved at a time", and once they share a
// pipeline you can join centralized and on-chain flow in the stream tier
// without a second system.
package onchain

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/tapeline/ingest/internal/model"
)

// TransferTopic0 is keccak256("Transfer(address,address,uint256)"), the first
// topic of every ERC-20 Transfer log.
const TransferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// Token describes a contract the decoder can label.
type Token struct {
	Symbol   string
	Decimals int32
}

// DefaultTokens covers the contracts worth watching for a market data
// project: the two dollar stablecoins and wrapped ETH, on Ethereum mainnet
// and Base.
var DefaultTokens = map[string]Token{
	// Ethereum mainnet
	"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {Symbol: "USDC", Decimals: 6},
	"0xdac17f958d2ee523a2206206994597c13d831ec7": {Symbol: "USDT", Decimals: 6},
	"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {Symbol: "WETH", Decimals: 18},
	"0x6b175474e89094c44da98b954eedeac495271d0f": {Symbol: "DAI", Decimals: 18},
	// Base
	"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": {Symbol: "USDC", Decimals: 6},
	"0x4200000000000000000000000000000000000006": {Symbol: "WETH", Decimals: 18},
}

// EVM is a venue.Decoder over an Ethereum-compatible JSON-RPC WebSocket.
type EVM struct {
	chain  string
	wsURL  string
	tokens map[string]Token
	// addresses restricts the subscription. Empty means every contract,
	// which on mainnet is a firehose — the default is the tokens map.
	addresses []string

	// lastBlock is the highest block seen, so only the first log of each new
	// block carries a sequence. See the comment on the sequence below.
	lastBlock int64
}

// NewEVM builds a decoder for a chain. Pass nil tokens to use DefaultTokens.
func NewEVM(chain, wsURL string, tokens map[string]Token, addresses []string) *EVM {
	if tokens == nil {
		tokens = DefaultTokens
	}
	if len(addresses) == 0 {
		for addr := range tokens {
			addresses = append(addresses, addr)
		}
	}
	lower := make([]string, 0, len(addresses))
	for _, a := range addresses {
		lower = append(lower, strings.ToLower(a))
	}
	return &EVM{chain: chain, wsURL: wsURL, tokens: tokens, addresses: lower}
}

// Name implements venue.Decoder.
func (e *EVM) Name() string { return e.chain }

// URL implements venue.Decoder.
func (e *EVM) URL() string { return e.wsURL }

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type logFilter struct {
	Address []string `json:"address,omitempty"`
	Topics  []any    `json:"topics"`
}

// Subscriptions implements venue.Decoder. The symbols argument is ignored:
// an EVM subscription is filtered by contract address, not by trading pair.
func (e *EVM) Subscriptions(_ []string) []any {
	return []any{
		rpcRequest{
			JSONRPC: "2.0", ID: 1, Method: "eth_subscribe",
			Params: []any{"logs", logFilter{
				Address: e.addresses,
				Topics:  []any{TransferTopic0},
			}},
		},
	}
}

type subscriptionEnvelope struct {
	Method string `json:"method"`
	Params struct {
		Subscription string          `json:"subscription"`
		Result       json.RawMessage `json:"result"`
	} `json:"params"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type rawLog struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
	Removed     bool     `json:"removed"`
}

// Decode implements venue.Decoder.
func (e *EVM) Decode(raw []byte, now time.Time) ([]model.Event, error) {
	var env subscriptionEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("evm envelope: %w", err)
	}
	if env.Error != nil {
		return nil, fmt.Errorf("evm rpc error %d: %s", env.Error.Code, env.Error.Message)
	}
	if env.Method != "eth_subscription" {
		// The subscription confirmation, which carries the id we do not need.
		return nil, nil
	}

	var l rawLog
	if err := json.Unmarshal(env.Params.Result, &l); err != nil {
		return nil, fmt.Errorf("evm log: %w", err)
	}

	// A removed log means a reorg undid it. Dropping it here is wrong for an
	// accounting system and right for a market data feed: the stream tier
	// treats on-chain flow as a signal, not a ledger. Documented in
	// docs/DESIGN_DECISIONS.md#d6.
	if l.Removed {
		return nil, nil
	}
	if len(l.Topics) < 3 || !strings.EqualFold(l.Topics[0], TransferTopic0) {
		return nil, nil
	}

	amount, ok := new(big.Int).SetString(strings.TrimPrefix(l.Data, "0x"), 16)
	if !ok {
		return nil, fmt.Errorf("evm transfer: undecodable amount %q", l.Data)
	}

	addr := strings.ToLower(l.Address)
	tok, known := e.tokens[addr]
	symbol := tok.Symbol
	if !known {
		symbol = addr
	}

	blockNum, err := hexToInt64(l.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("evm transfer block number: %w", err)
	}
	logIdx, err := hexToInt64(l.LogIndex)
	if err != nil {
		return nil, fmt.Errorf("evm transfer log index: %w", err)
	}

	transfer := &model.ChainTransfer{
		Chain:    e.chain,
		Token:    addr,
		Symbol:   symbol,
		FromAddr: topicToAddress(l.Topics[1]),
		ToAddr:   topicToAddress(l.Topics[2]),
		// Kept as a decimal string: a uint256 does not survive float64.
		AmountRaw:   amount.String(),
		Decimals:    tok.Decimals,
		BlockNumber: blockNum,
		LogIndex:    int32(logIdx),
		TxHash:      strings.ToLower(l.TxHash),
		// Logs carry no timestamp. Resolving one costs an eth_getBlockByNumber
		// round trip per block; the stream tier assigns block time from a
		// separate header subscription instead, so ingestion stays
		// single-round-trip.
		EventTimeUS:  now.UnixMicro(),
		IngestTimeUS: now.UnixMicro(),
	}

	// The sequence is the BLOCK, and only the first log of each block carries
	// one. Everything after it in the same block is NoSequence.
	//
	// This was blockNumber*10000 + logIndex, described as "globally ordered
	// within a chain". It is globally ordered and it is not contiguous, which
	// is what a gap detector needs. Two independent reasons:
	//
	//   - logIndex counts every log in the block, across all contracts. This
	//     subscription covers four token addresses, so the vast majority of
	//     indices belong to contracts we never asked for.
	//   - the detector keys on (venue, symbol, channel), which splits one
	//     block-wide counter across four tokens, so each sees only its own
	//     scattered subset.
	//
	// Running it live produced a continuous stream of WARN "sequence
	// discontinuity" lines within seconds - every one a false positive, on a
	// feed that was losing nothing. That is worse than no detection: it
	// trains you to ignore the alert that means something.
	//
	// A blockchain cannot skip a log within a block. What it can do, and what
	// actually matters, is deliver block N+2 after block N - a missed block,
	// which is real data loss. Sequencing on blocks detects exactly that.
	//
	// This is the same mistake as the Coinbase envelope sequence
	// (POSTMORTEM 2): a counter scoped to the frame, stamped on every event
	// inside it.
	seq := int64(model.NoSequence)
	if blockNum > e.lastBlock {
		e.lastBlock = blockNum
		seq = blockNum
	}

	return []model.Event{{
		Kind:     model.KindChainTransfer,
		Venue:    e.chain,
		Symbol:   symbol,
		Sequence: seq,
		Chain:    transfer,
	}}, nil
}

// topicToAddress extracts the 20-byte address from a 32-byte left-padded topic.
func topicToAddress(topic string) string {
	t := strings.ToLower(strings.TrimPrefix(topic, "0x"))
	if len(t) < 40 {
		return "0x" + t
	}
	return "0x" + t[len(t)-40:]
}

func hexToInt64(s string) (int64, error) {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 16, 64)
}
