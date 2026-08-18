-- Postgres schema for the serving tier's metadata.
--
-- Postgres holds the small, transactional, correctness-critical state: who
-- may call the API and at what rate. It deliberately holds no market data —
-- that lives in Kafka, the OLAP store, and Iceberg, each of which is better
-- at it than a relational database would be.

CREATE TABLE IF NOT EXISTS api_keys (
    key_id                        TEXT PRIMARY KEY,

    -- Stored, not hashed. HMAC requires the server to hold the same secret
    -- the client signs with, so unlike a password this cannot be a one-way
    -- hash. The mitigations are encryption at rest and tight grants on this
    -- table, and the tradeoff is stated in ApiKey.java rather than glossed.
    secret                        TEXT        NOT NULL,

    owner                         TEXT        NOT NULL,
    rate_limit_capacity           INTEGER     NOT NULL DEFAULT 100,
    rate_limit_refill_per_second  DOUBLE PRECISION NOT NULL DEFAULT 10.0,

    -- Disabling rather than deleting keeps an audit trail, and revocation
    -- takes effect within the 30-second repository cache TTL.
    enabled                       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at                  TIMESTAMPTZ,

    CONSTRAINT rate_limit_capacity_positive CHECK (rate_limit_capacity > 0),
    CONSTRAINT rate_limit_refill_positive   CHECK (rate_limit_refill_per_second > 0)
);

-- Enabled-key lookups are on the path of every request; a partial index keeps
-- the hot set small.
CREATE INDEX IF NOT EXISTS api_keys_enabled_idx ON api_keys (key_id) WHERE enabled;

CREATE TABLE IF NOT EXISTS symbols (
    symbol        TEXT PRIMARY KEY,       -- canonical BASE-QUOTE
    base_asset    TEXT NOT NULL,
    quote_asset   TEXT NOT NULL,
    tick_size     NUMERIC(38, 18),
    min_size      NUMERIC(38, 18),
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The per-venue spelling of each canonical symbol. This table is the
-- authority the Go tier's hard-coded mappings should eventually read from;
-- until then it documents them. See docs/ROADMAP.md.
CREATE TABLE IF NOT EXISTS venue_symbols (
    venue         TEXT NOT NULL,
    symbol        TEXT NOT NULL REFERENCES symbols (symbol) ON DELETE CASCADE,
    venue_symbol  TEXT NOT NULL,
    PRIMARY KEY (venue, symbol)
);

INSERT INTO symbols (symbol, base_asset, quote_asset, tick_size, min_size)
VALUES
    ('BTC-USD', 'BTC', 'USD', 0.01, 0.00001),
    ('ETH-USD', 'ETH', 'USD', 0.01, 0.0001),
    ('SOL-USD', 'SOL', 'USD', 0.01, 0.001)
ON CONFLICT (symbol) DO NOTHING;

INSERT INTO venue_symbols (venue, symbol, venue_symbol)
VALUES
    ('coinbase', 'BTC-USD', 'BTC-USD'),
    ('coinbase', 'ETH-USD', 'ETH-USD'),
    ('coinbase', 'SOL-USD', 'SOL-USD'),
    ('kraken',   'BTC-USD', 'BTC/USD'),
    ('kraken',   'ETH-USD', 'ETH/USD'),
    ('kraken',   'SOL-USD', 'SOL/USD'),
    -- Binance lists no USD spot pairs; USDT is the standing proxy.
    ('binance',  'BTC-USD', 'BTCUSDT'),
    ('binance',  'ETH-USD', 'ETHUSDT'),
    ('binance',  'SOL-USD', 'SOLUSDT')
ON CONFLICT (venue, symbol) DO NOTHING;

-- A local development key. The secret is in the repository on purpose: it is
-- worthless outside this compose file, and a key that is documented is a key
-- nobody invents a worse one to replace. Production keys are issued by
-- scripts/issue-api-key.sh and never committed.
INSERT INTO api_keys (key_id, secret, owner, rate_limit_capacity, rate_limit_refill_per_second)
VALUES ('tk_local_dev', 'local-development-secret-not-for-production', 'local', 1000, 100.0)
ON CONFLICT (key_id) DO NOTHING;
