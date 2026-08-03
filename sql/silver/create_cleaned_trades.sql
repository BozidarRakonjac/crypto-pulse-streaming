-- Silver layer: cleaned_trades
-- Same grain as raw_trades (one row per trade) -- NOT aggregated.
-- Just: no nulls, no duplicates, no nonsensical values.
-- This is the "trustworthy" version of raw_trades. Gold aggregates FROM here,
-- never directly from Bronze.

CREATE TABLE IF NOT EXISTS cleaned_trades (
    id             BIGSERIAL PRIMARY KEY,
    agg_trade_id   BIGINT NOT NULL UNIQUE,   -- Binance's own unique ID: guarantees no dupes
    symbol         VARCHAR(20) NOT NULL,
    price          NUMERIC NOT NULL CHECK (price > 0),
    quantity       NUMERIC NOT NULL CHECK (quantity > 0),
    trade_time_ms  BIGINT NOT NULL,
    trade_time     TIMESTAMP NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    cleaned_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cleaned_trades_symbol_time
    ON cleaned_trades (symbol, trade_time);