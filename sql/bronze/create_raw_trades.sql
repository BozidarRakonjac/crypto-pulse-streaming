-- Bronze layer: raw_trades
-- Mirrors the Kafka message almost 1:1. No cleaning, no aggregation.
-- If anything downstream is ever wrong, this table is the source of truth
-- to compare against.

CREATE TABLE IF NOT EXISTS raw_trades (
    id             BIGSERIAL PRIMARY KEY,
    symbol         VARCHAR(20) NOT NULL,
    price          NUMERIC NOT NULL,
    quantity       NUMERIC NOT NULL,
    trade_time_ms  BIGINT NOT NULL,      -- original Binance event time (epoch ms)
    is_buyer_maker BOOLEAN NOT NULL,
    ingested_at    TIMESTAMP NOT NULL DEFAULT NOW()  -- when OUR pipeline wrote this row
);

-- Useful once you start querying by symbol/time for candle building later
CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_time
    ON raw_trades (symbol, trade_time_ms);