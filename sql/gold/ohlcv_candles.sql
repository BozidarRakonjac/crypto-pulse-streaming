-- Gold layer: ohlcv_candles
-- One row per symbol per 1-minute window, aggregated from cleaned_trades (Silver).
-- This is business/analytics-ready -- what a dashboard or chart queries directly.

CREATE TABLE IF NOT EXISTS ohlcv_candles (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL,
    candle_start    TIMESTAMP NOT NULL,     -- start of the 1-minute window (UTC)
    open            NUMERIC NOT NULL,
    high            NUMERIC NOT NULL,
    low             NUMERIC NOT NULL,
    close           NUMERIC NOT NULL,
    volume          NUMERIC NOT NULL,
    trade_count     INTEGER NOT NULL,
    volatility_pct  NUMERIC NOT NULL,        -- (high - low) / open * 100
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    -- One candle per symbol per minute. This is what makes the upsert
    -- in the aggregation script possible (ON CONFLICT needs this).
    UNIQUE (symbol, candle_start)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time
    ON ohlcv_candles (symbol, candle_start);