"""
Silver -> Gold: aggregates cleaned_trades into 1-minute OHLCV candles.

Same structure as the Silver script: a callable function so Dagster can
wrap it later, plus a __main__ block for manual testing.

Steps:
  1. Configuration     -> connect to Postgres
  2. Read              -> pull all of cleaned_trades into a DataFrame
  3. Bucket            -> floor each trade's timestamp to its 1-minute window
  4. Aggregate          -> group by (symbol, candle_start), compute OHLCV +
                           volatility_pct + trade_count
  5. Write (upsert)     -> insert new candles, UPDATE existing ones
                           (the current, still-forming minute gets refined
                           every time this runs with more trades added)
"""

import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# ON CONFLICT ... DO UPDATE: unlike Silver (which only ever inserts NEW
# trades), Gold candles for the current minute need to be RECOMPUTED as
# more trades arrive within that same minute. So instead of skipping
# duplicates, we overwrite the existing candle with fresher numbers.
UPSERT_SQL = """
    INSERT INTO ohlcv_candles
        (symbol, candle_start, open, high, low, close, volume, trade_count, volatility_pct)
    VALUES %s
    ON CONFLICT (symbol, candle_start) DO UPDATE SET
        open           = EXCLUDED.open,
        high           = EXCLUDED.high,
        low            = EXCLUDED.low,
        close          = EXCLUDED.close,
        volume         = EXCLUDED.volume,
        trade_count    = EXCLUDED.trade_count,
        volatility_pct = EXCLUDED.volatility_pct,
        updated_at     = NOW()
"""


def run_gold_aggregation() -> dict:
    conn = psycopg2.connect(**PG_CONFIG)

    # --- 2. READ ---
    trades_df = pd.read_sql(
        "SELECT symbol, price, quantity, trade_time FROM cleaned_trades", conn
    )

    if trades_df.empty:
        conn.close()
        return {"candles_written": 0, "trades_read": 0}

    # --- 3. BUCKET ---
    # floor('min') rounds each timestamp DOWN to the start of its minute.
    # e.g. 14:32:47.300 -> 14:32:00. This is what makes every trade within
    # the same minute land in the same group in the next step.
    trades_df["candle_start"] = trades_df["trade_time"].dt.floor("min")

    # Sort by time WITHIN each group so "first row" = open, "last row" = close
    # are actually correct -- groupby alone doesn't guarantee order.
    trades_df = trades_df.sort_values("trade_time")

    # --- 4. AGGREGATE ---
    grouped = trades_df.groupby(["symbol", "candle_start"])

    candles = grouped.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("quantity", "sum"),
        trade_count=("price", "count"),
    ).reset_index()

    candles["volatility_pct"] = (
        (candles["high"] - candles["low"]) / candles["open"] * 100
    )

    # --- 5. WRITE (upsert) ---
    records = list(
        candles[
            ["symbol", "candle_start", "open", "high", "low",
             "close", "volume", "trade_count", "volatility_pct"]
        ].itertuples(index=False, name=None)
    )

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, records)
    conn.commit()
    conn.close()

    return {"trades_read": len(trades_df), "candles_written": len(records)}


if __name__ == "__main__":
    result = run_gold_aggregation()
    print(f"Read {result['trades_read']} trades from cleaned_trades")
    print(f"Wrote/updated {result['candles_written']} candles in ohlcv_candles")