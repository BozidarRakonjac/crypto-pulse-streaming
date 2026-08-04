"""
Bronze -> Silver: cleans raw_trades and loads into cleaned_trades.

Refactored so the logic lives in run_silver_cleaning(), a plain function.
This is what lets Dagster (or anything else) CALL this logic on demand,
instead of it firing automatically the moment the file is imported.

Still runnable standalone for manual testing:
    python pipelines/silver_cleaned_trades.py
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

INSERT_SQL = """
    INSERT INTO cleaned_trades
        (agg_trade_id, symbol, price, quantity, trade_time_ms, trade_time, is_buyer_maker)
    VALUES %s
    ON CONFLICT (agg_trade_id) DO NOTHING
"""


def run_silver_cleaning() -> dict:
    """
    Runs the full Bronze -> Silver cleaning pass.
    Returns a small summary dict -- useful later for Dagster to log/display.
    """
    conn = psycopg2.connect(**PG_CONFIG)

    raw_df = pd.read_sql("SELECT * FROM raw_trades", conn)

    clean_df = raw_df.dropna(subset=["agg_trade_id"])
    clean_df = clean_df[(clean_df["price"] > 0) & (clean_df["quantity"] > 0)]
    clean_df["trade_time"] = pd.to_datetime(clean_df["trade_time_ms"], unit="ms")

    before_dedup = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["agg_trade_id"], keep="first")
    duplicates_dropped = before_dedup - len(clean_df)

    records = list(
        clean_df[
            ["agg_trade_id", "symbol", "price", "quantity",
             "trade_time_ms", "trade_time", "is_buyer_maker"]
        ].itertuples(index=False, name=None)
    )

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, records)
        inserted = cur.rowcount
    conn.commit()
    conn.close()

    summary = {
        "rows_read": len(raw_df),
        "rows_after_filters": len(clean_df),
        "duplicates_dropped": duplicates_dropped,
        "rows_inserted": inserted,
        "rows_already_present": len(records) - inserted,
    }
    return summary


if __name__ == "__main__":
    result = run_silver_cleaning()
    print(f"Read {result['rows_read']} rows from raw_trades")
    print(f"{result['rows_after_filters']} rows remain after quality filters")
    print(f"Dropped {result['duplicates_dropped']} in-batch duplicate agg_trade_ids")
    print(
        f"Inserted {result['rows_inserted']} new rows into cleaned_trades "
        f"({result['rows_already_present']} were already present, skipped)"
    )