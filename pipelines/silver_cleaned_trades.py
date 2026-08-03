"""
Bronze -> Silver: silver_cleaned_trades.py

Reads everything currently in raw_trades, applies data-quality rules, and
loads the result into cleaned_trades. Safe to re-run repeatedly -- rows
already present in cleaned_trades are silently skipped (via ON CONFLICT),
not duplicated.

This is a plain, manually-run script for now. Later, this exact logic gets
wrapped into a Dagster asset so it runs on a schedule instead of by hand.

Steps:
  1. Configuration        -> connect to Postgres
  2. Read                 -> pull all of raw_trades into a DataFrame
  3. Clean                -> drop nulls, invalid values
  4. Deduplicate           -> drop in-batch duplicate agg_trade_ids
  5. Write                -> insert into cleaned_trades, skip existing
"""

import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# --- 1. CONFIGURATION ---
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

conn = psycopg2.connect(**PG_CONFIG)

# --- 2. READ ---
# For now we read the whole table every run (Option B from our discussion:
# simple, correct, a bit wasteful at scale -- fine for this project's size).
# The ON CONFLICT in the insert step is what makes re-running this safe.
raw_df = pd.read_sql("SELECT * FROM raw_trades", conn)
print(f"Read {len(raw_df)} rows from raw_trades")

# --- 3. CLEAN ---
# Rule 1: agg_trade_id must exist. Rows from before we added this column
# (or any row where the producer/consumer somehow lost it) get dropped here.
clean_df = raw_df.dropna(subset=["agg_trade_id"])

# Rule 2: price and quantity must be positive numbers, not zero or negative.
# (Binance shouldn't ever send these, but "don't trust upstream data" is a
# good default habit -- validate what you receive, don't assume it's clean.)
clean_df = clean_df[(clean_df["price"] > 0) & (clean_df["quantity"] > 0)]

# Rule 3: convert the raw millisecond timestamp into a real Postgres
# TIMESTAMP, computed once here so Silver/Gold never have to redo this.
clean_df["trade_time"] = pd.to_datetime(clean_df["trade_time_ms"], unit="ms")

print(f"{len(clean_df)} rows remain after quality filters "
      f"({len(raw_df) - len(clean_df)} dropped)")

# --- 4. DEDUPLICATE ---
# Even after filtering nulls, raw_trades can legitimately contain the same
# agg_trade_id twice -- remember, our consumer guarantees AT-LEAST-once
# delivery, so reprocessing after a crash can insert a trade twice into
# Bronze. This is expected, not a bug -- Silver is exactly where we resolve it.
before_dedup = len(clean_df)
clean_df = clean_df.drop_duplicates(subset=["agg_trade_id"], keep="first")
print(f"Dropped {before_dedup - len(clean_df)} in-batch duplicate agg_trade_ids")

# --- 5. WRITE ---
# execute_values: inserts many rows in one efficient batch instead of one
# INSERT statement per row (which would be slow for thousands of rows).
# ON CONFLICT (agg_trade_id) DO NOTHING: if this trade is already in
# cleaned_trades from a previous run of this script, silently skip it
# instead of erroring out. This is what makes re-running the script safe.
records = list(
    clean_df[
        ["agg_trade_id", "symbol", "price", "quantity",
         "trade_time_ms", "trade_time", "is_buyer_maker"]
    ].itertuples(index=False, name=None)
)

INSERT_SQL = """
    INSERT INTO cleaned_trades
        (agg_trade_id, symbol, price, quantity, trade_time_ms, trade_time, is_buyer_maker)
    VALUES %s
    ON CONFLICT (agg_trade_id) DO NOTHING
"""

with conn.cursor() as cur:
    execute_values(cur, INSERT_SQL, records)
    inserted = cur.rowcount   # how many rows were ACTUALLY new
conn.commit()

print(f"Inserted {inserted} new rows into cleaned_trades "
      f"({len(records) - inserted} were already present, skipped)")

conn.close()