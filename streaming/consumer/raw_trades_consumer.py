"""
Kafka -> Postgres consumer (Bronze layer).

Reads from the 'trades.raw' topic and inserts each message into the
raw_trades table, as-is, no transformation.

Mirrors the producer's 6-step mental model, consumer-side:
  1. Configuration        -> how do we reach Kafka AND Postgres?
  2. Consumer instance     -> the client object that reads messages
  3. Postgres connection   -> where we're writing to
  4. The poll loop         -> continuously read + process messages
  5. Insert + commit       -> write to DB, THEN tell Kafka "got it"
  6. Graceful shutdown     -> close both connections cleanly
"""

import json
import signal
import sys

import os

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()  # reads the .env file sitting in your project root

# --- 1. CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "trades.raw"

# Every consumer belongs to a "consumer group". Kafka tracks this group's
# offset per topic/partition. If this script crashes and restarts under the
# SAME group id, it resumes exactly where it left off -- no reprocessing,
# no data loss.
CONSUMER_GROUP_ID = "bronze-loader"

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


# --- 2. CONSUMER INSTANCE ---
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP_ID,
    # value/key were JSON-encoded strings by the producer -> decode them back
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    # auto_offset_reset: what to do the VERY FIRST time this group_id reads
    # this topic (no committed offset exists yet). 'earliest' = start from
    # the beginning of the log, so we don't miss the messages already sitting
    # there from your producer test run.
    auto_offset_reset="earliest",
    # IMPORTANT: we commit offsets ourselves, manually, AFTER a successful
    # DB insert -- not automatically. See the poll loop below for why.
    enable_auto_commit=False,
)


# --- 3. POSTGRES CONNECTION ---
pg_conn = psycopg2.connect(**PG_CONFIG)
pg_conn.autocommit = False   # we control commits explicitly, same reasoning as Kafka
pg_cursor = pg_conn.cursor()

INSERT_SQL = """
    INSERT INTO raw_trades (agg_trade_id, symbol, price, quantity, trade_time_ms, is_buyer_maker)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


def shutdown(signum, frame):
    print("\nShutting down: closing Kafka consumer and Postgres connection...")
    consumer.close()
    pg_cursor.close()
    pg_conn.close()
    print("Done. Goodbye.")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)


# --- 4. THE POLL LOOP ---
# Iterating over `consumer` blocks and waits whenever there's nothing new to
# read, then yields a message the instant one arrives. This runs forever
# until we stop it (Ctrl+C -> shutdown() above).
print(f"Listening on '{KAFKA_TOPIC}' as group '{CONSUMER_GROUP_ID}'...\n")

for message in consumer:
    trade = message.value  # already deserialized back into a dict

    try:
        # --- 5. INSERT, THEN COMMIT ---
        pg_cursor.execute(
            INSERT_SQL,
            (
                trade["agg_trade_id"],
                trade["symbol"],
                trade["price"],
                trade["quantity"],
                trade["trade_time_ms"],
                trade["is_buyer_maker"],
            ),
        )
        pg_conn.commit()

        # ONLY after Postgres confirms the write do we tell Kafka "processed."
        # Why this order matters: if we committed the Kafka offset FIRST and
        # then the Postgres insert failed/crashed, Kafka would think this
        # message was handled -- and it would be lost forever, since the
        # consumer would never see it again. Committing Kafka's offset last
        # means: if anything goes wrong before this line, on restart we just
        # re-read and re-insert the same message. A duplicate row is a much
        # smaller problem than a silently missing one.
        consumer.commit()

        print(
            f"Inserted -> symbol={trade['symbol']} price={trade['price']} "
            f"agg_id={trade['agg_trade_id']} offset={message.offset}"
        )

    except Exception as e:
        print(f"Failed to process message at offset {message.offset}: {e}", file=sys.stderr)
        pg_conn.rollback()
        # We deliberately do NOT commit the Kafka offset here -- this message
        # will be re-delivered on restart instead of silently skipped.