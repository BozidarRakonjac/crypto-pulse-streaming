"""
Binance -> Kafka producer.

This takes the exact same data source that worked in test_binance_stream.py,
and instead of printing each trade, publishes it onto a Kafka topic.

Read the comments in order -- they map to the 6-step mental model:
  1. Configuration      -> how do we reach the Kafka cluster?
  2. Producer instance   -> the client object that does the sending
  3. Data source loop    -> the Binance websocket callback
  4. topic / key / value -> per-message routing + payload
  5. Send + delivery check -> did it actually get through?
  6. Graceful shutdown    -> flush + close on exit
"""

import json
import signal
import sys

import websocket
from kafka import KafkaProducer

# --- 1. CONFIGURATION ---
# bootstrap_servers: the address(es) the producer uses to *first* connect to
# the cluster and learn about topics/partitions/brokers. Since this script
# runs on your host machine (not inside Docker), we use the host listener
# we set up in docker-compose: PLAINTEXT://localhost:9092
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "trades.raw"

STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"


# --- 2. PRODUCER INSTANCE ---
# value_serializer: Kafka only sends raw bytes. We give it a function that
# turns our Python dict into JSON, then encodes it to bytes -- this runs
# automatically every time we call producer.send().
# key_serializer: same idea, but for the key (a plain string here).
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",           # wait for full broker acknowledgment (safer, slightly slower)
    retries=3,            # retry a failed send a few times before giving up
)


def on_delivery_success(record_metadata):
    # Optional but useful while learning: proves messages are actually
    # landing in a real topic/partition, not just vanishing silently.
    print(
        f"Delivered -> topic={record_metadata.topic} "
        f"partition={record_metadata.partition} offset={record_metadata.offset}"
    )


def on_delivery_failure(exc):
    print(f"Failed to deliver message: {exc}", file=sys.stderr)


# --- 3. DATA SOURCE LOOP (Binance websocket callback) ---
def on_message(ws, message):
    trade = json.loads(message)

    # --- 4. topic / key / value ---
    # key = symbol: guarantees all BTCUSDT trades land in the same partition,
    # so a consumer reading that partition sees them in the exact order
    # Binance sent them. This matters later for correct OHLCV candle building.
    key = trade["s"]              # e.g. "BTCUSDT"

    value = {
        "symbol": trade["s"],
        "price": trade["p"],
        "quantity": trade["q"],
        "trade_time_ms": trade["T"],
        "is_buyer_maker": trade["m"],
    }

    # --- 5. SEND + delivery check ---
    # .send() is asynchronous -- it returns immediately, it does NOT block
    # waiting for Kafka to confirm. It returns a "future" we can attach
    # callbacks to, so we find out later (non-blocking) whether it worked.
    future = producer.send(KAFKA_TOPIC, key=key, value=value)
    future.add_callback(on_delivery_success)
    future.add_errback(on_delivery_failure)


def on_error(ws, error):
    print("WebSocket error:", error, file=sys.stderr)


def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")


def on_open(ws):
    print(f"Connected to Binance. Publishing BTC/USDT trades to topic '{KAFKA_TOPIC}'...\n")


# --- 6. GRACEFUL SHUTDOWN ---
# If we don't flush() before exiting, any messages still buffered in the
# producer (Kafka batches sends for efficiency) could be lost when the
# process dies. Ctrl+C triggers this via the signal handler below.
def shutdown(signum, frame):
    print("\nShutting down: flushing pending messages...")
    producer.flush(timeout=10)
    producer.close()
    print("Done. Goodbye.")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)   # handles Ctrl+C


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        STREAM_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()