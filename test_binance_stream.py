"""
Step 0: prove Binance's public WebSocket actually gives us live trade data.
No Kafka, no Docker, no API key — just a raw connection.

Using @aggTrade instead of @trade: Binance combines same-price, same-side
trades within the same millisecond into one event, so it's a bit less
noisy than raw @trade while still giving us everything we need for
OHLCV candles later.

Run it, watch trades print, then Ctrl+C to stop.
"""

import json
import websocket

# btcusdt@aggTrade = aggregated trade stream for BTC/USDT, no auth needed
STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"


def on_message(ws, message):
    trade = json.loads(message)
    price = trade["p"]           # price
    qty = trade["q"]              # quantity
    trade_time = trade["T"]        # trade time (ms epoch)
    is_buyer_maker = trade["m"]    # True = sell-side initiated
    print(f"[{trade_time}] BTC/USDT price={price} qty={qty} sell_side={is_buyer_maker}")


def on_error(ws, error):
    print("ERROR:", error)


def on_close(ws, close_status_code, close_msg):
    print("Connection closed")


def on_open(ws):
    print("Connected to Binance BTC/USDT trade stream. Watching live trades...\n")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        STREAM_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()