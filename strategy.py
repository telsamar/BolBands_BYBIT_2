
from bybit_keys import bybit_api_key, bybit_secret_key
from pybit.unified_trading import WebSocket
from time import sleep

api_key = bybit_api_key
api_secret = bybit_secret_key

ws = WebSocket(
    testnet=True,
    channel_type="linear",
    api_key=api_key,
    api_secret=api_secret
)

def handle_message(message):
    print("Received Message:")
    print(message)

ws.kline_stream(
    interval='5',
    symbol="BTCUSDT",
    callback=handle_message
)

try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    ws.close()
    print("WebSocket closed.")