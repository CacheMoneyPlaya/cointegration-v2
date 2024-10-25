import json
import csv
import websocket
import ccxt
from dotenv import load_dotenv
import os
import time
import ssl
import threading
import keyboard  # For listening to the spacebar press

# Load API keys and password from .env file
load_dotenv()
ACCESS_KEY = os.getenv("BITGET_ACCESS_KEY")
SECRET_KEY = os.getenv("BITGET_SECRET_KEY")
PASSWORD = os.getenv("BITGET_PASSWORD")  # Load the passphrase

exchange = ccxt.bitget({
    'apiKey': ACCESS_KEY,
    'secret': SECRET_KEY,
    'password': PASSWORD,
    'enableRateLimit': True,
    'options': {
        'createMarketBuyOrderRequiresPrice': False,  # Override the price requirement for market buys
    },
})

ACTIVE_TRADES_FILE = 'active_trades.csv'
pairs_to_monitor = []  # Store pairs we are monitoring
ticker_prices = {}  # Track latest close prices for tickers
ws = None  # WebSocket connection placeholder

def load_active_trades():
    """Load active trade details with conditions from a CSV file."""
    trades = []
    with open(ACTIVE_TRADES_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            trades.append({
                'pair': row['PAIR'],
                'side': row['SIDE'],
                'trade_id': row['TRADE_ID'],
                'amount': float(row['AMOUNT']),
                'mean_reversion_ratio': float(row['MEAN_REVERSION_RATIO']),
            })
    return trades

def close_position(trade, base_symbol, quote_symbol, side):
    """Attempt to close a position with retry logic if an error occurs."""
    symbol = base_symbol if side == 'long' else quote_symbol  # Close the correct ticker based on the trade side
    for attempt in range(3):
        try:
            if side == 'long':
                order = exchange.create_market_sell_order(base_symbol, trade['amount'])
                print(f"Closed long position on {base_symbol} for trade ID: {trade['trade_id']} with order: {order['id']}")
            elif side == 'short':
                # Fetch the latest price for the quote symbol, if available
                price = ticker_prices.get(quote_symbol, None)
                if price is not None:
                    order = exchange.create_market_buy_order(quote_symbol, trade['amount'], {'price': price})
                else:
                    order = exchange.create_market_buy_order(quote_symbol, trade['amount'])
                print(f"Closed short position on {quote_symbol} for trade ID: {trade['trade_id']} with order: {order['id']}")
            return
        except Exception as e:
            print(f"Error closing {side} position on {symbol} for trade ID {trade['trade_id']}: {e}")
            time.sleep(1)
    print(f"Failed to close {side} position on {symbol} after multiple attempts.")

def on_message(ws, message):
    message = json.loads(message)
    if 'data' in message:
        for candle_data in message['data']:
            instId = message['arg']['instId']
            close_price = float(candle_data[4])  # Extract the close price from candlestick data
            ticker_prices[instId] = close_price  # Update latest close price for this ticker

            # Calculate the ratio for each pair in pairs_to_monitor
            for trade in pairs_to_monitor:
                base, quote = trade['pair'].split('/')
                if base in ticker_prices and quote in ticker_prices:
                    # Calculate current ratio between base and quote close prices
                    current_ratio = ticker_prices[base] / ticker_prices[quote]
                    # Check if the current ratio meets mean reversion criteria
                    if (trade['side'] == 'long' and current_ratio >= trade['mean_reversion_ratio']) or \
                       (trade['side'] == 'short' and current_ratio <= trade['mean_reversion_ratio']):
                        close_position(trade, base, quote, trade['side'])

def on_open(ws):
    """Subscribes to 1-minute candlestick data for each individual ticker."""
    print("WebSocket connection opened. Subscribing to 1-minute candlestick data.")

    # Gather unique tickers from the pairs to monitor
    ticker_symbols = set()
    for trade in pairs_to_monitor:
        base, quote = trade['pair'].split('/')
        ticker_symbols.update([base, quote])

    # Construct and send the subscription message
    subscribe_message = json.dumps({
        "op": "subscribe",
        "args": [{"instType": "mc", "channel": "candle1m", "instId": symbol} for symbol in ticker_symbols]
    })
    ws.send(subscribe_message)

def on_error(ws, error):
    """Handles any WebSocket errors."""
    print(f"WebSocket error: {error}. Reconnecting in 5 seconds...")
    time.sleep(5)
    start_monitoring()

def on_close(ws, close_status_code, close_msg):
    """Handles WebSocket closure."""
    print("WebSocket connection closed. Reconnecting...")
    start_monitoring()

def start_monitoring():
    """Start monitoring with auto-reload of trades and reconnection logic."""
    global pairs_to_monitor, ws
    pairs_to_monitor = load_active_trades()  # Reload trades on restart

    ws_url = "wss://ws.bitget.com/mix/v1/stream"  # Correct WebSocket URL for Bitget
    ws = websocket.WebSocketApp(ws_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

def listen_for_exit():
    """Listen for the spacebar press to close WebSocket and exit the program."""
    print("Press the spacebar to gracefully exit.")
    keyboard.wait("space")  # Wait for the spacebar press
    print("Spacebar pressed. Closing WebSocket and exiting.")
    if ws:
        ws.close()  # Close WebSocket connection
    exit(0)

if __name__ == "__main__":
    # Start the monitoring in a separate thread
    monitoring_thread = threading.Thread(target=start_monitoring)
    monitoring_thread.start()

    # Start listening for the spacebar to gracefully exit
    listen_for_exit()
