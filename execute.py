import os
import argparse
import csv
import ccxt
from dotenv import load_dotenv
import time
from DataUtils.tickerUtils import get_bitget_usdt_symbols  # Import from tickerUtils

# Load API keys from .env file
load_dotenv()
ACCESS_KEY = os.getenv("BITGET_ACCESS_KEY")
SECRET_KEY = os.getenv("BITGET_SECRET_KEY")
PASSWORD = os.getenv("BITGET_PASSWORD")  # Load the passphrase

# Initialize Bitget API using ccxt
exchange = ccxt.bitget({
    'apiKey': ACCESS_KEY,
    'secret': SECRET_KEY,
    'password': PASSWORD,
    'options': {'defaultType': 'swap'},
    'enableRateLimit': True,
})

ACTIVE_TRADES_FILE = 'active_trades.csv'

def clear_active_trades_file():
    """Initialize or clear the active trades file."""
    with open(ACTIVE_TRADES_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['PAIR', 'SIDE', 'TRADE_ID', 'AMOUNT'])
    print("Active trades CSV file initialized.")

def save_trade_id(pair, side, trade_id, amount):
    """Save trade details to the active trades CSV file."""
    with open(ACTIVE_TRADES_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([pair, side, trade_id, amount])
    print(f"Trade saved: {pair} {side} with ID {trade_id}")

def execute_trade(pair, side, monetary_value_per_ticker, leverage=10, retries=3):
    """Execute a long or short trade on each individual ticker in the pair with leveraged monetary exposure."""
    base, quote = pair.split('/')
    margin_coin = 'USDT'  # Set the margin coin for USDT-margined futures

    # Configure Bitget to handle market buy orders without a price argument
    exchange.options['createMarketBuyOrderRequiresPrice'] = False
    exchange.options['defaultType'] = 'swap'  # Reconfirm swap type for futures

    # Reformat symbols with 'TICKER/USDT:USDT' format
    base_symbol = f"{base.replace('USDT', '')}/USDT:USDT"
    quote_symbol = f"{quote.replace('USDT', '')}/USDT:USDT"

    # Retrieve market data to ensure minimum precision is met
    base_market = exchange.market(base_symbol)
    quote_market = exchange.market(quote_symbol)
    min_base_amount = base_market['limits']['amount']['min']
    min_quote_amount = quote_market['limits']['amount']['min']

    # Calculate the leveraged dollar equivalent for each side of the trade
    leveraged_value = monetary_value_per_ticker * leverage  # Apply leverage

    # Get the latest market prices to calculate the actual trade amounts
    base_price = exchange.fetch_ticker(base_symbol)['last']
    quote_price = exchange.fetch_ticker(quote_symbol)['last']

    # Ensure both sides meet the minimum constraints for trading volume
    base_amount = max(leveraged_value / base_price, min_base_amount)
    quote_amount = max(leveraged_value / quote_price, min_quote_amount)

    # Set leverage for each ticker
    try:
        exchange.set_leverage(leverage, base_symbol, params={'marginCoin': margin_coin})
        exchange.set_leverage(leverage, quote_symbol, params={'marginCoin': margin_coin})
        print(f"Leverage set to {leverage}x for {base_symbol} and {quote_symbol}")
    except Exception as e:
        print(f"Error setting leverage for {base_symbol} and {quote_symbol}: {e}")
        return

    # Place individual trades on `base` and `quote`
    order_type = 'market'
    base_params = {'type': 'swap', 'marginCoin': margin_coin, 'hedged': False, "oneWayMode": True, "marginMode": "isolated"}
    quote_params = {'type': 'swap', 'marginCoin': margin_coin, 'hedged': False, "oneWayMode": True, "marginMode": "isolated"}

    for attempt in range(retries):
        try:
            if side == 'long':
                order_base = exchange.create_order(base_symbol, order_type, 'buy', base_amount, None, base_params)
                order_quote = exchange.create_order(quote_symbol, order_type, 'sell', quote_amount, None, quote_params)
            else:
                order_base = exchange.create_order(base_symbol, order_type, 'sell', base_amount, None, base_params)
                order_quote = exchange.create_order(quote_symbol, order_type, 'buy', quote_amount, None, quote_params)

            trade_id_base = order_base['id']
            trade_id_quote = order_quote['id']
            save_trade_id(base, side, trade_id_base, base_amount)
            save_trade_id(quote, side, trade_id_quote, quote_amount)
            print(f"Executed {side} trade for {base_symbol} with trade ID: {trade_id_base}")
            print(f"Executed {side} trade for {quote_symbol} with trade ID: {trade_id_quote}")
            break

        except Exception as e:
            print(f"Error executing {side} trade for {base_symbol} and {quote_symbol} on attempt {attempt + 1}: {e}")
            time.sleep(1)  # Wait before retrying if there's an error

    if not trade_id_base or not trade_id_quote:
        print(f"Failed to execute trade for {pair} after {retries} attempts.")

def calculate_trade_amount(balance, risk_pct, num_tickers):
    """Calculate the monetary value allocated per ticker based on risk percentage."""
    allocated_balance = balance * (risk_pct / 100)
    monetary_value_per_ticker = allocated_balance / num_tickers
    return monetary_value_per_ticker

def get_account_balance():
    """Fetch total account balance in USDT."""
    balance = exchange.fetch_balance()

    # Access 'available' in the 'info' section
    usdt_available = None
    if 'info' in balance and isinstance(balance['info'], list) and balance['info']:
        usdt_available = float(balance['info'][0].get('available', 0))
    else:
        print("Could not find 'available' balance in balance data.")

    return usdt_available

def parse_trades_file(trades_file):
    """Parse the CSV file to get pairs and trading directions."""
    trades = []
    with open(trades_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            pair = row['PAIR']
            side = row['SIDE'].lower()
            trades.append((pair, side))
    return trades

def main():
    parser = argparse.ArgumentParser(description="Execute trades from a CSV file on Bitget.")
    parser.add_argument('--trades', required=True, help="Path to the CSV file with trades")
    parser.add_argument('--risk-pct', type=float, required=True, help="Percentage of account balance to allocate")
    args = parser.parse_args()

    # Fetch account balance and calculate trade amount per ticker
    account_balance = get_account_balance()
    trades = parse_trades_file(args.trades)
    num_tickers = len(trades) * 2  # Each pair has two tickers (hedged positions)
    monetary_value_per_ticker = calculate_trade_amount(account_balance, args.risk_pct, num_tickers)

    # Initialize or clear the active trades file
    clear_active_trades_file()

    # Execute each trade in the CSV file
    for pair, side in trades:
        try:
            execute_trade(pair, side, monetary_value_per_ticker)
        except Exception as e:
            print(f"Error executing trade for {pair}: {str(e)}")

if __name__ == "__main__":
    main()
