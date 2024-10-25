import os
import argparse
import csv
import ccxt
from dotenv import load_dotenv
import time

# Load API keys from .env file
load_dotenv()
ACCESS_KEY = os.getenv("BITGET_ACCESS_KEY")
SECRET_KEY = os.getenv("BITGET_SECRET_KEY")

# Initialize Bitget API using ccxt
exchange = ccxt.bitget({
    'apiKey': ACCESS_KEY,
    'secret': SECRET_KEY,
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

def execute_trade(pair, side, amount_per_ticker, retries=3):
    """Execute a long or short trade on each ticker separately in the pair."""
    base, quote = pair.split('/')
    leverage = 10

    # Set leverage on both tickers
    exchange.set_leverage(leverage, base)
    exchange.set_leverage(leverage, quote)
    trade_id_base = None
    trade_id_quote = None

    # Retry logic for executing trades
    for attempt in range(retries):
        try:
            if side == 'long':
                # Long base and short quote for hedging
                order_base = exchange.create_market_buy_order(base, amount_per_ticker)
                order_quote = exchange.create_market_sell_order(quote, amount_per_ticker)
            else:
                # Short base and long quote for hedging
                order_base = exchange.create_market_sell_order(base, amount_per_ticker)
                order_quote = exchange.create_market_buy_order(quote, amount_per_ticker)

            trade_id_base = order_base['id']
            trade_id_quote = order_quote['id']
            print(f"Executed {side} trade for {base} with trade ID: {trade_id_base}")
            print(f"Executed {side} trade for {quote} with trade ID: {trade_id_quote}")
            break

        except Exception as e:
            print(f"Error executing {side} trade for {base}/{quote} on attempt {attempt + 1}: {e}")
            time.sleep(1)  # Wait before retrying

    # Save trade details if successful
    if trade_id_base and trade_id_quote:
        save_trade_id(base, side, trade_id_base, amount_per_ticker)
        save_trade_id(quote, side, trade_id_quote, amount_per_ticker)
    else:
        print(f"Failed to execute trade for {pair} after {retries} attempts.")

def calculate_trade_amount(balance, risk_pct, num_tickers):
    """Calculate the amount allocated per ticker based on risk percentage."""
    allocated_balance = balance * (risk_pct / 100)
    amount_per_ticker = allocated_balance / num_tickers
    return amount_per_ticker

def get_account_balance():
    """Fetch total account balance in USDT."""
    balance = exchange.fetch_balance()
    usdt_balance = balance['total']['USDT']
    return usdt_balance

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
    amount_per_ticker = calculate_trade_amount(account_balance, args.risk_pct, num_tickers)

    # Initialize or clear the active trades file
    clear_active_trades_file()

    # Execute each trade in the CSV file
    for pair, side in trades:
        try:
            execute_trade(pair, side, amount_per_ticker)
        except Exception as e:
            print(f"Error executing trade for {pair}: {str(e)}")

if __name__ == "__main__":
    main()
