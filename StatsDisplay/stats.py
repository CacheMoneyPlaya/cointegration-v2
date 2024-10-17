import ccxt
import os
import csv
from dotenv import load_dotenv
from tabulate import tabulate
from tqdm import tqdm  # Import tqdm for progress bar
from datetime import datetime

# Constants
TRADES_DIR = 'StatsDisplay/Trades'
load_dotenv()

# Initialize Binance client
exchange = ccxt.binance()

def get_prices(symbols):
    """Fetch the current prices of given symbols in batch."""
    prices = {}
    for symbol in tqdm(symbols, desc="Fetching Prices", unit="symbol"):
        try:
            ticker = exchange.fetch_ticker(symbol)
            prices[symbol] = ticker['last'] if 'last' in ticker else None
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
            prices[symbol] = None
    return prices

def calculate_trade_performance(pair, side, half_life, mean_reversion_ratio, trade_price_ratio, prices):
    """Calculate performance based on current ratio or mean reversion target if achieved."""
    asset_a, asset_b = pair.split('/')

    asset_a_price = prices.get(asset_a)
    asset_b_price = prices.get(asset_b)

    # Handle cases where price data is not available
    if asset_a_price is None or asset_b_price is None:
        return {
            "pair": pair,
            "side": side,
            "half_life": half_life,
            "trade_price_ratio": trade_price_ratio,
            "current_ratio": "No Data",
            "profit_percent": "No Data",
            "target_ratio_reached": False,
        }

    # Calculate the current ratio
    current_ratio = asset_a_price / asset_b_price

    # Check if the mean reversion ratio has been reached or exceeded
    target_ratio_reached = (side == "long" and current_ratio >= mean_reversion_ratio) or (side == "short" and current_ratio <= mean_reversion_ratio)

    # Calculate profit percentage based on trade price ratio
    profit_percent = ((current_ratio - trade_price_ratio) / trade_price_ratio) * 100 if side == "long" else ((trade_price_ratio - current_ratio) / trade_price_ratio) * 100

    return {
        "pair": pair,
        "side": side,
        "half_life": half_life,
        "trade_price_ratio": trade_price_ratio,
        "current_ratio": current_ratio,
        "profit_percent": profit_percent,
        "target_ratio_reached": target_ratio_reached,
    }

def analyze_trade_results(filename):
    csv_file_path = os.path.join(TRADES_DIR, filename)

    if not os.path.isfile(csv_file_path):
        print(f"Error: File {csv_file_path} not found.")
        return

    with open(csv_file_path, mode='r') as file:
        trades = list(csv.DictReader(file))

    print(f"\nAnalyzing trade results from file: {filename}\n")

    performance_results = []  # List to hold performance results for sorting
    total_profit = 0  # Variable to accumulate total profit/loss percentage

    # Extract unique assets to fetch prices in batch
    unique_assets = set()
    for trade in trades:
        pair = trade['PAIR']
        asset_a, asset_b = pair.split('/')
        unique_assets.add(asset_a)
        unique_assets.add(asset_b)

    # Fetch prices for all unique assets
    prices = get_prices(unique_assets)

    # Calculate trade performance with progress bar
    for trade in tqdm(trades, desc="Calculating Trade Performance", unit="trade"):
        pair = trade['PAIR']
        side = trade['SIDE']
        half_life = float(trade['HALF_LIFE'])
        mean_reversion_ratio = float(trade['MEAN_REVERSION_RATIO'])
        trade_price_ratio = float(trade['TRADE_PRICE_RATIO'])

        performance = calculate_trade_performance(pair, side, half_life, mean_reversion_ratio, trade_price_ratio, prices)

        # Prepare values for output
        current_ratio = performance['current_ratio']
        profit_percent = performance['profit_percent']

        # Prepare the output values
        current_ratio_output = current_ratio if current_ratio == "No Data" else f"{current_ratio:.5f}"
        profit_percent_output = profit_percent if profit_percent == "No Data" else f"{profit_percent:.2f}%"

        # Determine target reached
        target_reached_output = "✓" if performance['target_ratio_reached'] else "✗"

        # Add performance results to the list
        performance_results.append([
            performance['pair'],
            side.upper(),
            half_life,  # Add half-life here
            f"{trade_price_ratio:.5f}",
            current_ratio_output,
            profit_percent_output,
            target_reached_output
        ])

        # Accumulate total profit/loss
        if profit_percent != "No Data":
            total_profit += profit_percent

    # Sort results by profit percentage in descending order
    sorted_results = sorted(performance_results, key=lambda x: float(x[5].replace('%', '')) if isinstance(x[5], str) and '%' in x[5] else float('-inf'), reverse=True)

    # Print the table
    headers = ["TICKER", "SIDE", "HALF-LIFE", "ENTRY RATIO", "CURRENT RATIO", "PERCENTAGE GAIN/LOSS", "TARGET REACHED"]
    print(tabulate(sorted_results, headers=headers, tablefmt="grid"))

    # Print net total % gain/loss
    print(f"\nNet Total Gain/Loss: {total_profit:.2f}%")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Analyze trade results from a CSV file.")
    parser.add_argument("--file", type=str, required=True, help="The CSV file containing trade data.")
    args = parser.parse_args()

    analyze_trade_results(args.file)

if __name__ == "__main__":
    main()
