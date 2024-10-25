import sys
import os
import pandas as pd

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import matplotlib
matplotlib.use('Agg')  # Use the Agg backend for writing to files
import matplotlib.pyplot as plt

import ccxt
import numpy as np
import os
import csv
from dotenv import load_dotenv
from tabulate import tabulate
from tqdm import tqdm  # Import tqdm for progress bar
from datetime import datetime
from DataUtils.candleUtils import fetch_all_candle_data, fetch_candle_data  # Correct import

# Constants
TRADES_DIR = 'StatsDisplay/Trades'
CHARTS_DIR = 'StatsDisplay/Charts'  # Directory to save charts
load_dotenv()

# Initialize Binance client
exchange = ccxt.binance()

def get_prices(symbols):
    """Fetch the current prices of given symbols in batch."""
    prices = {}
    for symbol in tqdm(symbols, desc="Fetching historical data since trade", unit="symbol"):
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

    # Extract the timestamp from the filename (assumed format: YYYY-MM-DD-HH-MM-SS.csv)
    timestamp_str = filename.split('.')[0]  # Extract "YYYY-MM-DD-HH-MM-SS"
    trade_time = datetime.strptime(timestamp_str, "%Y-%m-%d-%H-%M-%S")
    current_time = datetime.now()
    time_since_trade = current_time - trade_time
    hours_since_trade = time_since_trade.total_seconds() / 3600  # Convert to hours

    with open(csv_file_path, mode='r') as file:
        trades = list(csv.DictReader(file))

    print(f"\nAnalyzing trade results from file: {filename}\n")
    print(f"Time since trades taken: {hours_since_trade:.2f} hours\n")

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

    print(f"\nGenerating performance matrix...\n")

    # Calculate trade performance without progress bar
    for trade in trades:  # Removed tqdm here
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
            mean_reversion_ratio,
            target_reached_output
        ])

        # Accumulate total profit/loss
        if profit_percent != "No Data":
            total_profit += profit_percent

    # Sort results by profit percentage in descending order
    sorted_results = sorted(performance_results, key=lambda x: float(x[5].replace('%', '')) if isinstance(x[5], str) and '%' in x[5] else float('-inf'), reverse=True)

    print(f"\nGenerating performance chart...\n")

    # Generate performance chart
    # generate_profit_chart(trades, timestamp_str)

    # Store results for later printing
    return sorted_results, total_profit

def generate_profit_chart(trades, timestamp_str):
    """Generate a profit chart that shows net % profit across all trades."""
    # Parse the timestamp from the filename
    trade_time = datetime.strptime(timestamp_str, "%Y-%m-%d-%H-%M-%S")
    start_time = int(trade_time.timestamp() * 1000)  # Convert to milliseconds

    # Prepare a list to hold DataFrames for each trade
    profit_dfs = []

    # Extract unique pairs and process trades
    for trade in trades:
        pair = trade['PAIR'].strip()  # Remove any extra spaces
        side = trade['SIDE'].strip()   # Remove any extra spaces
        asset_a, asset_b = pair.split('/')

        # Debugging output
        print(f"Fetching historical data for: {asset_a}, {asset_b}, Side: {side}")

        # Fetch historical candle data for both assets from the start time
        historical_a = fetch_candle_data(asset_a, '5m', limit=None, since=start_time)
        historical_b = fetch_candle_data(asset_b, '5m', limit=None, since=start_time)

        # Check if data was fetched correctly
        if historical_a is None or historical_b is None:
            print(f"Warning: No historical data for {asset_a} or {asset_b}. Skipping...")
            continue

        # Convert to DataFrames
        df_a = pd.DataFrame(historical_a, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_b = pd.DataFrame(historical_b, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # Convert timestamps to datetime
        df_a['timestamp'] = pd.to_datetime(df_a['timestamp'], unit='ms')
        df_b['timestamp'] = pd.to_datetime(df_b['timestamp'], unit='ms')

        # Merge the two DataFrames on timestamp
        merged_df = pd.merge_asof(df_a.sort_values('timestamp'), df_b.sort_values('timestamp'), on='timestamp', suffixes=('_a', '_b'))

        # Calculate the profit ratio based on the trade side
        if side.upper() == 'LONG':
            merged_df['profit_ratio'] = (merged_df['close_a'] / merged_df['close_b'])
        elif side.upper() == 'SHORT':
            merged_df['profit_ratio'] = (merged_df['close_b'] / merged_df['close_a'])

        # Calculate percentage change from the entry point
        merged_df['percentage_change'] = (merged_df['profit_ratio'] - merged_df['profit_ratio'].iloc[0]) / merged_df['profit_ratio'].iloc[0] * 100

        # Handle NaN values by filling them with 0 for calculations
        merged_df['percentage_change'].fillna(0, inplace=True)

        # Add the DataFrame to the profit_dfs list
        profit_dfs.append(merged_df[['timestamp', 'percentage_change']])

    # Combine all DataFrames into one
    if not profit_dfs:
        print("No valid profit DataFrames to combine.")
        return

    overall_profit_df = profit_dfs[0]
    for df in profit_dfs[1:]:
        overall_profit_df = pd.merge_asof(overall_profit_df.sort_values('timestamp'), df.sort_values('timestamp'), on='timestamp', suffixes=('', '_new'))

        # Ensure the new percentage_change column exists
        if 'percentage_change_new' in overall_profit_df.columns:
            overall_profit_df['percentage_change'] += overall_profit_df['percentage_change_new'].fillna(0)

    # Plotting the overall cumulative profit as percentage
    plt.style.use('dark_background')
    plt.figure(figsize=(10, 5))

    plt.plot(overall_profit_df['timestamp'], overall_profit_df['percentage_change'], color='cyan', label='Total Cumulative Profit')  # Plot total cumulative profit percentage

    plt.title(f'Total Cumulative Profit Over Time since {timestamp_str}', fontsize=14)
    plt.xlabel('5-Minute Intervals', fontsize=12)
    plt.ylabel('Cumulative Profit (%)', fontsize=12)
    plt.axhline(0, color='white', linestyle='-', label='Profit = 0')
    plt.legend()
    plt.grid()

    # Save the chart in the Trades directory
    trades_directory = os.path.join('StatsDisplay', 'Trades')
    png_file_name = f"{timestamp_str}.png"  # Use the same name as the CSV file
    os.makedirs(trades_directory, exist_ok=True)  # Ensure the Trades directory exists
    plt.savefig(os.path.join(trades_directory, png_file_name))
    plt.close()  # Close the figure to avoid display

    print(f"Chart saved as {os.path.join(trades_directory, png_file_name)}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Analyze trade results from a CSV file.")
    parser.add_argument("--file", type=str, required=True, help="The CSV file containing trade data.")
    args = parser.parse_args()

    # Call analyze_trade_results and capture results and total profit
    sorted_results, total_profit = analyze_trade_results(args.file)

    # Print the sorted results in a table format
    headers = ["TICKER", "SIDE", "HALF-LIFE", "ENTRY RATIO", "CURRENT RATIO", "PERCENTAGE GAIN/LOSS", "MEAN REVERSION RATIO", "TARGET REACHED"]
    print(tabulate(sorted_results, headers=headers, tablefmt="grid"))

    # Print net total % gain/loss
    print(f"\nNet Total Gain/Loss: {total_profit:.2f}%")

if __name__ == "__main__":
    main()
