import os
import csv
import pandas as pd
from datetime import datetime
from Cointegration.cointegration import run_cointegration_analysis
from Reversion.zScore import run_zscore_analysis
from dotenv import load_dotenv
from termcolor import colored

# Load environment variables from baskets.env
load_dotenv('baskets.env')

# Define basket categories and their associated color codes
BASKETS = {
    'CHINESE_BASED': os.getenv('CHINESE_BASED').split(','),
    'DEFI': os.getenv('DEFI').split(','),
    'GAMING_METAVERSE': os.getenv('GAMING_METAVERSE').split(','),  # Keep it but ensure the color is valid
    'LAYER1_PROTOCOLS': os.getenv('LAYER1_PROTOCOLS').split(','),
    'LAYER2_SCALING': os.getenv('LAYER2_SCALING').split(','),
    'PRIVACY_COINS': os.getenv('PRIVACY_COINS').split(','),
    'STABLECOINS': os.getenv('STABLECOINS').split(','),
    'INFRASTRUCTURE_ORACLES': os.getenv('INFRASTRUCTURE_ORACLES').split(','),
    'NFT_COLLECTIBLES': os.getenv('NFT_COLLECTIBLES').split(',')
}

COLORS = {
    'CHINESE_BASED': 'red',
    'DEFI': 'blue',
    'GAMING_METAVERSE': 'yellow',  # Changed from orange to yellow
    'LAYER1_PROTOCOLS': 'magenta',  # Changed from purple to magenta
    'LAYER2_SCALING': 'green',
    'PRIVACY_COINS': 'cyan',  # Changed from pink to cyan as pink isn't available
    'STABLECOINS': 'yellow',
    'INFRASTRUCTURE_ORACLES': 'white',
    'NFT_COLLECTIBLES': 'grey'  # Changed from brown to grey
}

TRADES_DIR = 'StatsDisplay/Trades'
TICKERS_DIR = 'Binance/Tickers'
os.makedirs(TRADES_DIR, exist_ok=True)

def get_latest_price_from_csv(ticker):
    """Fetch the latest close price from the CSV file for a given ticker."""
    file_path = os.path.join(TICKERS_DIR, f"{ticker}.csv")

    if not os.path.isfile(file_path):
        print(f"Warning: File for {ticker} not found.")
        return None

    # Read the last row from the CSV file
    df = pd.read_csv(file_path)
    if 'Close' in df.columns and not df.empty:
        return df['Close'].iloc[-1]
    else:
        print(f"Warning: 'Close' column missing or data empty in {ticker}.csv")
        return None

def find_basket(pair):
    """Identify which basket a given pair belongs to."""
    for basket_name, pairs in BASKETS.items():
        if pair in pairs:
            return basket_name
    return None

def process_and_display_stats():
    """Run cointegration and z-score analyses, then display filtered results and save trades to CSV."""
    # Step 1: Run cointegration analysis and get pairs with p < 0.05
    print("Running cointegration analysis...")
    passing_pairs = run_cointegration_analysis()

    # Step 2: Run z-score and half-life analysis on pairs passing cointegration
    print("\nRunning z-score analysis and related z-score metrics...")
    zscore_results = [result for result in run_zscore_analysis(passing_pairs) if result is not None]

    # Step 3: Prepare data for CSV output
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    csv_file_path = os.path.join(TRADES_DIR, f"{timestamp}.csv")
    trades = []

    # Step 4: Collect initial trade entries
    for result in zscore_results:
        asset_a = result['Ax']
        asset_b = result['Bx']
        side = "long" if result["Z_score"] < 0 else "short"
        half_life = result['half_life']

        # Fetch prices and calculate trade price ratio
        current_price_a = get_latest_price_from_csv(asset_a)
        current_price_b = get_latest_price_from_csv(asset_b)
        if current_price_a is not None and current_price_b is not None:
            current_price_ratio = round(current_price_a / current_price_b, 5)
        else:
            print(f"Warning: Skipping pair {asset_a}/{asset_b} due to missing data.")
            continue

        # Add trade entry to list
        trades.append({
            "PAIR": f"{asset_a}/{asset_b}",
            "ASSET_A": asset_a,
            "ASSET_B": asset_b,
            "SIDE": side,
            "HALF_LIFE": half_life,
            "MEAN_REVERSION_RATIO": result['mean_reversion_ratio'],
            "TRADE_PRICE_RATIO": current_price_ratio
        })

    # Step 5: Resolve conflicting positions for each asset
    asset_sides = {}  # Track the chosen side and lowest total half-life for each asset
    filtered_trades = []

    for trade in trades:
        asset_a, asset_b = trade["ASSET_A"], trade["ASSET_B"]
        side = trade["SIDE"]
        half_life = trade["HALF_LIFE"]

        # Check if the asset already has a recorded side
        if asset_a in asset_sides:
            # Skip conflicting trade if current side doesn't match recorded side
            if asset_sides[asset_a]["side"] != side:
                continue
        else:
            asset_sides[asset_a] = {"side": side, "total_half_life": half_life}

        if asset_b in asset_sides:
            if asset_sides[asset_b]["side"] != side:
                continue
        else:
            asset_sides[asset_b] = {"side": side, "total_half_life": half_life}

        # If side matches for asset, add to the filtered list
        filtered_trades.append(trade)

    # Step 6: Display and save filtered trades
    output_trades = []
    for trade in filtered_trades:
        output_trades.append({
            "PAIR": trade["PAIR"],
            "SIDE": trade["SIDE"],
            "HALF_LIFE": trade["HALF_LIFE"],
            "MEAN_REVERSION_RATIO": trade["MEAN_REVERSION_RATIO"],
            "TRADE_PRICE_RATIO": trade["TRADE_PRICE_RATIO"]
        })

        print(f"{trade['PAIR']} - {trade['SIDE'].upper()} - Half-life: {trade['HALF_LIFE']}, Mean Reversion Ratio: {trade['MEAN_REVERSION_RATIO']}, Trade Price Ratio: {trade['TRADE_PRICE_RATIO']}")

    # Step 7: Save final trade signals to CSV
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["PAIR", "SIDE", "HALF_LIFE", "MEAN_REVERSION_RATIO", "TRADE_PRICE_RATIO"])
        writer.writeheader()
        writer.writerows(output_trades)

    print(f"\nTrade signals saved to {csv_file_path}")
