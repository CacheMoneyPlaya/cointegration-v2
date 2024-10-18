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
    'LAYER1_PROTOCOLS': 'purple',
    'LAYER2_SCALING': 'green',
    'PRIVACY_COINS': 'pink',
    'STABLECOINS': 'yellow',
    'INFRASTRUCTURE_ORACLES': 'white',
    'NFT_COLLECTIBLES': 'brown'
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

    # Step 4: Display and record results
    print("\nGenerated pair metrics:")
    for result in zscore_results:
        asset_a = result['Ax']
        asset_b = result['Bx']
        side = "long" if result["Z_score"] < 0 else "short"

        # Fetch the latest prices from CSV
        current_price_a = get_latest_price_from_csv(asset_a)
        current_price_b = get_latest_price_from_csv(asset_b)

        # Calculate the current price ratio
        if current_price_a is not None and current_price_b is not None:
            current_price_ratio = round(current_price_a / current_price_b, 5)
        else:
            print(f"Warning: Skipping pair {asset_a}/{asset_b} due to missing data.")
            continue  # Skip this pair if prices are not available

        # Add the trade entry data to list for CSV
        trades.append({
            "PAIR": f"{asset_a}/{asset_b}",
            "SIDE": side,
            "HALF_LIFE": result['half_life'],
            "MEAN_REVERSION_RATIO": result['mean_reversion_ratio'],
            "TRADE_PRICE_RATIO": current_price_ratio
        })

        # Color output based on basket
        basket_a = find_basket(asset_a)
        basket_b = find_basket(asset_b)
        if basket_a and basket_a == basket_b:
            color = COLORS.get(basket_a, None)
            output = colored(
                f"{asset_a} / {asset_b} - p: {result['p_value']:.4f} Z: {result['Z_score']} "
                f"Half-life: {result['half_life']}H Mean Reversion Ratio: {result['mean_reversion_ratio']} "
                f"TRADE_PRICE_RATIO: {current_price_ratio} ✅ ({side.upper()})",
                color
            ) if color else (
                f"{asset_a} / {asset_b} - p: {result['p_value']:.4f} Z: {result['Z_score']} "
                f"Half-life: {result['half_life']}H Mean Reversion Ratio: {result['mean_reversion_ratio']} "
                f"TRADE_PRICE_RATIO: {current_price_ratio} ✅ ({side.upper()})"
            )
        else:
            output = (
                f"{asset_a} / {asset_b} - p: {result['p_value']:.4f} Z: {result['Z_score']} "
                f"Half-life: {result['half_life']}H Mean Reversion Ratio: {result['mean_reversion_ratio']} "
                f"TRADE_PRICE_RATIO: {current_price_ratio} ✅ ({side.upper()})"
            )
        print(output)

    # Step 5: Write trades to CSV
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["PAIR", "SIDE", "HALF_LIFE", "MEAN_REVERSION_RATIO", "TRADE_PRICE_RATIO"])
        writer.writeheader()
        writer.writerows(trades)

    print(f"\nTrade signals saved to {csv_file_path}")
