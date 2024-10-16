import os
import pandas as pd
import numpy as np
from itertools import combinations
from statsmodels.tsa.stattools import coint
from statsmodels.api import OLS, add_constant
from tqdm import tqdm
import matplotlib
import matplotlib.pyplot as plt
import multiprocessing

# Set non-interactive backend for matplotlib
matplotlib.use('Agg')

TICKERS_DIR = 'Binance/Tickers'
SIGNAL_CHARTS_DIR = 'StatsDisplay/SignalCharts'

def clear_signal_charts():
    """Clear out the SignalCharts directory at the start of each run."""
    if os.path.exists(SIGNAL_CHARTS_DIR):
        for file in os.listdir(SIGNAL_CHARTS_DIR):
            os.remove(os.path.join(SIGNAL_CHARTS_DIR, file))
    else:
        os.makedirs(SIGNAL_CHARTS_DIR)

def load_data(file_path):
    """Load the CSV data into a pandas DataFrame."""
    df = pd.read_csv(file_path)
    return df[['Close', 'Time']] if 'Close' in df.columns and 'Time' in df.columns else None

def align_data(data_a, data_b):
    """Align two time series data frames on their timestamps."""
    merged = pd.merge(data_a, data_b, on="Time", suffixes=('_a', '_b'))
    return merged['Close_a'], merged['Close_b']

def calculate_zscore(series):
    """Calculate the z-score for a series."""
    return (series - series.mean()) / series.std()

def calculate_half_life(spread):
    """Calculate the half-life of mean reversion for the spread series."""
    spread = spread.replace([np.inf, -np.inf], np.nan).dropna()

    # Prepare lagged spread for regression
    lagged_spread = np.roll(spread, 1)
    lagged_spread[0] = 0
    returns = spread - lagged_spread
    lagged_spread = add_constant(lagged_spread)

    model = OLS(returns, lagged_spread)
    result = model.fit()

    # Check if the slope parameter is zero or very close to zero
    slope = result.params.iloc[1]
    if slope == 0 or np.isclose(slope, 0, atol=1e-8):
        return None  # Return None for non-mean-reverting pairs

    half_life = -np.log(2) / slope
    return round(half_life, 2) if half_life > 0 else None

def chart_zscore(a, b, z_scores):
    """Generate and save a Z-score chart with a black background."""
    plt.style.use('dark_background')
    plt.figure(figsize=(10, 5))

    # Plot Z-score lines
    plt.axhline(y=2, color='red', linestyle='--', label="Z=2")
    plt.axhline(y=-2, color='green', linestyle='--', label="Z=-2")
    plt.axhline(y=0, color='white', linestyle='-', label="Z=0")
    plt.plot(z_scores.index, z_scores, color='cyan')

    # Chart title and labels
    plt.title(f'{a} - {b}', fontsize=14)
    plt.xlabel('Time (Hours)', fontsize=14)
    plt.ylabel('Z-SCORE', fontsize=14)

    # Save the chart in the SignalCharts directory
    chart_path = os.path.join(SIGNAL_CHARTS_DIR, f"{a}_{b}.png")
    plt.savefig(chart_path)
    plt.close()

def analyze_pair(pair):
    """Analyze a single pair to calculate p-value, Z-score, half-life, and mean-reversion target price and ratio."""
    # Load data for both assets in the pair
    file_a = os.path.join(TICKERS_DIR, f"{pair['Ax']}.csv")
    file_b = os.path.join(TICKERS_DIR, f"{pair['Bx']}.csv")
    data_a = load_data(file_a)
    data_b = load_data(file_b)

    # Ensure we have data for both
    if data_a is None or data_b is None:
        return None

    # Align data on timestamps to ensure same length
    aligned_data_a, aligned_data_b = align_data(data_a, data_b)

    # Skip if aligned data has insufficient length
    if len(aligned_data_a) < 2:
        return None

    # Perform Engle-Granger cointegration test to get p-value
    score, p_value, _ = coint(aligned_data_a, aligned_data_b)
    if p_value >= 0.05:
        return None  # Skip if p-value doesn't meet criteria

    # Calculate the log of the price ratio as the spread
    spread = np.log(aligned_data_a / aligned_data_b)

    # Calculate Z-score for the spread
    z_scores = calculate_zscore(spread)
    if abs(z_scores.iloc[-1]) < 2:
        return None  # Skip if Z-score doesn't meet criteria

    # Calculate half-life of mean reversion using the spread
    half_life = calculate_half_life(spread)
    if half_life is None or half_life > 24:
        return None  # Skip if half-life doesn't meet criteria

    # Calculate the mean reversion target price and ratio for BTC/ETH
    spread_mean = spread.mean()
    last_price_b = aligned_data_b.iloc[-1]
    mean_reversion_target_price = np.exp(spread_mean) * last_price_b  # Target price for BTC if ETH stays constant
    mean_reversion_ratio = np.exp(spread_mean)  # Target ratio for BTC/ETH to reach Z=0

    # Generate Z-score chart
    chart_zscore(pair["Ax"], pair["Bx"], z_scores)

    # Return result with mean-reversion target price and ratio
    return {
        "Ax": pair["Ax"],
        "Bx": pair["Bx"],
        "p_value": round(p_value, 4),
        "Z_score": round(z_scores.iloc[-1], 2),
        "half_life": round(half_life, 2),
        "mean_reversion_target_price": round(mean_reversion_target_price, 2),
        "mean_reversion_ratio": round(mean_reversion_ratio, 2)
    }

def analyze_pairs(pairs):
    """Analyze pairs to calculate p-values, Z-scores, and half-lives in parallel."""
    clear_signal_charts()  # Clear the SignalCharts directory at the start

    with multiprocessing.Pool(processes=8) as pool:
        results = list(tqdm(pool.imap(analyze_pair, pairs), total=len(pairs), desc="Calculating Z-Scores and Half-Lives"))

    # Filter out None results and return only valid results
    return [result for result in results if result is not None]

def run_zscore_analysis():
    """Run the z-score analysis on pairs with p < 0.05."""
    # Create a list of pairs based on CSV files in the TICKERS_DIR
    files = [f.replace('.csv', '') for f in os.listdir(TICKERS_DIR) if f.endswith('.csv')]
    pairs = [{"Ax": pair[0], "Bx": pair[1]} for pair in combinations(files, 2)]

    # Return the results after analysis
    return analyze_pairs(pairs)
