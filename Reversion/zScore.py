# zScore.py

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from statsmodels.regression.linear_model import OLS
from statsmodels.tools.tools import add_constant
from scipy.fft import fft, fftfreq

# Use the non-interactive Agg backend
matplotlib.use('Agg')

TICKERS_DIR = 'Binance/Tickers'
CHARTS_DIR = 'StatsDisplay/SignalCharts'

# Thresholds for determining cyclical behavior
AUTO_CORRELATION_THRESHOLD = 0.3
FREQUENCY_THRESHOLD = 0.3
LAG_INTERVAL = 24  # For example, checking for a daily cycle in hourly data

def load_data(file_path):
    """Load CSV data into a pandas DataFrame."""
    df = pd.read_csv(file_path)
    return df['Close'] if 'Close' in df.columns else None

def align_data(df1, df2):
    """Align two DataFrames based on their timestamps."""
    aligned_data = pd.concat([df1, df2], axis=1, join='inner')
    return aligned_data.iloc[:, 0], aligned_data.iloc[:, 1]

def calculate_zscore(series):
    """Calculate the Z-score for a series."""
    return (series - series.mean()) / series.std()

def calculate_half_life(spread):
    """Calculate the half-life of mean reversion for the spread."""
    spread = spread.replace([np.inf, -np.inf], np.nan).dropna()
    lagged_spread = np.roll(spread, 1)
    lagged_spread[0] = 0
    returns = spread - lagged_spread
    lagged_spread = add_constant(lagged_spread)
    model = OLS(returns, lagged_spread)
    result = model.fit()

    slope = result.params.iloc[1]
    if np.isclose(slope, 0, atol=1e-8):
        return None
    half_life = -np.log(2) / slope
    return round(half_life, 2) if half_life > 0 else None

def clear_charts_directory():
    """Delete all files in the charts directory."""
    if os.path.exists(CHARTS_DIR):
        for file in os.listdir(CHARTS_DIR):
            file_path = os.path.join(CHARTS_DIR, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        os.makedirs(CHARTS_DIR)  # Create the directory if it doesn't exist

def chart_zscore(pair_name, z_scores):
    """Generate a Z-score chart for pairs meeting all criteria."""
    plt.style.use('dark_background')
    plt.figure(figsize=(10, 5))

    # Plot Z-score with horizontal lines for thresholds
    plt.plot(z_scores, label='Z-score')
    plt.axhline(2, color='red', linestyle='--', label='Z = 2')
    plt.axhline(-2, color='green', linestyle='--', label='Z = -2')
    plt.axhline(0, color='white', linestyle='-', label='Z = 0')

    plt.title(f'Z-Score for {pair_name}', fontsize=14)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Z-Score', fontsize=12)
    plt.legend()

    # Save the chart
    plt.savefig(os.path.join(CHARTS_DIR, f"{pair_name}.png"))
    plt.close()

def check_periodic_autocorrelation(z_scores, threshold=AUTO_CORRELATION_THRESHOLD, lag_interval=LAG_INTERVAL):
    """Check if a Z-score series has periodic autocorrelation peaks."""
    autocorr_values = [pd.Series(z_scores).autocorr(lag=i) for i in range(1, len(z_scores) // 2)]
    periodic_peaks = [i for i, ac in enumerate(autocorr_values) if abs(ac) > threshold]
    return any(np.diff(periodic_peaks) == lag_interval)

def check_dominant_frequency(z_scores, frequency_threshold=FREQUENCY_THRESHOLD):
    """Check if a Z-score series has a dominant frequency indicating cyclical behavior."""
    N = len(z_scores)
    yf = fft(z_scores)
    xf = fftfreq(N, d=1)[:N // 2]
    amplitudes = 2.0 / N * np.abs(yf[:N // 2])

    dominant_amplitude = max(amplitudes)
    return dominant_amplitude > frequency_threshold

def run_zscore_analysis(passing_pairs):
    """Run Z-score and half-life analysis on pairs meeting p-value criteria."""
    # Clear previous charts at the start of each run
    clear_charts_directory()

    results = []
    for pair in passing_pairs:
        file_a = os.path.join(TICKERS_DIR, f"{pair['Ax']}.csv")
        file_b = os.path.join(TICKERS_DIR, f"{pair['Bx']}.csv")
        data_a = load_data(file_a)
        data_b = load_data(file_b)

        aligned_data_a, aligned_data_b = align_data(data_a, data_b)

        # Calculate the log of the price ratio as the spread
        spread = np.log(aligned_data_a / aligned_data_b)

        # Calculate Z-score for the spread
        z_scores = calculate_zscore(spread)
        if abs(z_scores.iloc[-1]) < 2:
            continue

        # Calculate half-life of mean reversion using the spread
        half_life = calculate_half_life(spread)
        if half_life is None or half_life > 24:
            continue

        # Check for dominant frequency and periodic autocorrelation
        has_dominant_frequency = (
            1 if check_periodic_autocorrelation(z_scores) and check_dominant_frequency(z_scores) else 0
        )

        # Calculate the mean-reversion ratio for BTC/ETH
        spread_mean = spread.mean()
        mean_reversion_ratio = round(np.exp(spread_mean), 5)

        results.append({
            "Ax": pair["Ax"],
            "Bx": pair["Bx"],
            "p_value": pair["p_value"],
            "Z_score": round(z_scores.iloc[-1], 2),
            "half_life": half_life,
            "mean_reversion_ratio": mean_reversion_ratio,
            "has_dominant_frequency": has_dominant_frequency
        })

        # Generate chart for pairs that pass all checks
        pair_name = f"{pair['Ax']}_{pair['Bx']}"
        chart_zscore(pair_name, z_scores)

    return results
