import os
from statsmodels.tsa.stattools import coint
import pandas as pd
from tqdm import tqdm  # For progress bar

TICKERS_DIR = 'Binance/Tickers'

def load_data(file_path):
    """Load CSV data into a pandas DataFrame and ensure it has at least 1000 rows."""
    df = pd.read_csv(file_path)
    # Only return data if it has at least 1000 rows
    return df['Close'] if 'Close' in df.columns and len(df) >= 1000 else None

def align_data(df1, df2):
    """Align two DataFrames based on their timestamps."""
    aligned_data = pd.concat([df1, df2], axis=1, join='inner')
    return aligned_data.iloc[:, 0], aligned_data.iloc[:, 1]

def run_cointegration_analysis():
    """Run cointegration test and return pairs meeting the p-value criteria."""
    tickers = [f.replace('.csv', '') for f in os.listdir(TICKERS_DIR) if f.endswith('.csv')]
    pairs = [{"Ax": tickers[i], "Bx": tickers[j]} for i in range(len(tickers)) for j in range(i + 1, len(tickers))]

    passing_pairs = []
    for pair in tqdm(pairs, desc="Calculating Cointegration"):
        file_a = os.path.join(TICKERS_DIR, f"{pair['Ax']}.csv")
        file_b = os.path.join(TICKERS_DIR, f"{pair['Bx']}.csv")
        data_a = load_data(file_a)
        data_b = load_data(file_b)

        # Skip pair if either ticker has insufficient data
        if data_a is None or data_b is None:
            continue

        aligned_data_a, aligned_data_b = align_data(data_a, data_b)

        _, p_value, _ = coint(aligned_data_a, aligned_data_b)
        if p_value < 0.04:
            passing_pairs.append({
                "Ax": pair["Ax"],
                "Bx": pair["Bx"],
                "p_value": round(p_value, 4)
            })
    return passing_pairs
