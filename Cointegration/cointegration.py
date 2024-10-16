import os
import pandas as pd
from itertools import combinations
from statsmodels.tsa.stattools import coint
from tqdm import tqdm
import multiprocessing

TICKERS_DIR = 'Binance/Tickers'

def get_all_csv_files(directory):
    """Retrieve all .csv files in the specified directory."""
    return [f for f in os.listdir(directory) if f.endswith('.csv')]

def create_pair_combinations(files):
    """Create unique pairs of files with proper symbol formatting."""
    return [{"Ax": pair[0], "Bx": pair[1]} for pair in combinations(files, 2)]

def load_data(file_path):
    """Load the CSV data into a pandas DataFrame."""
    df = pd.read_csv(file_path)
    return df[['Close', 'Time']] if 'Close' in df.columns and 'Time' in df.columns else None

def align_data(data_a, data_b):
    """Align two time series data frames on their timestamps."""
    merged = pd.merge(data_a, data_b, on="Time", suffixes=('_a', '_b'))
    return merged['Close_a'], merged['Close_b']  # Return the aligned Close prices

def calculate_pair_cointegration(pair):
    """Calculate the cointegration p-value for a given pair."""
    file_a = os.path.join(TICKERS_DIR, pair["Ax"])
    file_b = os.path.join(TICKERS_DIR, pair["Bx"])

    # Load data with timestamps
    data_a = load_data(file_a)
    data_b = load_data(file_b)

    # Skip pairs with incomplete data
    if data_a is None or data_b is None:
        return None

    # Align data on timestamps to ensure same length
    aligned_data_a, aligned_data_b = align_data(data_a, data_b)

    # Skip if aligned data has insufficient length
    if len(aligned_data_a) < 2:
        return None

    # Perform Engle-Granger cointegration test
    score, p_value, _ = coint(aligned_data_a, aligned_data_b)

    # Only return pairs with p-value < 0.05
    if p_value < 0.05:
        return {
            "Ax": pair["Ax"],
            "Bx": pair["Bx"],
            "p_value": p_value
        }
    return None

def calculate_cointegration(pairs):
    """Calculate Engle-Granger cointegration p-values for each pair in parallel."""
    with multiprocessing.Pool(processes=8) as pool:  # Set to 8 concurrent processes
        results = list(tqdm(pool.imap(calculate_pair_cointegration, pairs), total=len(pairs), desc="Calculating Cointegration"))
    return [result for result in results if result is not None]

def run_cointegration_analysis():
    """Run the full cointegration analysis and return significant pairs."""
    files = get_all_csv_files(TICKERS_DIR)
    pairs = create_pair_combinations(files)
    return calculate_cointegration(pairs)
