import os
import csv
import ccxt
import multiprocessing
from tqdm import tqdm  # Make sure to import tqdm at the top of your file
import time  # For sleep in case of retries

DATA_DIR = 'Binance/Tickers'
TICKERS_FILE = 'Binance/binanceActiveTickers.csv'  # Save in the Binance directory

def clear_existing_csv_files(directory):
    """Delete all existing CSV files in the specified directory."""
    if os.path.exists(directory):
        for file in os.listdir(directory):
            if file.endswith('.csv'):
                os.remove(os.path.join(directory, file))
        print("Cleared existing CSV files.")

def save_symbols_to_csv(symbols):
    """Save list of ticker symbols to CSV in the Binance directory."""
    # Ensure the Binance directory exists
    os.makedirs(os.path.dirname(TICKERS_FILE), exist_ok=True)

    with open(TICKERS_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["symbol"])
        for symbol in symbols:
            writer.writerow([symbol])
    print("Active tickers CSV regenerated in the Binance directory.")

def fetch_candle_data(symbol, timeframe, limit, since=None, retries=3):
    """Fetch OHLCV data for a given symbol and return it."""
    exchange = ccxt.binance()
    symbol_with_usdt = f"{symbol.replace('USDT', '')}/USDT"

    for attempt in range(retries):
        try:
            exchange.load_markets()
            # Fetching with the since parameter if provided
            ohlcv = exchange.fetch_ohlcv(symbol_with_usdt, timeframe, limit=limit, since=since)

            if ohlcv:
                return ohlcv  # Return the fetched OHLCV data
            else:
                return None

        except Exception as e:
            print(f"Error fetching data for {symbol_with_usdt}: {e}")
            time.sleep(1)  # Wait before retrying

    print(f"Failed to fetch data for {symbol_with_usdt} after {retries} attempts.")
    return None

def save_candle_data(symbol, ohlcv):
    """Save OHLCV data to a CSV file for the given symbol."""
    filename = os.path.join(DATA_DIR, f"{symbol}.csv")  # Save as {symbol}.csv, e.g., BTCUSDT.csv

    if ohlcv:  # Only save if there is data
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
            writer.writerows(ohlcv)

def fetch_and_save_candle_data(symbol, timeframe, limit, since=None):
    """Fetch and save OHLCV data for a given symbol."""
    ohlcv = fetch_candle_data(symbol, timeframe, limit, since)  # Fetch the data
    if ohlcv:  # Only save if there is data
        save_candle_data(symbol, ohlcv)  # Save the fetched data
        print(f"{symbol} ✅")  # Print confirmation of download

def fetch_all_candle_data(symbols, timeframe, limit, since=None, save=False):
    """Fetch and optionally save time series data"""
    with multiprocessing.Pool(processes=8) as pool:  # Set to 8 concurrent processes
        if save:
            # If saving is requested, call the fetch_and_save_candle_data function
            pool.starmap(fetch_and_save_candle_data, [(symbol, timeframe, limit, since) for symbol in symbols])
        else:
            # If not saving, return the fetched data
            results = pool.starmap(fetch_candle_data, [(symbol, timeframe, limit, since) for symbol in symbols])
            return results  # Return the list of results
