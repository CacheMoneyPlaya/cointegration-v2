import os
import csv
import ccxt
import multiprocessing
from tqdm import tqdm  # Ensure tqdm is imported
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
    os.makedirs(os.path.dirname(TICKERS_FILE), exist_ok=True)
    with open(TICKERS_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["symbol"])
        for symbol in symbols:
            writer.writerow([symbol])
    print("Active tickers CSV regenerated in the Binance directory.")

def fetch_candle_data(symbol, timeframe, limit, since=None, retries=3):
    """Fetch OHLCV data for a given symbol in the futures market and return it."""
    exchange = ccxt.binance()
    symbol_with_usdt = f"{symbol.replace('USDT', '')}/USDT"

    for attempt in range(retries):
        try:
            exchange.load_markets()
            ohlcv = exchange.fetch_ohlcv(symbol_with_usdt, timeframe, limit=limit, since=since)

            if ohlcv:
                # Check for variability in the 'Close' price column
                close_prices = [row[4] for row in ohlcv]  # Close price is the 5th element in each row
                if len(set(close_prices)) > 1:  # Proceed if there is more than one unique close price
                    return ohlcv
                else:
                    print(f"Skipping {symbol_with_usdt} due to lack of variability in close prices.")
                    return None

        except Exception as e:
            error_message = str(e)
            if "binance does not have market symbol" in error_message:
                print(f"Error fetching data for {symbol_with_usdt}: {error_message}. Skipping further retries.")
                return None
            else:
                print(f"Error fetching data for {symbol_with_usdt} on attempt {attempt + 1}: {error_message}")
                time.sleep(1)

    print(f"Failed to fetch data for {symbol_with_usdt} after {retries} attempts.")
    return None

def save_candle_data(symbol, ohlcv):
    """Save OHLCV data to a CSV file for the given symbol."""
    filename = os.path.join(DATA_DIR, f"{symbol}.csv")
    if ohlcv:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
            writer.writerows(ohlcv)

def fetch_and_save_candle_data(symbol, timeframe, limit, since=None):
    """Fetch and save OHLCV data for a given symbol."""
    ohlcv = fetch_candle_data(symbol, timeframe, limit, since)
    if ohlcv:
        save_candle_data(symbol, ohlcv)
        print(f"{symbol} ✅\n")

def fetch_all_candle_data(symbols, timeframe, limit, since=None, save=False):
    """Fetch and optionally save time series data."""
    with multiprocessing.Pool(processes=8) as pool:
        if save:
            pool.starmap(fetch_and_save_candle_data, [(symbol, timeframe, limit, since) for symbol in symbols])
        else:
            results = pool.starmap(fetch_candle_data, [(symbol, timeframe, limit, since) for symbol in symbols])
            return results
