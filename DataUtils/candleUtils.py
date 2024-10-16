import os
import csv
import ccxt
import multiprocessing

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

def fetch_and_save_candle_data(symbol, timeframe, limit):
    """Fetch and save OHLCV data for a given symbol."""
    exchange = ccxt.binance()
    symbol_with_usdt = f"{symbol.replace('USDT', '')}/USDT"
    filename = os.path.join(DATA_DIR, f"{symbol}.csv")  # Save as {symbol}.csv, e.g., BTCUSDT.csv

    try:
        exchange.load_markets()
        ohlcv = exchange.fetch_ohlcv(symbol_with_usdt, timeframe, limit=limit)

        if ohlcv:
            with open(filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
                writer.writerows(ohlcv)
            print(f"Completed download for {symbol_with_usdt} - saved to {filename}")
        else:
            print(f"No data returned for {symbol_with_usdt}, skipping.")

    except Exception as e:
        print(f"Error fetching data for {symbol_with_usdt}: {e}")

def fetch_all_time_series_data(symbols, timeframe, limit):
    """Fetch and save time series data for each symbol using up to 8 concurrent processes."""
    with multiprocessing.Pool(processes=8) as pool:  # Set to 8 concurrent processes
        pool.starmap(fetch_and_save_candle_data, [(symbol, timeframe, limit) for symbol in symbols])

def save_symbols_and_fetch_data(symbols, timeframe, limit):
    """Combined function to clear CSVs, save symbols, and fetch their time series data."""
    # Step 1: Clear all existing CSV files in the data directory
    clear_existing_csv_files(DATA_DIR)

    # Step 2: Always regenerate the active tickers CSV file
    save_symbols_to_csv(symbols)

    # Step 3: Fetch and save time series data for each symbol
    fetch_all_time_series_data(symbols, timeframe, limit)
