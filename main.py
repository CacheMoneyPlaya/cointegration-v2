import time
import schedule
import argparse
import os
from DataUtils.tickerUtils import get_usdt_symbols
from DataUtils.candleUtils import save_symbols_to_csv, fetch_all_candle_data, clear_existing_csv_files
from StatsDisplay.postStatProcess import process_and_display_stats
from Reversion.zScore import clear_charts_directory

TICKERS_DATA_DIR = 'Binance/Tickers'

def fetch_and_process_data(reuse=False, limit=None):
    symbols = get_usdt_symbols()

    if limit is not None:
        symbols = symbols[:limit]

    save_symbols_to_csv(symbols)

    if not reuse:
        clear_existing_csv_files(TICKERS_DATA_DIR)
        # Fetching hourly data now
        fetch_all_candle_data(symbols, '1h', 1000, save=True)

    # After fetching and saving data, check for CSV files again
    if any(f.endswith('.csv') for f in os.listdir(TICKERS_DATA_DIR)):
        process_and_display_stats()
    else:
        print("No CSV files found in TICKERS_DATA_DIR; skipping cointegration and z-score analysis.")

def run_hourly_job(reuse=False, limit=None):
    schedule.every().hour.at(":00").do(fetch_and_process_data, reuse=reuse, limit=limit)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Binance USDT symbols, hourly candle data, and perform cointegration and z-score analysis.")
    parser.add_argument("--test", action="store_true", help="Run the fetch and process once immediately and exit.")
    parser.add_argument("--reuse", action="store_true", help="Skip data fetching but update available tickers.")
    parser.add_argument("--limit", type=int, help="Limit the number of tickers to download data for.")
    args = parser.parse_args()

    clear_charts_directory()

    if args.test:
        # Run immediately and exit if --test flag is provided
        fetch_and_process_data(reuse=args.reuse, limit=args.limit)
    else:
        # Run hourly job scheduling
        run_hourly_job(reuse=args.reuse, limit=args.limit)
