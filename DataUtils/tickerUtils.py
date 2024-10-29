import os
import re
import ccxt
from dotenv import load_dotenv
from binance.client import Client

# Load API keys from .env file
load_dotenv()
ACCESS_KEY = os.getenv("BITGET_ACCESS_KEY")
SECRET_KEY = os.getenv("BITGET_SECRET_KEY")
PASSWORD = os.getenv("BITGET_PASSWORD")

# Initialize Bitget API using ccxt
exchange = ccxt.bitget({
    'apiKey': ACCESS_KEY,
    'secret': SECRET_KEY,
    'password': PASSWORD,
    'enableRateLimit': True,
})


def contains_invalid_words(symbol):
    """Filter out symbols with specific keywords like UP, DOWN, BULL, BEAR, or USDC."""
    invalid_words = ["UP", "DOWN", "BULL", "BEAR", "USDC"]
    return any(word in symbol for word in invalid_words)

def format_symbol(symbol):
    """Ensure the symbol is formatted as TICKERUSDT, without numbers and only one 'USDT' suffix."""
    formatted = symbol.replace(":USDT", "").replace("/USDT", "")
    if not formatted.endswith("USDT"):
        formatted += "USDT"

    # Exclude symbols with numbers
    if re.search(r'\d', formatted):
        return None

    return formatted

def get_binance_usdt_symbols():
    """Fetch USDT-margined perpetual symbols from Binance in TICKERUSDT format."""
    client = Client()
    info = client.futures_exchange_info()
    symbols = [
        format_symbol(symbol['symbol'])
        for symbol in info['symbols']
        if symbol['quoteAsset'] == 'USDT' and not contains_invalid_words(symbol['symbol'])
    ]
    return list(filter(None, symbols))  # Exclude None values

def get_bitget_usdt_symbols():
    """Fetch USDT-margined futures symbols from Bitget API in TICKERUSDT format."""
    # Load all markets data
    markets = exchange.load_markets()

    # Uncomment these lines to inspect the markets structure
    # print("Loaded markets data:", markets)

    symbols = []

    # Filter for USDT futures symbols
    for market in markets.values():
        # Check if market has 'USDT' as quote and is a futures market
        if market.get('quote') == 'USDT' and (market.get('future') or market.get('swap')) and not contains_invalid_words(market['symbol']):
            formatted_symbol = format_symbol(market['symbol'])
            if formatted_symbol:
                symbols.append(formatted_symbol)

    # Uncomment this line to inspect which symbols are being appended
    # print("Filtered USDT-margined futures symbols:", symbols)

    return symbols  # Filtered list of symbols

def get_usdt_symbols():
    """Get the combined USDT-margined tickers from both Binance and Bitget, excluding those with USDC."""
    bitget_symbols = set(get_bitget_usdt_symbols())

    # Filter out symbols that contain "USDC"
    final_symbols = [symbol for symbol in bitget_symbols if "USDC" not in symbol]
    return final_symbols

# Example usage
if __name__ == "__main__":
    all_symbols = get_usdt_symbols()
    print("All unique, formatted USDT-margined futures tickers across Binance and Bitget:", all_symbols)
