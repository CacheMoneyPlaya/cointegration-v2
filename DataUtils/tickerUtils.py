from binance.client import Client

def contains_invalid_words(symbol):
    # Define any rules for invalid symbols
    invalid_words = ["UP", "DOWN", "BULL", "BEAR"]
    return any(word in symbol for word in invalid_words)

def get_usdt_symbols():
    client = Client()
    info = client.futures_exchange_info()
    symbols = [symbol['symbol'] for symbol in info['symbols'] if symbol['quoteAsset'] == 'USDT' and not contains_invalid_words(symbol['symbol'])]
    return symbols
