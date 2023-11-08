import yfinance as yf
import json
from datetime import datetime

def fetch_closing_prices(tickers, start_date, end_date, output):
    data = {}
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        # Convert Timestamp objects to strings
        closing_prices = {date.strftime('%Y-%m-%d'): price for date, price in hist['Close'].items()}
        data[ticker] = closing_prices
    
    # Output the results to a JSON file
    with open(output, "w") as file:
        json.dump(data, file, indent=4)