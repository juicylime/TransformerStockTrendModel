import yfinance as yf
import json
from datetime import datetime

def fetch_closing_prices(tickers, start_date, end_date):
    data = {}
    
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        # Convert Timestamp objects to strings
        closing_prices = {date.strftime('%Y-%m-%d'): price for date, price in hist['Close'].items()}
        data[ticker] = closing_prices
    
    return data

def main():
    # List of SPDR sector fund tickers
    spdr_tickers = ["XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLRE", "XLK", "XLU", "XLC"]
    
    # Define the date range
    start_date = "2021-01-30"
    end_date = "2023-10-15"
    
    # Fetch the closing prices
    closing_prices_data = fetch_closing_prices(spdr_tickers, start_date, end_date)
    
    # Output the results to a JSON file
    with open("spdr_closing_prices.json", "w") as file:
        json.dump(closing_prices_data, file, indent=4)

if __name__ == "__main__":
    main()
