import yfinance as yf
import json
from datetime import datetime

def fetch_closing_prices(tickers, start_date, end_date):
    data = {}
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        fund = yf.Ticker(ticker)
        hist = fund.history(start=start_date, end=end_date)
        # Convert Timestamp objects to strings
        closing_prices = {date.strftime('%Y-%m-%d'): price for date, price in hist['Close'].items()}
        data[ticker] = closing_prices
    return data

def main():
    # List of ETF tickers to track major markets
    etf_tickers = ["MCHI", "VGK", "EWJ", "VWO", "VXUS", "VT"]
    # Define the date range
    start_date = "2021-01-01"
    end_date = "2023-10-15"
    # Fetch the closing prices
    closing_prices_data = fetch_closing_prices(etf_tickers, start_date, end_date)
    # Output the results to a JSON file
    with open("etf_closing_prices.json", "w") as file:
        json.dump(closing_prices_data, file, indent=4)

if __name__ == "__main__":
    main()
