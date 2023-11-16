import yfinance as yf
import json
import pandas_ta as ta
from datetime import datetime, timedelta
import pytz
from fetch_closing_prices import fetch_closing_prices

def fetch_stock_data(ticker, start_date, end_date):
    stock = yf.Ticker(ticker)
    
    # Convert start_date and end_date to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Create a buffer by subtracting 150 days from the start date
    buffer_date = start_date - timedelta(days=370)
    
    # Convert buffer_date back to string format
    buffer_date_str = buffer_date.strftime('%Y-%m-%d')
    
    data = stock.history(start=buffer_date_str, end=end_date.strftime('%Y-%m-%d'))
    
    # Calculating technical indicators using pandas_ta
    data.ta.ema(length=10, append=True)  # Short-term EMA
    data.ta.ema(length=30, append=True)
    data.ta.macd(append=True)  # MACD
    data.ta.rsi(append=True)  # RSI
    data.ta.bbands(append=True)  # Bollinger Bands
    data.ta.sma(length=20, column='Volume', append=True)
    data.rename(columns={'SMA_20': 'avgTradingVolume'}, inplace=True)

    # Adding Stochastic Oscillator
    data.ta.stoch(high='High', low='Low', close='Close', k=14, d=3, append=True)

    # Adding Parabolic SAR
    data.ta.psar(high='High', low='Low', close='Close', append=True)

    # Combine the two PSAR columns into one, filling NaN values from one column with values from the other
    # Downward trends (PSARs) are multiplied by -1 to make them negative
    data['PSAR_combined'] = data['PSARl_0.02_0.2'].fillna(-data['PSARs_0.02_0.2'])

    # Drop the individual PSARl and PSARs columns
    data.drop(columns=['PSARl_0.02_0.2', 'PSARs_0.02_0.2', 'PSARaf_0.02_0.2', 'PSARr_0.02_0.2'], inplace=True)

    # Calculate 52-week high and low
    data['52_week_high'] = data['Close'].rolling(window=252).max()
    data['52_week_low'] = data['Close'].rolling(window=252).min()

    # Drop 'Dividends' and 'Stock_Splits' columns
    data.drop(columns=['Dividends', 'Stock Splits'], inplace=True)

    return data

def fetch_market_indices(start_date, end_date):
    nasdaq = yf.Ticker('^IXIC')
    sp500 = yf.Ticker('^GSPC')

    # Convert start_date and end_date to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Create a buffer by subtracting 100 days from the start date
    buffer_date = start_date - timedelta(days=100)
    
    # Convert buffer_date back to string format
    buffer_date_str = buffer_date.strftime('%Y-%m-%d')

    nasdaq_data = nasdaq.history(start=buffer_date_str, end=end_date)[['Close']]
    sp500_data = sp500.history(start=buffer_date_str, end=end_date)[['Close']]
    
    # Calculate SMA and LMA for market indices
    nasdaq_data.ta.ema(length=10, append=True)
    nasdaq_data.ta.ema(length=30, append=True)
    sp500_data.ta.ema(length=10, append=True)
    sp500_data.ta.ema(length=30, append=True)
    
    nasdaq_data.rename(columns={'Close': 'NASDAQ_Close', 'EMA_10': 'NASDAQ_EMA_10', 'EMA_30': 'NASDAQ_EMA_30'}, inplace=True)
    sp500_data.rename(columns={'Close': 'SP500_Close', 'EMA_10': 'SP500_EMA_10', 'EMA_30': 'SP500_EMA_30'}, inplace=True)
    
    return nasdaq_data, sp500_data

def main():
    # Load the list of stocks from stock_list.json
    with open('stock_list.json', 'r') as file:
        stock_list = json.load(file)

    # Extract the stock tickers from the dictionary
    stocks = list(stock_list.keys())

    # Prompt user for input
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
    
    all_data = {}
    nasdaq_data, sp500_data = fetch_market_indices(start_date, end_date)
    
    for stock in stocks:
        stock = stock.strip()
        print(f"Fetching data for {stock}...")
        stock_data = fetch_stock_data(stock, start_date, end_date)
        
        # Merge market indices data with stock data
        stock_data = stock_data.merge(nasdaq_data, left_index=True, right_index=True)
        stock_data = stock_data.merge(sp500_data, left_index=True, right_index=True)
        
        # Convert start_date to a timezone-aware datetime object
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)

        # Trim data to original date range
        stock_data_trimmed = stock_data[start_date_obj:]
        
        # Check for any NaN values in the trimmed stock_data DataFrame
        if stock_data_trimmed.isna().any().any():
            print(f"Skipping {stock} due to NaN values.")
            continue
        
        # Trim data to original date range
        stock_data = stock_data[start_date_obj:]

        # Convert Timestamp objects to strings
        stock_data.index = stock_data.index.strftime('%Y-%m-%d')

        all_data[stock] = stock_data.to_dict(orient='index')
    
    # Saving data to JSON
    with open('G:/StockData/stock_data.json', 'w') as f:
        json.dump(all_data, f, indent=4)
    
    print("Data saved to stock_data.json")

    # List of SPDR sector fund tickers
    spdr_tickers = ["XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLRE", "XLK", "XLU", "XLC"]
    fetch_closing_prices(spdr_tickers, start_date, end_date, "G:/StockData/market_data/spdr_closing_prices.json")

    # List of ETF tickers to track major markets
    etf_tickers = ["MCHI", "VGK", "EWJ", "VWO", "VXUS", "VT"]
    fetch_closing_prices(etf_tickers, start_date, end_date, "G:/StockData/market_data/etf_closing_prices.json")

if __name__ == "__main__":
    main()
