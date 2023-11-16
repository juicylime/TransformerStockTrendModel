import requests
import json
from time import sleep

def fetch_earnings_data(symbol):
    url = f"https://www.alphavantage.co/query?function=EARNINGS&symbol={symbol}&apikey=F2A0OP5KPI7BSBL8"
    response = requests.get(url)
    response_data = response.json()
    if "quarterlyEarnings" in response_data:
        quarterly_earnings = response_data["quarterlyEarnings"]
        return {symbol: quarterly_earnings}
    else:
        print(f"Failed to retrieve earnings data for {symbol}")
        return None

def main():
    with open('stock_list.json', 'r') as file:
        stocks = json.load(file)
    all_earnings_data = {}

    for stock, info in stocks.items():
        print(f"Fetching earnings data for {stock}...")
        earnings_data = fetch_earnings_data(stock)
        if earnings_data is not None:
            all_earnings_data.update(earnings_data)
        sleep(5)  # Sleep for 5 seconds to avoid hitting rate limits

    with open('earnings_data_1.json', 'w') as f:
        json.dump(all_earnings_data, f, indent=4)

if __name__ == "__main__":
    main()
