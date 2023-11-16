import requests
import json
from time import sleep

def fetch_balance_sheet_data(symbol):
    url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey=F2A0OP5KPI7BSBL8"
    response = requests.get(url)
    response_data = response.json()
    if "quarterlyReports" in response_data:
        quarterly_reports = response_data["quarterlyReports"]
        return {symbol: quarterly_reports}
    else:
        print(f"Failed to retrieve balance sheet data for {symbol}")
        return None

def main():
    with open('stock_list.json', 'r') as file:
        stocks = json.load(file)
    all_balance_sheet_data = {}

    for stock, info in stocks.items():
        print(f"Fetching balance sheet data for {stock}...")
        balance_sheet_data = fetch_balance_sheet_data(stock)
        if balance_sheet_data is not None:
            all_balance_sheet_data.update(balance_sheet_data)
        sleep(1)  # Sleep for 1 seconds to avoid hitting rate limits

    with open('G:/StockData/balance_sheet_data/balance_sheet.json', 'w') as f:
        json.dump(all_balance_sheet_data, f, indent=4)

if __name__ == "__main__":
    main()
