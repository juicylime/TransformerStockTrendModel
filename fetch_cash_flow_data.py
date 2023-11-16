import requests
import json
from time import sleep

# Gotta combine all these scripts except for earnings report eventually.

def fetch_cash_flow_data(symbol):
    url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey=F2A0OP5KPI7BSBL8"
    response = requests.get(url)
    response_data = response.json()
    if "quarterlyReports" in response_data:
        quarterly_reports = response_data["quarterlyReports"]
        return {symbol: quarterly_reports}
    else:
        print(f"Failed to retrieve cash flow data for {symbol}")
        return None

def main():
    with open('stock_list.json', 'r') as file:
        stocks = json.load(file)
    all_cash_flow_data = {}

    for stock, info in stocks.items():
        print(f"Fetching cash flow data for {stock}...")
        cash_flow_data = fetch_cash_flow_data(stock)
        if cash_flow_data is not None:
            all_cash_flow_data.update(cash_flow_data)
        sleep(1)  # Sleep for 1 seconds to avoid hitting rate limits

    with open('G:/StockData/cash_flow_data/cash_flow.json', 'w') as f:
        json.dump(all_cash_flow_data, f, indent=4)

if __name__ == "__main__":
    main()
