import requests
import json
from time import sleep

def fetch_income_statement_data(symbol):
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey=F2A0OP5KPI7BSBL8"
    response = requests.get(url)
    response_data = response.json()
    if "quarterlyReports" in response_data:
        quarterly_reports = response_data["quarterlyReports"]
        return {symbol: quarterly_reports}
    else:
        print(f"Failed to retrieve income statement data for {symbol}")
        return None

def main():
    with open('stock_list.json', 'r') as file:
        stocks = json.load(file)
    all_income_statement_data = {}

    for stock, info in stocks.items():
        print(f"Fetching income statement data for {stock}...")
        income_statement_data = fetch_income_statement_data(stock)
        if income_statement_data is not None:
            all_income_statement_data.update(income_statement_data)
        sleep(5)  # Sleep for 5 seconds to avoid hitting rate limits

    with open('income_statement_data.json', 'w') as f:
        json.dump(all_income_statement_data, f, indent=4)

if __name__ == "__main__":
    main()
