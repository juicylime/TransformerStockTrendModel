import json
from datetime import datetime

# Function to load JSON data
def load_json(filename):
    with open(f"G:/StockData/" + filename, 'r') as file:
        return json.load(file)

# Load all data
earnings_data = load_json('earnings_data/earnings_data.json')
etf_closing_price = load_json('market_data/etf_closing_prices.json')
spdr_closing_prices = load_json('market_data/spdr_closing_prices.json')
income_statement_data = load_json('income_statement_data/income_statement_data.json')
stock_data = load_json('stock_data.json')

# Function to get the next earnings report date
def get_next_earnings_date(current_date, earnings_dates):
    future_dates = [date for date in earnings_dates if date > current_date]
    return min(future_dates, default=None) if future_dates else None

# Function to match earnings and income statements based on fiscalDateEnding
def match_financial_reports(earnings, income_statements):
    earnings_dict = {entry['fiscalDateEnding']: entry for entry in earnings}
    income_dict = {entry['fiscalDateEnding']: entry for entry in income_statements}
    combined_reports = {}
    for date, earnings_entry in earnings_dict.items():
        income_entry = income_dict.get(date)
        if income_entry:
            combined_reports[date] = {**earnings_entry, **income_entry}
    return combined_reports

# Index and match financial data by company
indexed_financials = {
    company: match_financial_reports(earnings_data.get(company, []), income_statement_data.get(company, []))
    for company in stock_data
}

# Sort earnings dates for each company to facilitate finding the next earnings date
sorted_earnings_dates = {
    company: sorted([datetime.strptime(report['fiscalDateEnding'], '%Y-%m-%d').date() for report in reports])
    for company, reports in earnings_data.items()
}

combined_stock_data = {}
for company, dates in stock_data.items():
    combined_stock_data[company] = {}
    financial_data = indexed_financials.get(company)
    earnings_dates = sorted_earnings_dates.get(company, [])
    
    for date in sorted(dates.keys()):
        # Ensure we start with the stock data for the date
        combined_stock_data[company][date] = dates[date]

        # Add the most recent earnings and income data before the stock date
        date_obj = datetime.strptime(date, '%Y-%m-%d').date()
        most_recent_financial_data = None
        for report_date, report_data in sorted(financial_data.items(), key=lambda x: datetime.strptime(x[0], '%Y-%m-%d'), reverse=True):
            if datetime.strptime(report_date, '%Y-%m-%d').date() <= date_obj:
                most_recent_financial_data = report_data
                break
        
        if most_recent_financial_data:
            combined_stock_data[company][date].update(most_recent_financial_data)
        
        # Add ETF and SPDR closing prices for the day
        for etf_symbol, etf_data in etf_closing_price.items():
            etf_price = etf_data.get(date)
            if etf_price:
                combined_stock_data[company][date][f'{etf_symbol}_Closing_Price'] = etf_price
        
        for spdr_symbol, spdr_data in spdr_closing_prices.items():
            spdr_price = spdr_data.get(date)
            if spdr_price:
                combined_stock_data[company][date][f'{spdr_symbol}_Closing_Price'] = spdr_price

        # Determine the next earnings report date and add it to the day's data
        current_date_obj = datetime.strptime(date, '%Y-%m-%d').date()
        next_earnings_date = get_next_earnings_date(current_date_obj, earnings_dates)
        if next_earnings_date:
            combined_stock_data[company][date]['Next_Earnings_Report_Date'] = next_earnings_date.strftime('%Y-%m-%d')

# Output the combined and sorted data
with open('combined_stock_data.json', 'w') as file:
    json.dump(combined_stock_data, file, indent=4)
