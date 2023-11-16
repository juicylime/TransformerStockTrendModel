import json
from datetime import datetime

# Clean this cluster fuck up at some point.

# Function to load JSON data
def load_json(filename):
    with open(f"G:/StockData/" + filename, 'r') as file:
        return json.load(file)

# Load all data
earnings_data = load_json('earnings_data/earnings_data.json')
etf_closing_price = load_json('market_data/etf_closing_prices.json')
spdr_closing_prices = load_json('market_data/spdr_closing_prices.json')
income_statement_data = load_json('income_statement_data/income_statement_data.json')
economic_indicators = load_json('economic_indicators/economic_indicators_with_yearly_changes.json')
sentiment_data = load_json('combined_sentiment_scores.json')
stock_data = load_json('stock_data.json')
cash_flow_data = load_json('cash_flow_data/cash_flow.json')
balance_sheet_data = load_json('balance_sheet_data/balance_sheet.json')


# Function to get the next earnings report date and estimated EPS
def get_next_earnings_info(current_date, earnings_info):
    future_earnings = [(datetime.strptime(entry['fiscalDateEnding'], '%Y-%m-%d').date(), entry)
                       for entry in earnings_info if datetime.strptime(entry['fiscalDateEnding'], '%Y-%m-%d').date() > current_date]
    future_earnings.sort(key=lambda x: x[0])
    return future_earnings[0] if future_earnings else (None, None)

# Function to match financial reports based on fiscalDateEnding
def match_financial_reports(earnings, income_statements, cash_flows, balance_sheets):
    earnings_dict = {entry['fiscalDateEnding']: entry for entry in earnings}
    income_dict = {entry['fiscalDateEnding']: entry for entry in income_statements}
    cash_flow_dict = {entry['fiscalDateEnding']: entry for entry in cash_flows}
    balance_sheet_dict = {entry['fiscalDateEnding']: entry for entry in balance_sheets}
    
    combined_reports = {}
    for date, earnings_entry in earnings_dict.items():
        combined_entry = {}
        combined_entry.update(earnings_entry)
        
        if date in income_dict:
            combined_entry.update(income_dict[date])
        if date in cash_flow_dict:
            combined_entry.update(cash_flow_dict[date])
        if date in balance_sheet_dict:
            combined_entry.update(balance_sheet_dict[date])
        
        combined_reports[date] = combined_entry
    return combined_reports

# Index and match financial data by company
indexed_financials = {
    company: match_financial_reports(
        earnings_data.get(company, []),
        income_statement_data.get(company, []),
        cash_flow_data.get(company, []),
        balance_sheet_data.get(company, [])
    )
    for company in stock_data
}

# Sort earnings dates for each company to facilitate finding the next earnings date
sorted_earnings_dates = {
    company: sorted([datetime.strptime(report['fiscalDateEnding'], '%Y-%m-%d').date() for report in reports])
    for company, reports in earnings_data.items()
}

# Function to find the earliest report date for earnings and income statements
def find_earliest_dates(financial_data):
    earliest_date = None
    for report_date in financial_data.keys():
        date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()
        if earliest_date is None or date_obj < earliest_date:
            earliest_date = date_obj
    return earliest_date

# Function to add sentiment score to stock data
def add_sentiment_score(date, company_data, sentiment_data):
    sentiment_info = sentiment_data.get(company, {}).get(date)
    if sentiment_info:
        company_data['average_sentiment_score'] = sentiment_info['average_sentiment_score']
        company_data['article_count'] = sentiment_info['article_count']
    else:
        company_data['average_sentiment_score'] = None
        company_data['article_count'] = None
    return company_data

start_date = datetime.strptime('2018-12-31', '%Y-%m-%d').date()

combined_stock_data = {}
for company, dates in stock_data.items():
    print(f"Processing {company}")

    combined_stock_data[company] = {}
    financial_data = indexed_financials.get(company)
    earnings_info = earnings_data.get(company, [])
    earnings_dates = sorted_earnings_dates.get(company, [])

    earliest_financial_date = find_earliest_dates(financial_data)
    # If the earliest financial date is before the start date, use the start date instead
    earliest_date_to_process = max(earliest_financial_date, start_date) if earliest_financial_date else start_date

    if not earliest_financial_date:
        continue
    
    for date in sorted(dates.keys()):
        date_obj = datetime.strptime(date, '%Y-%m-%d').date()

        # Skip dates before the earliest date to process
        if date_obj < earliest_date_to_process:
            continue

        combined_stock_data[company][date] = dates[date]
        combined_stock_data[company][date] = add_sentiment_score(date, combined_stock_data[company][date], sentiment_data)

        most_recent_financial_data = None
        for report_date, report_data in sorted(financial_data.items(), key=lambda x: datetime.strptime(x[0], '%Y-%m-%d'), reverse=True):
            if datetime.strptime(report_date, '%Y-%m-%d').date() <= date_obj:
                most_recent_financial_data = report_data
                break
        
        if most_recent_financial_data:
            combined_stock_data[company][date].update(most_recent_financial_data)
        
        for etf_symbol, etf_data in etf_closing_price.items():
            etf_price = etf_data.get(date)
            if etf_price:
                combined_stock_data[company][date][f'{etf_symbol}_Closing_Price'] = etf_price
        
        for spdr_symbol, spdr_data in spdr_closing_prices.items():
            spdr_price = spdr_data.get(date)
            if spdr_price:
                combined_stock_data[company][date][f'{spdr_symbol}_Closing_Price'] = spdr_price

        current_date_obj = datetime.strptime(date, '%Y-%m-%d').date()
        next_earnings_date, next_earnings_info = get_next_earnings_info(current_date_obj, earnings_info)
        if next_earnings_date:
            combined_stock_data[company][date]['nextEarningsReportDate'] = next_earnings_date.strftime('%Y-%m-%d')
            estimated_eps = next_earnings_info.get('estimatedEPS') if next_earnings_info else None
            if estimated_eps:
                combined_stock_data[company][date]['nextEstimatedEPS'] = estimated_eps


# for company, dates in combined_stock_data.items():
#     print(f"Processing economic data for {company}")
#     for date, record in dates.items():
#         current_date_obj = datetime.strptime(date, '%Y-%m-%d').date()
        
#         # Merge economic indicators
#         for indicator_name, indicator_records in economic_indicators.items():
#             # Find the most recent indicator value before the current date
#             recent_indicator = next((ind for ind in reversed(indicator_records) 
#                                     if datetime.strptime(ind['release_date'], '%Y-%m-%d').date() <= current_date_obj), None)
#             if recent_indicator:
#                 record.update({
#                     f'{indicator_name}_value': recent_indicator.get('value'),
#                     f'{indicator_name}_percent_change': recent_indicator.get('percent_change'),
#                     f'{indicator_name}_yearly_percent_change': recent_indicator.get('yearly_percent_change')
#                 })
#             # Find the next indicator release date after the current date
#             next_indicator = next((ind for ind in indicator_records
#                                   if datetime.strptime(ind['release_date'], '%Y-%m-%d').date() > current_date_obj), None)
#             if next_indicator:
#                 record[f'{indicator_name}_next_release_date'] = next_indicator['release_date']


for company, dates in combined_stock_data.items():
    print(f"Processing economic data for {company}")
    last_seen_indicators = {}  # Dictionary to keep track of the last seen indicators
    for date, record in dates.items():
        current_date_obj = datetime.strptime(date, '%Y-%m-%d').date()
        
        # Merge economic indicators
        for indicator_name, indicator_records in economic_indicators.items():
            # Find the most recent indicator value before the current date
            recent_indicator = next((ind for ind in reversed(indicator_records) 
                                    if datetime.strptime(ind['release_date'], '%Y-%m-%d').date() <= current_date_obj), None)
            if recent_indicator:
                # If percent_change is None, use the last seen values
                if recent_indicator.get('percent_change') is None and indicator_name in last_seen_indicators:
                    recent_indicator.update(last_seen_indicators[indicator_name])
                else:
                    # Update last seen indicators with current values
                    last_seen_indicators[indicator_name] = {
                        'value': recent_indicator.get('value'),
                        'percent_change': recent_indicator.get('percent_change'),
                        'yearly_percent_change': recent_indicator.get('yearly_percent_change')
                    }
                record.update({
                    f'{indicator_name}_value': recent_indicator.get('value'),
                    f'{indicator_name}_percent_change': recent_indicator.get('percent_change'),
                    f'{indicator_name}_yearly_percent_change': recent_indicator.get('yearly_percent_change')
                })
            # Find the next indicator release date after the current date
            next_indicator = next((ind for ind in indicator_records
                                  if datetime.strptime(ind['release_date'], '%Y-%m-%d').date() > current_date_obj), None)
            if next_indicator:
                record[f'{indicator_name}_next_release_date'] = next_indicator['release_date']


with open('G:/StockData/combined_stock_data.json', 'w') as file:
    json.dump(combined_stock_data, file, indent=4)
