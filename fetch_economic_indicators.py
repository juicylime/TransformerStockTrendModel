import requests
import json
import os
from operator import itemgetter
from datetime import datetime, timedelta


def get_economic_data(api_key, series_id, start_date):
    base_url = 'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'api_key': api_key,
        'series_id': series_id,
        'file_type': 'json',
        'observation_start': start_date,
        'realtime_start': start_date
    }
    response = requests.get(base_url, params=params)
    return response.json()

api_key = '6dbcb67e7161b35809a7e0e4e0d7e08c'  
start_date = '2014-01-01'

series_ids = [
    'GDP', 'CPIAUCSL', 'UNRATE', 'FEDFUNDS', 'DGS10', 'CONSUMER', 'INDPRO', 'RSAFS', 'USEPUINDXD'
]

# Function to calculate percentage change
def calculate_percentage_change(prev_value, current_value):
    try:
        prev_value = float(prev_value)
        current_value = float(current_value)
        return ((current_value - prev_value) / prev_value) * 100
    except (ValueError, TypeError):
        return None

economic_indicators = {}

for series_id in series_ids:
    series_data = get_economic_data(api_key, series_id, start_date)
    if 'observations' in series_data:
        sorted_observations = sorted(series_data['observations'], key=itemgetter('realtime_start'))
        processed_data = {}
        previous_latest_value = None
        previous_latest_release_date = None
        for obs in sorted_observations:
            date = obs['date']
            value = obs['value']
            release_date = obs['realtime_start']
            
            if date not in processed_data:
                percent_change = calculate_percentage_change(previous_latest_value, value) if previous_latest_value is not None else None
                processed_data[date] = {
                    'date': date,
                    'value': value,
                    'percent_change': percent_change,
                    'release_date': release_date,
                    'previous_value': previous_latest_value,
                    'previous_release_date': previous_latest_release_date
                }
            previous_latest_value = value
            previous_latest_release_date = release_date

        economic_indicators[series_id] = list(processed_data.values())

        
os.makedirs('G:/StockData/economic_indicators', exist_ok=True)

# Save the data to a JSON file
with open('G:/StockData/economic_indicators/economic_indicators.json', 'w') as f:
    json.dump(economic_indicators, f, indent=4)


# Function to add yearly percentage changes to each report
def add_yearly_changes(data):
    for series, records in data.items():
        # Initialize an empty list to store the percentage changes and their corresponding dates
        change_records = []

        # Iterate over the records in each series
        for record in records:
            # Attempt to convert the 'value' to a float, if conversion fails, skip the record
            try:
                value = float(record['value'])
            except ValueError:
                record['yearly_percent_change'] = None
                continue

            # Parse the 'release_date' of the current record
            release_date = datetime.strptime(record['release_date'], "%Y-%m-%d")

            # Append the current percent change and release date to the list
            if record['percent_change'] is not None:
                change_records.append({
                    'percent_change': record['percent_change'],
                    'release_date': release_date
                })

            # Calculate the sum of percent changes over the last year
            yearly_change_sum = sum(
                change_record['percent_change'] for change_record in change_records
                if (release_date - change_record['release_date']).days <= 365
            )

            # Update the 'yearly_percent_change' in the record
            record['yearly_percent_change'] = yearly_change_sum if change_records else None

            # Remove changes that are older than a year from the list
            change_records = [
                change_record for change_record in change_records
                if (release_date - change_record['release_date']).days <= 365
            ]

    return data

# Apply the function to the economic data
updated_economic_data = add_yearly_changes(economic_indicators)


# Save the data to a JSON file
with open('G:/StockData/economic_indicators/economic_indicators_with_yearly_changes.json', 'w') as f:
    json.dump(updated_economic_data, f, indent=4)