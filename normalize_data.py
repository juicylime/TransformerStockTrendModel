import json
import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

def normalize_stock_data(input_path, output_path):
    # Load the data from the json file
    with open(input_path, 'r') as file:
        data = json.load(file)

    # Flatten the nested dictionary and convert it into a DataFrame
    records = []
    for ticker, dates in data.items():
        for date, features in dates.items():
            features['Date'] = date
            features['Ticker'] = ticker
            records.append(features)
    
    df = pd.DataFrame(records)
    none_mask = df.isnull()

    # Select numeric columns for normalization
    numeric_cols = df.select_dtypes(include=[float, int]).columns.tolist()

    # Initialize the MinMaxScaler with a range from -1 to 1
    scaler = MinMaxScaler(feature_range=(-1, 1))

    # Normalize all numeric columns
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols].astype(float))

    # Replace None values back after scaling
    df[none_mask] = None

    # Reconstruct the nested dictionary format for JSON
    normalized_data = {}
    for _, row in df.iterrows():
        ticker = row['Ticker']
        date = row['Date']
        normalized_data.setdefault(ticker, {})[date] = {key: (None if pd.isnull(val) else val) for key, val in row.drop(['Ticker', 'Date']).to_dict().items()}

    # Write the normalized data back to a json file
    with open(output_path, 'w') as file:
        json.dump(normalized_data, file, indent=4)

# Define the directories
input_directory = 'G:/StockData/master_datasets/'
output_directory = 'G:/StockData/normalized_master_datasets/'

# Create the output directory if it does not exist
os.makedirs(output_directory, exist_ok=True)

# Process each file in the input directory
for file_name in os.listdir(input_directory):
    if file_name.endswith('.json'):
        input_path = os.path.join(input_directory, file_name)
        output_file_name = f"{os.path.splitext(file_name)[0]}_normalized.json"
        output_path = os.path.join(output_directory, output_file_name)
        normalize_stock_data(input_path, output_path)
