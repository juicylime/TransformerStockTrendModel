import json
import os
import pandas as pd
from sklearn.preprocessing import StandardScaler

def standardize_column(df, column_name):
    # Z-score standardization
    values = df[column_name].values.reshape(-1, 1)
    scaler = StandardScaler()
    df[column_name] = scaler.fit_transform(values).flatten()
    return df[column_name]

def standardize_stock_data(input_path, output_path):
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

    # Select numeric columns for standardization
    numeric_cols = df.select_dtypes(include=[float, int]).columns.tolist()

    # Standardize each numeric column individually
    for col in numeric_cols:
        df[col] = standardize_column(df, col)

    df[none_mask] = None

    # Reconstruct the nested dictionary format for JSON
    standardized_data = {}
    for _, row in df.iterrows():
        ticker = row['Ticker']
        date = row['Date']
        standardized_data.setdefault(ticker, {})[date] = {key: (None if pd.isnull(val) else val) for key, val in row.drop(['Ticker', 'Date']).to_dict().items()}

    # Save the standardized data to a json file
    with open(output_path, 'w') as file:
        json.dump(standardized_data, file, indent=4)

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
        # normalize_stock_data(input_path, output_path)
        standardize_stock_data(input_path, output_path)

        
