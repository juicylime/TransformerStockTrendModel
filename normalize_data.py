import json
import os
import pandas as pd
from sklearn.preprocessing import StandardScaler

def denormalize(name, value, normalization_params):
    mean = normalization_params[name]['mean']
    std = normalization_params[name]['std']
    denormalized_value = (value * std) + mean
    return denormalized_value

def standardize_columns(df, column_names, load_stats=None):
    if load_stats is None:
        values = df[column_names].values
        scaler = StandardScaler()
        scaled_values = scaler.fit_transform(values)
        df[column_names] = scaled_values

        # Creating a dictionary to return
        stats = {}
        for i, col in enumerate(column_names):
            stats[col] = {'mean': scaler.mean_[i], 'std': scaler.scale_[i]}
    else:
        stats = load_stats
        for col in column_names:
            if col in stats.keys():
                mean = stats[col]['mean']
                std = stats[col]['std']
                df[col] = (df[col] - mean) / std

    return stats


def standardize_stock_data(data, stats=None):
    columns_to_standardize_together = ['Open', 'High', 'Low', 'Close', 'SMA_30', 'EMA_10', 'BBL_5_2.0', 'BBM_5_2.0',
                                       'BBU_5_2.0', 'ISA_9', 'ISB_26', 'ITS_9', 'IKS_26', 'PSAR_combined', '52_week_high', '52_week_low']



    # Flatten the nested dictionary and convert it into a DataFrame
    records = []
    for ticker, dates in data.items():
        for date, features in dates.items():
            features['Date'] = date
            features['Ticker'] = ticker
            records.append(features)

    df = pd.DataFrame(records)
    none_mask = df.isnull()

    # Dictionary to store normalization parameters
    normalization_params = {}

    # Standardize selected columns together and store their parameters
    group_stats = standardize_columns(
        df, columns_to_standardize_together, stats)
    normalization_params = group_stats

    # Standardize other numeric columns individually
    other_numeric_cols = [col for col in df.select_dtypes(include=[float, int]).columns.tolist()
                          if col not in columns_to_standardize_together]
    for col in other_numeric_cols:
        col_stats = standardize_columns(df, [col], stats)
        normalization_params[col] = col_stats[col]

    # Restore NaN values
    df[none_mask] = None

    # Reconstruct the nested dictionary format for JSON
    standardized_data = {}
    for _, row in df.iterrows():
        ticker = row['Ticker']
        date = row['Date']
        standardized_data.setdefault(ticker, {})[date] = {key: (None if pd.isnull(
            val) else val) for key, val in row.drop(['Ticker', 'Date']).to_dict().items()}

    return standardized_data, normalization_params


def main():
    # Define the directories
    input_file = 'G:\StockData\stock_data.json'
    output_directory = 'G:/StockData/normalized_master_datasets/'


    # Create the output directory if it does not exist
    os.makedirs(output_directory, exist_ok=True)

    output_file_name = f"abs_normalized.json"
    normalization_params_name = f"abs_normalized_parameters.json"
    output_path = os.path.join(output_directory, output_file_name)
    normalization_params_path = os.path.join(
        output_directory, normalization_params_name)

    # Load the data from the json file
    with open(input_file, 'r') as file:
        data = json.load(file)


    standardized_data, normalization_params = standardize_stock_data(data)

    # Save the standardized data to a json file
    with open(output_path, 'w') as file:
        json.dump(standardized_data, file, indent=4)

    # Save the normalization parameters to a separate json file
    with open(normalization_params_path, 'w') as file:
        json.dump(normalization_params, file, indent=4)


if __name__ == "__main__":
    main()