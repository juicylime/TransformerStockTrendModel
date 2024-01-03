import json
import numpy as np
from collections import deque
import os

def calculate_correlation(stocks_data, base_prices):
    correlations = {}
    for stock, prices in stocks_data.items():
        if None not in prices:
            correlation = np.corrcoef(base_prices, prices)[0, 1]
            correlations[stock] = correlation
            # print(f"Calculated correlation for {stock}")
    return correlations

def create_training_examples(input_file, split_percentage=85, n=30, chunk_size=1000):
    with open(input_file, 'r') as file:
        data = json.load(file)

    stocks_data = {
        stock: [info.get('Close', None) for date, info in sorted(dates.items())]
        for stock, dates in data.items()
    }

    for stock_symbol, dates in data.items():
        dates_window = deque(maxlen=n + 10)
        stocks_window = {stock: deque(maxlen=n) for stock in stocks_data}
        sorted_dates = sorted(dates.keys())
        
        split_index = int(len(sorted_dates) * (split_percentage / 100))
        training_split = sorted_dates[:split_index]
        validation_split = sorted_dates[split_index + n:] # Offset by sequence length

        splits = [('training', training_split),('validation', validation_split)]

        for split_name, split_data in splits:
            # Define the directory for the output files
            output_dir = f"G:/StockData/{split_name}_sequence_{n}_split_{split_percentage}/{stock_symbol}/"
            # Create the directory if it does not exist
            os.makedirs(output_dir, exist_ok=True)

            # Initialize chunk variables
            chunk_count = 0
            output_data = {}

            for date in split_data:
                if date in dates:
                    dates_window.append(dates[date])
                    for stock, prices_deque in stocks_window.items():
                        if date in data[stock]:
                            prices_deque.append(data[stock][date]['Close'])
                        else:
                            prices_deque.append(None)

                if len(dates_window) == n + 10: # number of days in future to predict
                    # Ensure we have all the necessary prices to calculate correlations
                    if all(len(prices_deque) == n for prices_deque in stocks_window.values()):
                        # Prepare data for correlation calculation
                        # base_prices = [entry['Close'] for entry in list(dates_window)[:-1] if 'Close' in entry]
                        # correlations = calculate_correlation({k: list(v) for k, v in stocks_window.items() if k != stock_symbol}, base_prices)

                        # Find the most correlated stocks and their data
                        # if correlations:
                        #     max_positive_stock, max_negative_stock = max(correlations, key=correlations.get), min(correlations, key=correlations.get)
                        if True:
                            
                            # Prepare X values with additional correlation data and related stocks' closing prices
                            X = [
                                {**entry,
                                # 'correlation_with_max_positive': correlations[max_positive_stock],
                                # 'max_positive_stock_close': stocks_data[max_positive_stock][idx],
                                # 'correlation_with_max_negative': correlations[max_negative_stock],
                                # 'max_negative_stock_close': stocks_data[max_negative_stock][idx]
                                }
                                for idx, entry in enumerate(list(dates_window)[:-10])
                            ]
                            
                            # Prepare Y value
                            # Y = 1 if dates_window[-1]['Close'] > 0 else 0

                            Y = dates_window[-1]['EMA_23']

                            
                            # Y is determined if the percentage change is + or -. + means stock went up. 
                            # Y = 1 if dates_window[-1]['Close'] > dates_window[-2]['Close'] else 0

                            # Add the training example to output_data list
                            start_date, end_date = sorted_dates[sorted_dates.index(date)-n], date
                            training_example_key = f"{start_date}_to_{end_date}"
                            output_data[training_example_key] = {'X': X, 'Y': Y}

                            # Print the most correlated stocks and their scores
                            # print(f"Most positively related stock to {stock_symbol}: {max_positive_stock} with a score of {correlations[max_positive_stock]:.4f}")
                            # print(f"Most negatively related stock to {stock_symbol}: {max_negative_stock} with a score of {correlations[max_negative_stock]:.4f}")

                    # Check if we need to start a new chunk
                    if len(output_data) == chunk_size:
                        # When saving the output, include the directory and subdirectory in the path
                        output_file = os.path.join(output_dir, f"{split_name}_examples_chunk_{chunk_count}.json")
                        with open(output_file, 'w') as file:
                            json.dump(output_data, file, indent=4)
                        output_data = {}
                        chunk_count += 1

                    # Move the window forward
                    dates_window.popleft()
                    for prices_deque in stocks_window.values():
                        prices_deque.popleft()

            # Write any remaining data to a file in the specified directory
            if output_data:
                output_file = os.path.join(output_dir, f"{split_name}_examples_chunk_{chunk_count}.json")
                with open(output_file, 'w') as file:
                    json.dump(output_data, file, indent=4)

    print("All done!")


# Example usage
input_file = 'G:/StockData/normalized_master_datasets/abs_normalized.json'
create_training_examples(input_file, split_percentage=85, n=20)