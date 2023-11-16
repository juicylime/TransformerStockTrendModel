import os
import json
from collections import OrderedDict

# This function will create a combined JSON object with ordered dates for each symbol
def combine_sentiment_data(symbols, directory_path):
    combined_data = {}

    for symbol in symbols:
        file_path = f'{directory_path}/sentiment_output_{symbol}.json'
        
        # Check if the file exists before attempting to open
        if os.path.isfile(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f, object_pairs_hook=OrderedDict)
                sorted_data = OrderedDict(sorted(data.items()))
                combined_data[symbol] = sorted_data

    return combined_data


def main():
    with open('stock_list.json', 'r', encoding='utf-8') as file:
        stock_list = json.load(file)
    
    combined_data = combine_sentiment_data(stock_list, 'G:/StockData/sentiment_scores')
    
    with open('G:/StockData/combined_sentiment_scores.json', 'w', encoding='utf-8') as file:
        json.dump(combined_data, file, indent=4)

if __name__ == "__main__":
    main()