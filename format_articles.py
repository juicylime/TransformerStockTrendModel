import json
import os
from collections import defaultdict

def format_data(ticker_symbol, entity_names, data):
    # Initialize a dictionary to hold the grouped data
    grouped_data = defaultdict(lambda: {'number_of_articles': 0, 'articles': []})

    # Prepare entity names prefix
    # entity_prefix = ''.join(f'[ENT]{entity}[/ENT]' for entity in entity_names)

    for item in data:
        # Skip articles that have missing body
        if item.get('summary', '') is None:
            continue

        if item.get('topic', '') == 'sport' or item.get('topic', '') == 'gaming':
            continue

        # Extract the date from the 'published_at' field
        # Assuming 'published_at' is formatted as 'YYYY-MM-DD HH:MM:SS'
        date = item.get('published_date', '').split(' ')[0]

        # Format the item
        formatted_item = {
            'published_date': item.get('published_date', ''),
            'link': item.get('link', ''),
            'page_rank': item.get('rank', ''),
            'match_score': item.get('_score', ''),
            'author': item.get('author', ''),
            'title': item.get('title', ''),
            'body': item.get('summary', ''),
            'topic': item.get('topic', '')
        }

        # Group the item by date and update the count
        grouped_data[date]['articles'].append(formatted_item)
        grouped_data[date]['number_of_articles'] += 1

    return grouped_data

def main():
    # Load the list of stocks from the specified JSON file
    with open('stock_list_filtered.json', 'r') as file:
        stock_list = json.load(file)

    # Create formatted_news_articles directory if it doesn't exist
    formatted_dir = 'G:\\StockData\\formatted_news_articles'
    if not os.path.exists(formatted_dir):
        os.makedirs(formatted_dir)

    for ticker_symbol, info in stock_list.items():
        # Load the original data from the respective news articles file
        file_path = f'G:\\StockData\\news_articles\\{ticker_symbol}_news_articles.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as input_file:
                original_data = json.load(input_file)

            # Format the original data
            entity_names = info['EntityNames']
            formatted_data = format_data(ticker_symbol, entity_names, original_data)

            # Compute the total number of articles
            total_articles = sum(info['number_of_articles'] for date, info in formatted_data.items())

            # Prepare the output data including the total number of articles
            output_data = {
                'total_articles': total_articles,
                'data': formatted_data
            }

            # Write the formatted data to the output file in the 'formatted_news_articles' directory
            output_file_path = f'{formatted_dir}\\formatted_{ticker_symbol}_articles.json'
            with open(output_file_path, 'w') as output_file:
                json.dump(output_data, output_file, indent=4)

            print(f'Data formatting complete for {ticker_symbol}. Output written to {output_file_path}.')
        else:
            print(f'No news articles file found for {ticker_symbol}. Skipping...')

if __name__ == "__main__":
    main()
