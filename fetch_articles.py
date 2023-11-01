import requests
import json
import os
from datetime import datetime, timedelta
import time
import re

def fetch_articles(ticker_symbol, company_name):
    # Calculate the date 1000 days from today
    date_to = datetime.utcnow()
    date_from = (date_to - timedelta(days=1000))
    chunks = 20

    interval = 1000 // chunks  # Split the 1000 days into three intervals
    all_articles = []  # List to hold all articles across intervals and pages

    for i in range(chunks):
        # Calculate the date range for this interval
        current_from = (date_from + timedelta(days=interval*i)).strftime('%Y/%m/%d')
        current_to = (date_from + timedelta(days=interval*(i+1))).strftime('%Y/%m/%d')

        # URL for the Newscatcher v2 API
        url = 'https://api.newscatcherapi.com/v2/search'

        # Set up the headers with your API key
        headers = {
            'x-api-key': 'fItRA0vYKlXsLzLNcaTJhWuPuWCZxv5zXRf8rZZLy0U'
        }

        # Define a function to calculate the number of pages
        def calculate_pages(articles_count):
            page_size = 100  # The number of articles per page
            pages = articles_count // page_size
            if articles_count % page_size > 0:
                pages += 1  # Account for a partial page of articles
            return pages

        to_rank = 100  # Start with a to_rank value of 100
        upper_to_rank = 200
        lower_to_rank = 0
        last_to_rank = 0

        unchanged_counter = 0  # Counter to track unchanged to_rank values
        process_log = {
            "Stock": f'{ticker_symbol} - {company_name}',
            "Skipped": False,
            "API_Calls_Info": []
        }

        while True:
            # Define the query parameters
            query_params = {
                'q': f'{ticker_symbol}',
                'from': current_from,
                'to': current_to,
                'lang': 'en',
                'page_size': 100,
                'to_rank': to_rank,
                'not_sources': 'google.com, youtube.com',
                'topic': 'finance',
                'countries': 'US,CA'
            }
            
            # Make an initial request to check the total number of articles
            print(f'Sending initial request with to_rank={to_rank}...')

            if to_rank == last_to_rank:
                unchanged_counter += 1
            else:
                unchanged_counter = 0  # Reset the counter if to_rank changes
            
            if unchanged_counter == 2:
                process_log["Skipped"] = True
                break  # Exit the loop and move on to the next stock if to_rank remains unchanged for two iterations

            if to_rank > 999999:
                process_log["Skipped"] = True
                break
            
            # Add a delay to avoid hitting rate limits
            time.sleep(1)
            response = requests.get(url, headers=headers, params=query_params)
            if response.status_code == 200:
                articles_count = response.json().get('total_hits', 0)
                print(f'{articles_count} articles found with to_rank={to_rank}.')
                pages = calculate_pages(articles_count)
                print(f'It would take {pages} API calls to retrieve all articles.')
                if 200 <= articles_count <= 5000:
                    process_log["API_Calls_Info"].append({
                    "Chunk": i + 1,
                    "Total_Hits": articles_count,
                    "Total_Pages": pages,
                    "API_Calls_Per_Chunk": pages  # Assuming one API call per page
                    })
                    break  # Exit the loop if the number of articles is within the desired range
                elif articles_count < 250:
                    last_to_rank = to_rank
                    lower_to_rank = to_rank
                    to_rank = (upper_to_rank + to_rank) // 2
                    if upper_to_rank >= 100:
                        upper_to_rank *= 2
                else:
                    last_to_rank = to_rank
                    upper_to_rank = to_rank
                    to_rank = (lower_to_rank + to_rank) // 2
            else:
                print(f'Failed to fetch articles: {response.status_code}')
                return []  # Return an empty list if the request fails
            
        
        page = 1  # Start on the first page

        while True:
            break
            # Define the query parameters
            query_params = {
                'q': f'{ticker_symbol}',
                'from': current_from,
                'to': current_to,
                'lang': 'en',
                'page_size': 100,
                'to_rank': to_rank,
                'not_sources': 'google.com, youtube.com',
                'topic': 'finance',
                'countries': 'US,CA'
            }

            query_params['page'] = page  # Update the page parameter
            print(f'Sending request to {url} with parameters {query_params}')

            time.sleep(1)
            # Make the HTTP request
            response = requests.get(url, headers=headers, params=query_params)

            # Check for a valid response
            if response.status_code == 200:
                print(f'Request succeeded for page {page} in interval {i+1}.')
                articles = response.json().get('articles', [])
                if not articles or page > 99:
                    break  # Exit the loop if no more articles are found
                all_articles.extend(articles)  # Add the articles from this page to the list
                page += 1  # Increment the page number for the next iteration
            else:
                print(f'Failed to fetch articles: {response.status_code}')
                break  # Exit the loop if the request fails
            
        # Save the process log to process_log.json
        with open('process_log_7.json', 'a') as file:
            json.dump(process_log, file, indent=4)
            file.write('\n')  # Write a newline character to separate entries

    return all_articles

def main():
    # Load the list of stocks from stock_list.json
    with open('stock_list_filtered.json', 'r') as file:
        stock_list = json.load(file)

    # Create news_articles directory if it doesn't exist
    if not os.path.exists('news_articles'):
        os.makedirs('news_articles')

    for stock, info in stock_list.items():
        ticker_symbol = info['Symbol']
        company_name = info['Name']

        # Remove common suffixes from the company name, and an optional comma
        company_name = re.sub(r'\s*,?\s*(Inc|Corporation|Company|PLC|Corp|Ltd|Limited)\.?\s*$', '', company_name, flags=re.I)

        print(f'Fetching articles for {ticker_symbol} ({company_name})...')
        articles = fetch_articles(ticker_symbol, company_name)
        if articles:
            print(f'{len(articles)} articles fetched for {ticker_symbol}.')
            # Write articles for this stock to a separate JSON file
            with open(f'news_articles/{ticker_symbol}_news_articles.json', 'w') as file:
                json.dump(articles, file, indent=4)
            print(f'Articles for {ticker_symbol} written to {ticker_symbol}_news_articles.json')
        else:
            print(f'No articles found for {ticker_symbol}.')

if __name__ == "__main__":
    main()