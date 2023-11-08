import time
import json
from collections import defaultdict
from flair.data import Sentence
from flair.models import SequenceTagger
from flair import device
from multiprocessing import Pool, current_process
import logging
from logging.handlers import RotatingFileHandler
import gc
import torch
import os

# Set up the log directory
log_directory = "./logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging to write to a file in the logs directory
log_file_path = os.path.join(log_directory, 'process_articles.log')
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# Use a rotating file handler to keep the logs from growing indefinitely
file_handler = RotatingFileHandler(log_file_path, maxBytes=1024*1024*5, backupCount=5)  # 5 MB per file, max 5 files
file_handler.setFormatter(log_formatter)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    handlers=[file_handler]
)

# Global variable to hold the NER model
ner_model = None

def worker_initializer():
    # Access the global variable within the initializer
    global ner_model
    # Load the model only if it has not been loaded before
    if ner_model is None:
        print(f'Loading the model for process {current_process().pid}')
        ner_model = SequenceTagger.load('flair/ner-english-fast')

def process_articles(stock_symbol, tokenized_articles, entity_names):
    global ner_model
    tagger = ner_model
    try:
        filtered_dir = 'G:/StockData/filtered_news_articles_sentences'
        os.makedirs(filtered_dir, exist_ok=True)

        filtered_articles_data = defaultdict(lambda: {'number_of_articles': 0, 'articles': []})
        days_processed = 0  # Variable to count days processed
        
        total_articles = 0

        for date, articles in tokenized_articles.items():
            start_time = time.time()
            days_processed += 1  # Increment the days processed count

            for article_idx, article in enumerate(articles):
                all_sentences = [Sentence(sent, use_tokenizer=False) for sent in article['tokenized_body']]
                tagger.predict(all_sentences)

                matching_sentences = []
                for sentence in all_sentences:
                    for entity in sentence.get_spans('ner'):
                        if entity.tag == 'ORG' and any(entity_name in entity.text for entity_name in entity_names):
                            matching_sentences.append(sentence.to_plain_string())
                            break  # Prevent adding the same sentence multiple times

                if matching_sentences:
                    filtered_articles_data[date]['articles'].append({
                        'published_date': article['published_date'],
                        'link': article['link'],
                        'title': article['title'],  
                        'tokenized_body': matching_sentences
                    })
                    filtered_articles_data[date]['number_of_articles'] += 1
                    total_articles += 1

            # torch.cuda.empty_cache()  # Clear the CUDA cache
            gc.collect()  # Force garbage collection
            end_time = time.time()
            elapsed_time = end_time - start_time

            print(f"\n{stock_symbol}: Day {days_processed} processed in {elapsed_time:.2f} seconds. Kept {filtered_articles_data[date]['number_of_articles']}/{len(articles)}")

        output_data = {
            'total_articles': total_articles,
            'data': filtered_articles_data
        }
        output_path = os.path.join(filtered_dir, f'filtered_{stock_symbol}_articles.json')
        with open(output_path, 'w', encoding='utf-8') as output_file:
            json.dump(output_data, output_file, ensure_ascii=False, indent=4)

        print(f'Filtered articles for {stock_symbol} saved to {output_path}')
        logging.info(f'Filtered articles for {stock_symbol} written to {output_path}')
    except Exception as e:
        logging.exception(f"Exception occurred while processing articles for {stock_symbol}: {e}")


def process_stock(stock_info_tuple):
    stock_symbol, stock_info = stock_info_tuple
    # Define the path to the tokenized and original articles JSON file
    tokenized_articles_path = f'G:/StockData/relevant_tokenized_news_articles/relevant_tokenized_{stock_symbol}_articles.json'

    # Load the tokenized and original articles
    with open(tokenized_articles_path, 'r', encoding='utf-8') as file:
        tokenized_articles = json.load(file)

    # Process the articles
    process_articles(stock_symbol, tokenized_articles, stock_info['EntityNames'])

def main():
    # Sanity Check the device
    print(f'Flair is using: {device}')
    # Load the stock information
    with open('stock_list.json', 'r') as file:
        stocks = json.load(file)

    stock_info_list = [(symbol, info) for symbol, info in stocks.items()]

    # Use a pool of 3 processes, with the initializer to load the model
    with Pool(processes=1, initializer=worker_initializer) as pool:
        pool.map(process_stock, stock_info_list)

if __name__ == "__main__":
    main()