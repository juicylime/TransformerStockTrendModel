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

def process_articles(stock_symbol, tokenized_articles, original_articles, entity_names):
    # Use the globally loaded NER tagger model
    global ner_model
    tagger = ner_model
    try:
        # Prepare a directory for the filtered articles
        filtered_dir = '../StockData/filtered_news_articles'
        os.makedirs(filtered_dir, exist_ok=True)

        filtered_articles_data = defaultdict(lambda: {'number_of_articles': 0, 'articles': []})
        article_num = 0 
        for date, articles in tokenized_articles.items():
            start_time = time.time() # Store the start time
            article_num += 1
            # Collect all relevant sentences and their mapping to the original article index
            all_sentences = []
            article_to_sentences = defaultdict(list)

            processed_articles_set = set()  # Set to track processed articles for the date

            # Use the pre-tokenized sentences for each article
            for article_idx, article in enumerate(articles):
                relevant_sentences = [Sentence(sent, use_tokenizer=False) for sent in article['tokenized_body'] 
                                    if any(entity_name in sent for entity_name in entity_names)]
                article_to_sentences[article_idx].extend(relevant_sentences)
                all_sentences.extend(relevant_sentences)

            # Skip date if no relevant sentences are found
            if not all_sentences:
                continue

            # Run NER on the sentences in batches of 40
            batch_size = 40
            for batch_start in range(0, len(all_sentences), batch_size):
                batch_end = batch_start + batch_size
                sentence_batch = all_sentences[batch_start:batch_end]
                tagger.predict(sentence_batch)
            
            torch.cuda.empty_cache()

            # After NER, map recognized entities back to the original articles
            for article_idx, sentences in article_to_sentences.items():
                found_matching_entity = False
                for sentence in sentences:
                    for entity in sentence.get_spans('ner'):
                        if entity.tag == 'ORG' and any(entity_name in entity.text for entity_name in entity_names):
                            if article_idx not in processed_articles_set:
                                # Append the original article here
                                original_article = original_articles['data'][date]['articles'][article_idx]
                                filtered_articles_data[date]['articles'].append(original_article)
                                filtered_articles_data[date]['number_of_articles'] += 1
                                processed_articles_set.add(article_idx)
                                found_matching_entity = True
                                break  # Found an entity, no need to check other entities in this sentence
                    if found_matching_entity:
                        break  # Found a matching entity in this article, move to next article

            end_time = time.time()  # Store the end time
            elapsed_time = end_time - start_time  # Calculate the elapsed time

            print(f"""\n--------------------------
                {stock_symbol}: {article_num}
                Processing for date {date}:
                {filtered_articles_data[date]['number_of_articles']}/{len(articles)} articles kept
                Took {elapsed_time:.2f} seconds.""")
            
            gc.collect()

        # Output the filtered articles to a JSON file
        output_data = {
            'total_articles': sum(info['number_of_articles'] for date, info in filtered_articles_data.items()),
            'data': filtered_articles_data
        }
        output_path = os.path.join(filtered_dir, f'filtered_{stock_symbol}_articles.json')
        with open(output_path, 'w', encoding='utf-8') as output_file:
            json.dump(output_data, output_file, ensure_ascii=False, indent=4)

        print(f'Filtered articles for {stock_symbol} saved to {output_path}')
        logging.info(f'Processing articles for {stock_symbol}')
    except Exception as e:
        logging.exception(f"Exception occurred while processing articles for {stock_symbol}: {e}")

def process_stock(stock_info_tuple):
    stock_symbol, stock_info = stock_info_tuple
    # Define the path to the tokenized and original articles JSON file
    tokenized_articles_path = f'../StockData/tokenized_news_articles/tokenized_{stock_symbol}_articles.json'
    original_articles_path = f'../StockData/formatted_news_articles/formatted_{stock_symbol}_articles.json'

    # Load the tokenized and original articles
    with open(tokenized_articles_path, 'r', encoding='utf-8') as file:
        tokenized_articles = json.load(file)
    with open(original_articles_path, 'r', encoding='utf-8') as file:
        original_articles = json.load(file)

    # Process the articles
    process_articles(stock_symbol, tokenized_articles, original_articles, stock_info['EntityNames'])

def main():
    # Sanity Check the device
    print(f'Flair is using: {device}')
    # Load the stock information
    with open('stock_list.json', 'r') as file:
        stocks = json.load(file)

    stock_info_list = [(symbol, info) for symbol, info in stocks.items()]

    # Use a pool of 3 processes, with the initializer to load the model
    with Pool(processes=3, initializer=worker_initializer) as pool:
        pool.map(process_stock, stock_info_list)

if __name__ == "__main__":
    main()