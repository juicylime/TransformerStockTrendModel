import os
import time
import json
import re
from collections import defaultdict
from nltk.tokenize import sent_tokenize, word_tokenize
from transformers import TFAutoModelForTokenClassification, AutoTokenizer, pipeline
import tensorflow as tf

# Ensure NLTK data is downloaded (used for sentence tokenization)
import nltk
nltk.download('punkt')

def process_articles(stock_symbol, formatted_articles, entity_names):
    # Detect hardware, return appropriate distribution strategy
    try:
        tpu = tf.distribute.cluster_resolver.TPUClusterResolver()  # TPU detection
        print('Running on TPU ', tpu.cluster_spec().as_dict()['worker'])
    except ValueError:
        tpu = None

    if tpu:
        tf.config.experimental_connect_to_cluster(tpu)
        tf.tpu.experimental.initialize_tpu_system(tpu)
        strategy = tf.distribute.TPUStrategy(tpu)
    else:
        strategy = tf.distribute.get_strategy()

    print("REPLICAS: ", strategy.num_replicas_in_sync)

    with strategy.scope():
        model = TFAutoModelForTokenClassification.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
        tokenizer = AutoTokenizer.from_pretrained("Jean-Baptiste/roberta-large-ner-english")

     # Create the NER pipeline
    ner_pipeline = pipeline("ner", model=model, tokenizer=tokenizer, grouped_entities=True)

    # Prepare a directory for the filtered articles
    filtered_dir = 'G:/StockData/filtered_news_articles'
    os.makedirs(filtered_dir, exist_ok=True)

    filtered_articles_data = defaultdict(lambda: {'number_of_articles': 0, 'articles': []})

    for date, info in formatted_articles.items():
        start_time = time.time() # Store the start time

        for article in info['articles']:
            # Remove the [ENT]...[/ENT] tokens and the values in between
            body_text = re.sub(r'\[ENT\].*?\[\/ENT\]', '', article['body'])

            # Tokenize the body text using the word_tokenize function
            tokens = word_tokenize(body_text)

            # Join tokens back into a single string and split into sentences
            body_text = ' '.join(tokens)
            sentences = sent_tokenize(body_text)

            # Filter sentences to only include those that contain any of the specified entity names
            relevant_sentences = [sent for sent in sentences if any(name in sent for name in entity_names)]

            # Skip this article if no relevant sentences are found
            if not relevant_sentences:
                continue
            
            # Check the recognized entities for each sentence
            found_matching_entity = False  # Initialize a flag to false
            for sentence in relevant_sentences:
                # Run NER on the sentence
                entities = ner_pipeline(sentence)
                # Check if any of the entities match the specified entity names and are organizations
                for entity in entities:
                    # Remove leading and trailing whitespace from the entity text
                    entity_text = entity['word'].strip()
                    if entity['entity_group' ] == 'ORG' and entity_text in entity_names:
                        filtered_articles_data[date]['articles'].append(article)
                        filtered_articles_data[date]['number_of_articles'] += 1
                        found_matching_entity = True  # Set the flag to true
                        break  # Exit the inner loop as we've found a matching entity
                if found_matching_entity:
                    break  # Exit the outer loop as we've found a matching entity

        end_time = time.time()  # Store the end time
        elapsed_time = end_time - start_time  # Calculate the elapsed time

        print(f"Processing for date {date} with {len(info['articles'])} articles: Took {elapsed_time:.2f} seconds.")

    # Output the filtered articles to a JSON file
    output_data = {
        'total_articles': sum(info['number_of_articles'] for date, info in filtered_articles_data.items()),
        'data': filtered_articles_data
    }
    output_path = os.path.join(filtered_dir, f'filtered_{stock_symbol}_articles.json')
    with open(output_path, 'w', encoding='utf-8') as output_file:
        json.dump(output_data, output_file, ensure_ascii=False, indent=4)

    print(f'Filtered articles for {stock_symbol} saved to {output_path}')

def main():
    # Load the stock information
    with open('stock_list.json', 'r') as file:
        stocks = json.load(file)

    # Process each stock
    for stock_symbol, stock_info in stocks.items():
        # Define the path to the formatted articles JSON file
        formatted_articles_path = f'G:/StockData/formatted_news_articles/formatted_{stock_symbol}_articles.json'

        # Load the formatted articles
        with open(formatted_articles_path, 'r') as file:
            formatted_articles = json.load(file)['data']  # Assuming the articles are under the 'data' key

        # Process the articles
        process_articles(stock_symbol, formatted_articles, stock_info['EntityNames'])

if __name__ == "__main__":
    main()
