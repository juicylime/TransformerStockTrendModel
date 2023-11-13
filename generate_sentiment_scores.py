import json
import os
import multiprocessing
from transformers import pipeline, AutoTokenizer
import torch
import logging

# Set up logging
logging.basicConfig(filename='./logs/sentiment_logs.log', 
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# Initialize the tokenizer and model for each process
def init_process():
    device = 0 if torch.cuda.is_available() else -1
    tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")
    model_pipe = pipeline(
        "sentiment-analysis",
        model="yiyanghkust/finbert-tone",
        tokenizer=tokenizer,
        truncation=True,
        max_length=512,
        device=device
    )
    return model_pipe

# Function to flatten all sentences from all dates and create mappings
def flatten_sentences(data):
    all_sentences = []
    mappings = []
    for date, articles in data.items():
        for article_index, article in enumerate(articles['articles']):
            for sentence in article['tokenized_body']:
                all_sentences.append(sentence)
                mappings.append((date, article_index))  # Keep track of the date and article index
    return all_sentences, mappings

# Function to process all sentences and keep track of their origins
def get_sentiments(stock, sentences, mappings, model_pipe, batch_size=16):
    sentiments = []
    total_batch_count = len(sentences) // batch_size
    # Process the sentences in batches
    for i in range(0, len(sentences), batch_size):
        batch = sentences[i:i + batch_size]
        print(f"""\n-----------------------------------
              Processing batch {i // batch_size + 1}/{total_batch_count} with {len(batch)} sentences... for {stock}""")
        batch_sentiments = model_pipe(batch)
        # Process and store sentiments
        sentiments.extend(((mappings[i+j], sentiment['score']) if sentiment['label'] == 'Positive' 
                           else (mappings[i+j], -sentiment['score']) if sentiment['label'] == 'Negative'
                           else None  # Exclude Neutral or other labels
                           for j, sentiment in enumerate(batch_sentiments)))

        # Remove None entries (neutral sentiments)
        sentiments = [sentiment for sentiment in sentiments if sentiment is not None]

        # Empty CUDA cache
        torch.cuda.empty_cache()
    return sentiments

# Function to process articles for a single stock
def process_stock(stock):
    try:
        print(f"Processing articles for stock: {stock}")
        logging.info(f"Processing articles for stock: {stock}")
        model_pipe = init_process()  # Initialize model and tokenizer for the process
        file_path = f'../StockData/filtered_news_articles_sentences/filtered_{stock}_articles.json'
        
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)['data']
            output = {}
            
            # Flatten sentences from all dates and create mappings
            all_sentences, mappings = flatten_sentences(data)
            
            # Get sentiments for all flattened sentences
            sentiments_with_mappings = get_sentiments(stock, all_sentences, mappings, model_pipe)

            # Initialize a dictionary to collect sentiments by date and article index
            daily_sentiments = {date: {index: [] for index in range(len(data[date]['articles']))} for date in data}
            
            # Aggregate sentiments back to the respective articles
            for mapping, sentiment_score in sentiments_with_mappings:
                date, article_index = mapping
                daily_sentiments[date][article_index].append(sentiment_score)
            
            # Calculate the average sentiment score per article and per date
            for date, articles_scores in daily_sentiments.items():
                daily_scores = [sum(scores)/len(scores) if scores else 0 for scores in articles_scores.values()]
                daily_average = sum(daily_scores) / len(daily_scores) if daily_scores else 0
                if daily_average != 0:
                    output[date] = {
                        'average_sentiment_score': daily_average,
                        'article_count': len(data[date]['articles'])
                    }

            # Write the output to a separate JSON file for each stock
            output_file_path = f'../StockData/sentiment_scores/sentiment_output_{stock}.json'
            with open(output_file_path, 'w', encoding='utf-8') as outfile:
                json.dump(output, outfile, indent=4)
            print(f"Processing complete for {stock}. Sentiment scores saved to '{output_file_path}'.")
            logging.info(f"Processing complete for {stock}. Sentiment scores saved to '{output_file_path}'.")
    except Exception as e:
        print(f"Error processing stock {stock}: {e}")
        logging.error(f"Error processing stock {stock}: {e}")

def main():
    # Load the stock list
    with open('./stock_list.json', 'r') as file:
        stock_list = json.load(file)

    # Ensure output directory exists
    os.makedirs('../StockData/sentiment_scores', exist_ok=True)
    os.makedirs('./logs', exist_ok=True)

    # Use multiprocessing to process stocks
    with multiprocessing.Pool(processes=3) as pool:
        pool.map(process_stock, stock_list)

if __name__ == "__main__":
    main()
