import json
from transformers import pipeline

import torch

# Check if CUDA is available and set the device accordingly
device = 0 if torch.cuda.is_available() else -1  # 0 is the index of the first GPU device

# Load the FinBERT sentiment analysis pipeline with CUDA if available
finbert = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone", tokenizer="yiyanghkust/finbert-tone", device=device)

# Load the list of stock symbols
with open('./stock_list.json', 'r') as file:
    stock_list = json.load(file)

# Function to calculate sentiment for a given body of text
def get_sentiment(tokenized_body):
    results = finbert(tokenized_body)
    return results

# Main processing function
def process_articles():
    output = {}
    for stock in stock_list:
        file_path = f'G:/StockData/filtered_news_articles_sentences/filtered_{stock}_articles.json'
        with open(file_path, 'r') as file:
            data = json.load(file)
            output[stock] = {}
            for date, articles in data.items():
                total_sentiment_score = 0
                article_count = len(articles)
                for article in articles:
                    sentiment_results = get_sentiment(article['tokenized_body'])
                    # Here you would aggregate the sentiment scores as needed
                    # For simplicity, we're just summing up the positive scores as an example
                    total_sentiment_score += sum([result['score'] for result in sentiment_results if result['label'] == 'POSITIVE'])
                output[stock][date] = {
                    'total_sentiment_score': total_sentiment_score,
                    'article_count': article_count
                }

    # Output the results to a JSON file
    with open('G:/StockData/sentiment_output.json', 'w') as outfile:
        json.dump(output, outfile)

# Run the processing function
process_articles()
