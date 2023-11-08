import json
import spacy
import os
import argparse

# Initialize the parser
parser = argparse.ArgumentParser(description='Tokenize articles from a list of stocks.')
parser.add_argument('stock_list', type=str, help='Path to the stock_list.json file')
args = parser.parse_args()

print("Loading SpaCy model...")
# Load the SpaCy model for tokenization
nlp = spacy.load("en_core_web_sm", disable=["parser", "tagger", "ner"])
nlp.add_pipe('sentencizer')

def tokenize(text):
    """Tokenize the input text into a list of sentences."""
    doc = nlp(text)
    tokenized_sentences = [" ".join(token.text for token in sent) for sent in doc.sents]
    return tokenized_sentences

def load_json(file_path):
    """Load JSON from a file."""
    print(f"Loading data from {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(data, file_path):
    """Save data to a JSON file."""
    print(f"Saving data to {file_path}...")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print("Data saved.")

def tokenize_articles_by_date(formatted_articles):
    article_day = 0
    """Tokenize articles and group them by their date using SpaCy's pipe for batch processing."""
    tokenized_articles_by_date = {}
    for date, info in formatted_articles['data'].items():
        article_day += 1
        print(f"Processing date: {date}")
        print(f"Tokenizing article: {article_day}")
        # Prepare the texts for batch tokenization
        texts = [article['body'] for article in info['articles']]
        # Tokenize the texts as a batch
        tokenized_docs = list(nlp.pipe(texts, batch_size=20))  # Adjust the batch size based on your needs and system capabilities
        tokenized_articles = []
        for doc, article in zip(tokenized_docs, info['articles']):
            tokenized_sentences = [" ".join(token.text for token in sent) for sent in doc.sents]
            tokenized_articles.append({
                "published_date": article['published_date'],
                "link": article['link'],
                "title": article['title'],
                "tokenized_body": tokenized_sentences
            })
        tokenized_articles_by_date[date] = tokenized_articles
    return tokenized_articles_by_date


if __name__ == "__main__":
    # Load the list of stock symbols
    stock_list_path = args.stock_list
    print(f"Loading stock list from {stock_list_path}...")
    stock_list = load_json(stock_list_path)

    # Directory where the formatted articles JSON files are stored
    formatted_articles_dir = 'G:/StockData/formatted_news_articles/'

    # Directory where the tokenized articles JSON files will be saved
    output_dir = 'G:/StockData/tokenized_news_articles/'
    os.makedirs(output_dir, exist_ok=True)

    # Process each stock symbol
    for stock_symbol in stock_list.keys():
        print(f"Processing stock symbol: {stock_symbol}")
        # Construct the file path for the formatted articles JSON file
        formatted_articles_path = f'{formatted_articles_dir}formatted_{stock_symbol}_articles.json'
        
        # Load the formatted articles
        formatted_articles = load_json(formatted_articles_path)
        
        # Tokenize the articles by date
        tokenized_articles_by_date = tokenize_articles_by_date(formatted_articles)
        
        # Save the tokenized articles to a JSON file
        tokenized_articles_path = f'{output_dir}tokenized_{stock_symbol}_articles.json'
        save_json(tokenized_articles_by_date, tokenized_articles_path)

        print(f'Tokenized articles for {stock_symbol} saved to {tokenized_articles_path}')
