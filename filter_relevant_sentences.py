import json
import os

def filter_sentences(article, entity_names):
    # Filter sentences in 'tokenized_body' that contain any of the entity names
    return [sentence for sentence in article['tokenized_body'] if any(entity_name in sentence for entity_name in entity_names)]

def process_stock(stock_symbol, entity_names):
    input_path = f'G:/StockData/tokenized_news_articles/tokenized_{stock_symbol}_articles.json'
    output_path = f'G:/StockData/relevant_tokenized_news_articles/relevant_tokenized_{stock_symbol}_articles.json'

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(input_path, 'r', encoding='utf-8') as file:
        tokenized_articles = json.load(file)

    # Assuming 'tokenized_articles' is a dictionary with dates as keys and list of article dictionaries as values
    relevant_tokenized_data = {}
    for date, articles in tokenized_articles.items():
        relevant_articles = []
        for article in articles:
            filtered_sentences = filter_sentences(article, entity_names)
            if filtered_sentences:
                relevant_article = article.copy()
                relevant_article['tokenized_body'] = filtered_sentences
                relevant_articles.append(relevant_article)
        relevant_tokenized_data[date] = relevant_articles

    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(relevant_tokenized_data, file, ensure_ascii=False, indent=4)

    print(f"Processed relevant tokenized articles for {stock_symbol}")

def main():
    # Load the stock information
    with open('stock_list.json', 'r', encoding='utf-8') as file:
        stocks = json.load(file)

    # Process each stock's articles
    for stock_symbol, stock_info in stocks.items():
        entity_names = stock_info['EntityNames']
        process_stock(stock_symbol, entity_names)

if __name__ == "__main__":
    main()
