import json

# Load the JSON data from a file
with open('G:/StockData/formatted_news_articles_old/formatted_GOOG_articles.json', 'r') as file:
    data = json.load(file)

# Function to prioritize articles by their match score and then page rank, then trim to 50
def prioritize_and_trim(articles):
    # Filter articles where topic is 'finance' or 'business'
    filtered_articles = [
        article for article in articles 
        if article.get('topic') in ['finance', 'business', 'news']
        and not article['link'].startswith("https://github.com")
    ]
    # Sort the filtered articles by 'match_score' in descending order, then by 'page_rank' in ascending order
    sorted_articles = sorted(filtered_articles, key=lambda x: (-x.get('match_score', 0), x['page_rank']))
    # Keep only the top 50 articles
    return sorted_articles[:18]

# Initialize a counter for the total number of articles after trimming
total_articles_after_trimming = 0

# Process the data
for date, daily_data in data['data'].items():
    trimmed_articles = prioritize_and_trim(daily_data['articles'])
    data['data'][date]['articles'] = trimmed_articles
    # Update the daily article count
    daily_article_count = len(trimmed_articles)
    data['data'][date]['number_of_articles'] = daily_article_count
    # Add to the total article count
    total_articles_after_trimming += daily_article_count

# Update the total article count in the data
data['total_articles'] = total_articles_after_trimming

# Output the new JSON data to a file
with open('G:/StockData/formatted_news_articles/formatted_GOOG_articles.json', 'w') as file:
    json.dump(data, file, indent=4)

print("JSON file has been filtered and the total article count updated. The updated file has been saved.")
