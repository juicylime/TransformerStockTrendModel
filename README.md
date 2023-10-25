# Stock Trend Prediction Using Transformer Model

This project aims to build a stock trend prediction model utilizing the transformer architecture. The model is trained and validated on a dataset that includes 30-day financial sequences for various stocks. Each sequence encapsulates daily stock information such as Open, High, Low, Close (OHLC) prices, Adjusted Close, Long/Short-Term Moving Averages (LMA/SMA), along with some other more standard financial indicators. The model will also utilize daily news sentiment scores.

There will be a focus on curating solid sentiment scores by feeding the articles retrieved from [Newscatcher API](https://newscatcherapi.com/) into a model that can calculate the individual sentiments of each stock in a post or article if present. Currently most sentiment analysis classifiers don't take into account multiple tickers in a given post and would return the general sentiment of the entire post without actually looking at the indivual sentiments of each stock so this project aims to tackle that issue to provide the transformer model a more accurate and clean dataset. 

![Badge](https://img.shields.io/badge/license-MIT-green)

## Features

- **Transformer-based Model**: Utilizing the efficient attention mechanism to capture complex temporal relationships among stock features over time.
- **Multi-Feature Sequences**: Each example in the dataset includes a sequence of 30 days, with each day encompassing OHLC prices, Adjusted Close, LMA/SMA, and averaged news sentiment scores from relevant articles.
- **News Sentiment Analysis**: Daily news sentiment scores are calculated using [Newscatcher API](https://newscatcherapi.com/).
- **Competitor/Partner Insights**: Additional features include OHLC prices and averaged news sentiment of related stocks to provide a comparative market perspective.

## Installation

```bash
git clone https://github.com/your-username/stock-trend-prediction.git
cd stock-trend-prediction
```

## Data Collection

- **Stock Financial Data**: Acquire historical financial data of the stocks in focus.
- **News Sentiment**: Utilize the [Newscatcher API](https://newscatcherapi.com/) to collect and analyze news articles for daily sentiment scores.
- **Social Media Sentiment**: Exploration is underway to determine an efficient method for collecting and analyzing social media sentiment, which is believed to be a crucial feature for the model.

## Model Training and Validation

- Prepare the dataset with 30-day sequences for each stock , ensuring each day has the necessary financial and news sentiment features.
- Train the transformer model on the training dataset and validate its performance on the validation dataset.

## Future Work

- Incorporate social media sentiment analysis once an efficient data collection method is established.
- Evaluate the model's performance in a live market scenario and iterate on the model architecture and features as necessary.

## License

[MIT](https://choosealicense.com/licenses/mit/)

## Authors

- [JuicyLime](https://github.com/juicylime)

