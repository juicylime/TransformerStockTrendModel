from selenium import webdriver
from bs4 import BeautifulSoup
import json
import time

def fetch_most_traded_nasdaq_stocks():
    url = "https://finance.yahoo.com/screener/predefined/most_actives?offset=0&count=100"
    
    # Start the web driver (you'll need to specify the correct path to the driver for your browser)
    driver = webdriver.Chrome()
    
    # Navigate to the URL
    driver.get(url)

    # Wait for 10 seconds to allow the page to fully load
    time.sleep(10)

    # Get the page source and parse it with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    # Find the table and extract the rows
    table = soup.find('table', {'class': 'W(100%)'})
    rows = table.find_all('tr')[1:]  # Skip the header row

    most_traded_stocks = {}

    for row in rows:
        columns = row.find_all('td')
        symbol = columns[0].text
        name = columns[1].text
        most_traded_stocks[symbol] = {
            "Symbol": symbol,
            "Name": name,
        }

    with open('stock_list.json', 'w') as file:
        json.dump(most_traded_stocks, file, indent=4)

if __name__ == "__main__":
    fetch_most_traded_nasdaq_stocks()
