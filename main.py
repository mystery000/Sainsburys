import os
import sys
import csv
import json
import logging
import requests
import pandas as pd
import logging.handlers
from typing import List
import multiprocessing as mp
from bs4 import BeautifulSoup
from selenium.webdriver import Remote, ChromeOptions
from urllib.parse import urlparse, parse_qs, urlunparse
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection

BASE_URL = "https://www.sainsburys.co.uk/shop"
AUTH = 'brd-customer-hl_6c86c9b0-zone-scraping_browser:ynhgmc04x4i5'
SBR_WEBDRIVER = f'https://{AUTH}@brd.superproxy.io:9515'

def get_categories() -> List[str]:
    url = "https://www.sainsburys.co.uk/groceries-api/gol-services/product/categories/tree"
    response = requests.get(url)
    content = json.loads(response.content)
    categories = (f"{BASE_URL}/{category['s']}/seeall?fromMegaNav=1" for category in content["category_hierarchy"]["c"])    
    return list(categories)

class CategoryScraper():
    _queue: mp.Queue
    _sbr_connection: ChromiumRemoteConnection
    _categories: str

    def __init__(self, queue: mp.Queue, sbr_connection: ChromiumRemoteConnection, categories: List[str]) -> None:
        self._queue = queue
        self._sbr_connection = sbr_connection
        self._categories = categories

    def get_category_products(self, category_url: str) -> List[str]:
        _products: List[str] = []

        print(f"Category: {category_url}\n")

        try:
            with Remote(self._sbr_connection, options=ChromeOptions()) as driver:
                driver.get(category_url)

                html = driver.page_source

                page = BeautifulSoup(html, 'html5lib')
                
                pagination = page.find('div', class_='pagination')

                if pagination:
                    # Get the total page number
                    next = pagination.find('li', class_='next')
                    previous = next.find_previous_sibling('li')
                    last_page_number = int(previous.find_all('span')[1].text)
                    parsed_url = urlparse(previous.a['href'])
                    query_params = parse_qs(parsed_url.query, keep_blank_values=True)

                    # Iterate pagination
                    for page_number in range(0, last_page_number):
                        query_params['beginIndex'] = [str(60 * page_number)]
                        modified_query = '&'.join([f"{k}={v[0]}" for k, v in query_params.items()])
                        current_page_url = urlunparse(parsed_url._replace(query=modified_query))

                        print(f"Scraping page content[{os.getpid()}]: {current_page_url}")

                        response = requests.get(current_page_url)
                        current_page = BeautifulSoup(response.content, 'html5lib')

                        if current_page.html.find('div', class_='pagination'):
                            for product in current_page.html.findAll('div', class_='product'):
                                try:
                                    url = product.find('div', class_='productInfo').a['href']
                                except TypeError:
                                    continue

                                if url: 
                                    _products.append(url)

        except Exception as e:
            print(f'Exception: {str(e)}')
        
        finally :
            return _products
    
    def run(self) -> List[str]:
        for category in self._categories:
            category_products = self.get_category_products(category)
            csv_file_name = "product_detail_links.csv"
            start_index = 1
            try:
                if os.path.exists(csv_file_name):
                    df = pd.read_csv(csv_file_name)
                    start_index = df['Id'].iloc[-1] + 1  # Get the last index and increment by 1
            except pd.errors.EmptyDataError:
                pass
        
            with open(csv_file_name, 'a', newline='') as csv_file:
                fieldnames = ['Id', 'Link']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                if csv_file.tell() == 0:
                    writer.writeheader()
                for i, product in enumerate(category_products, start=start_index):
                    writer.writerow({'Id': i, 'Link': product})

def main(log_to_file: bool = False):
    csv_file_name = "product_detail_links.csv"
    if os.path.exists(csv_file_name):
        os.remove(csv_file_name)

    queue = mp.Queue(maxsize=100)

    try:
        print("Starting...")
        if log_to_file:
            logging.basicConfig(
                format="[%(asctime)s] %(message)s",
                level=logging.INFO,
                handlers=[
                    logging.handlers.RotatingFileHandler(
                        "logs.txt",
                        maxBytes=1024 * 1024,
                        backupCount=10),
                ]
            )
        else:
            logging.basicConfig(
                format="[%(asctime)s] %(message)s",
                level=logging.INFO,
                handlers=[logging.StreamHandler(sys.stdout)]
            )

        categories = get_categories()

        try:
            print(f'Connecting to Scraping Browser: {SBR_WEBDRIVER} ...')
            sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, 'goog', 'chrome')
            print('Connected!')
            processes = [
                mp.Process(target=CategoryScraper(queue, sbr_connection, categories[:6]).run), 
                mp.Process(target=CategoryScraper(queue, sbr_connection, categories[6:12]).run),
                mp.Process(target=CategoryScraper(queue, sbr_connection, categories[12:18]).run),
                ]
            for process in processes: process.start()
            for process in processes: process.join()

        except Exception as e:
            logging.error(f"Scraping Browser connection failed due to {str(e)}")

    except KeyboardInterrupt:
        print("Quitting...")
    except Exception as e:
        logging.warning(f"Exception: {str(e)}")

    print("Finished!")

if (__name__ == '__main__'):
    main()