import sys
import json
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
from selenium.webdriver import Remote, ChromeOptions
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection

BASE_URL = "https://www.sainsburys.co.uk/shop"
AUTH = 'brd-customer-hl_4d0a14cd-zone-sainsburys:jx4uczrgz9p2'
SBR_WEBDRIVER = f'https://{AUTH}@brd.superproxy.io:9515'

def get_categories() -> List[str]:
    url = "https://www.sainsburys.co.uk/groceries-api/gol-services/product/categories/tree"
    response = requests.get(url)
    content = json.loads(response.content)
    categories = (f"{BASE_URL}/{category['s']}/seeall?fromMegaNav=1" for category in content["category_hierarchy"]["c"])    
    return categories

def get_category_products(connection: ChromiumRemoteConnection, category_url: str) -> List[str]:
    print(f"Category: {category_url}")
    with Remote(connection, options=ChromeOptions()) as driver:
        
        products: List[str] = []

        driver.get(category_url)

        # logging.info('Taking page screenshot to file page.png')
        # driver.get_screenshot_as_file('./page.png')
        # logging.info('Navigated! Scraping page content...')

        html = driver.page_source

        page = BeautifulSoup(html, 'html5lib')
        
        pagination = page.find('div', attrs={'class': 'pagination'})

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

                print(f"Scraping page content: {current_page_url}")
                driver.get(current_page_url)
                current_page = BeautifulSoup(driver.page_source, 'html5lib')
                if current_page.find('div', attrs={'class': 'pagination'}):
                    for product in current_page.findAll('div', attrs={'class': 'product'}):
                        try:
                            url = product.a['href']
                        except TypeError:
                            continue

                        if url: 
                            products.append(url)

        return products

def main(log_to_file: bool = False):
    try:
        print("Starting...")

        products: List[str] = []

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

        try:
            logging.info('Connecting to Scraping Browser...')
            sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, 'goog', 'chrome')
            logging.info('Connected!')
        except Exception as ex:
            logging.error("Scraping Browser connection failed due to ", str(ex))

        categories = get_categories()
        for category in list(categories)[:3]:
            category_products = get_category_products(sbr_connection, category)
            products += category_products
            print(len(category_products))

        for product in products:
            print(product)

        print("Finished!")
    except KeyboardInterrupt:
        logging.info("Quitting...")
    except Exception as ex:
        logging.warning("Exception")
        logging.warning(str(ex))

if (__name__ == '__main__'):
    main()