import os
import csv
import sys
import math
import json
import time
import random
import pandas
import logging
import logging.handlers
from typing import List
import multiprocessing as mp
from bs4 import BeautifulSoup
from datetime import datetime
from selenium.webdriver import Remote, ChromeOptions
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection

def get_product_page_links() -> List[str]:
    csv_file_name = 'sainsburys_product_links.csv'
    links: List[str] = []
    try:
        if os.path.exists(csv_file_name):
            products = pandas.read_csv(csv_file_name)
            products.drop_duplicates(subset='Link', inplace=True)
            links.extend(products['Link'].values.tolist())
    except pandas.errors.EmptyDataError as e:
        logging.error(f"Error: {str(e)}")
    finally:
        return links

def get_product_detail_link(page_link: str) -> str:
    link = page_link.split('/product/details/', 1)[1]
    product_detail_link = f"https://www.sainsburys.co.uk/groceries-api/gol-services/product/v1/product?filter[product_seo_url]=gb/groceries/{link}&include[ASSOCIATIONS]=true&include[PRODUCT_AD]=citrus"
    return product_detail_link
 

class ProductScraper:
    _sbr_connection: ChromiumRemoteConnection
    _product_links: List[str]
    
    def __init__(self, sbr_connection: ChromiumRemoteConnection, product_links: List[str]) -> None:
        self._sbr_connection = sbr_connection
        self._product_links = product_links
        
    def scrape(self):
        chrome_options = ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        
        for link in self._product_links:
            time.sleep(random.choice([0.1, 0.15, 0.2, 0.25, 0.3]))
            try:
                with Remote(self._sbr_connection, options=chrome_options) as driver:
                    driver.get(get_product_detail_link(link))
                    html = driver.page_source
                    page = BeautifulSoup(html, "html5lib")
                    content = json.loads(page.pre.get_text())
                    csv_file_name = "sainsburys_products.csv"

                    with open(csv_file_name, 'a', newline='') as csv_file:
                        fieldnames = [
                            'source',
                            'title', 
                            'summary', 
                            'description',
                            'unit_price',
                            'nectar_price',
                            'average_rating',
                            'review_count',
                            'categories',
                            'product_url',
                            'image_url',
                            'size',
                            'tags',
                            'last_updated' ]
                        
                        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        if csv_file.tell() == 0:
                            writer.writeheader()

                        source = "Sainsburys"
                        product = content['products'][0]
                        title = product['name'] if 'name' in product else None
                        raw_description = product['description'] if 'description' in product else []
                        summary = raw_description[0] if len(raw_description) else ''
                        description = ('\n').join(raw_description)
                        unit_price = product['unit_price']['price'] if 'unit_price' in product else None
                        nectar_price = product['nectar_price']['retail_price'] if 'nectar_price' in product else None
                        image_url = product['image'] if 'image' in product else None
                        size = product['size'] if 'size' in product else None
                        tags = (',').join([label['text'] for label in product['labels']]) if 'labels' in product else None
                        reviews = product['reviews'] if 'reviews' in product else {"total": 0, "average_rating": 0}
                        average_rating = reviews["average_rating"] if 'average_rating' in reviews else 0
                        review_count = reviews["total"] if 'total' in reviews else 0
                        categories = ",".join([category["label"] for category in product["breadcrumbs"]] if 'breadcrumbs' in product else [])
                        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

                        logging.info({
                            'source': source,
                            'title': title, 
                            'summary': summary,
                            'description': description,
                            'unit_price': unit_price,
                            'nectar_price': nectar_price,
                            'average_rating': average_rating,
                            'review_count': review_count,
                            'categories': categories,
                            'product_url': link,
                            'image_url': image_url,
                            'size': size,
                            'tags': tags,
                            'last_updated': now,
                            })
                        
                        writer.writerow({
                            'source': source,
                            'title': title, 
                            'summary': summary,
                            'description': description,
                            'unit_price': unit_price,
                            'nectar_price': nectar_price,
                            'average_rating': average_rating,
                            'review_count': review_count,
                            'categories': categories,
                            'product_url': link,
                            'image_url': image_url,
                            'size': size,
                            'tags': tags,
                            'last_updated': now,
                            })

            except Exception as e:
                logging.info(f'Error: {str(e)}')
            
def run_product_scraper(log_to_file: bool = False):
    if log_to_file:
        logging.basicConfig(
            format="[%(asctime)s] %(message)s",
            level=logging.INFO,
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "product_scraper.log",
                    maxBytes=1024 * 1024 * 1024,
                    backupCount=10),
            ]
        )
    else:
        logging.basicConfig(
            format="[%(asctime)s] %(message)s",
            level=logging.INFO,
            handlers=[logging.StreamHandler(sys.stdout)]
        )

    processes: List[mp.Process] = []

    try:
        logging.info("Starting Product Scraper...")

        csv_file_name = "sainsburys_products.csv"
        if os.path.exists(csv_file_name):
            os.remove(csv_file_name)

        process_count = 6
        product_page_links = get_product_page_links()
        unit = math.floor(len(product_page_links) / process_count)

        SELENIUM_GRID_IP_ADDRESSES = [
            "95.217.141.220:9515",
            "65.109.54.105:9515",
            "65.21.132.89:9515",
        ]
        
        sbr_connections = [ChromiumRemoteConnection(f"http://{IP}", "goog", "chrome") for IP in SELENIUM_GRID_IP_ADDRESSES]

        processes = [
            mp.Process(target=ProductScraper(sbr_connections[i % len(SELENIUM_GRID_IP_ADDRESSES)], product_page_links[unit * i : ]).scrape)
            if i == process_count - 1
            else mp.Process(target=ProductScraper(sbr_connections[i % len(SELENIUM_GRID_IP_ADDRESSES)], product_page_links[unit * i : unit * (i + 1)]).scrape)
            for i in range(process_count)
        ]
        
        for process in processes: process.start()
        for process in processes: process.join()

    except KeyboardInterrupt:
        logging.info("Quitting...")
    except Exception as e:
        logging.warning(f"Exception: {str(e)}")
    finally:
        for process in processes: process.terminate()
        logging.info("Product Scraper: Finished!")

if __name__ == '__main__':
    run_product_scraper()