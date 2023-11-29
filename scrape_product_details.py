import os
import csv
import sys
import json
import pandas
import logging
import requests
import logging.handlers
import multiprocessing as mp
from datetime import datetime
from typing import List, Dict

def get_product_page_links() -> List[str]:
    csv_file_name = 'product_detail_links.csv'
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
        
def get_product_details(proxy: Dict, links: List[str]):
    for link in links:
        try:
            if proxy:
                response = requests.get(get_product_detail_link(link), proxies=proxy)
            else:
                response = requests.get(get_product_detail_link(link))
            content = json.loads(response.content)
            csv_file_name = "products.csv"

            with open(csv_file_name, 'a', newline='') as csv_file:
                fieldnames = [
                    'title', 
                    'summary', 
                    'description',
                    'unit_price',
                    'nectar_price',
                    'image_url',
                    'size',
                    'tags',
                    'last_updated' ]
                
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                if csv_file.tell() == 0:
                    writer.writeheader()

                product = content['products'][0]
                title = product['name'] if 'name' in product else None
                summary = product['description'][0] if 'description' in product else None
                description = ('\n').join(product['description']) if 'description' in product else None
                unit_price = product['unit_price']['price'] if 'unit_price' in product else None
                nectar_price = product['nectar_price']['retail_price'] if 'nectar_price' in product else None
                image_url = product['image'] if 'image' in product else None
                size = product['size'] if 'size' in product else None
                tags = (',').join([label['text'] for label in product['labels']]) if 'labels' in product else None
                
                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

                logging.info({
                    'title': title, 
                    'summary': summary,
                    'description': description,
                    'unit_price': unit_price,
                    'nectar_price': nectar_price,
                    'image_url': image_url,
                    'size': size,
                    'tags': tags,
                    'last_updated': now,
                    })
                
                writer.writerow({
                    'title': title, 
                    'summary': summary,
                    'description': description,
                    'unit_price': unit_price,
                    'nectar_price': nectar_price,
                    'image_url': image_url,
                    'size': size,
                    'tags': tags,
                    'last_updated': now,
                    })

        except Exception as e:
            logging.info(f'Error: {str(e)}')
        
def main(log_to_file: bool = False):
    try:
        logging.info("Starting...")

        csv_file_name = "products.csv"
        if os.path.exists(csv_file_name):
            os.remove(csv_file_name)

        if log_to_file:
            logging.basicConfig(
                format="[%(asctime)s] %(message)s",
                level=logging.INFO,
                handlers=[
                    logging.handlers.RotatingFileHandler(
                        "scrape_product_details_log.txt",
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

        product_page_links = get_product_page_links()

        proxies = [
            {
                'http': 'http://arthlo:7ujm&UJM@95.217.141.220:808',
                'https': 'http://arthlo:7ujm&UJM@95.217.141.220:808',
            }
        ]
        processes = [
            mp.Process(target=get_product_details, args=[None, product_page_links[:5]]),
            mp.Process(target=get_product_details, args=[proxies[0], product_page_links[5:10]]),
            # mp.Process(target=get_product_details, args=[product_page_links[40000:]])
            ]
        for process in processes: process.start()
        for process in processes: process.join()
    except KeyboardInterrupt:
        logging.info("Quitting...")
    except Exception as e:
        logging.warning(f"Exception: {str(e)}")
    finally:
        for process in processes: process.terminate()
        logging.info("Finished!")

if __name__ == '__main__':
    main()