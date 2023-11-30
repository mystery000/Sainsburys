import sys
import logging
import asyncio
import logging.handlers
from datetime import datetime
from category_scraper import run_category_scraper
from product_scraper import run_product_scraper

class Watcher():
    def __init__(self):
        self._last_check = datetime.now()

    def get_scheduled_time(self) -> datetime:
        with open("watcher.txt", "rt") as fp:
            parts = fp.read().strip().split(':')
        now = datetime.now()
        return datetime(now.year, now.month, now.day, int(parts[0]), int(parts[1]), 0)

    def check_schedule(self) -> bool:
        rtime = self.get_scheduled_time()
        now = datetime.now()
        
        if self._last_check < rtime and now > rtime:
            self._last_check = now
            return True
        
        return False

async def run():
    watcher = Watcher()
    while True:
        if watcher.check_schedule():
            run_category_scraper()
            await asyncio.sleep(10)
            run_product_scraper()

        await asyncio.sleep(0.1)

def main(log_to_file: bool = False):
    if log_to_file:
        logging.basicConfig(
            format="[%(asctime)s] %(message)s",
            level=logging.INFO,
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "sainsburys_scraper.log",
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

    logging.info("Starting Sainsburys scraper...")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        logging.info("Quitting Sainsburys Scraper...")
    except Exception as e:
        logging.warning(f"Exception: {str(e)}")
    finally:
        logging.info("Sainsburys Scraper: Finished!")
        
    loop.close()
    asyncio.set_event_loop(None)

if (__name__ == '__main__'):
    main()
    
