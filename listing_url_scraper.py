#!/usr/bin/env python3
"""
BizBuySell Listing URL Scraper
Scrapes listing URLs from BizBuySell's main listing pages
"""

import csv
import json
import logging
import os
import random

from fake_useragent import UserAgent
import time
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import requests

from dotenv import load_dotenv

load_dotenv()


class ListingURLScraper:
    """Scrapes listing URLs from BizBuySell's main listing pages"""

    def __init__(self):
        """Initialize the listing URL scraper"""
        self.url = "https://www.bizbuysell.com/businesses-for-sale/"
        self.logger = logging.getLogger(__name__)
        self.ua = UserAgent()

        timestamp = datetime.now().strftime("%Y%m%d")
        self.csv_filename = f"listing_urls_{timestamp}.csv"

        self.csv_headers = [
            'title', 'url', 'listing_id', 'scraped_date'
        ]
        self.recent_scrapped_listings_urls = []


    def _try_real_scraping(self) -> List[Dict]:
        """Try to scrape real listing URLs from BizBuySell"""

        listings = []
        PAGE_NUM = 1

        while PAGE_NUM < 2:
            try:
                self.logger.info(f"Scraping page {PAGE_NUM}...")

                session = requests.Session()
                response = requests.get(
                    "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25",
                    headers={"Authorization": f"Token {os.getenv('WEBSHARE_TOKEN')}"}
                )

                proxy_data = response.json()
                while True:
                    try:
                        # Extract a random proxy from the list
                        proxy_list = proxy_data.get("results", [])
                        if not proxy_list:
                            raise RuntimeError("No proxies returned from Webshare API")

                        proxy = random.choice(proxy_list)
                        proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['proxy_address']}:{proxy['port']}"

                        # Apply proxy to session
                        session.proxies.update({
                            "http": proxy_url,
                            "https": proxy_url
                        })

                        session.headers.update({
                            'User-Agent': self.ua.chrome
                        })

                        # Quick attempt with short timeout
                        response = session.get(self.url + f"{PAGE_NUM}", timeout=15)
                        if response.status_code != 200:
                            self.logger.error("retrying failed request to bizbuysell.com")
                            time.sleep(10)
                            continue
                        else:
                            break
                    except:
                        self.logger.error("retrying failed request to bizbuysell.com")
                        time.sleep(10)
                        continue
                response.raise_for_status()

                self.logger.info(f"Real scraping successful! Got {len(response.text)} characters")

                # Parse with BeautifulSoup
                soup = BeautifulSoup(response.text, 'lxml')

                # Look for JSON-LD data
                scripts = soup.find_all('script', type='application/ld+json')
                for script in scripts:
                    try:
                        data = json.loads(script.string)

                        if data.get('@type') == 'SearchResultsPage' and data.get('about'):
                            for item in data['about']:
                                if (item.get('@type') == 'ListItem' and
                                        item.get('item') and
                                        item['item'].get('@type') == 'Product'):
                                    listing = self._parse_listing_url(item['item'])
                                    if listing:
                                        listings.append(listing)
                    except InterruptedError as Ierr:
                        self.logger.error(f"❌ STOPPING SCRIPT: {Ierr}")
                        raise InterruptedError(Ierr)
                    except Exception as err:
                        continue

                if listings:
                    self.logger.info(f"Extracted {len(listings)} listing URLs from page {PAGE_NUM}")

                PAGE_NUM += 1
            except InterruptedError as Ierr:
                self.logger.error(f"❌ STOPPING SCRIPT: {Ierr}")
                raise InterruptedError(Ierr)
            except Exception as e:
                self.logger.warning(f"Real scraping failed: {str(e)}")
                break
        return listings

    def get_urls_only(self) -> List[Dict]:
        """Get URLs without saving to CSV or checking duplicates"""
        self.logger.info("Getting URLs without saving to CSV...")
        return self._try_real_scraping()

    def _parse_listing_url(self, item: Dict) -> Optional[Dict]:
        """Parse listing URL from JSON-LD data"""
        try:
            title = item.get('name', '')
            url = item.get('url', '')
            product_id = item.get('productId', '')

            if url in self.recent_scrapped_listings_urls:
                raise InterruptedError(f"URL {url} has been scrapped recently")

            if title and url:
                return {
                    'title': title.strip() if title else '',
                    'url': url,
                    'listing_id': product_id,
                    'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
        except InterruptedError as Ierr:
            self.logger.error(f"❌ STOPPING SCRIPT: {Ierr}")
            raise InterruptedError(Ierr)
        except Exception as err:
            self.logger.info(f"ERROR PARSING LISTING URL: {err}")

        return {}

    def _load_existing_urls(self) -> None:
        """Load existing URLs from CSV"""
        rows = []

        if os.path.exists(self.csv_filename):
            try:
                with open(self.csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    csv_list = list(reader)

                    for row in csv_list:
                        rows.append(row)
                    self.logger.info(f"Loaded {len(rows)} existing URLs from CSV")

            except Exception as e:
                self.logger.warning(f"Error loading existing URLs: {str(e)}")

        self.recent_scrapped_listings_urls = set(row.get('url') for row in rows)

    def _save_listings_to_csv(self, listings: List[Dict]) -> int:
        """Save new listing URLs to CSV"""
        new_count = 0

        try:
            with open(self.csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_headers)

                for listing in listings:
                    if listing.get('url'):
                        # Clean data for CSV
                        clean_listing = {}
                        for key in self.csv_headers:
                            value = listing.get(key, '')
                            if value is None:
                                clean_listing[key] = ''
                            elif isinstance(value, str):
                                clean_listing[key] = value.replace('\n', ' ').replace('\r', ' ').strip()
                            else:
                                clean_listing[key] = value

                        writer.writerow(clean_listing)
                        new_count += 1

        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")

        return new_count

    def scrape_listing_urls(self) -> int:
        """Scrape listing URLs from BizBuySell"""
        self.logger.info("🚀 Starting listing URL scraper...")

        # Load existing URLs
        self._load_existing_urls()

        all_listings = []

        real_listings = self._try_real_scraping()
        if real_listings:
            all_listings.extend(real_listings)
            self.logger.info(f"✅ Got {len(real_listings)} listing URLs")
        else:
            self.logger.info("❌ Scraping failed")

        if not all_listings:
            self.logger.error("No listing URLs generated")
            return 0

        # Save all listing URLs
        new_count = self._save_listings_to_csv(all_listings)

        self.logger.info(f"🎉 Listing URL scraping completed!")
        self.logger.info(f"   New URLs saved: {new_count}")

        return new_count

    def get_stats(self) -> Dict:
        """Get scraping statistics"""
        stats = {
            'csv_file': self.csv_filename,
            'total_urls': 0,
            'file_size': 0,
            'last_modified': None,
            'sample_urls': []
        }

        if os.path.exists(self.csv_filename):
            file_stat = os.stat(self.csv_filename)
            stats['file_size'] = file_stat.st_size
            stats['last_modified'] = datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

            try:
                with open(self.csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    rows = list(reader)
                    stats['total_urls'] = len(rows)
                    stats['sample_urls'] = rows[-5:] if rows else []
            except Exception as e:
                self.logger.warning(f"Error reading CSV stats: {str(e)}")

        return stats


def main():
    """Main function for the listing URL scraper"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        # Initialize scraper
        scraper = ListingURLScraper()

        # Run listing URL scraping
        new_urls = scraper.scrape_listing_urls()

        # Get statistics
        stats = scraper.get_stats()

        print(f"\n📊 LISTING URL SCRAPING RESULTS:")
        print(f"   New URLs saved: {new_urls}")
        print(f"   CSV file: {stats['csv_file']}")

        if stats['sample_urls']:
            print(f"\n📋 Sample URLs:")
            for i, url_data in enumerate(stats['sample_urls'], 1):
                print(f"   {i}. {url_data.get('title', 'No title')}")
                print(f"      URL: {url_data.get('url', 'No URL')}")
                print(f"      ID: {url_data.get('listing_id', 'No ID')}")
                print()

        print(f"🎉 SUCCESS! URL scraper completed with {new_urls} new URLs!")
        print(f"📁 Data saved to: {stats['csv_file']}")

        return True

    except Exception as e:
        print(f"❌ URL scraper failed: {str(e)}")
        logging.error("URL scraper failed", exc_info=True)
        return False


if __name__ == "__main__":
    success = main() 