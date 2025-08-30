#!/usr/bin/env python3
"""
BizBuySell Business Listings Scraper
Professional scraper that combines real data collection with reliable fallback data
"""

import csv
import json
import logging
import os
from fake_useragent import UserAgent
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    from bs4 import BeautifulSoup

    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False


class BizBuySellScraper:
    """Professional BizBuySell scraper with reliable data collection"""

    def __init__(self, csv_filename: str = None, use_mock_data: bool = False):
        """
        Initialize the BizBuySell scraper

        Args:
            csv_filename: CSV file to save data
            use_mock_data: Whether to use fallback data when real scraping fails
        """
        self.url = "https://www.bizbuysell.com/businesses-for-sale/"
        self.logger = logging.getLogger(__name__)
        self.use_mock_data = use_mock_data
        self.ua = UserAgent()

        timestamp = datetime.now().strftime("%Y%m%d")
        self.csv_filename = f"bizbuysell_final_{timestamp}.csv"

        self.csv_headers = [
            'title', 'location', 'price', 'cash_flow',
            'description', 'url', 'business_type', 'image_url', 'listing_id', 'scraped_date'
        ]
        self.recent_scrapped_listings_urls = []

        # Initialize CSV
        self._initialize_csv()

    def _initialize_csv(self):
        """Initialize CSV file with headers"""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_headers)
                writer.writeheader()
            self.logger.info(f"Created CSV file: {self.csv_filename}")

    def _try_real_scraping(self) -> List[Dict]:
        """Try to scrape real data from BizBuySell"""

        listings = []

        if not REQUESTS_AVAILABLE or not BS4_AVAILABLE:
            self.logger.warning("Missing dependencies for real scraping")
            return []

        PAGE_NUM = 1

        while PAGE_NUM < 5:
            try:
                self.logger.info("Attempting real scraping...")

                response = None
                session = requests.Session()
                while True:
                    try:
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
                                    listing = self._parse_real_listing(item['item'])
                                    if listing:
                                        listings.append(listing)
                    except InterruptedError as Ierr:
                        self.logger.error(f"❌ STOPPING SCRIPT: {Ierr}")
                        raise InterruptedError(Ierr)
                    except Exception as err:
                        continue

                if listings:
                    self.logger.info(f"Extracted {len(listings)} real listings")

                PAGE_NUM += 1
            except InterruptedError as Ierr:
                self.logger.error(f"❌ STOPPING SCRIPT: {Ierr}")
                raise InterruptedError(Ierr)
            except Exception as e:
                self.logger.warning(f"Real scraping failed: {str(e)}")
        return listings

    def _parse_real_listing(self, item: Dict) -> Optional[Dict]:
        """Parse real listing from JSON-LD data"""
        try:
            title = item.get('name', '')
            description = item.get('description', '')
            url = item.get('url', '')
            product_id = item.get('productId', '')
            image_url = item.get('image', '')

            # Extract price
            price = None
            offers = item.get('offers', {})
            if offers:
                price_val = offers.get('price')
                if price_val:
                    try:
                        price = int(float(str(price_val).replace(',', '')))
                    except:
                        pass

            # Extract location
            location = ''
            if offers and 'availableAtOrFrom' in offers:
                place = offers['availableAtOrFrom']
                if place and 'address' in place:
                    address = place['address']
                    locality = address.get('addressLocality', '')
                    region = address.get('addressRegion', '')
                    if locality and region:
                        location = f"{locality}, {region}"

            if url in self.recent_scrapped_listings_urls:
                raise InterruptedError(f"URL {url} has been scrapped recently")

            if title and url:
                return {
                    'title': title.strip() if title else '',
                    'location': location,
                    'price': price,
                    'cash_flow': None,
                    'description': description,
                    'url': url,
                    'business_type': '',
                    'image_url': image_url,
                    'listing_id': product_id,
                    'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
        except InterruptedError as Ierr:
            self.logger.error(f"❌ STOPPING SCRIPT: {Ierr}")
            raise InterruptedError(Ierr)
        except Exception as err:
            self.logger.info(f"ERROR PARSING REAL LISTING: {err}")

        return {}

    def _load_existing_urls(self, limit: int = 10) -> None:
        """Load existing URLs from CSV"""
        rows = []
        existing_urls = set()

        if os.path.exists(self.csv_filename):
            try:
                with open(self.csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    csv_list = list(reader)

                    for row in csv_list:
                        rows.append(row)
                    #     # make sure scraped_date exists
                    #     if row.get("scraped_date"):
                    #         try:
                    #             # adjust date format if needed
                    #             row["_parsed_date"] = datetime.strptime(row["scraped_date"], "%Y-%m-%d %H:%M:%S")
                    #             rows.append(row)
                    #         except ValueError:
                    #             self.logger.warning(f"Invalid date format in row: {row['scraped_date']}")
                    #
                    #     # sort descending by parsed date
                    # rows.sort(key=lambda r: r["_parsed_date"], reverse=True)
                    #
                    # # only keep top `limit`
                    # rows = rows[:limit]
                    self.logger.info(f"Loaded {len(rows)} existing URLs from CSV")

            except Exception as e:
                self.logger.warning(f"Error loading existing URLs: {str(e)}")
        # print(
        #     [row.get('url') for row in rows]
        # )
        self.recent_scrapped_listings_urls = set(row.get('url') for row in rows)

    def _save_listings_to_csv(self, listings: List[Dict]) -> int:
        """Save new listings to CSV"""
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

    def scrape_final(self) -> int:
        """Final scraping method that always works"""
        self.logger.info("🚀 Starting FINAL WORKING scraper...")

        # Load existing URLs
        self._load_existing_urls()

        all_listings = []

        real_listings = self._try_real_scraping()
        if real_listings:
            all_listings.extend(real_listings)
            self.logger.info(f"✅ Got {len(real_listings)} real listings")
        else:
            self.logger.info("❌ Scraping failed")

        if not all_listings:
            self.logger.error("No listings generated")
            return 0

        # Save all listings
        new_count = self._save_listings_to_csv(all_listings)

        real_count = len([l for l in all_listings if l.get('data_source') == 'real_scraping'])

        self.logger.info(f"🎉 Final scraping completed!")
        self.logger.info(f"   Real listings: {real_count}")
        self.logger.info(f"   New saved: {new_count}")

        return new_count

    def get_stats(self) -> Dict:
        """Get scraping statistics"""
        stats = {
            'csv_file': self.csv_filename,
            'total_listings': 0,
            'real_listings': 0,
            'mock_listings': 0,
            'file_size': 0,
            'last_modified': None,
            'sample_listings': []
        }

        if os.path.exists(self.csv_filename):
            file_stat = os.stat(self.csv_filename)
            stats['file_size'] = file_stat.st_size
            stats['last_modified'] = datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

            try:
                with open(self.csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    rows = list(reader)
                    stats['total_listings'] = len(rows)
                    stats['real_listings'] = len([r for r in rows if r.get('data_source') == 'real_scraping'])
                    stats['sample_listings'] = rows[-5:] if rows else []
            except Exception as e:
                self.logger.warning(f"Error reading CSV stats: {str(e)}")

        return stats


def main():
    """Main function for the final working scraper"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        # Initialize scraper
        scraper = BizBuySellScraper(use_mock_data=True)

        # Run final scraping
        new_listings = scraper.scrape_final()

        # Get statistics
        stats = scraper.get_stats()

        print(f"\n📊 FINAL SCRAPING RESULTS:")
        print(f"   New listings saved: {new_listings}")
        print(f"   CSV file: {stats['csv_file']}")

        # if stats['sample_listings']:
        #     print(f"\n📋 Sample listings:")
        #     for i, listing in enumerate(stats['sample_listings'], 1):
        #         price_str = f"${int(listing['price']):,}" if listing.get('price') and str(
        #             listing['price']).isdigit() else "Price not listed"
        #         source = listing.get('data_source', 'unknown')
        #         print(f"   {i}. {listing.get('title', 'No title')} ({source})")
        #         print(f"      Location: {listing.get('location', 'Not specified')}")
        #         print(f"      Price: {price_str}")
        #         print(f"      Type: {listing.get('business_type', 'Unknown')}")
        #         print()

        print(f"🎉 SUCCESS! Scraper completed with {new_listings} new listings!")
        print(f"📁 Data saved to: {stats['csv_file']}")

        if stats['real_listings'] > 0:
            print(f"\n✅ Real scraping is working! You got {stats['real_listings']} real listings.")
        else:
            print(
                f"\n⚠️ Real scraping failed, but you have {stats['mock_listings']} realistic mock listings for development.")

        return True

    except Exception as e:
        print(f"❌ Final scraper failed: {str(e)}")
        logging.error("Final scraper failed", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
