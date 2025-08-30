#!/usr/bin/env python3
"""
BizBuySell Listing Detail Scraper
Scrapes detailed information from individual BizBuySell listing pages
"""

import csv
import json, random
import logging
import os
import re
from fake_useragent import UserAgent
import time
import asyncio
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import requests

from seleniumbase import Driver
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import ChromiumDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from helpers.actions import *
from templates.extension import proxies
from pymongo import MongoClient, UpdateOne
import openai

load_dotenv()

class ListingDetailScraper:
    """Scrapes detailed information from individual BizBuySell listing pages"""

    def __init__(self, url_csv_filename: str = None, max_concurrent: int = 5):
        """
        Initialize the listing detail scraper

        Args:
            url_csv_filename: CSV file containing listing URLs to scrape
            max_concurrent: Maximum number of concurrent requests (default: 5)
        """
        self.logger = logging.getLogger(__name__)
        self.ua = UserAgent()
        self.max_concurrent = max_concurrent

        timestamp = datetime.now().strftime("%Y%m%d")
        self.detail_csv_filename = f"listing_details_{timestamp}.csv"
        
        # MongoDB setup
        self.mongo_uri = "mongodb+srv://trentlee702:pHHJETqffYMINqbN@bizbuysell-cluster.homaclh.mongodb.net/"
        self.mongo_db_name = "business-broker-las-vegas-db"
        self.mongo_collection_name = "bizbuysell-data"
        self.mongo_client = MongoClient(self.mongo_uri)
        self.mongo_db = self.mongo_client[self.mongo_db_name]
        self.mongo_col = self.mongo_db[self.mongo_collection_name]

        # OpenAI setup
        self.openai_client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

        # Predefined categories for classification
        self.categories = [
            "Agriculture", "Automotive", "Boat", "Beauty", "Personal Care",
            "Building", "Construction", "Communication", "Media", "Education",
            "Children", "Entertainment", "Recreation", "Financial Services",
            "Health Care", "Fitness", "Manufacturing", "Non-classifiable Establishments",
            "Online", "Technology", "Pet Services", "Restaurants", "Food",
            "Retail", "Service Businesses", "Real Estate"
        ]

        self.csv_headers = [
            'title', 'location', 'asking_price', 'gross_revenue', 'established', 
            'cashflow', 'description', 'url', 'category', 'original_category', 'listing_id',
            'broker_name', 'broker_profile', 'broker_number', 'scraped_date'
        ]
        self.recent_scrapped_listings_urls = []

        self.proxy_pwd = ''
        self.proxy_user = ''
        self.proxy_host = ''
        self.proxy_port = ''

        self._set_proxy()

        # Initialize Selenium driver
        self.driver = self._create_driver()

    def __del__(self):
        """Cleanup Selenium driver on object destruction"""
        if self.driver:
            try:
                self.driver.close()
                self.driver.quit()
            except:
                pass

    def _set_proxy(self):
        try:
            response = requests.get(
                "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&valid=true&page_size=25",
                headers={"Authorization": "Token rckp1i7bwo0qgi1zd2f4q6scm7wpcxtvk587926t"}
            )
            proxy_data = response.json()
            # print(proxy_data)
            chosen_proxy = random.choice(proxy_data['results'])
            self.proxy_pwd = chosen_proxy['password']
            self.proxy_user = chosen_proxy['username']
            self.proxy_host = chosen_proxy['proxy_address']
            self.proxy_port = chosen_proxy['port']
        except Exception as err:
            print("error setting proxy: ", err)

    def _find_latest_url_csv(self) -> str:
        """Find the most recent listing URLs CSV file"""
        csv_files = [f for f in os.listdir('.') if f.startswith('listing_urls_') and f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("No listing URLs CSV file found. Run listing_url_scraper.py first.")
        
        # Sort by modification time and get the most recent
        csv_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        return csv_files[0]

    def _initialize_csv(self):
        """Initialize CSV file with headers"""
        if not os.path.exists(self.detail_csv_filename):
            with open(self.detail_csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_headers)
                writer.writeheader()
            self.logger.info(f"Created CSV file: {self.detail_csv_filename}")

    def _load_existing_details(self) -> None:
        """Load existing scraped URLs to avoid duplicates"""
        rows = []

        if os.path.exists(self.detail_csv_filename):
            try:
                with open(self.detail_csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    csv_list = list(reader)

                    for row in csv_list:
                        rows.append(row)
                    self.logger.info(f"Loaded {len(rows)} existing details from CSV")

            except Exception as e:
                self.logger.warning(f"Error loading existing details: {str(e)}")

        self.recent_scrapped_listings_urls = set(row.get('url') for row in rows)

    def process_urls_directly(self, urls_to_process: List[Dict]) -> List:
        """Process URLs directly without loading existing details"""
        self.logger.info("🚀 Starting direct URL processing with Selenium...")
        
        # Initialize empty set for recent URLs to avoid duplicates within this session
        self.recent_scrapped_listings_urls = set()
        
        # Use threaded processing since Selenium doesn't work well with asyncio
        return self._process_urls_threaded(urls_to_process)

    def _process_urls_threaded(self, urls_to_process: List[Dict]) -> List:
        """Process URLs using threading with Selenium"""
        self.logger.info(f"🚀 Starting threaded processing of {len(urls_to_process)} URLs with Selenium...")
        
        all_details = []
        successful_count = 0
        failed_count = 0
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submit all tasks
            future_to_url = {executor.submit(self._scrape_listing_detail_selenium, url_data): url_data 
                           for url_data in urls_to_process}
            
            # Process completed tasks
            for future in future_to_url:
                try:
                    detail = future.result()
                    if detail:
                        all_details.append(detail)
                        successful_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    self.logger.error(f"Error processing URL: {e}")
                    failed_count += 1
        
        if not all_details:
            self.logger.error("No details generated")
            return 0

        # Save all details to MongoDB
        new_count = self._save_details_to_mongo(all_details)

        self.logger.info(f"🎉 Threaded URL processing completed!")
        self.logger.info(f"   Successful: {successful_count}")
        self.logger.info(f"   Failed: {failed_count}")
        # self.logger.info(f"   Upserted/Modified in Mongo: {new_count}")

        return all_details

    def _create_driver(self) -> webdriver.Chrome:
        """Create a new Chrome driver instance for a thread"""
        try:
            proxies_extension_path = proxies(self.proxy_user, self.proxy_pwd, self.proxy_host, self.proxy_port)
            print(f"{self.proxy_user}:{self.proxy_pwd}@{self.proxy_host}:{self.proxy_port}")
            driver = Driver(
                browser="chrome",
                uc=True,
                headless2=True,
                incognito=True,
                agent=self.ua.chrome,
                do_not_track=True,
                chromium_arg=["--disable-features=DisableLoadExtensionCommandLineSwitch"],
                disable_features="DisableLoadExtensionCommandLineSwitch",
                proxy=f"{self.proxy_user}:{self.proxy_pwd}@{self.proxy_host}:{self.proxy_port}",
                # extension_dir=proxies_extension_path,
                undetectable=True,
                ad_block_on=True
            )
            return driver
        except Exception as e:
            self.logger.error(f"Failed to create driver for thread: {e}")
            return None

    def reset_driver(self):
        try:
            self.driver.close()
            self.driver.quit()
            self._set_proxy()
            self.driver = self._create_driver()
        except:
            pass

    def _scrape_listing_detail_selenium(self, url_data: Dict) -> Optional[Dict]:
        """Scrape detailed information from a single listing page using Selenium"""
        url = url_data.get('url', '')
        title = url_data.get('title', '')
        listing_id = url_data.get('listing_id', '')

        if not url:
            return None

        if url in self.recent_scrapped_listings_urls:
            self.logger.info(f"Skipping already scraped URL: {url}")
            return None

        max_trials = 2
        trials = 1

        while trials <= max_trials:

            if not self.driver:
                return None

            try:
                self.logger.info(f"Scraping detail for: {title[:50]}...")

                # Navigate to the listing page
                self.driver.get(url)

                # Wait for the page to load and ensure the main content is available
                try:
                    access_denied_txt = get_element_text(self.driver, "//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'access denied')]")
                    if access_denied_txt:
                        print("access_denied_txt found!: ", access_denied_txt)
                        self.reset_driver()
                        continue
                    get_element(self.driver, "//span[contains(@class, 'f-l')]")
                except:
                    return None

                # Extract data using the specified selectors
                detail_data = self._extract_detail_data(self.driver.page_source, url_data)

                if detail_data:
                    self.logger.info(f"Successfully scraped detail for: {title[:50]}")
                    return detail_data
                else:
                    bid_post = get_element(self.driver, "//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'starting bid')]")
                    if bid_post:
                        return detail_data
                    self.logger.warning(f"Failed to extract data for: {title[:50]}")
                    self.reset_driver()
                    continue

            except TimeoutException as e:
                self.logger.error(f"Timeout waiting for page to load or elements to be present for {url}: {e}")
                self.reset_driver()
                continue
            except WebDriverException as e:
                self.logger.error(f"WebDriver error scraping {url}: {e}")
                self.reset_driver()
            except Exception as e:
                self.logger.error(f"Error scraping {url}: {str(e)}")
                self.reset_driver()
                continue
            finally:
                trials += 1

    def _extract_detail_data(self, html_content: str, url_data: Dict) -> Optional[Dict]:
        """Extract detailed data from the listing page"""
        try:
            # Create BeautifulSoup object from HTML content
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Extract JSON-LD data first
            json_data = self._extract_json_ld_data(soup)
            
            # Extract data using CSS selectors
            location = self._extract_location(soup)
            broker_number = self._extract_broker_number(soup)
            gross_revenue = self._extract_gross_revenue(soup)
            cashflow = self._extract_cashflow(soup)

            self.logger.info(f"asking_price: {json_data.get('asking_price')}")
            self.logger.info(f"broker_name: {json_data.get('broker_name')}")
            self.logger.info(f"location: {location}")

            # Combine all data
            if json_data.get('asking_price') and (location):
                # Get the original category from JSON-LD
                original_category = json_data.get('category', '')
                title = json_data.get('title', url_data.get('title', ''))
                description = json_data.get('description', '')
                
                # Use OpenAI to categorize the listing
                ai_categories = self.categorize_listing(original_category)
                location = self.ai_extract_city_and_state(location)
                
                detail_data = {
                    'title': title,
                    'city': location.get('city', ''),
                    'state': location.get('state', ''),
                    'asking_price': json_data.get('asking_price'),
                    'gross_revenue': gross_revenue,
                    'established': json_data.get('established'),
                    'cashflow': cashflow,
                    'description': description,
                    'url': url_data.get('url', ''),
                    'category': ai_categories,  # Use AI-categorized categories
                    'original_category': original_category,  # Keep original for reference
                    'listing_id': json_data.get('listing_id', url_data.get('listing_id', '')),
                    'broker_name': json_data.get('broker_name', ''),
                    'broker_profile': json_data.get('broker_profile', ''),
                    'broker_number': broker_number,
                    'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
                self.logger.info(detail_data)
                return detail_data
            return None
        except Exception as e:
            self.logger.error(f"Error extracting detail data: {str(e)}")
            return None

    def _extract_json_ld_data(self, soup: BeautifulSoup) -> Dict:
        """Extract data from JSON-LD scripts"""
        json_data = {}
        
        try:
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    data = json.loads(script.string.strip().replace('\n', '').replace('\t', ''))

                    if data.get('@type') == 'Product':
                        # Extract title
                        json_data['title'] = data.get('name', '')

                        # Extract description
                        json_data['description'] = data.get('description', '')
                        
                        # Extract price
                        offers = data.get('offers', {})
                        if offers:
                            broker = offers.get('offeredBy', {})
                            if broker:
                                broker_name = broker.get('name', '')
                                if not broker_name:
                                    broker_name_selector = soup.select_one('.broker-card > div')
                                    if broker_name_selector:
                                        broker_name = broker_name_selector.get_text(strip=True)
                                json_data['broker_name'] = broker_name.replace('Business Listed By:', '')
                                json_data['broker_profile'] = broker.get('url', '')

                            price_val = offers.get('price')
                            if price_val:
                                try:
                                    json_data['asking_price'] = price_val
                                except:
                                    pass
                        
                        # Extract business type
                        category = data.get('category', '')
                        if category:
                            json_data['category'] = category
                        
                        # Extract listing ID
                        product_id = data.get('productId', '')
                        if product_id:
                            json_data['listing_id'] = product_id
                        
                        break
                        
                except Exception as e:
                    print(e)
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Error extracting JSON-LD data: {str(e)}")
            
        return json_data

    def _extract_location(self, soup: BeautifulSoup) -> str:
        """Extract location using CSS selector"""
        try:
            location_elements = soup.select("span.f-l")
            if location_elements:
                location_text = location_elements[0].get_text(strip=True)
                return location_text
        except Exception as e:
            self.logger.warning(f"Error extracting location: {str(e)}")
        return ''

    def _extract_broker_number(self, soup: BeautifulSoup) -> str:
        """Extract broker number using CSS selector"""
        try:
            broker_elements = soup.select("span.ctc_phone a span")
            if broker_elements:
                broker_text = broker_elements[0].get_text(strip=True)
                return broker_text
        except Exception as e:
            self.logger.warning(f"Error extracting broker number: {str(e)}")
        return ''

    def _extract_gross_revenue(self, soup: BeautifulSoup) -> str:
        """Extract gross revenue using CSS selector"""
        try:
            elements = soup.select("p.help span.g4")
            if len(elements) >= 3:
                third_text = elements[2].get_text(strip=True)
                return third_text
        except Exception as e:
            self.logger.warning(f"Error extracting gross revenue: {str(e)}")
        return ''

    def _extract_cashflow(self, soup: BeautifulSoup) -> str:
        """Extract cashflow using CSS selector"""
        try:
            elements = soup.select("p.help span.g4")
            if len(elements) >= 2:
                second_text = elements[1].get_text(strip=True)
                return second_text
        except Exception as e:
            self.logger.warning(f"Error extracting cashflow: {str(e)}")
        return ''

    def _save_details_to_csv(self, details: List[Dict]) -> int:
        """Save new listing details to CSV"""
        new_count = 0

        try:
            with open(self.detail_csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_headers)

                for detail in details:
                    if detail.get('url'):
                        # Clean data for CSV
                        clean_detail = {}
                        for key in self.csv_headers:
                            value = detail.get(key, '')
                            if value is None:
                                clean_detail[key] = ''
                            elif isinstance(value, str):
                                clean_detail[key] = value.replace('\n', ' ').replace('\r', ' ').strip()
                            else:
                                clean_detail[key] = value

                        writer.writerow(clean_detail)
                        new_count += 1

        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")

        return new_count

    def _save_details_to_mongo(self, details: List[Dict]) -> int:
        """Save new listing details to MongoDB with upsert semantics"""
        if not details:
            return 0
        try:
            ops = []
            for d in details:
                # Use listing_id primarily; fallback to url
                key = {'listing_id': d.get('listing_id')} if d.get('listing_id') else {'url': d.get('url')}
                ops.append(UpdateOne(key, {'$set': d}, upsert=True))
            if ops:
                result = self.mongo_col.bulk_write(ops, ordered=False)
                # inserted_count is not available on BulkWriteResult; estimate via upserted_ids
                upserts = len(result.upserted_ids) if result.upserted_ids else 0
                modified = result.modified_count or 0
                return upserts + modified
            return 0
        except Exception as e:
            self.logger.error(f"Error saving to MongoDB: {e}")
            return 0

    def get_stats(self) -> Dict:
        """Get scraping statistics from MongoDB"""
        stats = {
            'collection': self.mongo_collection_name,
            'database': self.mongo_db_name,
            'total_details': 0,
            'last_inserted_at': None,
            'sample_details': []
        }
        try:
            stats['total_details'] = self.mongo_col.count_documents({})
            # Get last 3 inserted/updated by scraped_date
            cursor = self.mongo_col.find({}, {'_id': 0}).sort('scraped_date', -1).limit(3)
            stats['sample_details'] = list(cursor)
            if stats['sample_details']:
                stats['last_inserted_at'] = stats['sample_details'][0].get('scraped_date')
        except Exception as e:
            self.logger.warning(f"Error reading Mongo stats: {str(e)}")
        return stats

    def ai_extract_city_and_state(self, location: str) -> Dict:
        try:
            prompt = f"""
            Extract the state and city from this location: {location}.
            
            Do not return abbreviations. 
            Return your output as a JSON like this:
            {{"state": "oklahoma", "city": "oklahoma city"}}
            """
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system",
                     "content": "You are a location extraction expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=200,
                response_format={"type": "json_object"}
            )

            # Parse the response
            response_text = response.choices[0].message.content.strip()
            try:
                result = json.loads(response_text)
                return result
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse OpenAI response as JSON: {response_text}")
        except Exception as err:
            return {}

    def categorize_listing(self, category: str) -> List[str]:
        """
        Use OpenAI API to categorize the listing based on predefined categories
        """
        try:
            # Prepare the content for categorization
            content = f"Original Category: {category}"
            
            prompt = f"""
            Analyze this business listing category and categorize it using ONLY the predefined categories below. 
            You can select multiple categories if applicable. Return ONLY a JSON response with the key "category" and value as an array of selected categories.

            Predefined categories:
            {', '.join(self.categories)}

            Business listing category:
            {content}

            Return ONLY a JSON response like this:
            {{"category": ["Category1", "Category2"]}}
            """

            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a business categorization expert. Return only valid JSON with the 'category' key containing an array of categories."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=200,
                response_format={"type": "json_object"}
            )

            # Parse the response
            response_text = response.choices[0].message.content.strip()
            
            # Try to extract JSON from the response
            try:
                # Remove any markdown formatting if present
                if response_text.startswith('```json'):
                    response_text = response_text[7:]
                if response_text.endswith('```'):
                    response_text = response_text[:-3]
                
                result = json.loads(response_text)
                categories = result.get('category', [])
                
                # Validate that all categories are from the predefined list
                valid_categories = [cat for cat in categories if cat in self.categories]
                
                if valid_categories:
                    self.logger.info(f"Categorized as: {valid_categories}")
                    return valid_categories
                else:
                    self.logger.warning(f"No valid categories found in response: {categories}")
                    return [category] if category else []
                    
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse OpenAI response as JSON: {response_text}")
                return [category] if category else []
                
        except Exception as e:
            self.logger.error(f"OpenAI categorization failed: {str(e)}")
            return [category] if category else []


def main():
    """Main function for the listing detail scraper"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        # Initialize scraper
        scraper = ListingDetailScraper()

        # Run listing detail scraping (limit to 10 for testing)
        new_details = scraper.process_urls_directly(max_listings=10)

        # Get statistics
        stats = scraper.get_stats()

        print(f"\n📊 LISTING DETAIL SCRAPING RESULTS:")
        print(f"   New details saved: {new_details}")
        print(f"   CSV file: {stats['csv_file']}")

        if stats['sample_details']:
            print(f"\n📋 Sample details:")
            for i, detail in enumerate(stats['sample_details'], 1):
                print(f"   {i}. {detail.get('title', 'No title')[:50]}...")
                print(f"      Location: {detail.get('location', 'Not specified')}")
                print(f"      Price: ${detail.get('asking_price', 'Not specified')}")
                print(f"      Cashflow: {detail.get('cashflow', 'Not specified')}")
                print(f"      Broker: {detail.get('broker_name', 'Not specified')} - {detail.get('broker_number', 'Not specified')}")
                print()

        print(f"🎉 SUCCESS! Detail scraper completed with {new_details} new details!")
        print(f"📁 Data saved to: {stats['csv_file']}")

        return True

    except Exception as e:
        print(f"❌ Detail scraper failed: {str(e)}")
        logging.error("Detail scraper failed", exc_info=True)
        return False


if __name__ == "__main__":
    success = main() 