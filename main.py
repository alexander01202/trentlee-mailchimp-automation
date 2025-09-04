#!/usr/bin/env python3
"""
BizBuySell Main Scraper
Combines URL scraper and detail scraper to scrape URLs and immediately process details
"""

import logging
import os
import time
from datetime import datetime, timedelta

import requests

from listing_url_scraper import ListingURLScraper
from listing_detail_scraper import ListingDetailScraper
from pymongo import MongoClient
from mailchimp_notifier import notify_subscribers


def main():
    """Main function that combines URL scraping and detail scraping"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🚀 Starting BizBuySell Combined Scraper...")
        logger.info("=" * 50)

        # Step 1: Scrape listing URLs (without saving to CSV)
        logger.info("\n📋 STEP 1: Scraping listing URLs...")
        url_scraper = ListingURLScraper()
        
        # Get URLs directly from the scraper without saving to CSV
        listing_urls = url_scraper.get_urls_only()
        
        if not listing_urls:
            logger.info("❌ No URLs found. Exiting.")
            return False

        logger.info(f"✅ Found {len(listing_urls)} listing URLs")

        # Step 1.5: Filter out URLs that already exist in MongoDB
        logger.info("\n📋 STEP 1.5: Filtering URLs against MongoDB...")
        mongo_uri = "mongodb+srv://trentlee702:pHHJETqffYMINqbN@bizbuysell-cluster.homaclh.mongodb.net/"
        mongo_db_name = "business-broker-las-vegas-db"
        mongo_collection_name = "bizbuysell-data"
        client = MongoClient(mongo_uri)
        col = client[mongo_db_name][mongo_collection_name]

        # Fetch last 24h docs (for logging)
        cutoff_dt = datetime.now() - timedelta(hours=24)
        cutoff_str = cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')
        last24 = list(col.find({ 'scraped_date': { '$gte': cutoff_str } }, { '_id': 0, 'url': 1 }))
        logger.info(f"📊 Docs scraped in the last 24h: {len(last24)}")

        # Filter new URLs: only those not existing in DB at all
        url_list = [u.get('url') for u in listing_urls if u.get('url')]
        existing = set(d.get('url') for d in col.find({ 'url': { '$in': url_list } }, { '_id': 0, 'url': 1 }))
        filtered_urls = [u for u in listing_urls if u.get('url') not in existing]
        logger.info(f"🔍 Filtered out {len(existing)} URLs already in DB. New URLs to process: {len(filtered_urls)}")

        if not filtered_urls:
            logger.info("⚠️ No new URLs to process after filtering against MongoDB.")
            # Still run notifier for last 24h matches
            notif = notify_subscribers([], list_id="7881b0503b")
            logger.info(f"📨 Mailchimp notify: matched_subscribers={notif['matched_subscribers']}, campaign_sent={notif['campaign_sent']}")
            return True

        # Step 2: Initialize detail scraper and process URLs directly
        logger.info("\n📋 STEP 2: Scraping listing details with Selenium...")
        timestamp = datetime.now().strftime("%Y%m%d")
        detail_csv_filename = f"bizbuysell_detailed_{timestamp}.csv"
        
        # Initialize detail scraper with concurrency settings
        detail_scraper = ListingDetailScraper(max_concurrent=1)  # Process 1 URL at a time with Selenium
        detail_scraper.detail_csv_filename = detail_csv_filename
        # detail_scraper._initialize_csv()

        # Step 3: Process URLs directly without loading existing details (Selenium)
        logger.info(f"🚀 Processing {len(filtered_urls)} URLs with Selenium (concurrent: {detail_scraper.max_concurrent})...")
        filtered_urls_details = detail_scraper.process_urls_directly(filtered_urls)

        # Get statistics
        stats = detail_scraper.get_stats()

        logger.info(f"\n📊 BIZBUYSELL SCRAPING RESULTS:")
        logger.info(f"   URLs processed (total scraped vs. new): {len(listing_urls)} vs. {len(filtered_urls)}")
        logger.info(f"   Database: {stats['database']}")
        logger.info(f"   Collection: {stats['collection']}")

        # if stats['sample_details']:
        #     print(f"\n📋 Sample details (latest):")
        #     for i, detail in enumerate(stats['sample_details'], 1):
        #         print(f"   {i}. {detail.get('title', 'No title')[:50]}...")
        #         print(f"      Location: {detail.get('location', 'Not specified')}")
        #         print(f"      Price: {detail.get('asking_price', 'Not specified')}")
        #         print(f"      Cashflow: {detail.get('cashflow', 'Not specified')}")
        #         print(f"      Broker: {detail.get('broker_name', 'Not specified')} - {detail.get('broker_number', 'Not specified')}")
        #         print()

        # Step 4: Notify subscribers via Mailchimp for last 24h listings
        notif = notify_subscribers(filtered_urls_details, list_id="7881b0503b")

        logger.info(f"🎉 SUCCESS! Scraper completed. Data saved to MongoDB and notifications sent.")

        return True

    except Exception as e:
        print(f"❌ Combined scraper failed: {str(e)}")
        logger.error("Combined scraper failed", exc_info=True)
        return False


if __name__ == "__main__":
    while True:
        main()
        time.sleep(86400/2)
