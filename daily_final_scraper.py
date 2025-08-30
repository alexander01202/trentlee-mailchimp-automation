#!/usr/bin/env python3
"""
Daily automation script for the BizBuySell scrapers
Runs daily: collects listing URLs, scrapes details, saves to MongoDB
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from listing_url_scraper import ListingURLScraper
from listing_detail_scraper import ListingDetailScraper
from pymongo import MongoClient


def setup_logging():
    """Setup logging for daily automation"""
    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create daily log file
    today = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f"daily_scraper_{today}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def run_daily_scraping():
    """Run daily scraping: URLs -> details -> MongoDB"""
    logger = logging.getLogger(__name__)
    
    try:
        start_time = datetime.now()

        # Step 1: Scrape listing URLs (in-memory)
        url_scraper = ListingURLScraper()
        listing_urls = url_scraper.get_urls_only()
        num_urls = len(listing_urls)
        logger.info(f"Collected {num_urls} listing URLs")

        if num_urls == 0:
            logger.error("No listing URLs found. Exiting.")
            return False

        # Step 1.5: Filter out URLs that already exist in MongoDB
        mongo_uri = "mongodb+srv://trentlee702:pHHJETqffYMINqbN@bizbuysell-cluster.homaclh.mongodb.net/"
        mongo_db_name = "business-broker-las-vegas-db"
        mongo_collection_name = "bizbuysell-data"
        client = MongoClient(mongo_uri)
        col = client[mongo_db_name][mongo_collection_name]

        # Fetch last 24h docs (for logging)
        cutoff_dt = datetime.now() - timedelta(hours=24)
        cutoff_str = cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')
        last24 = list(col.find({ 'scraped_date': { '$gte': cutoff_str } }, { '_id': 0, 'url': 1 }))
        logger.info(f"Docs scraped in the last 24h: {len(last24)}")

        # Filter new URLs: only those not existing in DB at all
        url_list = [u.get('url') for u in listing_urls if u.get('url')]
        existing = set(d.get('url') for d in col.find({ 'url': { '$in': url_list } }, { '_id': 0, 'url': 1 }))
        filtered_urls = [u for u in listing_urls if u.get('url') not in existing]
        logger.info(f"Filtered out {len(existing)} URLs already in DB. New URLs to process: {len(filtered_urls)}")

        if not filtered_urls:
            logger.warning("No new URLs to process after filtering against MongoDB.")
            return True

        # Step 2: Scrape listing details (saves to MongoDB)
        detail_scraper = ListingDetailScraper(max_concurrent=1)
        upserts = detail_scraper.process_urls_directly(filtered_urls)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Stats from Mongo
        stats = detail_scraper.get_stats()

        # Log results
        logger.info(f"Scraping completed in {duration:.1f} seconds")
        logger.info(f"URLs processed (total scraped vs. new): {num_urls} vs. {len(filtered_urls)}")
        logger.info(f"Mongo upserted/modified: {upserts}")
        logger.info(f"Database: {stats['database']}")
        logger.info(f"Collection: {stats['collection']}")
        logger.info(f"Total docs in collection: {stats['total_details']}")
        
        # Generate daily report
        generate_daily_report(stats, len(filtered_urls), upserts, duration)
        
        logger.info("=" * 60)
        logger.info("DAILY SCRAPING COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Daily scraping failed: {str(e)}", exc_info=True)
        return False


def generate_daily_report(stats: dict, num_urls: int, upserts: int, duration: float):
    """Generate daily report (MongoDB)"""
    logger = logging.getLogger(__name__)
    
    # Create reports directory
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Generate report filename
    today = datetime.now().strftime("%Y%m%d")
    report_file = reports_dir / f"daily_report_{today}.txt"
    
    # Save report (user customized content)
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"Daily report {today}: URLs processed={num_urls}, upserts={upserts}, DB={stats['database']}, COL={stats['collection']}, total_docs={stats['total_details']}\n")
        
        logger.info(f"Daily report saved: {report_file}")
    
    except Exception as e:
        logger.error(f"Failed to save daily report: {str(e)}")


def main():
    """Main function for daily automation"""
    setup_logging()
    
    success = run_daily_scraping()
    
    if success:
        print("\n🎉 Daily scraping completed successfully!")
        print("📦 Data saved to MongoDB. Check the 'reports' directory for the report file.")
    else:
        print("\n❌ Daily scraping failed!")
        print("📝 Check the log files for error details")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
