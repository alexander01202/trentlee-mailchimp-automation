# BizBuySell Scraper - Separated Architecture

This project now uses a two-stage scraping approach for better data collection and management.

## Overview

The scraper has been separated into two distinct components:

1. **Listing URL Scraper** (`listing_url_scraper.py`) - Scrapes listing URLs from BizBuySell's main pages
2. **Listing Detail Scraper** (`listing_detail_scraper.py`) - Scrapes detailed information from individual listing pages

## Files

- `listing_url_scraper.py` - Scrapes listing URLs and saves them to `listing_urls_YYYYMMDD.csv`
- `listing_detail_scraper.py` - Scrapes detailed information from individual listings and saves to `listing_details_YYYYMMDD.csv`
- `combined_scraper.py` - Runs both scrapers in sequence
- `SCRAPER_README.md` - This documentation file

## Data Structure

### URL Scraper Output (`listing_urls_YYYYMMDD.csv`)
```
title, url, listing_id, scraped_date
```

### Detail Scraper Output (`listing_details_YYYYMMDD.csv`)
```
title, location, asking_price, gross_revenue, established, cashflow, description, url, business_type, listing_id, broker_name, broker_number, scraped_date
```

## Usage

### Option 1: Run Combined Scraper (Recommended)
```bash
python combined_scraper.py
```

This will:
1. Scrape listing URLs from BizBuySell
2. Scrape detailed information from each listing
3. Save results to separate CSV files

### Option 2: Run Scrapers Separately

#### Step 1: Scrape URLs
```bash
python listing_url_scraper.py
```

#### Step 2: Scrape Details
```bash
python listing_detail_scraper.py
```

The detail scraper will automatically find the most recent URL CSV file.

### Option 3: Specify URL File for Details
```python
from listing_detail_scraper import ListingDetailScraper

scraper = ListingDetailScraper("listing_urls_20250820.csv")
scraper.scrape_listing_details(max_listings=50)  # Limit to 50 listings
```

## Features

### URL Scraper Features
- Scrapes multiple pages of listings
- Avoids duplicate URLs
- Saves basic listing information (title, URL, ID)
- Handles rate limiting and retries

### Detail Scraper Features
- Extracts comprehensive listing information
- Uses CSS selectors for specific data fields:
  - Location: `span.f-l`
  - Broker number: `span.ctc_phone a span`
  - Gross revenue: `p.help span.g4` (3rd element)
  - Cashflow: `p.help span.g4` (2nd element)
- Extracts JSON-LD data for additional fields
- Includes 2-second delay between requests
- Avoids duplicate scraping

### Data Fields Extracted

| Field | Source | Description |
|-------|--------|-------------|
| title | JSON-LD | Business title |
| location | CSS: `span.f-l` | Business location |
| asking_price | JSON-LD | Asking price |
| gross_revenue | CSS: `p.help span.g4`[2] | Annual gross revenue |
| established | JSON-LD | Year established |
| cashflow | CSS: `p.help span.g4`[1] | Annual cash flow |
| description | JSON-LD | Business description |
| url | URL CSV | Listing URL |
| business_type | JSON-LD | Type of business |
| listing_id | JSON-LD | Unique listing ID |
| broker_name | JSON-LD | Broker name |
| broker_number | CSS: `span.ctc_phone a span` | Broker phone number |
| scraped_date | Generated | Date and time of scraping |

## Configuration

### Rate Limiting
- URL scraper: 10-second delay between page requests
- Detail scraper: 2-second delay between listing requests

### Page Limits
- URL scraper: Scrapes up to 10 pages (configurable in `_try_real_scraping`)
- Detail scraper: Can limit number of listings with `max_listings` parameter

### File Naming
- URL files: `listing_urls_YYYYMMDD.csv`
- Detail files: `listing_details_YYYYMMDD.csv`

## Error Handling

Both scrapers include:
- Duplicate URL detection
- Request retry logic
- Graceful error handling
- Detailed logging
- CSV data validation

## Dependencies

Make sure you have the required packages installed:
```bash
pip install requests beautifulsoup4 fake-useragent lxml
```

## Example Output

### URL Scraper Output
```csv
title,url,listing_id,scraped_date
"Profitable Restaurant for Sale","https://www.bizbuysell.com/...",12345,2025-08-20 10:30:00
"Retail Store with Strong Cash Flow","https://www.bizbuysell.com/...",12346,2025-08-20 10:30:01
```

### Detail Scraper Output
```csv
title,location,asking_price,gross_revenue,established,cashflow,description,url,business_type,listing_id,broker_name,broker_number,scraped_date
"Profitable Restaurant for Sale","New York, NY",450000,"$850,000",2010,"$180,000","Well-established restaurant...","https://www.bizbuysell.com/...","Restaurant",12345,"John Smith","(555) 123-4567",2025-08-20 10:35:00
```

## Troubleshooting

### Common Issues

1. **No URLs found**: Check if BizBuySell's structure has changed
2. **Detail scraping fails**: Verify CSS selectors are still valid
3. **Rate limiting**: Increase delays between requests
4. **Missing dependencies**: Install required packages

### Debug Mode
Enable debug logging by modifying the logging level:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Future Enhancements

- Add proxy support for better rate limiting
- Implement database storage option
- Add email notifications for completed scraping
- Create web interface for monitoring
- Add data validation and cleaning features 