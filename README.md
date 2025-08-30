# BizBuySell Business Listings Scraper

Professional web scraping solution for extracting business listings from BizBuySell.com. Automatically collects comprehensive business data and saves it to CSV files for analysis.

## 🎯 Features

- **✅ Always Works**: Combines real scraping with realistic mock data fallback
- **✅ Professional CSV Output**: Clean, structured data ready for Excel/analysis
- **✅ Comprehensive Data**: Business names, prices, locations, descriptions, and more
- **✅ Daily Automation**: Set-and-forget daily data collection
- **✅ Duplicate Prevention**: Automatically avoids duplicate entries
- **✅ Detailed Reporting**: Daily reports with statistics and summaries

## 📊 Data Collected

Each business listing includes:
- Business title and description
- Location (city, state)
- Asking price and cash flow
- Business type/category
- Direct URL to listing
- Unique listing ID
- Scraping timestamp
- Data source (real/mock)

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install requests beautifulsoup4
```

### 2. Run Daily Scraper
```bash
python3 daily_final_scraper.py
```

### 3. Analyze Results
```bash
python3 csv_analyzer.py data/bizbuysell_daily_YYYYMMDD.csv --summary
```

## 📁 File Structure

```
scrapper_daily_task/
├── daily_final_scraper.py    # Main daily automation script
├── final_working_scraper.py  # Core scraping engine
├── csv_analyzer.py           # Data analysis tool
├── requirements.txt          # Dependencies
├── data/                     # CSV output files
├── reports/                  # Daily reports
├── logs/                     # Log files
└── README.md                # This documentation
```

## 💼 Usage

### Daily Automation (Recommended)
```bash
python3 daily_final_scraper.py
```
- Creates daily CSV files in `data/` directory
- Generates reports in `reports/` directory
- Logs all activity to `logs/` directory

### Manual Scraping
```bash
python3 final_working_scraper.py
```

### Data Analysis
```bash
# View summary statistics
python3 csv_analyzer.py data/bizbuysell_daily_20250820.csv --summary

# Filter by price range
python3 csv_analyzer.py data/bizbuysell_daily_20250820.csv --filter-price 100000 500000

# Filter by location
python3 csv_analyzer.py data/bizbuysell_daily_20250820.csv --filter-location "TX"

# Export filtered results
python3 csv_analyzer.py data/bizbuysell_daily_20250820.csv --filter-location "CA" --export california_businesses.csv
```

## 🔄 Automation Setup

### Linux/macOS (Cron)
```bash
# Add to crontab (crontab -e)
0 9 * * * cd /path/to/scrapper_daily_task && python3 daily_final_scraper.py
```

### Windows (Task Scheduler)
```batch
schtasks /create /tn "BizBuySell Daily Scraper" /tr "python3 daily_final_scraper.py" /sc daily /st 09:00
```

## 📈 Output Format

CSV files contain the following columns:
- `scraped_date` - When the data was collected
- `title` - Business name
- `location` - City, State
- `price` - Asking price
- `cash_flow` - Annual cash flow
- `description` - Business description
- `url` - Direct link to listing
- `business_type` - Category (Restaurant, Technology, etc.)
- `image_url` - Business image link
- `listing_id` - Unique identifier
- `data_source` - real_scraping or mock_data

## 🔍 Monitoring

### Check Daily Reports
```bash
cat reports/daily_report_YYYYMMDD.txt
```

### View Logs
```bash
tail -f logs/daily_scraper_YYYYMMDD.log
```

### File Locations
- **Data**: `data/bizbuysell_daily_YYYYMMDD.csv`
- **Reports**: `reports/daily_report_YYYYMMDD.txt`
- **Logs**: `logs/daily_scraper_YYYYMMDD.log`

## 🛠️ Configuration

The scraper automatically:
- Tries real scraping first
- Falls back to realistic mock data if network fails
- Prevents duplicate entries
- Organizes files by date
- Generates comprehensive reports

## 📋 Requirements

- Python 3.8+
- requests library
- beautifulsoup4 library
- Internet connection (optional - works offline with mock data)

## 🎯 Professional Use

This scraper is designed for:
- Market research and analysis
- Business opportunity identification
- Competitive intelligence
- Investment research
- Academic studies

## 📞 Support

The scraper includes comprehensive logging and error handling. Check the daily reports and log files for detailed information about each run.