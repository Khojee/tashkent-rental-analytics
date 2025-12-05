# OLX Tashkent Rental Analytics

A comprehensive web scraping pipeline for collecting and analyzing rental apartment listings from OLX Tashkent.

## 📋 Overview

This project scrapes rental apartment listings from OLX for all 11 districts in Tashkent, cleans the data, and collects detailed information for each listing. The entire pipeline is built using object-oriented programming for modularity and reusability.

## 🏗️ Architecture

The project consists of three main components:

### 1. **DistrictScraper** (`olx_cards_by_district.py`)
Scrapes basic listing information from OLX search pages by district.

**Features:**
- Scrapes all 11 Tashkent districts
- Extracts: title, URL, price, location, posting date
- Handles pagination automatically
- Saves raw data to `district_listing_page/`

### 2. **DistrictListingCleaner** (`list_cleaning.py`)
Cleans the scraped data by removing duplicates with missing price information.

**Features:**
- Processes all CSV files in `district_listing_page/`
- Removes duplicate listings without price data
- Saves cleaned data to `district_listing_page_cleaned/`
- Provides detailed cleaning statistics

### 3. **CardDetailsScraper** (`info_by_card.py`)
Scrapes detailed information from individual listing pages.

**Features:**
- Processes cleaned data from `district_listing_page_cleaned/`
- Extracts: area, number of rooms, furniture status, condition, posting date
- Resume capability (can continue from where it stopped)
- Saves detailed data to `cards_details/`

### 4. **OLXScraperPipeline** (`main.py`)
Orchestrates the complete pipeline execution.

## 📁 Project Structure

```
OLX_Scrap/
├── main.py                          # Main pipeline orchestrator
├── olx_cards_by_district.py         # District scraper (Step 1)
├── list_cleaning.py                 # Data cleaner (Step 2)
├── info_by_card.py                  # Details scraper (Step 3)
├── requirements.txt                 # Python dependencies
├── district_listing_page/           # Raw scraped data
├── district_listing_page_cleaned/   # Cleaned data
└── cards_details/                   # Detailed card information
```

## 🚀 Quick Start

### Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Usage

#### Run the Complete Pipeline

```bash
# Run all three steps (scrape → clean → details)
python main.py
```

#### Run Individual Steps

```bash
# Only scrape raw listings
python main.py --scrape-only

# Only clean existing data
python main.py --clean-only

# Only scrape details from cleaned data
python main.py --details-only
```

#### Advanced Options

```bash
# Scrape 20 pages per district instead of default 10
python main.py --max-pages 20

# Process only specific districts (by ID)
python main.py --districts 26,25,24

# Combine options
python main.py --scrape-only --max-pages 15 --districts 26,25
```

## 🗺️ District IDs

| ID | District Name    |
|----|------------------|
| 26 | Yakkasarai       |
| 25 | Yunusabad        |
| 24 | Shaykhantohur    |
| 23 | Chilonzor        |
| 22 | Yashnabad        |
| 21 | Uchtepa          |
| 20 | Almazar          |
| 19 | Sergeli          |
| 18 | Bektemir         |
| 13 | Mirabad          |
| 12 | Mirzo-Ulugbek    |

## 💻 Programmatic Usage

You can also use the classes directly in your own Python code:

### Example: Scrape Specific Districts

```python
from olx_cards_by_district import DistrictScraper

scraper = DistrictScraper()
results = scraper.scrape_all_districts(
    max_pages=10,
    district_ids=[26, 25, 24]  # Only Yakkasarai, Yunusabad, Shaykhantohur
)
```

### Example: Clean Data

```python
from list_cleaning import DistrictListingCleaner

cleaner = DistrictListingCleaner()
results = cleaner.process_all_files()

print(f"Cleaned {results['processed']} files")
```

### Example: Scrape Details

```python
from info_by_card import CardDetailsScraper

scraper = CardDetailsScraper(
    save_interval=100,  # Save every 100 cards
    min_delay=2.0,      # 2-3 second delay between requests
    max_delay=3.0
)
results = scraper.process_all_districts()
```

### Example: Custom Pipeline

```python
from main import OLXScraperPipeline

pipeline = OLXScraperPipeline(max_pages=15)

# Run individual steps
pipeline.step1_scrape_listings()
pipeline.step2_clean_listings()
pipeline.step3_scrape_details()

# Or run everything
pipeline.run_full_pipeline()
```

## 📊 Output Data

### Raw Listings (`district_listing_page/`)
CSV files with columns:
- `card_id`, `title`, `url`, `price_raw`, `price_value`, `price_currency`
- `location_text`, `posted_date_raw`, `posted_date`, `time_raw`
- `district_id`, `district_name`

### Cleaned Listings (`district_listing_page_cleaned/`)
Same structure as raw data, but with duplicates removed.

### Card Details (`cards_details/`)
CSV files with columns:
- `card_id`, `area`, `number_rooms`, `furniture`, `condition`, `date`

## ⚙️ Configuration

You can customize the scraping behavior by modifying class parameters:

```python
# Adjust delays to avoid rate limiting
scraper = CardDetailsScraper(
    min_delay=3.0,      # Minimum 3 seconds
    max_delay=5.0,      # Maximum 5 seconds
    request_timeout=15  # 15 second timeout
)

# Change save frequency
scraper = CardDetailsScraper(save_interval=25)  # Save every 25 cards
```

## 🔄 Resume Capability

The detail scraper automatically resumes from where it stopped:
- Checks for existing output files
- Skips already processed cards
- Continues scraping new cards

This is useful if the scraper is interrupted or if you want to update the dataset incrementally.

## 🛡️ Best Practices

1. **Respect Rate Limits**: Use appropriate delays between requests
2. **Monitor Progress**: The pipeline provides detailed progress logs
3. **Incremental Updates**: Use `--details-only` to update just the details without re-scraping everything
4. **Error Handling**: The pipeline continues even if individual cards fail

## 📝 Requirements

- Python 3.7+
- pandas
- requests
- beautifulsoup4
- lxml

## 🤝 Contributing

Feel free to submit issues or pull requests to improve the scraper!

## ⚠️ Disclaimer

This tool is for educational purposes only. Always respect the website's terms of service and robots.txt when scraping.
