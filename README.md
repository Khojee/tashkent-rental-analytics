# OLX Tashkent Rental Analytics & City Intelligence

A comprehensive data analysis suite for Tashkent, combining rental market data from OLX, public transit data from OpenStreetMap, and tech job vacancies from HeadHunter.

## 📋 Overview

This project provides a 360-degree view of Tashkent's districts by analyzing:
1.  **Rental Market**: Scrapes and cleans rental listings from OLX for all 11 districts.
2.  **Public Transit**: Maps metro stations and bus stops to evaluate transport accessibility.
3.  **Job Market**: Analyzes tech job vacancies and maps them to districts.
4.  **Integrated Dashboard**: Combines all data to score districts and visualize insights.

## 🏗️ Architecture

The project consists of four main components orchestrated by a master pipeline:

### 1. **Transit Analytics** (`Tashkent_Transit_Analytics/map.py`)
Fetches real-time public transport data (Metro & Bus) from OpenStreetMap via Overpass API.

### 2. **Tech Job Analytics** (`Tashkent_Tech_Job_Analytics/main.py`)
Scrapes and maps tech job vacancies to districts, analyzing employment hubs.

### 3. **OLX Rental Pipeline** (`main.py`)
The core scraping engine consisting of:
-   **DistrictScraper**: Scrapes basic listings.
-   **DistrictListingCleaner**: Cleans and deduplicates data.
-   **CardDetailsScraper**: Fetches detailed apartment attributes.

### 4. **Real Data Processor** (`real_data_processor.py`)
Consolidates all datasets, calculates composite scores (Transit + Affordability + Jobs), and generates the final dashboard data.

## 📁 Project Structure

```
OLX_Scrap/
├── main.py                          # Master pipeline orchestrator
├── real_data_processor.py           # Data consolidation & dashboard generator
├── real_data_dashboard.html         # Interactive visualization dashboard
├── Tashkent_Transit_Analytics/      # Transit data module
├── Tashkent_Tech_Job_Analytics/     # Job market module
├── olx_cards_by_district.py         # OLX scraper components
├── list_cleaning.py                 # ...
├── info_by_card.py                  # ...
└── ...
```

## 🚀 Quick Start

### Installation

1.  Clone the repository
2.  Install dependencies:

```bash
pip install -r requirements.txt
```

### Usage

#### Run the Master Pipeline

To run the complete analysis suite (Transit -> Jobs -> OLX -> Dashboard Data):

```bash
python main.py
```

This will:
1.  Fetch latest transit data.
2.  Scrape latest job vacancies.
3.  Scrape/Update OLX rental listings.
4.  Process all data and generate `dashboard_data.json`.

#### Run Only OLX Scraper

If you only want to update rental data:

```bash
python main.py --olx-only
# or
python main.py --olx-only --max-pages 5
```

### 📊 Visualization

Once the pipeline completes, you can view the interactive dashboard:

1.  Start a local web server in the project root:

```bash
python3 -m http.server 8000
```

2.  Open your browser and navigate to:
    **[http://localhost:8000/real_data_dashboard.html](http://localhost:8000/real_data_dashboard.html)**

## 🗺️ District Analysis Features

The analysis scores each district (0-10) on:
-   **Transit Score**: Density of metro and bus stations.
-   **Affordability Score**: Inverse of median rental price.
-   **Employment Score**: Concentration of tech job opportunities.

## 💻 Programmatic Usage & Configuration

See `main.py --help` for full command-line options.

## ⚠️ Disclaimer

This tool is for educational purposes only. Always respect the website's terms of service and robots.txt when scraping.
