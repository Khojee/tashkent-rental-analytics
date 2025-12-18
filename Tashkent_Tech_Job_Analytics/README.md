# HH Uzbekistan Tech Job Scraper - Project Summary

## Overview
This project successfully scrapes tech job vacancies from HH Uzbekistan's public API and saves them to CSV with geographic coordinates.

## Project Structure
```
tashkent_tech_job_analytics/
├── scraper/
│   ├── __init__.py           # Package initializer
│   ├── hh_map_scraper.py     # Main scraper class
│   └── utils.py              # Utility functions
├── data/
│   └── vacancies_map.csv     # Output CSV file
├── main.py                   # Entry point
├── check_data.py             # Data analysis script
└── test_api.py               # API testing script
```

## Features
- ✅ Collects data from HH.ru public API (JSON responses)
- ✅ Extracts vacancy name, company name, coordinates, district, and URL
- ✅ Handles pagination (up to 20 pages by default)
- ✅ Respectful scraping with delays between requests
- ✅ Clean, modular code structure
- ✅ No HTML parsing, no Selenium, no BeautifulSoup

## Data Collected
- **Total vacancies**: 2,001 (including header)
- **Vacancies with coordinates**: 2,000 (100%)
- **Output format**: CSV with exact column order specified

## CSV Columns
1. `vacancy_name` - Job title
2. `company_name` - Employer name
3. `latitude` - Geographic latitude (float or NULL)
4. `longitude` - Geographic longitude (float or NULL)
5. `district` - District name (or NULL if missing)
6. `vacancy_url` - Full URL to vacancy page

## Usage
```bash
python main.py
```

## Technical Details
- **API Endpoint**: `https://api.hh.ru/vacancies`
- **Search Query**: IT OR программист OR developer OR разработчик OR software OR python OR java OR javascript OR frontend OR backend
- **Area Code**: 2759 (Tashkent)
- **Per Page**: 100 vacancies
- **Max Pages**: 20 (configurable)

## Dependencies
- requests
- pandas

All dependencies are already available in the root `requirements.txt` and `venv`.

## Notes
- The scraper uses HH.ru's public API which provides structured JSON data
- Coordinates are extracted from the `address` field in the API response
- Some vacancies may not have coordinates if the employer didn't provide an address
- The scraper includes respectful delays (1-2 seconds) between page requests
