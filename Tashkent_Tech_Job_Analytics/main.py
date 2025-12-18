"""
Main entry point for HH Uzbekistan tech job scraper
"""

import os
from scraper.hh_map_scraper import HHMapScraper


def main():
    """
    Main function to run the scraper
    """
    print("=" * 60)
    print("HH Uzbekistan Tech Job Vacancy Scraper")
    print("=" * 60)
    print()
    
    # Initialize scraper
    scraper = HHMapScraper()
    
    # Scrape vacancies from map API
    vacancies = scraper.scrape()
    
    if not vacancies:
        print("\nNo vacancies found. Please check the API endpoint or parameters.")
        return
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Save to CSV
    output_file = os.path.join(data_dir, 'vacancies_map.csv')
    scraper.save_to_csv(output_file)
    
    print()
    print("=" * 60)
    print("Scraping completed successfully!")
    print(f"Total vacancies collected: {len(vacancies)}")
    print(f"Output file: {output_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
