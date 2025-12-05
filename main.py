"""
OLX Tashkent Rental Analytics - Main Pipeline
==============================================

This script orchestrates the complete data collection pipeline:
1. Scrape raw listing data from OLX by district
2. Clean the scraped data (remove duplicates with missing prices)
3. Scrape detailed information for each cleaned listing

Usage:
    python main.py [options]

Options:
    --scrape-only       Only run the scraping step
    --clean-only        Only run the cleaning step
    --details-only      Only run the details scraping step
    --max-pages N       Maximum pages to scrape per district (default: 10)
    --districts ID1,ID2 Comma-separated district IDs to process (default: all)
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Import our OOP classes
from olx_cards_by_district import DistrictScraper
from list_cleaning import DistrictListingCleaner
from info_by_card import CardDetailsScraper


class OLXScraperPipeline:
    """
    Main pipeline orchestrator for OLX rental data collection.
    """
    
    def __init__(self, max_pages: int = 10, district_ids: list = None):
        """
        Initialize the pipeline.
        
        Args:
            max_pages: Maximum pages to scrape per district
            district_ids: Optional list of specific district IDs to process
        """
        self.max_pages = max_pages
        self.district_ids = district_ids
        
        # Initialize components
        self.scraper = DistrictScraper(output_folder="district_listing_page")
        self.cleaner = DistrictListingCleaner(
            input_folder="district_listing_page",
            output_folder="district_listing_page_cleaned"
        )
        self.details_scraper = CardDetailsScraper(
            input_folder="district_listing_page_cleaned",
            output_folder="cards_details"
        )
        
        self.start_time = None
        self.results = {
            'scrape': None,
            'clean': None,
            'details': None
        }
    
    def print_header(self, title: str):
        """Print a formatted section header."""
        print("\n" + "=" * 80)
        print(f"  {title}")
        print("=" * 80 + "\n")
    
    def print_step(self, step_num: int, step_name: str):
        """Print a step indicator."""
        print(f"\n{'─' * 80}")
        print(f"STEP {step_num}: {step_name}")
        print(f"{'─' * 80}\n")
    
    def step1_scrape_listings(self) -> bool:
        """
        Step 1: Scrape raw listing data from OLX.
        
        Returns:
            True if successful
        """
        self.print_step(1, "SCRAPING RAW LISTINGS FROM OLX")
        
        try:
            results = self.scraper.scrape_all_districts(
                max_pages=self.max_pages,
                sleep_between_pages=1.5,
                district_ids=self.district_ids
            )
            
            self.results['scrape'] = results
            
            if results['scraped'] > 0:
                print(f"\n✅ Step 1 Complete: Scraped {results['scraped']} districts")
                return True
            else:
                print(f"\n⚠ Step 1 Warning: No districts scraped")
                return False
                
        except Exception as e:
            print(f"\n❌ Step 1 Failed: {str(e)}")
            return False
    
    def step2_clean_listings(self) -> bool:
        """
        Step 2: Clean the scraped data.
        
        Returns:
            True if successful
        """
        self.print_step(2, "CLEANING SCRAPED DATA")
        
        try:
            results = self.cleaner.process_all_files()
            self.results['clean'] = results
            
            if results['processed'] > 0:
                print(f"\n✅ Step 2 Complete: Cleaned {results['processed']} files")
                return True
            else:
                print(f"\n⚠ Step 2 Warning: No files cleaned")
                return False
                
        except Exception as e:
            print(f"\n❌ Step 2 Failed: {str(e)}")
            return False
    
    def step3_scrape_details(self) -> bool:
        """
        Step 3: Scrape detailed information for each listing.
        
        Returns:
            True if successful
        """
        self.print_step(3, "SCRAPING DETAILED CARD INFORMATION")
        
        try:
            results = self.details_scraper.process_all_districts()
            self.results['details'] = results
            
            if results['processed'] > 0:
                print(f"\n✅ Step 3 Complete: Processed {results['processed']} districts")
                return True
            else:
                print(f"\n⚠ Step 3 Warning: No districts processed")
                return False
                
        except Exception as e:
            print(f"\n❌ Step 3 Failed: {str(e)}")
            return False
    
    def print_final_summary(self):
        """Print final pipeline summary."""
        self.print_header("PIPELINE SUMMARY")
        
        elapsed = datetime.now() - self.start_time
        hours, remainder = divmod(elapsed.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print(f"Total execution time: {int(hours)}h {int(minutes)}m {int(seconds)}s\n")
        
        # Step 1 summary
        if self.results['scrape']:
            r = self.results['scrape']
            print("Step 1 - Scraping:")
            print(f"  ✓ Districts scraped: {r['scraped']}")
            print(f"  ✗ Errors: {len(r['errors'])}")
        
        # Step 2 summary
        if self.results['clean']:
            r = self.results['clean']
            print("\nStep 2 - Cleaning:")
            print(f"  ✓ Files processed: {r['processed']}")
            if r['files']:
                total_removed = sum(rows for _, _, rows in r['files'])
                print(f"  ✓ Total rows removed: {total_removed}")
            print(f"  ✗ Errors: {len(r['errors'])}")
        
        # Step 3 summary
        if self.results['details']:
            r = self.results['details']
            print("\nStep 3 - Detail Scraping:")
            print(f"  ✓ Districts processed: {r['processed']}")
            if r['results']:
                total_cards = sum(res['final_count'] for res in r['results'])
                total_success = sum(res['successful'] for res in r['results'])
                total_failed = sum(res['failed'] for res in r['results'])
                print(f"  ✓ Total cards scraped: {total_cards}")
                print(f"  ✓ Successful: {total_success}")
                print(f"  ✗ Failed: {total_failed}")
            print(f"  ✗ Errors: {len(r['errors'])}")
        
        print("\n" + "=" * 80)
        print("Pipeline execution completed!")
        print("=" * 80 + "\n")
    
    def run_full_pipeline(self) -> bool:
        """
        Run the complete pipeline: scrape → clean → details.
        
        Returns:
            True if all steps completed successfully
        """
        self.start_time = datetime.now()
        
        self.print_header("OLX TASHKENT RENTAL ANALYTICS - FULL PIPELINE")
        print(f"Started at: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Max pages per district: {self.max_pages}")
        if self.district_ids:
            print(f"Processing districts: {self.district_ids}")
        else:
            print(f"Processing all districts")
        
        # Step 1: Scrape
        if not self.step1_scrape_listings():
            print("\n⚠ Pipeline stopped: Scraping failed")
            return False
        
        # Step 2: Clean
        if not self.step2_clean_listings():
            print("\n⚠ Pipeline stopped: Cleaning failed")
            return False
        
        # Step 3: Details
        if not self.step3_scrape_details():
            print("\n⚠ Pipeline stopped: Detail scraping failed")
            return False
        
        # Summary
        self.print_final_summary()
        return True
    
    def run_scrape_only(self) -> bool:
        """Run only the scraping step."""
        self.start_time = datetime.now()
        self.print_header("SCRAPE ONLY MODE")
        return self.step1_scrape_listings()
    
    def run_clean_only(self) -> bool:
        """Run only the cleaning step."""
        self.start_time = datetime.now()
        self.print_header("CLEAN ONLY MODE")
        return self.step2_clean_listings()
    
    def run_details_only(self) -> bool:
        """Run only the details scraping step."""
        self.start_time = datetime.now()
        self.print_header("DETAILS ONLY MODE")
        return self.step3_scrape_details()


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="OLX Tashkent Rental Analytics - Data Collection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with default settings
  python main.py
  
  # Run full pipeline with 20 pages per district
  python main.py --max-pages 20
  
  # Only scrape specific districts
  python main.py --scrape-only --districts 26,25,24
  
  # Only clean existing data
  python main.py --clean-only
  
  # Only scrape details from cleaned data
  python main.py --details-only
        """
    )
    
    parser.add_argument(
        '--scrape-only',
        action='store_true',
        help='Only run the scraping step'
    )
    
    parser.add_argument(
        '--clean-only',
        action='store_true',
        help='Only run the cleaning step'
    )
    
    parser.add_argument(
        '--details-only',
        action='store_true',
        help='Only run the details scraping step'
    )
    
    parser.add_argument(
        '--max-pages',
        type=int,
        default=10,
        help='Maximum pages to scrape per district (default: 10)'
    )
    
    parser.add_argument(
        '--districts',
        type=str,
        help='Comma-separated district IDs to process (e.g., "26,25,24")'
    )
    
    args = parser.parse_args()
    
    # Parse district IDs if provided
    district_ids = None
    if args.districts:
        try:
            district_ids = [int(d.strip()) for d in args.districts.split(',')]
        except ValueError:
            print("❌ Error: Invalid district IDs format. Use comma-separated integers.")
            sys.exit(1)
    
    # Create pipeline
    pipeline = OLXScraperPipeline(
        max_pages=args.max_pages,
        district_ids=district_ids
    )
    
    # Run appropriate mode
    success = False
    
    if args.scrape_only:
        success = pipeline.run_scrape_only()
    elif args.clean_only:
        success = pipeline.run_clean_only()
    elif args.details_only:
        success = pipeline.run_details_only()
    else:
        # Run full pipeline
        success = pipeline.run_full_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
