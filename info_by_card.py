from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
import os
import glob
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class CardDetailsScraper:
    """
    A class to scrape detailed information from individual OLX rental listing cards.
    """
    
    MONTHS_RU = {
        "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
        "мая": "05", "июня": "06", "июля": "07", "августа": "08",
        "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
    }
    
    def __init__(self, 
                 input_folder: str = "district_listing_page_cleaned",
                 output_folder: str = "cards_details",
                 save_interval: int = 50,
                 min_delay: float = 0.2,
                 max_delay: float = 0.6,
                 request_timeout: int = 10):
        """
        Initialize the card details scraper.
        
        Args:
            input_folder: Directory containing cleaned district CSV files
            output_folder: Directory where detailed card info will be saved
            save_interval: Save progress every N cards
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
            request_timeout: Request timeout (seconds)
        """
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.save_interval = save_interval
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.request_timeout = request_timeout
        
        # Create output directory
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    
    @classmethod
    def parse_olx_date(cls, text: str) -> str:
        """
        Parse Russian date text from OLX to ISO format.
        
        Args:
            text: Date text (e.g., "Сегодня", "Вчера", "21 ноября 2024 г.")
            
        Returns:
            Date in YYYY-MM-DD format
        """
        text = text.strip()
        today = datetime.today()
        
        if text.startswith("Сегодня"):
            return today.strftime("%Y-%m-%d")
        
        if text.startswith("Вчера"):
            d = today - timedelta(days=1)
            return d.strftime("%Y-%m-%d")
        
        # Parse "21 ноября 2024 г." format
        parts = text.replace(" г.", "").split()
        day = parts[0]
        month = cls.MONTHS_RU.get(parts[1].lower())
        year = parts[2]
        
        return f"{year}-{month}-{day}"
    
    def parse_detail_page(self, html: str, card_id: str) -> Dict:
        """
        Parse detailed information from a card's detail page.
        
        Args:
            html: HTML content of the detail page
            card_id: Card ID
            
        Returns:
            Dictionary with parsed details
        """
        soup = BeautifulSoup(html, "lxml")
        
        params = {
            "card_id": card_id,
            "area": None,
            "number_rooms": None,
            "furniture": None,
            "condition": None,
            "date": None
        }
        
        # Parse parameters container
        container = soup.find("div", {"data-testid": "ad-parameters-container"})
        if container:
            for p in container.find_all("p"):
                text = p.get_text(strip=True)
                
                if text.startswith("Количество комнат"):
                    params["number_rooms"] = text.split(":")[-1].strip()
                
                elif text.startswith("Общая площадь"):
                    params["area"] = text.split(":")[-1].strip()
                
                elif text.startswith("Меблирована"):
                    val = text.split(":")[-1].strip()
                    params["furniture"] = 1 if val.lower() == "да" else 0
                
                elif text.startswith("Ремонт"):
                    params["condition"] = text.split(":")[-1].strip()
        
        # Parse posted date
        date_block = soup.find("span", {"data-testid": "ad-posted-at"})
        if date_block:
            params["date"] = self.parse_olx_date(date_block.get_text(strip=True))
        
        return params
    
    def fetch_detail(self, card_id: str, url: str) -> Optional[Dict]:
        """
        Fetch details for a single card with error handling.
        
        Args:
            card_id: Card ID
            url: URL of the card detail page
            
        Returns:
            Dictionary with card details or None if failed
        """
        try:
            r = self.session.get(url, timeout=self.request_timeout)
            if r.status_code == 200:
                return self.parse_detail_page(r.text, card_id)
            else:
                print(f"  ⚠ HTTP {r.status_code}")
                return None
        except requests.Timeout:
            print(f"  ⚠ Timeout after {self.request_timeout}s")
            return None
        except requests.RequestException as e:
            print(f"  ⚠ Request error: {e}")
            return None
        except Exception as e:
            print(f"  ⚠ Parse error: {e}")
            return None
    
    def save_progress(self, details_list: List[Dict], output_file: Path) -> bool:
        """
        Save current progress to CSV.
        
        Args:
            details_list: List of detail dictionaries
            output_file: Path to output CSV file
            
        Returns:
            True if saved successfully
        """
        if details_list:
            df = pd.DataFrame(details_list)
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            return True
        return False
    
    def process_district(self, input_csv: Path, district_name: str) -> Dict:
        """
        Process a single district CSV file to scrape card details.
        
        Args:
            input_csv: Path to input CSV file
            district_name: Name of the district
            
        Returns:
            Dictionary with processing statistics
        """
        output_file = self.output_folder / f"{district_name}_cards_details.csv"
        
        print("\n" + "=" * 70)
        print(f"DISTRICT: {district_name.upper()}")
        print("=" * 70)
        
        # Load card list
        print(f"Loading cards from {input_csv}...")
        cards_csv_rows = pd.read_csv(input_csv, encoding="utf-8-sig").to_dict("records")
        total_cards = len(cards_csv_rows)
        print(f"Found {total_cards} cards to process")
        
        # Load existing results if file exists (resume capability)
        if output_file.exists():
            print(f"Found existing {output_file}, loading processed cards...")
            existing_df = pd.read_csv(output_file, encoding="utf-8-sig")
            processed_ids = set(existing_df["card_id"].values)
            details = existing_df.to_dict("records")
            print(f"Already processed: {len(processed_ids)} cards")
            print(f"Remaining: {total_cards - len(processed_ids)} cards")
        else:
            processed_ids = set()
            details = []
            print("Starting fresh scrape...")
        
        # Main scraping loop
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        print("-" * 70)
        
        for idx, row in enumerate(cards_csv_rows, 1):
            card_id = row["card_id"]
            url = row["url"]
            
            # Skip if already processed
            if card_id in processed_ids:
                skipped_count += 1
                continue
            
            # Progress indicator
            print(f"[{idx}/{total_cards}] Card: {card_id}")
            
            # Fetch details
            info = self.fetch_detail(card_id, url)
            
            if info:
                details.append(info)
                success_count += 1
                print(f"  ✓ Success (Total: {success_count})")
            else:
                failed_count += 1
                print(f"  ✗ Failed (Total failed: {failed_count})")
            
            # Incremental save
            if (idx % self.save_interval == 0) or (idx == total_cards):
                if self.save_progress(details, output_file):
                    print(f"  💾 Progress saved: {len(details)} cards")
            
            # Random delay (except on last iteration)
            if idx < total_cards:
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
        
        # Final save
        self.save_progress(details, output_file)
        
        # Summary for this district
        print("-" * 70)
        print(f"District Summary ({district_name}):")
        print(f"  Total cards:     {total_cards}")
        print(f"  Skipped:         {skipped_count} (already processed)")
        print(f"  Successful:      {success_count}")
        print(f"  Failed:          {failed_count}")
        print(f"  Final dataset:   {len(details)} cards")
        print(f"  Saved to:        {output_file}")
        
        return {
            "district": district_name,
            "total": total_cards,
            "skipped": skipped_count,
            "successful": success_count,
            "failed": failed_count,
            "final_count": len(details),
            "output_file": str(output_file)
        }
    
    def get_csv_files(self) -> List[Tuple[Path, str]]:
        """
        Get all CSV files from the input folder.
        
        Returns:
            List of tuples (file_path, district_name)
        """
        csv_files = []
        for file in self.input_folder.glob("*.csv"):
            # Extract district name (remove _cleaned suffix if present)
            district_name = file.stem.replace("_cleaned", "")
            csv_files.append((file, district_name))
        return csv_files
    
    def process_all_districts(self) -> Dict:
        """
        Process all district CSV files in the input folder.
        
        Returns:
            Dictionary with overall processing results
        """
        csv_files = self.get_csv_files()
        
        if not csv_files:
            print(f"❌ No CSV files found in {self.input_folder}/")
            return {
                'processed': 0,
                'results': [],
                'errors': []
            }
        
        print(f"\nFound {len(csv_files)} district CSV files:")
        for file_path, district_name in csv_files:
            print(f"  • {district_name}")
        print(f"\nOutput directory: {self.output_folder}/")
        print(f"Configuration:")
        print(f"  • Save interval: {self.save_interval} cards")
        print(f"  • Delay range: {self.min_delay}-{self.max_delay}s")
        print(f"  • Timeout: {self.request_timeout}s")
        
        # Process each district
        all_results = []
        errors = []
        
        for file_path, district_name in csv_files:
            try:
                result = self.process_district(file_path, district_name)
                all_results.append(result)
            except Exception as e:
                error_msg = f"Error processing {district_name}: {str(e)}"
                errors.append((district_name, str(e)))
                print(f"\n✗ {error_msg}\n")
        
        # Final overall summary
        print("\n" + "=" * 70)
        print("OVERALL SUMMARY - ALL DISTRICTS")
        print("=" * 70)
        
        if all_results:
            total_all = sum(r["total"] for r in all_results)
            successful_all = sum(r["successful"] for r in all_results)
            failed_all = sum(r["failed"] for r in all_results)
            final_all = sum(r["final_count"] for r in all_results)
            
            print(f"\nProcessed {len(all_results)} districts:")
            for r in all_results:
                print(f"  • {r['district']:20s} - {r['final_count']:4d} cards "
                      f"({r['successful']:3d} new, {r['failed']:2d} failed)")
            
            print(f"\nGrand Total:")
            print(f"  Total cards:     {total_all}")
            print(f"  Successful:      {successful_all}")
            print(f"  Failed:          {failed_all}")
            print(f"  Final dataset:   {final_all} cards")
            print(f"\n✅ All results saved to {self.output_folder}/ folder")
        
        if errors:
            print(f"\n⚠ Errors encountered:")
            for district, error in errors:
                print(f"  • {district}: {error}")
        
        print("=" * 70)
        
        return {
            'processed': len(all_results),
            'results': all_results,
            'errors': errors
        }


# Convenience function
def scrape_all_card_details(input_folder: str = "district_listing_page_cleaned",
                           output_folder: str = "cards_details",
                           save_interval: int = 50,
                           min_delay: float = 1.0,
                           max_delay: float = 2.5) -> Dict:
    """
    Convenience function to scrape details for all district cards.
    
    Args:
        input_folder: Directory containing cleaned district CSV files
        output_folder: Directory for output files
        save_interval: Save progress every N cards
        min_delay: Minimum delay between requests
        max_delay: Maximum delay between requests
        
    Returns:
        Dictionary with processing results
    """
    scraper = CardDetailsScraper(
        input_folder=input_folder,
        output_folder=output_folder,
        save_interval=save_interval,
        min_delay=min_delay,
        max_delay=max_delay
    )
    return scraper.process_all_districts()


# Example usage
if __name__ == "__main__":
    scraper = CardDetailsScraper()
    results = scraper.process_all_districts()
