import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime, date, timedelta
from urllib.parse import urljoin
from pathlib import Path
from typing import Dict, List, Optional


class DistrictScraper:
    """
    A class to scrape OLX rental listings by district in Tashkent.
    """
    
    BASE_URL = "https://www.olx.uz"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    RUS_MONTHS = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
        'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    
    # District mapping: ID -> Name
    DISTRICT_MAP = {
        26: "yakkasarai",
        25: "yunusabad",
        24: "shaykhantohur",
        23: "chilonzor",
        22: "yashnabad",
        21: "uchtepa",
        20: "almazar",
        19: "sergeli",
        18: "bektemir",
        13: "mirabad",
        12: "mirzo-ulugbek"
    }
    
    def __init__(self, output_folder: str = "district_listing_page"):
        """
        Initialize the scraper.
        
        Args:
            output_folder: Directory where scraped CSV files will be saved
        """
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    @staticmethod
    def extract_card_id(url: str) -> Optional[str]:
        """Extract card ID from OLX URL."""
        m = re.search(r'ID([A-Za-z0-9]+)', url)
        return m.group(1) if m else None
    
    @staticmethod
    def parse_price(price_text: str) -> tuple:
        """
        Parse price text from OLX listing.
        
        Args:
            price_text: Raw price text (e.g., "1 200 у.е. Договорная")
            
        Returns:
            Tuple of (price_value, currency, raw_text)
        """
        if not price_text:
            return None, None, None
        
        raw = price_text.strip()
        s = raw.replace("\xa0", " ")
        
        # Extract numeric value
        price_val = None
        m_num = re.search(r"([\d\s,.]+)", s)
        if m_num:
            num = m_num.group(1).replace(" ", "").replace(",", ".")
            try:
                price_val = float(num)
            except:
                price_val = None
        
        # Extract currency
        m_cur = re.search(r"[\d\s,.]+\s*([^\d\s,\.]+(?:\.[^\d\s,\.]+)?)", s)
        currency = m_cur.group(1).strip() if m_cur else None
        
        return price_val, currency, raw
    
    @classmethod
    def parse_location_date(cls, text: str) -> Dict:
        """
        Parse location and date information from listing.
        
        Args:
            text: Location and date text (e.g., "Ташкент, Шайхантахурский район - Сегодня в 10:47")
            
        Returns:
            Dictionary with location_text, posted_date_raw, posted_date, time_raw
        """
        if not text:
            return {
                "location_text": None,
                "posted_date_raw": None,
                "posted_date": None,
                "time_raw": None
            }
        
        s = text.strip()
        
        # Split location and date
        if " - " in s:
            loc, dt = s.split(" - ", 1)
        else:
            parts = s.split("  ")
            loc = parts[0]
            dt = parts[1] if len(parts) > 1 else ""
        
        loc = loc.strip()
        dt = dt.strip()
        
        parsed_date = None
        time_part = None
        
        # Parse "Сегодня" (Today)
        if "Сегодня" in dt:
            m = re.search(r"Сегодня\s*в\s*([0-2]?\d:[0-5]\d)", dt)
            if m:
                time_part = m.group(1)
            parsed_date = date.today()
        
        # Parse "Вчера" (Yesterday)
        elif "Вчера" in dt:
            m = re.search(r"Вчера\s*в\s*([0-2]?\d:[0-5]\d)", dt)
            if m:
                time_part = m.group(1)
            parsed_date = date.today() - timedelta(days=1)
        
        else:
            # Try "21 ноября в 13:20" format
            m1 = re.search(r"(\d{1,2})\s+([а-я]+)\s*(?:в\s*([0-2]?\d:[0-5]\d))?", dt, flags=re.IGNORECASE)
            if m1:
                day = int(m1.group(1))
                month_name = m1.group(2).lower()
                month = cls.RUS_MONTHS.get(month_name)
                if month:
                    yr = date.today().year
                    if month > date.today().month:
                        yr -= 1
                    try:
                        parsed_date = date(yr, month, day)
                    except:
                        parsed_date = None
                    time_part = m1.group(3)
            else:
                # Try "01.11.2025" format
                m2 = re.search(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", dt)
                if m2:
                    day = int(m2.group(1))
                    mon = int(m2.group(2))
                    yr = int(m2.group(3)) if m2.group(3) else date.today().year
                    if yr < 100:
                        yr += 2000
                    try:
                        parsed_date = date(yr, mon, day)
                    except:
                        parsed_date = None
        
        return {
            "location_text": loc,
            "posted_date_raw": dt,
            "posted_date": parsed_date.isoformat() if parsed_date else None,
            "time_raw": time_part
        }
    
    def parse_card(self, card_tag) -> Dict:
        """
        Parse a single listing card element.
        
        Args:
            card_tag: BeautifulSoup Tag representing a listing card
            
        Returns:
            Dictionary with card information
        """
        # Extract title and URL
        a = card_tag.select_one("a.css-1tqlkj0")
        title = None
        url = None
        if a:
            h4 = a.find("h4")
            title = h4.get_text(strip=True) if h4 else a.get_text(strip=True)
            href = a.get("href")
            if href:
                url = urljoin(self.BASE_URL, href)
        
        # Extract price
        price_tag = card_tag.select_one('p[data-testid="ad-price"]')
        price_raw = price_tag.get_text(" ", strip=True) if price_tag else None
        price_value, price_currency, _ = self.parse_price(price_raw)
        
        # Extract location and date
        loc_tag = card_tag.select_one('p[data-testid="location-date"]')
        loc_text = loc_tag.get_text(" ", strip=True) if loc_tag else None
        loc_parsed = self.parse_location_date(loc_text)
        
        return {
            "title": title,
            "url": url,
            "price_raw": price_raw,
            "price_value": price_value,
            "price_currency": price_currency,
            "location_text": loc_parsed["location_text"],
            "posted_date_raw": loc_parsed["posted_date_raw"],
            "posted_date": loc_parsed["posted_date"],
            "time_raw": loc_parsed["time_raw"]
        }
    
    def scrape_district(self, district_id: int, district_name: str, 
                       max_pages: int = 20, sleep_between_pages: float = 1.5) -> str:
        """
        Scrape listings for a single district.
        
        Args:
            district_id: OLX district ID
            district_name: Name of the district
            max_pages: Maximum number of pages to scrape
            sleep_between_pages: Delay between page requests (seconds)
            
        Returns:
            Path to the saved CSV file
        """
        results = []
        
        for page in range(1, max_pages + 1):
            page_url = (
                f"{self.BASE_URL}/nedvizhimost/kvartiry/arenda-dolgosrochnaya/tashkent/"
                f"?search[district_id]={district_id}&currency=UZS&page={page}"
            )
            
            print(f"[{district_name}] Fetching page {page}: {page_url}")
            
            try:
                r = self.session.get(page_url, timeout=20)
                if r.status_code != 200:
                    print(f"  HTTP {r.status_code} — stopping.")
                    break
            except Exception as e:
                print(f"  Error fetching page: {e}")
                break
            
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Find listing cards
            cards = soup.select("div[data-testid='listing-grid'] a.css-1tqlkj0")
            if not cards:
                cards = soup.select("div.css-1sw7q4x")
            
            parsed_count = 0
            for el in cards:
                # Navigate to parent card element
                parent = el
                for _ in range(4):
                    if parent.name in ("article", "div", "li"):
                        break
                    if parent.parent:
                        parent = parent.parent
                
                card = parent
                row = self.parse_card(card)
                
                # Validate: must have URL
                if row.get("url"):
                    row["card_id"] = self.extract_card_id(row["url"])
                    row["district_id"] = district_id
                    row["district_name"] = district_name
                    results.append(row)
                    parsed_count += 1
            
            print(f"  Parsed {parsed_count} listings on page {page}.")
            
            # Stop if no listings found
            if parsed_count == 0:
                break
            
            time.sleep(sleep_between_pages)
        
        # Save to CSV
        df = pd.DataFrame(results)
        outpath = self.output_folder / f"{district_name.replace(' ', '_').lower()}.csv"
        df.to_csv(outpath, index=False, encoding="utf-8-sig")
        print(f"[{district_name}] Saved {len(df)} rows to {outpath}")
        
        return str(outpath)
    
    def scrape_all_districts(self, max_pages: int = 20, 
                            sleep_between_pages: float = 1.5,
                            district_ids: Optional[List[int]] = None) -> Dict:
        """
        Scrape all districts or a subset of districts.
        
        Args:
            max_pages: Maximum pages per district
            sleep_between_pages: Delay between page requests
            district_ids: Optional list of specific district IDs to scrape
            
        Returns:
            Dictionary with scraping results
        """
        results = {
            'scraped': 0,
            'files': [],
            'errors': []
        }
        
        # Determine which districts to scrape
        if district_ids:
            districts_to_scrape = {did: self.DISTRICT_MAP[did] 
                                  for did in district_ids if did in self.DISTRICT_MAP}
        else:
            districts_to_scrape = self.DISTRICT_MAP
        
        print(f"\n{'='*70}")
        print(f"Starting scrape for {len(districts_to_scrape)} districts")
        print(f"{'='*70}\n")
        
        for district_id, district_name in districts_to_scrape.items():
            try:
                output_path = self.scrape_district(
                    district_id, 
                    district_name, 
                    max_pages, 
                    sleep_between_pages
                )
                results['scraped'] += 1
                results['files'].append((district_id, district_name, output_path))
                print(f"✓ {district_name} completed\n")
            except Exception as e:
                error_msg = f"Error scraping {district_name}: {str(e)}"
                results['errors'].append((district_id, district_name, str(e)))
                print(f"✗ {error_msg}\n")
        
        return results


# Convenience function
def scrape_all_districts(output_folder: str = "district_listing_page",
                        max_pages: int = 20,
                        sleep_between_pages: float = 1.5) -> Dict:
    """
    Convenience function to scrape all districts.
    
    Args:
        output_folder: Directory for output CSV files
        max_pages: Maximum pages per district
        sleep_between_pages: Delay between requests
        
    Returns:
        Dictionary with scraping results
    """
    scraper = DistrictScraper(output_folder)
    return scraper.scrape_all_districts(max_pages, sleep_between_pages)


# Example usage
if __name__ == "__main__":
    scraper = DistrictScraper()
    results = scraper.scrape_all_districts(max_pages=10, sleep_between_pages=1.5)
    
    print("\n" + "="*70)
    print("SCRAPING SUMMARY")
    print("="*70)
    print(f"Districts scraped: {results['scraped']}")
    print(f"Errors: {len(results['errors'])}")
    
    if results['files']:
        print("\nScraped districts:")
        for district_id, district_name, output_path in results['files']:
            print(f"  • {district_name} (ID: {district_id}) → {Path(output_path).name}")
    
    if results['errors']:
        print("\nErrors:")
        for district_id, district_name, error in results['errors']:
            print(f"  • {district_name} (ID: {district_id}): {error}")
