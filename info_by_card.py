from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
import os
import glob
import random

# Configuration
INPUT_DIR = "district_listing_page"
OUTPUT_DIR = "cards_details"
SAVE_INTERVAL = 50  # Save progress every N cards
MIN_DELAY = 1.0     # Minimum delay between requests (seconds)
MAX_DELAY = 2.5     # Maximum delay between requests (seconds)
REQUEST_TIMEOUT = 10  # Reduced timeout for faster failure detection

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

MONTHS_RU = {
    "января": "01",
    "февраля": "02",
    "марта": "03",
    "апреля": "04",
    "мая": "05",
    "июня": "06",
    "июля": "07",
    "августа": "08",
    "сентября": "09",
    "октября": "10",
    "ноября": "11",
    "декабря": "12",
}

def parse_olx_date(text: str):
    text = text.strip()
    today = datetime.today()

    if text.startswith("Сегодня"):
        return today.strftime("%Y-%m-%d")

    if text.startswith("Вчера"):
        d = today - timedelta(days=1)
        return d.strftime("%Y-%m-%d")

    parts = text.replace(" г.", "").split()
    day = parts[0]
    month = MONTHS_RU.get(parts[1].lower())
    year = parts[2]

    return f"{year}-{month}-{day}"

def parse_detail_page(html, card_id):
    soup = BeautifulSoup(html, "lxml")

    params = {
        "card_id": card_id,
        "area": None,
        "number_rooms": None,
        "furniture": None,
        "condition": None,
        "date": None
    }

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

    date_block = soup.find("span", {"data-testid": "ad-posted-at"})
    if date_block:
        params["date"] = parse_olx_date(date_block.get_text(strip=True))

    return params

def fetch_detail(card_id, url):
    """Fetch details for a single card with improved error handling"""
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        if r.status_code == 200:
            return parse_detail_page(r.text, card_id)
        else:
            print(f"  ⚠ HTTP {r.status_code}")
            return None
    except requests.Timeout:
        print(f"  ⚠ Timeout after {REQUEST_TIMEOUT}s")
        return None
    except requests.RequestException as e:
        print(f"  ⚠ Request error: {e}")
        return None
    except Exception as e:
        print(f"  ⚠ Parse error: {e}")
        return None

def save_progress(details_list, output_file):
    """Save current progress to CSV"""
    if details_list:
        df = pd.DataFrame(details_list)
        df.to_csv(output_file, index=False)
        return True
    return False

def process_district(input_csv, district_name):
    """Process a single district CSV file"""
    
    output_file = os.path.join(OUTPUT_DIR, f"{district_name}_cards_details.csv")
    
    print("\n" + "=" * 70)
    print(f"DISTRICT: {district_name.upper()}")
    print("=" * 70)
    
    # Load card list
    print(f"Loading cards from {input_csv}...")
    cards_csv_rows = pd.read_csv(input_csv).to_dict("records")
    total_cards = len(cards_csv_rows)
    print(f"Found {total_cards} cards to process")
    
    # Load existing results if file exists (resume capability)
    if os.path.exists(output_file):
        print(f"Found existing {output_file}, loading processed cards...")
        existing_df = pd.read_csv(output_file)
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
        info = fetch_detail(card_id, url)
        
        if info:
            details.append(info)
            success_count += 1
            print(f"  ✓ Success (Total: {success_count})")
        else:
            failed_count += 1
            print(f"  ✗ Failed (Total failed: {failed_count})")
        
        # Incremental save
        if (idx % SAVE_INTERVAL == 0) or (idx == total_cards):
            if save_progress(details, output_file):
                print(f"  💾 Progress saved: {len(details)} cards")
        
        # Random delay to avoid detection (except on last iteration)
        if idx < total_cards:
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            time.sleep(delay)
    
    # Final save
    save_progress(details, output_file)
    
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
        "final_count": len(details)
    }

# Main execution
if __name__ == "__main__":
    # Find all CSV files in the input directory
    csv_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    
    if not csv_files:
        print(f"❌ No CSV files found in {INPUT_DIR}/")
        exit(1)
    
    print("=" * 70)
    print("OLX CARD DETAILS SCRAPER - MULTI-DISTRICT MODE")
    print("=" * 70)
    print(f"\nFound {len(csv_files)} district CSV files:")
    for csv_file in csv_files:
        district_name = os.path.splitext(os.path.basename(csv_file))[0]
        print(f"  • {district_name}")
    print(f"\nOutput directory: {OUTPUT_DIR}/")
    print(f"Configuration:")
    print(f"  • Save interval: {SAVE_INTERVAL} cards")
    print(f"  • Delay range: {MIN_DELAY}-{MAX_DELAY}s")
    print(f"  • Timeout: {REQUEST_TIMEOUT}s")
    
    # Process each district
    all_results = []
    
    for csv_file in csv_files:
        district_name = os.path.splitext(os.path.basename(csv_file))[0]
        result = process_district(csv_file, district_name)
        all_results.append(result)
    
    # Final overall summary
    print("\n" + "=" * 70)
    print("OVERALL SUMMARY - ALL DISTRICTS")
    print("=" * 70)
    
    total_all = sum(r["total"] for r in all_results)
    successful_all = sum(r["successful"] for r in all_results)
    failed_all = sum(r["failed"] for r in all_results)
    final_all = sum(r["final_count"] for r in all_results)
    
    print(f"\nProcessed {len(all_results)} districts:")
    for r in all_results:
        print(f"  • {r['district']:20s} - {r['final_count']:4d} cards ({r['successful']:3d} new, {r['failed']:2d} failed)")
    
    print(f"\nGrand Total:")
    print(f"  Total cards:     {total_all}")
    print(f"  Successful:      {successful_all}")
    print(f"  Failed:          {failed_all}")
    print(f"  Final dataset:   {final_all} cards")
    print(f"\n✅ All results saved to {OUTPUT_DIR}/ folder")
    print("=" * 70)
