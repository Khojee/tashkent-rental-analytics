import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime, date, timedelta
from urllib.parse import urljoin

BASE = "https://www.olx.uz"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Take card ID from URL
def extract_card_id(url):
    m = re.search(r'ID([A-Za-z0-9]+)', url)
    return m.group(1) if m else None

# --- Helper parsers --------------------------------------------------------

def parse_price(price_text):
    """
    Example price_text: "1 200 у.е. Договорная"
    Returns: (price_value (float or None), currency_str, price_raw)
    """
    if not price_text:
        return None, None, None
    raw = price_text.strip()
    # remove extra words like "Договорная"
    # Extract first numeric group and adjacent non-digit currency tokens
    # Normalize non-breaking spaces
    s = raw.replace("\xa0", " ")
    # Find number
    m_num = re.search(r"([\d\s,.]+)", s)
    price_val = None
    if m_num:
        num = m_num.group(1)
        num = num.replace(" ", "").replace(",", ".")
        try:
            price_val = float(num)
        except:
            price_val = None
    # Find currency (non-digit text near number)
    m_cur = re.search(r"[\d\s,.]+\s*([^\d\s,\.]+(?:\.[^\d\s,\.]+)?)", s)
    currency = m_cur.group(1).strip() if m_cur else None
    # If currency includes cyrillic like 'у.е.' or 'сум' keep it
    return price_val, currency, raw

RUS_MONTHS = {
    'января':1,'февраля':2,'марта':3,'апреля':4,'мая':5,'июня':6,
    'июля':7,'августа':8,'сентября':9,'октября':10,'ноября':11,'декабря':12
}

def parse_location_date(text):
    """
    Input examples:
      "Ташкент, Шайхантахурский район - Сегодня в 10:47"
      "Ташкент, Юнусабадский район - Вчера в 18:03"
      "Ташкент, Мирзо-Улугбекский район - 21 ноября в 13:20"
      "Ташкент, Чиланзар - 01.11.2025"
    Returns dict: {'location_text', 'posted_date_raw', 'posted_date' (ISO or None), 'time_raw'}
    """
    if not text:
        return {"location_text": None, "posted_date_raw": None, "posted_date": None, "time_raw": None}
    s = text.strip()
    # split location and date/time by ' - ' if present
    if " - " in s:
        loc, dt = s.split(" - ", 1)
    else:
        # sometimes comma separated
        parts = s.split("  ")
        loc = parts[0]
        dt = parts[1] if len(parts) > 1 else ""
    loc = loc.strip()
    dt = dt.strip()

    parsed_date = None
    time_part = None

    if dt.startswith("Сегодня") or "Сегодня" in dt:
        # extract time if present
        m = re.search(r"Сегодня\s*в\s*([0-2]?\d:[0-5]\d)", dt)
        if m:
            time_part = m.group(1)
        parsed_date = date.today()
    elif dt.startswith("Вчера") or "Вчера" in dt:
        m = re.search(r"Вчера\s*в\s*([0-2]?\d:[0-5]\d)", dt)
        if m:
            time_part = m.group(1)
        parsed_date = date.today() - timedelta(days=1)
    else:
        # try "21 ноября в 13:20" or "01.11.2025" or "21 ноября"
        m1 = re.search(r"(\d{1,2})\s+([а-я]+)\s*(?:в\s*([0-2]?\d:[0-5]\d))?", dt, flags=re.IGNORECASE)
        if m1:
            day = int(m1.group(1))
            month_name = m1.group(2).lower()
            month = RUS_MONTHS.get(month_name)
            if month:
                # year: assume current year unless month in future then previous year (simple heuristic)
                yr = date.today().year
                if month > date.today().month:
                    yr -= 1
                try:
                    parsed_date = date(yr, month, day)
                except:
                    parsed_date = None
                time_part = m1.group(3)
        else:
            # try dot-date e.g., 01.11.2025 or 01.11
            m2 = re.search(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", dt)
            if m2:
                day = int(m2.group(1))
                mon = int(m2.group(2))
                yr = int(m2.group(3)) if m2.group(3) else date.today().year
                if yr < 100: yr += 2000
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

# --- Card parsing (one listing card element) --------------------------------

def parse_card(card_tag):
    """
    Given a BeautifulSoup Tag representing a listing card, return a dict:
    { title, url, price_raw, price_value, price_currency, location_text, posted_date_raw, posted_date_iso }
    """
    # Title & URL
    a = card_tag.select_one("a.css-1tqlkj0")
    title = None
    url = None
    if a:
        h4 = a.find("h4")
        title = h4.get_text(strip=True) if h4 else a.get_text(strip=True)
        href = a.get("href")
        if href:
            url = urljoin(BASE, href)

    # Price block
    price_tag = card_tag.select_one('p[data-testid="ad-price"]')
    price_raw = price_tag.get_text(" ", strip=True) if price_tag else None
    price_value, price_currency, _ = parse_price(price_raw)

    # Location & date
    loc_tag = card_tag.select_one('p[data-testid="location-date"]')
    loc_text = loc_tag.get_text(" ", strip=True) if loc_tag else None
    loc_parsed = parse_location_date(loc_text)

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

# --- Main crawl for a district -----------------------------------------------

def scrape_district(district_id, district_name, max_pages=20, sleep_between_pages=1.5, out_dir="./district_listing_page"):
    """
    Crawl listing cards for a single district and save to {district_name}.csv
    """
    results = []
    session = requests.Session()
    session.headers.update(HEADERS)

    for page in range(1, max_pages + 1):
        page_url = f"{BASE}/nedvizhimost/kvartiry/arenda-dolgosrochnaya/tashkent/?search[district_id]={district_id}&currency=UZS&page={page}"
        print(f"[{district_name}] Fetching page {page}: {page_url}")
        r = session.get(page_url, timeout=20)
        if r.status_code != 200:
            print("  HTTP", r.status_code, " — stopping.")
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # --- SELECTOR: card container (may need slight tweaks if site changes)
        # We search for data-testid in nested elements as a general approach:
        cards = soup.select("div[data-testid='listing-grid'] a.css-1tqlkj0")  # sometimes cards are anchors directly
        # fallback: find containers and iterate
        if not cards:
            cards = soup.select("div.css-1sw7q4x")  # alternate OLX card wrapper
        # If we got anchors, their parent container is the card
        parsed_count = 0
        for el in cards:
            # If el is anchor (<a>), get its parent card element to pass to parse_card.
            parent = el
            # Try to climb up until a block element that looks like a card (heuristic)
            for _ in range(4):
                if parent.name in ("article", "div", "li"):
                    break
                if parent.parent:
                    parent = parent.parent
            card = parent
            row = parse_card(card)
            # minimal validation: must have url
            if row.get("url"):
                row["card_id"] = extract_card_id(row["url"])
                row["district_id"] = district_id
                row["district_name"] = district_name
                results.append(row)
                parsed_count += 1

        print(f"  Parsed {parsed_count} listings on page {page}.")
        # basic termination: if parsed_count==0 assume no more pages
        if parsed_count == 0:
            break

        time.sleep(sleep_between_pages)

    # Save to CSV
    df = pd.DataFrame(results)
    outpath = f"{out_dir}/{district_name.replace(' ', '_').lower()}.csv"
    df.to_csv(outpath, index=False, encoding="utf-8-sig")
    print(f"[{district_name}] Saved {len(df)} rows to {outpath}")
    return outpath

# --- Example usage ----------------------------------------------------------
if __name__ == "__main__":
    # Example: Yunusabad (id=25), Shaykhantohur (id=24)
    district_map = {26: "yakkasarai",
                    25: "yunusabad",
                    24: "shaykhantohur",
                    23: "chilonzor",
                    22: "yashnabad",
                    21: "uchtepa",
                    20: "almazar",
                    19: "sergeli",
                    18: "bektemir",
                    13: "mirabad",
                    12: "mirzo-ulugbek"}
    for did, name in district_map.items():
        scrape_district(did, name, max_pages=10, sleep_between_pages=1.5)
