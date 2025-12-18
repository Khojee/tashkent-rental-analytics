"""
HH Uzbekistan Map-based Job Scraper
Collects tech job vacancies from HH public API with coordinates
"""

import requests
import pandas as pd
from typing import List, Dict, Optional
from .utils import get_headers, respectful_delay, safe_get


class HHMapScraper:
    """
    Scraper for HH Uzbekistan vacancy data with coordinates
    """
    
    BASE_URL = "https://api.hh.ru"
    SITE_URL = "https://tashkent.hh.uz"
    VACANCIES_ENDPOINT = "/vacancies"
    
    def __init__(self):
        """Initialize the scraper"""
        self.session = requests.Session()
        # HH API prefers simpler headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        })
        self.vacancies = []
    
    def fetch_vacancies(self, page: int = 0, per_page: int = 100, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Fetch vacancy data from HH public API
        
        Args:
            page: Page number (0-indexed)
            per_page: Number of items per page (max 100)
            params: Additional query parameters
            
        Returns:
            JSON response data or None if request fails
        """
        url = f"{self.BASE_URL}{self.VACANCIES_ENDPOINT}"
        
        # Default parameters for tech jobs in Tashkent
        default_params = {
            'text': 'IT OR программист OR developer OR разработчик OR software OR python OR java OR javascript OR frontend OR backend',
            'area': '2759',  # Tashkent area code
            'per_page': str(per_page),
            'page': str(page),
            'only_with_salary': 'false',
        }
        
        if params:
            default_params.update(params)
        
        try:
            print(f"Fetching page {page} from HH API...")
            response = self.session.get(url, params=default_params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            print(f"Successfully fetched page {page}. Found {len(data.get('items', []))} vacancies")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    def fetch_vacancy_details(self, vacancy_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific vacancy
        
        Args:
            vacancy_id: Vacancy ID
            
        Returns:
            Detailed vacancy data or None
        """
        url = f"{self.BASE_URL}{self.VACANCIES_ENDPOINT}/{vacancy_id}"
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching vacancy {vacancy_id}: {e}")
            return None
    
    def parse_vacancy(self, vacancy: Dict, fetch_details: bool = True) -> Optional[Dict]:
        """
        Parse a single vacancy and extract required fields
        
        Args:
            vacancy: Vacancy data from API
            fetch_details: Whether to fetch full vacancy details for coordinates
            
        Returns:
            Parsed vacancy dictionary or None
        """
        try:
            vacancy_id = safe_get(vacancy, 'id', default='')
            
            # Extract basic info
            vacancy_name = safe_get(vacancy, 'name', default='NULL')
            company_name = safe_get(vacancy, 'employer', 'name', default='NULL')
            
            # Build vacancy URL
            vacancy_url = safe_get(vacancy, 'alternate_url', default='NULL')
            if vacancy_url == 'NULL' and vacancy_id:
                vacancy_url = f"{self.SITE_URL}/vacancy/{vacancy_id}"
            
            # Try to get coordinates from basic response
            lat = safe_get(vacancy, 'address', 'lat', default=None)
            lng = safe_get(vacancy, 'address', 'lng', default=None)
            district = safe_get(vacancy, 'area', 'name', default='NULL')
            
            # If coordinates not in basic response and fetch_details is True, get full details
            if (lat is None or lng is None) and fetch_details and vacancy_id:
                print(f"  Fetching details for vacancy {vacancy_id}...")
                details = self.fetch_vacancy_details(vacancy_id)
                if details:
                    lat = safe_get(details, 'address', 'lat', default='NULL')
                    lng = safe_get(details, 'address', 'lng', default='NULL')
                    if district == 'NULL':
                        district = safe_get(details, 'area', 'name', default='NULL')
                    
                    # Try alternative address fields
                    if lat == 'NULL':
                        lat = safe_get(details, 'address', 'location', 'lat', default='NULL')
                    if lng == 'NULL':
                        lng = safe_get(details, 'address', 'location', 'lng', default='NULL')
                
                # Be respectful - delay between detail requests
                respectful_delay(0.5, 1.5)
            
            # Set to NULL if still not found
            if lat is None:
                lat = 'NULL'
            if lng is None:
                lng = 'NULL'
            
            return {
                'vacancy_name': vacancy_name,
                'company_name': company_name,
                'latitude': lat,
                'longitude': lng,
                'district': district,
                'vacancy_url': vacancy_url
            }
            
        except Exception as e:
            print(f"Error parsing vacancy: {e}")
            return None
    
    def scrape(self, max_pages: int = 20, fetch_details: bool = False, custom_params: Optional[Dict] = None) -> List[Dict]:
        """
        Main scraping method
        
        Args:
            max_pages: Maximum number of pages to scrape (HH API allows max 2000 results = 20 pages of 100)
            fetch_details: Whether to fetch detailed info for each vacancy (slower but more coordinates)
            custom_params: Optional custom parameters for the API request
            
        Returns:
            List of scraped vacancies
        """
        print("Starting HH Uzbekistan vacancy scraper...")
        print(f"Will scrape up to {max_pages} pages")
        if fetch_details:
            print("⚠ Detailed fetching enabled - this will be slower but may get more coordinates")
        
        all_vacancies = []
        page = 0
        
        while page < max_pages:
            # Fetch page
            data = self.fetch_vacancies(page=page, params=custom_params)
            
            if not data:
                print(f"Failed to fetch page {page}, stopping")
                break
            
            items = data.get('items', [])
            if not items:
                print(f"No more vacancies found on page {page}")
                break
            
            # Parse vacancies
            for vacancy in items:
                parsed = self.parse_vacancy(vacancy, fetch_details=fetch_details)
                if parsed:
                    all_vacancies.append(parsed)
            
            # Check if there are more pages
            total_pages = data.get('pages', 0)
            print(f"Processed page {page + 1}/{min(total_pages, max_pages)}")
            
            if page >= total_pages - 1:
                print("Reached last page")
                break
            
            page += 1
            
            # Be respectful between page requests
            respectful_delay(1, 2)
        
        self.vacancies = all_vacancies
        print(f"\nSuccessfully scraped {len(self.vacancies)} vacancies")
        
        # Count how many have coordinates
        with_coords = sum(1 for v in self.vacancies if v['latitude'] != 'NULL' and v['longitude'] != 'NULL')
        print(f"Vacancies with coordinates: {with_coords}/{len(self.vacancies)} ({with_coords*100//len(self.vacancies) if self.vacancies else 0}%)")
        
        return self.vacancies
    
    def save_to_csv(self, filepath: str):
        """
        Save scraped vacancies to CSV file
        
        Args:
            filepath: Path to save the CSV file
        """
        if not self.vacancies:
            print("No vacancies to save")
            return
        
        df = pd.DataFrame(self.vacancies)
        
        # Ensure correct column order
        columns = ['vacancy_name', 'company_name', 'latitude', 'longitude', 'district', 'vacancy_url']
        df = df[columns]
        
        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"Saved {len(df)} vacancies to {filepath}")
