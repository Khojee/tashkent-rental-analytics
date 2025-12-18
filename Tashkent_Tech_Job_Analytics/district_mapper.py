"""
Script to map coordinates to Tashkent city districts
"""

import pandas as pd
import requests
import json
import os
import time
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import unary_union


class DistrictMapper:
    """
    Maps coordinates to districts using OSM boundaries
    """
    
    # Overpass API to fetch administrative boundaries
    OVERPASS_URL = "http://overpass-api.de/api/interpreter"
    DATA_FILE = "Tashkent_Tech_Job_Analytics/data/tashkent_districts.json"
    
    def __init__(self):
        """Initialize the mapper"""
        self.districts = {}
        self.data_dir = os.path.join(os.path.dirname(__file__), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.json_path = os.path.join(os.path.dirname(__file__), self.DATA_FILE)
        
        # Debug path
        print(f"Looking for data at: {self.json_path}")
        print(f"File exists: {os.path.exists(self.json_path)}")
        if not os.path.exists(self.json_path):
            # Check for backup/alternate name
            alt_path = os.path.join(self.data_dir, 'tashkent_overpass.json')
            if os.path.exists(alt_path):
                print(f"Found alternate file at {alt_path}, using that.")
                self.json_path = alt_path
                
        self._load_districts()
        
    def _fetch_districts(self):
        """Fetch district boundaries from Overpass API"""
        if os.path.exists(self.json_path):
            print("Using existing district data file")
            return
            
        print("Fetching Tashkent districts from Overpass API...")
        
        # Query for Tashkent City districts (admin_level=8 inside admin_level=4)
        query = """
        [out:json][timeout:90];
        area["name:en"="Tashkent"]->.searchArea;
        (
          relation(area.searchArea)["boundary"="administrative"]["name"];
        );
        out geom;
        """
        
        try:
            response = requests.get(self.OVERPASS_URL, params={'data': query}, timeout=100)
            if response.status_code == 200:
                with open(self.json_path, 'w', encoding='utf-8') as f:
                    json.dump(response.json(), f)
                print("Successfully downloaded district data")
            else:
                print(f"Failed to download data: {response.status_code}")
                
        except Exception as e:
            print(f"Error fetching data: {e}")
            
    def _load_districts(self):
        """Load and parse district polygons"""
        self._fetch_districts()
        
        if not os.path.exists(self.json_path):
            print("No district data file found")
            return
            
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print("Parsing district boundaries...")
            count = 0
            
            for element in data.get('elements', []):
                tags = element.get('tags', {})
                name = tags.get('name:en') or tags.get('name') or tags.get('name:uz')
                
                if not name or name == "Tashkent District": 
                    continue
                    
                # Clean up name (case insensitive)
                import re
                clean_name = re.sub(r'(\s+district|\s+tumani)$', '', name, flags=re.IGNORECASE).strip()
                
                try:
                    polygon = self._parse_osm_relation(element, clean_name)
                    if polygon:
                        self.districts[clean_name] = polygon
                        count += 1
                    else:
                        print(f"  WARNING: Failed to build polygon for {name}")
                except Exception as e:
                    print(f"  Error parsing polygon for {name}: {e}")
            
            print(f"Loaded {count} districts: {list(self.districts.keys())}")
            
        except Exception as e:
            print(f"Error loading districts: {e}")

    def _parse_osm_relation(self, element, debug_name=""):
        """
        Parse OSM relation with geometry into a Shapely Polygon/MultiPolygon
        """
        members = element.get('members', [])
        outer_ways = []
        
        for member in members:
            # We are interested in outer boundaries
            if member.get('type') == 'way' and 'geometry' in member:
                role = member.get('role', '')
                coords = [(p['lon'], p['lat']) for p in member['geometry']]
                
                # Check for empty coords
                if not coords:
                    continue
                    
                if role == 'outer' or role == '':
                    outer_ways.append(coords)
        
        if not outer_ways:
            return None
            
        from shapely.ops import linemerge, unary_union, polygonize
        from shapely.geometry import LineString, MultiLineString
        
        lines = []
        for coords in outer_ways:
            if len(coords) >= 2:
                lines.append(LineString(coords))
        
        if not lines:
            return None
            
        try:
            # Merge lines into a single continuous line (or multiple if disjoint)
            merged = linemerge(lines)
            
            # If merged is a single LineString and is closed (ring), make polygon
            if isinstance(merged, LineString):
                if merged.is_ring:
                    return Polygon(merged)
                else:
                    # Try to force close if endpoints are close?
                    # Or just return None if not a ring
                    # print(f"  {debug_name}: Merged line is not a ring")
                    return None
            
            elif isinstance(merged, MultiLineString):
                # We have multiple parts. Try to polygonize them.
                polys = list(polygonize(merged))
                if polys:
                    return unary_union(polys)
                else:
                    # print(f"  {debug_name}: Could not polygonize MultiLineString")
                    return None
                    
        except Exception as e:
            print(f"  {debug_name}: Error in geometry construction: {e}")
            return None
            
        return None

    def get_district(self, lat, lng):
        """
        Find district for a coordinate pair
        
        Args:
            lat: Latitude
            lng: Longitude
            
        Returns:
            District name or None
        """
        if pd.isna(lat) or pd.isna(lng) or str(lat) == 'NULL' or str(lng) == 'NULL':
            return None
            
        try:
            point = Point(float(lng), float(lat))
            
            for name, polygon in self.districts.items():
                if polygon.covers(point):
                    return name
            return None
        except Exception:
            return None

    def process_csv(self, input_file='Tashkent_Tech_Job_Analytics/data/vacancies_map.csv'):
        """
        Process the CSV file and update districts
        """
        print(f"Processing {input_file}...")
        try:
            df = pd.read_csv(input_file)
            
            # Create a backup
            backup_file = input_file + '.bak'
            df.to_csv(backup_file, index=False)
            print(f"Created backup at {backup_file}")
            
            # Update districts
            found_count = 0
            
            # Check if columns are correct strings "NULL" or NaN
            # Convert "NULL" string to NaN for processing if needed, but we check explicitly
            
            total = len(df)
            print(f"Analyzing {total} vacancies...")
            
            for index, row in df.iterrows():
                lat = row.get('latitude')
                lng = row.get('longitude')
                
                # Check for valid coordinates
                if str(lat) == 'NULL' or str(lng) == 'NULL' or pd.isna(lat) or pd.isna(lng):
                    continue
                    
                new_district = self.get_district(lat, lng)
                
                if new_district:
                    df.at[index, 'district'] = new_district
                    found_count += 1
                elif pd.isna(row.get('district')) or row.get('district') == 'NULL' or row.get('district') == 'Ташкент':
                     # If we couldn't map it but it had coordinates, maybe keep old or set to 'Unknown'
                     # User asked to identify... if we fail, we leave it.
                     pass
            
            # Save updated file
            df.to_csv(input_file, index=False)
            print(f"Updated {found_count} vacancies with district names")
            print(f"Saved to {input_file}")
            
        except Exception as e:
            print(f"Error processing CSV: {e}")



def main():
    mapper = DistrictMapper()
    mapper.process_csv()


if __name__ == "__main__":
    main()
