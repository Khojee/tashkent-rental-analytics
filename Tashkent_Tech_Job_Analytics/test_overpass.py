import requests
import json

def fetch_overpass():
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # "Toshkent" is the local name. "Tashkent" is English.
    # Searching for admin_level 4 area (City)
    query = """
    [out:json][timeout:60];
    area["name:en"="Tashkent"]->.searchArea;
    (
      relation(area.searchArea)["admin_level"="8"]["boundary"="administrative"];
    );
    out geom;
    """
    
    print("Fetching Tashkent districts from Overpass (attempt 2)...")
    try:
        response = requests.get(overpass_url, params={'data': query}, timeout=90)
        if response.status_code == 200:
            data = response.json()
            elements = data.get('elements', [])
            print(f"Found {len(elements)} elements")
            
            # Save raw if successful
            if len(elements) > 0:
                with open('data/tashkent_overpass.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f)
                print("Saved to data/tashkent_overpass.json")
                
                # Print names
                names = [el.get('tags', {}).get('name:en', el.get('tags', {}).get('name')) for el in elements]
                print(f"Districts found: {names}")
        else:
            print(f"Error: {response.status_code}")
            print(response.text[:200])
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    fetch_overpass()
