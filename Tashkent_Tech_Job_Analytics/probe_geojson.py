import requests
import json

base_url = "https://raw.githubusercontent.com/akbartus/GeoJSON-Uzbekistan/main/geojson/"
filenames = [
    "uzbekistan.geojson",
    "uzbekistan_regional.geojson",
    "uzbekistan_district.geojson",
    "uzbekistan_districts.geojson",
    "districts.geojson",
    "district/tashkent_city.geojson",
    "tashkent_city.json"
]

for name in filenames:
    url = base_url + name
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            try:
                data = r.json()
                count = len(data.get('features', []))
                print(f"FOUND: {name} - Features: {count}")
                if count > 0:
                    print(f"  First feature props: {data['features'][0].get('properties', {}).keys()}")
            except:
                print(f"FOUND: {name} - Invalid JSON")
        else:
            print(f"MISSING: {name}")
    except Exception as e:
        print(f"ERROR: {name} - {e}")
