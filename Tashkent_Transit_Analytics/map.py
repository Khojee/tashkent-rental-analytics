import requests
import pandas as pd

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
BBOX = (41.15, 69.05, 41.45, 69.40)

query = f"""
[out:json];
node
  ["public_transport"="station"]
  ["station"="subway"]
  ({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
out body;
"""

r = requests.get(OVERPASS_URL, params={"data": query})
data = r.json()

rows = []
for el in data["elements"]:
    tags = el.get("tags", {})
    rows.append({
        "station": tags.get("name"),
        "line": tags.get("line"),
        "lat": el["lat"],
        "lon": el["lon"]
    })

metro_df = pd.DataFrame(rows)

print(metro_df.head())

query = f"""
[out:json];
node
  ["highway"="bus_stop"]
  ({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
out body;
"""

r = requests.get(OVERPASS_URL, params={"data": query})
if r.status_code != 200:
    print(f"Error fetching bus stops: {r.status_code}")
    print(r.text[:200])
    data = {"elements": []} # Fallback
else:
    data = r.json()

rows = []
for el in data["elements"]:
    tags = el.get("tags", {})
    rows.append({
        "bus_stop": tags.get("name"),
        "lat": el["lat"],
        "lon": el["lon"]
    })

bus_stop_df = pd.DataFrame(rows)

metro_df['type'] = 'metro'
bus_stop_df['type'] = 'bus'

# Rename specific name columns to a common 'name' column for consistency
transit_df = pd.concat([
    metro_df.rename(columns={'station': 'name'}),
    bus_stop_df.rename(columns={'bus_stop': 'name'})
], ignore_index=True)

print(transit_df.head(10))

import os

# Create data directory relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(script_dir, "data")
os.makedirs(data_dir, exist_ok=True)

output_path = os.path.join(data_dir, "transit_data.csv")
transit_df.to_csv(output_path, index=False)
print(f"Saved data to {output_path}")

