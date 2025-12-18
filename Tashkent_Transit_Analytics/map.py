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

print(bus_stop_df.head(10))
