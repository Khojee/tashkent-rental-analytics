import requests
import pandas as pd
import os

def run_transit_analytics():
    print("=" * 60)
    print("Tashkent Transit Analytics")
    print("=" * 60)

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

    print("Fetching metro stations...")
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
    print(f"Found {len(metro_df)} metro stations")

    query = f"""
    [out:json];
    node
      ["highway"="bus_stop"]
      ({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
    out body;
    """

    print("Fetching bus stops...")
    r = requests.get(OVERPASS_URL, params={"data": query})
    if r.status_code != 200:
        print(f"Error fetching bus stops: {r.status_code}")
        print(r.text[:200])
        # Fallback empty structure if needed, or just let bus_stop_df be empty
        bus_data = {"elements": []}
    else:
        bus_data = r.json()

    bus_rows = []
    if "elements" in bus_data:
        for el in bus_data["elements"]:
            tags = el.get("tags", {})
            bus_rows.append({
                "bus_stop": tags.get("name"),
                "lat": el["lat"],
                "lon": el["lon"]
            })
    
    bus_stop_df = pd.DataFrame(bus_rows)
    print(f"Found {len(bus_stop_df)} bus stops")

    if not metro_df.empty:
        metro_df['type'] = 'metro'
    if not bus_stop_df.empty:
        bus_stop_df['type'] = 'bus'

    # Rename specific name columns to a common 'name' column for consistency
    dfs_to_concat = []
    if not metro_df.empty:
        dfs_to_concat.append(metro_df.rename(columns={'station': 'name'}))
    if not bus_stop_df.empty:
        dfs_to_concat.append(bus_stop_df.rename(columns={'bus_stop': 'name'}))
    
    if dfs_to_concat:
        transit_df = pd.concat(dfs_to_concat, ignore_index=True)
    else:
        transit_df = pd.DataFrame(columns=['name', 'line', 'lat', 'lon', 'type'])

    # Create data directory relative to the script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    output_path = os.path.join(data_dir, "transit_data.csv")
    transit_df.to_csv(output_path, index=False)
    print(f"Saved data to {output_path}")

if __name__ == "__main__":
    run_transit_analytics()

