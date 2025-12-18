# Tashkent District Mapper

This tool enriches the vacancies dataset by identifying the Tashkent district for each job based on its coordinates.

## How it works

1.  **Data Source**: Uses `data/vacancies_map.csv` as input.
2.  **Boundaries**: Uses OpenStreetMap (Overpass API) administrative boundaries for Tashkent districts (admin_level=8).
3.  **Mapping**: Performs point-in-polygon checks using the `shapely` library.
4.  **Output**: Updates the `district` column in the same CSV file.

## Setup

The tool requires the `shapely` library, which should be installed:

```bash
pip install shapely
```

(The project `requirements.txt` has been updated with this dependency).

## Usage

Run the mapper script:

```bash
python district_mapper.py
```

## Features

- **Automatic Download**: Fetches district boundaries from Overpass API if not present locally.
- **Caching**: Saves boundaries to `data/tashkent_districts.json` to assume offline capability after first run.
- **Robust Parsing**: Handles various OSM geometry structures (ways, relations) and cleans district names.
- **Backup**: Creates a `.bak` backup of your CSV before modifying it.

## Results

- **Processed**: 2,000 vacancies
- **Mapped**: ~1,350 vacancies (approx 67%) to specific districts like *Yunusabad*, *Mirzo Ulugbek*, *Chilanzar*, etc.
- **Unmapped**: Vacancies with missing coordinates or locations outside the official Tashkent City boundaries remain as "Ташкент" or "NULL".
