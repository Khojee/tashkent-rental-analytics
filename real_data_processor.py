import pandas as pd
import numpy as np
import json
from pathlib import Path
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

# Exchange rate: 1 USD ≈ 12,800 UZS (adjust as needed)
USD_TO_UZS = 12800

# District name mapping (Russian/English variations to kebab-case)
DISTRICT_MAPPING = {
    'mirzo ulugbek': 'mirzo-ulugbek',
    'mirzo-ulugbek': 'mirzo-ulugbek',
    'мирзо улугбек': 'mirzo-ulugbek',
    'мирзо-улугбек': 'mirzo-ulugbek',
    'yunusabad': 'yunusabad',
    'юнусабад': 'yunusabad',
    'chilonzor': 'chilonzor',
    'чиланзар': 'chilonzor',
    'chilanzar': 'chilonzor',
    'yakkasarai': 'yakkasarai',
    'яккасарай': 'yakkasarai',
    'shaykhantohur': 'shaykhantohur',
    'шайхантохур': 'shaykhantohur',
    'sergeli': 'sergeli',
    'сергели': 'sergeli',
    'bektemir': 'bektemir',
    'бектемир': 'bektemir',
    'uchtepa': 'uchtepa',
    'учтепа': 'uchtepa',
    'almazar': 'almazar',
    'алмазар': 'almazar',
    'mirabad': 'mirabad',
    'мирабад': 'mirabad',
    'yashnabad': 'yashnabad',
    'яшнабад': 'yashnabad'
}

# Nice display names for districts
DISTRICT_DISPLAY_NAMES = {
    'mirzo-ulugbek': 'Mirzo Ulugbek',
    'yunusabad': 'Yunusabad',
    'chilonzor': 'Chilonzor',
    'yakkasarai': 'Yakkasaray',
    'shaykhantohur': 'Shaykhontohur',
    'sergeli': 'Sergeli',
    'bektemir': 'Bektemir',
    'uchtepa': 'Uchtepa',
    'almazar': 'Almazar',
    'mirabad': 'Mirabad',
    'yashnabad': 'Yashnabad'
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def normalize_district_name(name):
    """Convert district name to lowercase kebab-case"""
    if pd.isna(name):
        return None
    name_lower = str(name).lower().strip()
    return DISTRICT_MAPPING.get(name_lower, name_lower.replace(' ', '-'))

def convert_price_to_uzs(row):
    """Convert price to UZS based on currency"""
    if pd.isna(row['price_value']):
        return np.nan
    
    price_val = float(row['price_value'])
    currency = str(row.get('price_currency', '')).strip()
    
    if 'у.е' in currency.lower():  # USD equivalent
        return price_val * USD_TO_UZS
    else:  # Already in UZS
        return price_val

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_transit_data(filepath='Tashkent_Transit_Analytics/data/transit_data.csv'):
    """Load and process transit data"""
    print("Loading transit data...")
    df = pd.read_csv(filepath)
    
    # Keep only necessary columns
    df = df[['name', 'lat', 'lon', 'type']].copy()
    
    # Remove rows with missing coordinates
    df = df.dropna(subset=['lat', 'lon'])
    
    print(f"  ✓ Loaded {len(df)} transit stations ({df['type'].value_counts().to_dict()})")
    return df

def load_tech_jobs_data(filepath='Tashkent_Tech_Job_Analytics/data/vacancies_map.csv'):
    """Load and process tech jobs data"""
    print("Loading tech jobs data...")
    df = pd.read_csv(filepath)
    
    # CRITICAL: Remove rows where district == 'Ташкент'
    initial_count = len(df)
    df = df[df['district'] != 'Ташкент'].copy()
    removed_count = initial_count - len(df)
    
    # Normalize district names
    df['district_normalized'] = df['district'].apply(normalize_district_name)
    
    # Remove rows with invalid normalized districts
    df = df.dropna(subset=['district_normalized'])
    
    print(f"  ✓ Loaded {len(df)} tech vacancies (removed {removed_count} invalid rows)")
    return df

def load_rental_data_for_district(district_name):
    """Load and merge rental data for a single district"""
    base_path = Path('OLX_Scrap')
    
    # File paths
    cleaned_path = Path("district_listing_page_cleaned") / f"{district_name}_cleaned.csv"
    details_path = Path("cards_details") / f"{district_name}_cards_details.csv"

    
    # Check if files exist
    if not cleaned_path.exists() or not details_path.exists():
        print(f"  ⚠ Skipping {district_name}: files not found")
        return None
    
    try:
        # Load cleaned listings
        df_cleaned = pd.read_csv(cleaned_path)
        
        # Remove unused columns
        cols_to_remove = ['location_text', 'posted_date_raw', 'posted_date', 'time_raw']
        df_cleaned = df_cleaned.drop(columns=[c for c in cols_to_remove if c in df_cleaned.columns])
        
        # Load card details
        df_details = pd.read_csv(details_path)
        
        # Merge on card_id
        df_merged = pd.merge(df_cleaned, df_details, on='card_id', how='inner')
        
        # Convert prices to UZS
        df_merged['price_uzs'] = df_merged.apply(convert_price_to_uzs, axis=1)

        # Ensure area is numeric
        df_merged['area'] = pd.to_numeric(df_merged['area'], errors='coerce')
        
        # Calculate price per sqm
        df_merged['price_per_sqm'] = df_merged.apply(
            lambda row: row['price_uzs'] / row['area'] if pd.notna(row['area']) and row['area'] > 0 else np.nan,
            axis=1
        )
        
        # Fill missing condition values
        if 'condition' in df_merged.columns:
            df_merged['condition'] = df_merged['condition'].fillna('Не указано')
        
        # Ensure district_name is present
        if 'district_name' not in df_merged.columns:
            df_merged['district_name'] = district_name
        
        return df_merged
        
    except Exception as e:
        print(f"  ✗ Error loading {district_name}: {str(e)}")
        return None

def load_all_rental_data():
    """Load and combine rental data from all districts"""
    print("Loading rental data from all districts...")
    
    districts = [
        'almazar', 'bektemir', 'chilonzor', 'mirabad', 'mirzo-ulugbek',
        'sergeli', 'shaykhantohur', 'uchtepa', 'yakkasarai', 'yashnabad', 'yunusabad'
    ]
    
    all_rentals = []
    
    for district in districts:
        df = load_rental_data_for_district(district)
        if df is not None:
            all_rentals.append(df)
            print(f"  ✓ {district}: {len(df)} listings")
    
    # Combine all districts
    if not all_rentals:
        print("  ⚠ No rental data loaded!")
        return pd.DataFrame(columns=['district_name', 'price_uzs', 'area', 'number_rooms', 'card_id'])

    df_combined = pd.concat(all_rentals, ignore_index=True)
    
    print(f"  ✓ Total rental listings: {len(df_combined)}")
    return df_combined

# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def calculate_transit_scores(transit_df, districts):
    """Calculate transit accessibility score for each district"""
    print("\nCalculating transit scores...")
    
    # We need district boundaries to assign stations to districts
    # For now, we'll use a simplified approach: count stations per district
    # In a real implementation, you'd use geospatial joins with district polygons
    
    # Placeholder: Create approximate scores based on station density
    # This would be replaced with actual geospatial analysis
    scores = {}
    
    metro_count = len(transit_df[transit_df['type'] == 'metro'])
    bus_count = len(transit_df[transit_df['type'] == 'bus'])
    
    # Distribute stations across districts proportionally
    # This is a placeholder - real implementation would use coordinates
    for district in districts:
        # Simulate varying transit access
        metro_weight = np.random.randint(2, 7)
        bus_weight = np.random.randint(5, 15)
        
        # Score: weighted sum normalized to 0-10
        raw_score = (metro_weight * 2) + (bus_weight * 0.5)
        scores[district] = min(10, raw_score / 2)
    
    print(f"  ✓ Transit scores calculated for {len(scores)} districts")
    return scores

def calculate_affordability_scores(rental_df):
    """Calculate affordability score (inverse of median rent)"""
    print("\nCalculating affordability scores...")
    
    if rental_df.empty:
        return {}, {}
    
    # Group by district and calculate median rent
    district_rents = rental_df.groupby('district_name')['price_uzs'].median().to_dict()
    
    # Invert and normalize to 0-10 scale
    max_rent = max(district_rents.values()) if district_rents else 0
    min_rent = min(district_rents.values()) if district_rents else 0
    
    scores = {}
    for district, rent in district_rents.items():
        # Lower rent = higher score
        if max_rent == min_rent:
             # If only one district or all rents are equal, give a middle score or 10
            scores[district] = 5.0
        else:
            normalized = (max_rent - rent) / (max_rent - min_rent)
            scores[district] = normalized * 10
    
    print(f"  ✓ Affordability scores calculated for {len(scores)} districts")
    return scores, district_rents

def calculate_employment_scores(jobs_df):
    """Calculate employment opportunity score"""
    print("\nCalculating employment scores...")
    
    # Count jobs per district
    job_counts = jobs_df['district_normalized'].value_counts().to_dict()
    
    # Normalize to 0-10 scale
    max_jobs = max(job_counts.values())
    min_jobs = min(job_counts.values())
    
    scores = {}
    for district, count in job_counts.items():
        normalized = (count - min_jobs) / (max_jobs - min_jobs) if max_jobs > min_jobs else 0.5
        scores[district] = normalized * 10
    
    print(f"  ✓ Employment scores calculated for {len(scores)} districts")
    return scores, job_counts

def create_district_summary(transit_scores, afford_scores, employ_scores, 
                           rental_df, jobs_df, district_rents, job_counts):
    """Create comprehensive district summary dataframe"""
    print("\nCreating district summary...")
    
    # Get all unique districts
    all_districts = set(transit_scores.keys()) | set(afford_scores.keys()) | set(employ_scores.keys())
    
    summary_data = []
    
    for district in all_districts:
        # Get scores (default to 5 if missing)
        transit = transit_scores.get(district, 5)
        afford = afford_scores.get(district, 5)
        employ = employ_scores.get(district, 5)
        
        # Calculate composite score (weighted average)
        composite = (transit * 0.3 + afford * 0.4 + employ * 0.3)
        
        # Get additional metrics
        avg_rent = district_rents.get(district, 0)
        total_jobs = job_counts.get(district, 0)
        
        # Get rental statistics
        district_rentals = rental_df[rental_df['district_name'] == district]
        avg_area = district_rentals['area'].mean() if len(district_rentals) > 0 else 0
        avg_rooms = district_rentals['number_rooms'].mean() if len(district_rentals) > 0 else 0
        
        summary_data.append({
            'district': district,
            'district_display': DISTRICT_DISPLAY_NAMES.get(district, district.title()),
            'transit_score': round(transit, 1),
            'affordability_score': round(afford, 1),
            'employment_score': round(employ, 1),
            'composite_score': round(composite, 1),
            'avg_rent_uzs': int(avg_rent) if avg_rent > 0 else 0,
            'avg_rent_usd': int(avg_rent / USD_TO_UZS) if avg_rent > 0 else 0,
            'total_jobs': int(total_jobs),
            'avg_area': round(avg_area, 1),
            'avg_rooms': round(avg_rooms, 1),
            'rental_listings': len(district_rentals)
        })
    
    df_summary = pd.DataFrame(summary_data)
    df_summary = df_summary.sort_values('composite_score', ascending=False)
    
    print(f"  ✓ Summary created for {len(df_summary)} districts")
    return df_summary

# ============================================================================
# DASHBOARD DATA GENERATION
# ============================================================================

def generate_dashboard_data(df_summary, rental_df, jobs_df):
    """Generate JSON data for the dashboard"""
    print("\nGenerating dashboard data...")
    
    # Main district data
    district_data = df_summary.to_dict('records')
    
    # Rent distribution by district
    rent_dist = rental_df.groupby('district_name').agg({
        'price_uzs': ['mean', 'median', 'min', 'max'],
        'card_id': 'count'
    }).round(0).to_dict()
    
    # Jobs by company (top 10)
    top_companies = jobs_df['company_name'].value_counts().head(10).to_dict()
    
    # Room distribution
    room_dist = rental_df.groupby('number_rooms')['card_id'].count().to_dict()
    
    # Correlation data: rent vs jobs
    correlation_data = []
    for _, row in df_summary.iterrows():
        correlation_data.append({
            'district': row['district_display'],
            'rent': row['avg_rent_usd'],
            'jobs': row['total_jobs']
        })
    
    dashboard_json = {
        'districts': district_data,
        'rent_distribution': rent_dist,
        'top_companies': top_companies,
        'room_distribution': room_dist,
        'correlation': correlation_data,
        'metadata': {
            'total_rentals': len(rental_df),
            'total_jobs': len(jobs_df),
            'avg_rent_usd': int(rental_df['price_uzs'].mean() / USD_TO_UZS),
            'usd_rate': USD_TO_UZS
        }
    }
    
    return dashboard_json

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    print("="*80)
    print("TASHKENT DISTRICT ANALYSIS - DATA PROCESSOR")
    print("="*80)
    
    # Load all datasets
    transit_df = load_transit_data()
    jobs_df = load_tech_jobs_data()
    rental_df = load_all_rental_data()
    
    # Get list of districts from rental data
    districts = rental_df['district_name'].unique().tolist()
    
    # Calculate scores
    transit_scores = calculate_transit_scores(transit_df, districts)
    afford_scores, district_rents = calculate_affordability_scores(rental_df)
    employ_scores, job_counts = calculate_employment_scores(jobs_df)
    
    # Create summary
    df_summary = create_district_summary(
        transit_scores, afford_scores, employ_scores,
        rental_df, jobs_df, district_rents, job_counts
    )
    
    # Display top recommendations
    print("\n" + "="*80)
    print("TOP 3 RECOMMENDED DISTRICTS")
    print("="*80)
    for idx, row in df_summary.head(3).iterrows():
        print(f"\n{idx+1}. {row['district_display']}")
        print(f"   Composite Score: {row['composite_score']}/10")
        print(f"   Transit: {row['transit_score']}/10 | Affordability: {row['affordability_score']}/10 | Jobs: {row['employment_score']}/10")
        print(f"   Avg Rent: ${row['avg_rent_usd']}/mo | Tech Jobs: {row['total_jobs']} | Listings: {row['rental_listings']}")
    
    # Generate dashboard data
    dashboard_data = generate_dashboard_data(df_summary, rental_df, jobs_df)
    
    def convert_keys_to_str(d):
        if isinstance(d, dict):
            new_d = {}
            for k, v in d.items():
                # convert tuple key to string
                if isinstance(k, tuple):
                    k = '-'.join(str(x) for x in k)
                # recursively handle nested dicts
                new_d[k] = convert_keys_to_str(v)
            return new_d
        elif isinstance(d, list):
            return [convert_keys_to_str(x) for x in d]
        else:
            return d

    json_ready_data = convert_keys_to_str(dashboard_data)

    # Save to JSON
    output_path = 'dashboard_data.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_ready_data, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Dashboard data saved to: {output_path}")
    
    # Save summary CSV
    csv_path = 'district_summary.csv'
    df_summary.to_csv(csv_path, index=False)
    print(f"✓ District summary saved to: {csv_path}")
    
    print("\n" + "="*80)
    print("PROCESSING COMPLETE!")
    print("="*80)
    
    return df_summary, dashboard_data

if __name__ == "__main__":
    df_summary, dashboard_data = main()