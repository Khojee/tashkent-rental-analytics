import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path
from typing import Dict, List, Optional
import plotly.express as px


class DistrictPriceAnalyzer:
    """
    Analyzes rental prices across Tashkent districts and creates visualizations.
    """
    
    # District names mapping (for proper display)
    DISTRICT_NAMES = {
        "yakkasarai": "Yakkasarai",
        "yunusabad": "Yunusabad",
        "shaykhantohur": "Shaykhantohur",
        "chilonzor": "Chilonzor",
        "yashnabad": "Yashnabad",
        "uchtepa": "Uchtepa",
        "almazar": "Almazar",
        "sergeli": "Sergeli",
        "bektemir": "Bektemir",
        "mirabad": "Mirabad",
        "mirzo-ulugbek": "Mirzo-Ulugbek"
    }
    
    # USD to UZS exchange rate
    USD_TO_UZS = 13933
    
    def __init__(self, 
                 details_folder: str = "cards_details",
                 cleaned_folder: str = "district_listing_page_cleaned"):
        """
        Initialize the price analyzer.
        
        Args:
            details_folder: Folder containing card details CSVs
            cleaned_folder: Folder containing cleaned listing CSVs
        """
        self.details_folder = Path(details_folder)
        self.cleaned_folder = Path(cleaned_folder)
        self.district_data = {}
        self.merged_data = None
    
    def load_and_merge_district(self, district_name: str) -> Optional[pd.DataFrame]:
        """
        Load and merge data for a single district.
        
        Args:
            district_name: Name of the district (e.g., 'yunusabad')
            
        Returns:
            Merged DataFrame or None if files not found
        """
        # File paths
        details_file = self.details_folder / f"{district_name}_cards_details.csv"
        cleaned_file = self.cleaned_folder / f"{district_name}_cleaned.csv"
        
        # Check if files exist
        if not details_file.exists() or not cleaned_file.exists():
            print(f"⚠ Skipping {district_name}: Missing files")
            return None
        
        try:
            # Load data
            df_details = pd.read_csv(details_file, encoding="utf-8-sig")
            df_cleaned = pd.read_csv(cleaned_file, encoding="utf-8-sig")
            
            # Merge on card_id
            merged = pd.merge(df_details, df_cleaned, on="card_id", how="inner")
            
            # Drop unnecessary columns
            cols_to_drop = ["location_text", "posted_date_raw", "posted_date", "time_raw"]
            merged = merged.drop(columns=[col for col in cols_to_drop if col in merged.columns])
            
            # Add district name
            merged["district"] = district_name
            
            return merged
            
        except Exception as e:
            print(f"✗ Error loading {district_name}: {e}")
            return None
    
    def process_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process price data: convert to UZS, extract area, calculate price per sq meter.
        
        Args:
            df: DataFrame with price and area data
            
        Returns:
            Processed DataFrame
        """
        # Convert all prices to UZS
        df["price_uzs"] = np.where(
            df["price_currency"] == "сум",
            df["price_value"],
            df["price_value"] * self.USD_TO_UZS
        )
        
        # Extract numeric area value
        df["area"] = (
            df["area"]
            .astype(str)
            .str.extract(r"(\d+\.?\d*)")
            .astype(float)
        )
        
        # Calculate price per square meter
        df["price_per_sq_meter"] = df["price_uzs"] / df["area"]
        
        # Fill missing condition values
        df["condition"] = df["condition"].fillna("Not Specified")
        
        # Remove invalid data (NaN prices or areas)
        df = df.dropna(subset=["price_per_sq_meter", "area"])
        df = df[df["price_per_sq_meter"] > 0]
        
        return df
    
    def load_all_districts(self) -> pd.DataFrame:
        """
        Load and process data for all districts.
        
        Returns:
            Combined DataFrame with all districts
        """
        all_data = []
        
        print("Loading district data...")
        print("-" * 50)
        
        for district_key in self.DISTRICT_NAMES.keys():
            df = self.load_and_merge_district(district_key)
            if df is not None:
                df = self.process_price_data(df)
                all_data.append(df)
                print(f"✓ {self.DISTRICT_NAMES[district_key]}: {len(df)} listings")
        
        if not all_data:
            raise ValueError("No district data loaded!")
        
        # Combine all districts
        self.merged_data = pd.concat(all_data, ignore_index=True)
        
        print("-" * 50)
        print(f"Total listings: {len(self.merged_data)}")
        print(f"Districts loaded: {len(all_data)}")
        
        return self.merged_data
    
    def calculate_avg_price_by_condition(self) -> pd.DataFrame:
        """
        Calculate average price per square meter by district and condition.
        
        Returns:
            Pivot table with districts as columns and conditions as rows
        """
        if self.merged_data is None:
            self.load_all_districts()
        
        # Calculate average price per sq meter by district and condition
        avg_prices = (
            self.merged_data
            .groupby(["district", "condition"])["price_per_sq_meter"]
            .mean()
            .reset_index()
        )
        
        # Create pivot table
        pivot = avg_prices.pivot(
            index="condition",
            columns="district",
            values="price_per_sq_meter"
        )
        
        # Reorder columns by district name
        ordered_districts = [d for d in self.DISTRICT_NAMES.keys() if d in pivot.columns]
        pivot = pivot[ordered_districts]
        
        # Rename columns to proper district names
        pivot.columns = [self.DISTRICT_NAMES[d] for d in pivot.columns]
        
        return pivot
    
    def create_stacked_bar_chart(self, 
                                 output_file: str = "price_by_condition.html",
                                 show_chart: bool = True) -> go.Figure:
        """
        Create a beautiful stacked column chart using Plotly.
        
        Args:
            output_file: Path to save the HTML chart
            show_chart: Whether to display the chart in browser
            
        Returns:
            Plotly Figure object
        """
        # Get the pivot table
        pivot = self.calculate_avg_price_by_condition()
        
        # Create figure
        fig = go.Figure()
        
        # Color palette - modern and vibrant
        colors = [
            '#FF6B6B',  # Red
            '#4ECDC4',  # Teal
            '#45B7D1',  # Blue
            '#FFA07A',  # Light Salmon
            '#98D8C8',  # Mint
            '#F7DC6F',  # Yellow
            '#BB8FCE',  # Purple
            '#85C1E2',  # Sky Blue
            '#F8B739',  # Orange
            '#52B788',  # Green
        ]
        
        # Add a trace for each condition (stack)
        for idx, condition in enumerate(pivot.index):
            fig.add_trace(go.Bar(
                name=condition,
                x=pivot.columns,
                y=pivot.loc[condition],
                marker_color=colors[idx % len(colors)],
                text=[f'{val:,.0f}' if pd.notna(val) else '' for val in pivot.loc[condition]],
                textposition='inside',
                textfont=dict(color='white', size=10),
                hovertemplate='<b>%{x}</b><br>' +
                             f'{condition}<br>' +
                             'Avg Price: %{y:,.0f} UZS/m²<br>' +
                             '<extra></extra>'
            ))
        
        # Update layout for beautiful styling
        fig.update_layout(
            title={
                'text': '<b>Average Rental Price per Square Meter by Condition</b><br>' +
                        '<sub>Across 11 Districts in Tashkent</sub>',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 24, 'color': '#2C3E50'}
            },
            xaxis={
                'title': {'text': '<b>District</b>', 'font': {'size': 16, 'color': '#34495E'}},
                'tickfont': {'size': 12, 'color': '#34495E'},
                'tickangle': -45,
                'showgrid': False
            },
            yaxis={
                'title': {'text': '<b>Average Price (UZS per m²)</b>', 'font': {'size': 16, 'color': '#34495E'}},
                'tickfont': {'size': 12, 'color': '#34495E'},
                'showgrid': True,
                'gridcolor': '#ECF0F1',
                'tickformat': ',.0f'
            },
            barmode='stack',
            plot_bgcolor='#FAFAFA',
            paper_bgcolor='white',
            hovermode='closest',
            legend={
                'title': {'text': '<b>Condition</b>', 'font': {'size': 14}},
                'font': {'size': 12},
                'bgcolor': 'rgba(255, 255, 255, 0.9)',
                'bordercolor': '#BDC3C7',
                'borderwidth': 1,
                'orientation': 'v',
                'yanchor': 'top',
                'y': 1,
                'xanchor': 'left',
                'x': 1.02
            },
            height=700,
            width=1400,
            margin=dict(l=80, r=200, t=120, b=120),
            font={'family': 'Arial, sans-serif'}
        )
        
        # Save to HTML
        fig.write_html(output_file)
        print(f"\n✅ Chart saved to: {output_file}")
        
        # Show in browser
        if show_chart:
            fig.show()
        
        return fig
    
    def create_grouped_bar_chart(self,
                                 output_file: str = "price_by_condition_grouped.html",
                                 show_chart: bool = True) -> go.Figure:
        """
        Create a grouped bar chart (alternative visualization).
        
        Args:
            output_file: Path to save the HTML chart
            show_chart: Whether to display the chart in browser
            
        Returns:
            Plotly Figure object
        """
        # Get the pivot table
        pivot = self.calculate_avg_price_by_condition()
        
        # Create figure
        fig = go.Figure()
        
        # Color palette
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', 
                 '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B739', '#52B788']
        
        # Add a trace for each condition
        for idx, condition in enumerate(pivot.index):
            fig.add_trace(go.Bar(
                name=condition,
                x=pivot.columns,
                y=pivot.loc[condition],
                marker_color=colors[idx % len(colors)],
                text=[f'{val:,.0f}' if pd.notna(val) else '' for val in pivot.loc[condition]],
                textposition='outside',
                textfont=dict(size=9),
                hovertemplate='<b>%{x}</b><br>' +
                             f'{condition}<br>' +
                             'Avg Price: %{y:,.0f} UZS/m²<br>' +
                             '<extra></extra>'
            ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': '<b>Average Rental Price per Square Meter by Condition</b><br>' +
                        '<sub>Grouped by District - Tashkent</sub>',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 24, 'color': '#2C3E50'}
            },
            xaxis={
                'title': {'text': '<b>District</b>', 'font': {'size': 16, 'color': '#34495E'}},
                'tickfont': {'size': 12, 'color': '#34495E'},
                'tickangle': -45
            },
            yaxis={
                'title': {'text': '<b>Average Price (UZS per m²)</b>', 'font': {'size': 16, 'color': '#34495E'}},
                'tickfont': {'size': 12, 'color': '#34495E'},
                'tickformat': ',.0f'
            },
            barmode='group',
            plot_bgcolor='#FAFAFA',
            paper_bgcolor='white',
            hovermode='closest',
            legend={
                'title': {'text': '<b>Condition</b>', 'font': {'size': 14}},
                'font': {'size': 12},
                'bgcolor': 'rgba(255, 255, 255, 0.9)',
                'bordercolor': '#BDC3C7',
                'borderwidth': 1
            },
            height=700,
            width=1400,
            margin=dict(l=80, r=200, t=120, b=120)
        )
        
        # Save and show
        fig.write_html(output_file)
        print(f"✅ Grouped chart saved to: {output_file}")
        
        if show_chart:
            fig.show()
        
        return fig
    
    def print_summary_statistics(self):
        """Print summary statistics for the analysis."""
        if self.merged_data is None:
            self.load_all_districts()
        
        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)
        
        # Overall statistics
        print(f"\nTotal listings analyzed: {len(self.merged_data):,}")
        print(f"Average price per m²: {self.merged_data['price_per_sq_meter'].mean():,.0f} UZS")
        print(f"Median price per m²: {self.merged_data['price_per_sq_meter'].median():,.0f} UZS")
        
        # By district
        print("\n" + "-" * 70)
        print("Average Price per m² by District:")
        print("-" * 70)
        district_avg = (
            self.merged_data
            .groupby("district")["price_per_sq_meter"]
            .mean()
            .sort_values(ascending=False)
        )
        for district, price in district_avg.items():
            print(f"  {self.DISTRICT_NAMES[district]:20s}: {price:>10,.0f} UZS/m²")
        
        # By condition
        print("\n" + "-" * 70)
        print("Average Price per m² by Condition:")
        print("-" * 70)
        condition_avg = (
            self.merged_data
            .groupby("condition")["price_per_sq_meter"]
            .mean()
            .sort_values(ascending=False)
        )
        for condition, price in condition_avg.items():
            print(f"  {condition:20s}: {price:>10,.0f} UZS/m²")
        
        print("=" * 70 + "\n")


# Convenience function
def analyze_and_visualize(details_folder: str = "cards_details",
                         cleaned_folder: str = "district_listing_page_cleaned",
                         chart_type: str = "stacked",
                         show_chart: bool = True) -> DistrictPriceAnalyzer:
    """
    Convenience function to analyze and visualize price data.
    
    Args:
        details_folder: Folder with card details
        cleaned_folder: Folder with cleaned listings
        chart_type: 'stacked' or 'grouped'
        show_chart: Whether to show chart in browser
        
    Returns:
        DistrictPriceAnalyzer instance
    """
    analyzer = DistrictPriceAnalyzer(details_folder, cleaned_folder)
    analyzer.load_all_districts()
    analyzer.print_summary_statistics()
    
    if chart_type == "stacked":
        analyzer.create_stacked_bar_chart(show_chart=show_chart)
    elif chart_type == "grouped":
        analyzer.create_grouped_bar_chart(show_chart=show_chart)
    else:
        # Create both
        analyzer.create_stacked_bar_chart(show_chart=show_chart)
        analyzer.create_grouped_bar_chart(show_chart=show_chart)
    
    return analyzer


# Example usage
if __name__ == "__main__":
    # Create analyzer
    analyzer = DistrictPriceAnalyzer()
    
    # Load all district data
    analyzer.load_all_districts()
    
    # Print statistics
    analyzer.print_summary_statistics()
    
    # Create visualizations
    print("\nCreating visualizations...")
    analyzer.create_stacked_bar_chart(show_chart=True)
    analyzer.create_grouped_bar_chart(show_chart=True)
    
    print("\n✅ Analysis complete!")
