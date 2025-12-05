import pandas as pd
import os
from pathlib import Path
from typing import Optional, List


class DistrictListingCleaner:
    """
    A class to clean district listing CSV files by removing duplicate entries
    with missing price information.
    """
    
    def __init__(self, input_folder: str = "district_listing_page", 
                 output_folder: str = "district_listing_page_cleaned"):
        """
        Initialize the cleaner with input and output folder paths.
        
        Args:
            input_folder: Path to folder containing raw district CSV files
            output_folder: Path to folder where cleaned CSV files will be saved
        """
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        
        # Create output folder if it doesn't exist
        self.output_folder.mkdir(parents=True, exist_ok=True)
    
    def clean_single_file(self, file_path: Path) -> pd.DataFrame:
        """
        Clean a single CSV file by removing duplicates with missing prices.
        
        Args:
            file_path: Path to the CSV file to clean
            
        Returns:
            Cleaned DataFrame
        """
        # Read the CSV file
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        
        # Convert price columns to NaN if empty strings
        df[['price_raw', 'price_value']] = df[['price_raw', 'price_value']].replace('', pd.NA)
        
        # Find rows to drop: duplicates by card_id with missing price information
        to_drop = df[df.duplicated(subset=['card_id'], keep=False) & 
                     df['price_raw'].isna() & 
                     df['price_value'].isna()]
        
        # Filter them out
        df_clean = df.drop(to_drop.index)
        
        return df_clean
    
    def save_cleaned_file(self, df: pd.DataFrame, district_name: str) -> str:
        """
        Save cleaned DataFrame to output folder.
        
        Args:
            df: Cleaned DataFrame to save
            district_name: Name of the district (without .csv extension)
            
        Returns:
            Path to the saved file
        """
        output_file = self.output_folder / f"{district_name}_cleaned.csv"
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        return str(output_file)
    
    def get_csv_files(self) -> List[Path]:
        """
        Get all CSV files from the input folder (excluding already cleaned files).
        
        Returns:
            List of Path objects for CSV files
        """
        csv_files = []
        for file in self.input_folder.glob("*.csv"):
            # Skip files that are already cleaned (ending with _cleaned.csv)
            if not file.stem.endswith("_cleaned"):
                csv_files.append(file)
        return csv_files
    
    def process_all_files(self) -> dict:
        """
        Process all CSV files in the input folder and save cleaned versions.
        
        Returns:
            Dictionary with processing results:
            {
                'processed': int,  # Number of files processed
                'files': list,     # List of tuples (input_file, output_file, rows_removed)
                'errors': list     # List of tuples (file, error_message)
            }
        """
        results = {
            'processed': 0,
            'files': [],
            'errors': []
        }
        
        csv_files = self.get_csv_files()
        
        if not csv_files:
            print(f"No CSV files found in {self.input_folder}")
            return results
        
        for file_path in csv_files:
            try:
                # Get district name (filename without extension)
                district_name = file_path.stem
                
                print(f"Processing {district_name}...")
                
                # Clean the file
                df_clean = self.clean_single_file(file_path)
                
                # Calculate rows removed
                original_rows = pd.read_csv(file_path, encoding="utf-8-sig").shape[0]
                rows_removed = original_rows - df_clean.shape[0]
                
                # Save cleaned file
                output_path = self.save_cleaned_file(df_clean, district_name)
                
                results['processed'] += 1
                results['files'].append((str(file_path), output_path, rows_removed))
                
                print(f"✓ {district_name}: {rows_removed} rows removed, saved to {output_path}")
                
            except Exception as e:
                error_msg = f"Error processing {file_path}: {str(e)}"
                results['errors'].append((str(file_path), str(e)))
                print(f"✗ {error_msg}")
        
        return results
    
    def process_single_district(self, district_name: str) -> Optional[str]:
        """
        Process a single district file by name.
        
        Args:
            district_name: Name of the district (with or without .csv extension)
            
        Returns:
            Path to the cleaned file, or None if processing failed
        """
        # Remove .csv extension if present
        if district_name.endswith('.csv'):
            district_name = district_name[:-4]
        
        file_path = self.input_folder / f"{district_name}.csv"
        
        if not file_path.exists():
            print(f"File not found: {file_path}")
            return None
        
        try:
            print(f"Processing {district_name}...")
            
            # Clean the file
            df_clean = self.clean_single_file(file_path)
            
            # Save cleaned file
            output_path = self.save_cleaned_file(df_clean, district_name)
            
            print(f"✓ {district_name} cleaned and saved to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"✗ Error processing {district_name}: {str(e)}")
            return None


# Convenience function for quick usage
def clean_all_districts(input_folder: str = "district_listing_page",
                       output_folder: str = "district_listing_page_cleaned") -> dict:
    """
    Convenience function to clean all district listing files.
    
    Args:
        input_folder: Path to folder containing raw district CSV files
        output_folder: Path to folder where cleaned CSV files will be saved
        
    Returns:
        Dictionary with processing results
    """
    cleaner = DistrictListingCleaner(input_folder, output_folder)
    return cleaner.process_all_files()


# Example usage when run as a script
if __name__ == "__main__":
    # Create cleaner instance
    cleaner = DistrictListingCleaner()
    
    # Process all files
    results = cleaner.process_all_files()
    
    # Print summary
    print("\n" + "="*50)
    print("CLEANING SUMMARY")
    print("="*50)
    print(f"Total files processed: {results['processed']}")
    print(f"Total errors: {len(results['errors'])}")
    
    if results['files']:
        print("\nProcessed files:")
        for input_file, output_file, rows_removed in results['files']:
            print(f"  • {Path(input_file).name} → {rows_removed} rows removed")
    
    if results['errors']:
        print("\nErrors:")
        for file, error in results['errors']:
            print(f"  • {Path(file).name}: {error}")
