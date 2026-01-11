import pandas as pd
from pathlib import Path

def combine_yearly_data():
    years = [2023, 2024, 2025]
    dfs = []
    
    for year in years:
        filepath = Path(f'f1_raw_session_data_{year}.csv')
        
        if filepath.exists():
            df = pd.read_csv(filepath)
            dfs.append(df)
            print(f"✓ Loaded {year}: {len(df)} records")
        else:
            print(f"✗ No data for {year} (file not found)")
    
    if len(dfs) == 0:
        print("\nNo data files found!")
        return None
    
    combined = pd.concat(dfs, ignore_index=True)
    combined.to_csv('f1_raw_session_data.csv', index=False)
    
    print(f"\n{'='*40}")
    print(f"Combined: {len(combined)} total records")
    print(f"Saved to: f1_raw_session_data.csv")
    print(f"{'='*40}")
    
    # Show summary
    print(f"\nRecords per year:")
    print(combined.groupby('Year').size())
    
    print(f"\nRecords per session type:")
    print(combined.groupby('Session').size())
    
    return combined


if __name__ == "__main__":
    combine_yearly_data()