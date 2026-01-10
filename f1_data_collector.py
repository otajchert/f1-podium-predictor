import fastf1
from pathlib import Path
import pandas as pd
from collections import defaultdict
import shutil
import warnings
warnings.filterwarnings('ignore')


cache_dir = Path('cache')
if cache_dir.exists():
    shutil.rmtree(cache_dir)
cache_dir.mkdir(exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

def load_session_data(year, weekend, session_type):
    try:
        session = fastf1.get_session(year, weekend, session_type)
        session.load()
        return session
    except Exception as e:
        print(f"  Skipping {session_type}: {e}")
        return None

def get_event_schedule(year):
    schedule = fastf1.get_event_schedule(year, include_testing=False)
    races = schedule[schedule['EventFormat'].notna()]
    return races

def extract_session_metrics(session, session_type, year, event_name):
    metrics = []
    
    if session is None:
        return metrics
    
    try:
        laps = session.laps
        results = session.results
        
        for _, driver_result in results.iterrows():
            driver_abbr = driver_result.get('Abbreviation', '')
            if not driver_abbr:
                continue
                
            driver_data = {
                'Driver': driver_abbr,
                'DriverNumber': driver_result.get('DriverNumber', ''),
                'Team': driver_result.get('TeamName', ''),
                'Year': year,
                'Event': event_name,
                'Session': session_type,
            }
            
            driver_laps = laps.pick_drivers(driver_abbr)
            
            if session_type in ['FP1', 'FP2', 'FP3', 'S']:
                if len(driver_laps) > 0:
                    best_lap = driver_laps.pick_fastest()
                    if best_lap is not None and hasattr(best_lap, 'LapTime'):
                        lap_time = best_lap['LapTime']
                        if pd.notna(lap_time):
                            driver_data['BestLapTime_seconds'] = lap_time.total_seconds()
                    driver_data['TotalLaps'] = len(driver_laps)
                    
                    valid_laps = driver_laps[driver_laps['LapTime'].notna()]
                    if len(valid_laps) > 0:
                        avg_time = valid_laps['LapTime'].mean()
                        if pd.notna(avg_time):
                            driver_data['AvgLapTime_seconds'] = avg_time.total_seconds()
                
                if pd.notna(driver_result.get('Position')):
                    driver_data['Position'] = int(driver_result['Position'])
                    
            elif session_type == 'Q' or session_type == 'SQ':
                for q_session in ['Q1', 'Q2', 'Q3']:
                    q_time = driver_result.get(q_session)
                    if pd.notna(q_time):
                        driver_data[f'{q_session}_seconds'] = q_time.total_seconds()
                
                if pd.notna(driver_result.get('Position')):
                    driver_data['Position'] = int(driver_result['Position'])
                    
            elif session_type == 'R':
                if pd.notna(driver_result.get('Position')):
                    driver_data['Position'] = int(driver_result['Position'])
                if pd.notna(driver_result.get('GridPosition')):
                    driver_data['GridPosition'] = int(driver_result['GridPosition'])
                if pd.notna(driver_result.get('Points')):
                    driver_data['Points'] = float(driver_result['Points'])
                driver_data['Status'] = driver_result.get('Status', '')
                
                if len(driver_laps) > 0:
                    best_lap = driver_laps.pick_fastest()
                    if best_lap is not None and hasattr(best_lap, 'LapTime'):
                        lap_time = best_lap['LapTime']
                        if pd.notna(lap_time):
                            driver_data['FastestLap_seconds'] = lap_time.total_seconds()
                    driver_data['TotalLaps'] = len(driver_laps)
            
            metrics.append(driver_data)
            
    except Exception as e:
        print(f"    Error: {e}")
    
    return metrics

def collect_all_data(years):
    all_session_data = []
    
    for year in years:
        print(f"\n{'='*60}")
        print(f"COLLECTING {year}")
        print('='*60)
        
        try:
            schedule = get_event_schedule(year)
        except Exception as e:
            print(f"Error getting schedule for {year}: {e}")
            continue
        
        for idx, event in schedule.iterrows():
            event_name = event['EventName']
            event_format = event.get('EventFormat', 'conventional')
            
            print(f"\n{event_name} ({event_format})")
            
            if 'sprint' in str(event_format).lower():
                sessions = ['FP1', 'SQ', 'S', 'Q', 'R']
            else:
                sessions = ['FP1', 'FP2', 'FP3', 'Q', 'R']
            
            for session_type in sessions:
                print(f"  Loading {session_type}...", end=" ")
                session = load_session_data(year, event_name, session_type)
                
                if session is not None:
                    metrics = extract_session_metrics(session, session_type, year, event_name)
                    all_session_data.extend(metrics)
                    print(f"✓ ({len(metrics)} drivers)")
                else:
                    print("✗")
    
    return pd.DataFrame(all_session_data)

def pivot_to_driver_team_format(df):
    df['Driver_Team'] = df['Driver'] + '_' + df['Team']
    pivoted_data = defaultdict(dict)
    
    for _, row in df.iterrows():
        driver_team = row['Driver_Team']
        year = row['Year']
        event = row['Event'].replace(' ', '_')
        session = row['Session']
        
        pivoted_data[driver_team]['Driver'] = row['Driver']
        pivoted_data[driver_team]['Team'] = row['Team']
        
        prefix = f"{year}_{event}_{session}"
        
        for col in row.index:
            if col in ['Driver', 'Team', 'Year', 'Event', 'Session', 'Driver_Team', 'DriverNumber']:
                continue
            
            value = row[col]
            if pd.notna(value):
                col_name = f"{prefix}_{col}"
                pivoted_data[driver_team][col_name] = value
    
    result_df = pd.DataFrame.from_dict(pivoted_data, orient='index')
    result_df.index.name = 'Driver_Team'
    result_df.reset_index(inplace=True)
    
    fixed_cols = ['Driver_Team', 'Driver', 'Team']
    other_cols = sorted([c for c in result_df.columns if c not in fixed_cols])
    result_df = result_df[fixed_cols + other_cols]
    
    return result_df

def create_summary_statistics(df):
    summary_data = []
    
    for (driver, team), group in df.groupby(['Driver', 'Team']):
        stats = {'Driver': driver, 'Team': team}
        
        race_data = group[group['Session'] == 'R']
        if len(race_data) > 0:
            stats['Total_Races'] = len(race_data)
            stats['Total_Points'] = race_data['Points'].sum() if 'Points' in race_data else 0
            stats['Wins'] = len(race_data[race_data['Position'] == 1])
            stats['Podiums'] = len(race_data[race_data['Position'] <= 3])
            stats['DNFs'] = len(race_data[race_data['Status'].str.contains(
                'Retired|Accident|Collision|Damage|Engine|Gearbox|Hydraulics|DNF', 
                case=False, na=False
            )])
            stats['Avg_Race_Position'] = race_data['Position'].mean()
            stats['Best_Race_Position'] = race_data['Position'].min()
        
        quali_data = group[group['Session'] == 'Q']
        if len(quali_data) > 0:
            stats['Avg_Quali_Position'] = quali_data['Position'].mean()
            stats['Best_Quali_Position'] = quali_data['Position'].min()
            stats['Pole_Positions'] = len(quali_data[quali_data['Position'] == 1])
        
        summary_data.append(stats)
    
    return pd.DataFrame(summary_data)


if __name__ == "__main__":
    years = [2025]
    
    print("Starting F1 data collection...")
    print(f"Years: {years}")
    
    raw_data = collect_all_data(years)
    raw_data.to_csv('f1_raw_session_data.csv', index=False)
    print(f"\nRaw data saved: {len(raw_data)} records")
    
    pivoted_data = pivot_to_driver_team_format(raw_data)
    pivoted_data.to_csv('f1_driver_team_detailed.csv', index=False)
    print(f"Detailed data saved: {len(pivoted_data)} driver-team combinations")
    print(f"Total columns: {len(pivoted_data.columns)}")
    
    summary_data = create_summary_statistics(raw_data)
    summary_data.to_csv('f1_driver_team_summary.csv', index=False)
    print(f"Summary data saved: {len(summary_data)} driver-team combinations")
    
    print("\nSample rows:")
    print(pivoted_data[['Driver_Team', 'Driver', 'Team']].head(10))
    
    print("\nTop drivers by points:")
    print(summary_data.sort_values('Total_Points', ascending=False).head(10))
