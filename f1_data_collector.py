import fastf1
from pathlib import Path
import pandas as pd

cache_dir = Path('cache')
cache_dir.mkdir(exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

def load_session_data(year, weekend, session_type):
    """Load session data with error handling for missing sessions"""
    try:
        session = fastf1.get_session(year, weekend, session_type)
        session.load()
        return session
    except ValueError as e:
        print(f"  Skipping {session_type}: {e}")
        return None
    except Exception as e:
        print(f"  Error loading {session_type}: {e}")
        return None

def get_available_sessions(year, weekend):
    """Get list of available sessions for a weekend"""
    try:
        event = fastf1.get_event(year, weekend)
        sessions = []
        
        for session_name in ['FP1', 'FP2', 'FP3', 'Q', 'R']:
            sessions.append(session_name)
        
        if hasattr(event, 'EventFormat') and 'sprint' in event.EventFormat.lower():
            sessions.insert(3, 'SQ')  
            sessions.insert(4, 'S')  
        
        return sessions
    except Exception as e:
        print(f"Error getting event info: {e}")
        return ['FP1', 'FP2', 'FP3', 'Q', 'R'] 

def collect_race_data(year, weekend):
    """Collect all available session data for a race weekend"""
    print(f"\n{'='*50}")
    print(f"Collecting data for {year} {weekend}")
    print('='*50)
    
    event = fastf1.get_event(year, weekend)
    print(f"Event: {event['EventName']}")
    print(f"Format: {event['EventFormat']}")
    
    if 'sprint' in event['EventFormat'].lower():
        sessions = ['FP1', 'SQ', 'S', 'Q', 'R'] 
        print("Sprint weekend detected!")
    else:
        sessions = ['FP1', 'FP2', 'FP3', 'Q', 'R'] 
        print("Standard weekend format")
    
    collected_data = {}
    
    for s in sessions:
        print(f"\nLoading {s}...")
        session = load_session_data(year, weekend, s)
        if session is not None:
            collected_data[s] = session
            print(f"  ✓ {s} loaded successfully")
            print(f"    Drivers: {len(session.drivers)}")
    
    return collected_data

if __name__ == "__main__":
    year = 2025
    weekend = 'Abu Dhabi'
    
    data = collect_race_data(year, weekend)
    
    print(f"\n{'='*50}")
    print("Data collection complete!")
    print(f"Sessions collected: {list(data.keys())}")
    print('='*50)
    
    if 'Q' in data:
        quali = data['Q']
        results = quali.results[['Abbreviation', 'Position', 'Q1', 'Q2', 'Q3']]
        print("\nQualifying Results:")
        print(results.head(10))