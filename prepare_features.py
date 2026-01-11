import pandas as pd
import numpy as np

def create_prediction_dataset(raw_data):
    races = raw_data[raw_data['Session'] == 'R'].copy()
    practices = raw_data[raw_data['Session'].isin(['FP1', 'FP2', 'FP3'])]
    qualifying = raw_data[raw_data['Session'] == 'Q']
    
    prediction_rows = []
    
    for _, race_row in races.iterrows():
        driver = race_row['Driver']
        team = race_row['Team']
        year = race_row['Year']
        event = race_row['Event']
        
        row = {
            'Driver': driver,
            'Team': team,
            'Year': year,
            'Event': event,
            'RacePosition': race_row.get('Position'),
            'GridPosition': race_row.get('GridPosition'),
        }
        
        quali = qualifying[
            (qualifying['Driver'] == driver) & 
            (qualifying['Year'] == year) & 
            (qualifying['Event'] == event)
        ]
        if len(quali) > 0:
            quali = quali.iloc[0]
            row['QualiPosition'] = quali.get('Position')
            row['Q1_seconds'] = quali.get('Q1_seconds')
            row['Q2_seconds'] = quali.get('Q2_seconds')
            row['Q3_seconds'] = quali.get('Q3_seconds')
        
        for fp in ['FP1', 'FP2', 'FP3']:
            fp_data = practices[
                (practices['Driver'] == driver) & 
                (practices['Year'] == year) & 
                (practices['Event'] == event) &
                (practices['Session'] == fp)
            ]
            if len(fp_data) > 0:
                fp_data = fp_data.iloc[0]
                row[f'{fp}_BestLap'] = fp_data.get('BestLapTime_seconds')
                row[f'{fp}_AvgLap'] = fp_data.get('AvgLapTime_seconds')
                row[f'{fp}_Laps'] = fp_data.get('TotalLaps')
        
        prediction_rows.append(row)
    
    return pd.DataFrame(prediction_rows)

def add_historical_features(df):
    df = df.sort_values(['Year', 'Event', 'Driver']).copy()
    
    df['DriverAvgPosition_Last5'] = df.groupby('Driver')['RacePosition'].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )
    
    df['DriverAvgQuali_Last5'] = df.groupby('Driver')['QualiPosition'].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )
    
    df['TeamAvgPosition_Last5'] = df.groupby('Team')['RacePosition'].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )
    
    df['DriverWins_Season'] = df.groupby(['Driver', 'Year'])['RacePosition'].transform(
        lambda x: (x.shift(1) == 1).cumsum()
    )
    
    df['DriverPodiums_Season'] = df.groupby(['Driver', 'Year'])['RacePosition'].transform(
        lambda x: (x.shift(1) <= 3).cumsum()
    )
    
    return df

def add_engineered_features(df):
    df['QualiToGridDiff'] = df['GridPosition'] - df['QualiPosition']
    
    df['FP_BestLap'] = df[['FP1_BestLap', 'FP2_BestLap', 'FP3_BestLap']].min(axis=1)
    
    df['FP_BestLap_Relative'] = df.groupby(['Year', 'Event'])['FP_BestLap'].transform(
        lambda x: x - x.min()
    )
    
    df['Quali_Relative'] = df.groupby(['Year', 'Event'])['Q3_seconds'].transform(
        lambda x: x - x.min()
    )
    
    return df


if __name__ == "__main__":
    raw_data = pd.read_csv('f1_raw_session_data.csv')
    print(f"Loaded {len(raw_data)} raw records")
    
    df = create_prediction_dataset(raw_data)
    print(f"Created {len(df)} prediction rows")
    
    df = add_historical_features(df)
    print("Added historical features")
    
    df = add_engineered_features(df)
    print("Added engineered features")
    
    df.to_csv('f1_prediction_dataset.csv', index=False)
    print(f"Saved to f1_prediction_dataset.csv")
    print(f"Columns: {list(df.columns)}")