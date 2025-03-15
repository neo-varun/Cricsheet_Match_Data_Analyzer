import os
import json
import pandas as pd
import numpy as np

# Directory containing JSON files
data_folder = "."  # Current directory for JSON files

def read_ipl_data():
    """Reads IPL JSON files and returns structured DataFrames."""
    # Data containers
    match_info = []
    innings_data = []
    over_data = []
    delivery_data = []
    powerplay_data = []
    
    # Look for IPL JSON files
    json_files = [f for f in os.listdir(data_folder) if f.endswith('.json') and 'ipl' in f.lower()]
    
    if not json_files:
        print("No IPL data JSON files found.")
        return {}
    
    print(f"Processing {len(json_files)} IPL data files")
    
    for json_file in json_files:
        print(f"Processing {json_file}")
        
        with open(os.path.join(data_folder, json_file), 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        # Process a list of matches or a single match
        match_list = content if isinstance(content, list) else [content]
        
        # Iterate through each match
        for match in match_list:
            # Generate a unique match ID if not present
            match_id = match.get('id', '')
            if not match_id:
                # Create a pseudo ID based on teams and date
                info = match.get('info', {})
                teams = info.get('teams', [])
                date = info.get('dates', [''])[0] if info.get('dates') else ''
                match_id = f"{'-'.join(teams)}-{date}" if teams and date else f"match-{len(match_info)}"
                
            meta_info = match.get('meta', {})
            info = match.get('info', {})
            innings = match.get('innings', [])
            
            # Extract match information (flattened) - IPL specific fields
            match_record = {
                'match_id': match_id,
                'data_version': meta_info.get('data_version', ''),
                'created': meta_info.get('created', ''),
                'revision': meta_info.get('revision', np.nan),
                'city': info.get('city', ''),
                'venue': info.get('venue', ''),
                'date': info.get('dates', [''])[0] if info.get('dates') else '',
                'season': info.get('season', ''),
                'match_type': info.get('match_type', ''),
                'balls_per_over': info.get('balls_per_over', 6),
                'overs': info.get('overs', 20),  # IPL matches are 20 overs
                'team1': info.get('teams', [''])[0] if info.get('teams') else '',
                'team2': info.get('teams', ['', ''])[1] if len(info.get('teams', [])) > 1 else '',
                'team_type': info.get('team_type', 'club'),  # IPL is club cricket
                'toss_winner': info.get('toss', {}).get('winner', ''),
                'toss_decision': info.get('toss', {}).get('decision', ''),
                'winner': info.get('outcome', {}).get('winner', ''),
                'result': info.get('outcome', {}).get('result', ''),
                'win_by_runs': info.get('outcome', {}).get('by', {}).get('runs', np.nan),
                'win_by_wickets': info.get('outcome', {}).get('by', {}).get('wickets', np.nan),
                'player_of_match': ', '.join(info.get('player_of_match', [])),
                'ipl_match_number': info.get('event', {}).get('match_number', np.nan),
                'match_referee': ', '.join(info.get('officials', {}).get('match_referees', [])),
                'umpires': ', '.join(info.get('officials', {}).get('umpires', [])),
                'tv_umpire': ', '.join(info.get('officials', {}).get('tv_umpires', [])),
                'reserve_umpire': ', '.join(info.get('officials', {}).get('reserve_umpires', []))
            }
            match_info.append(match_record)
            
            # Process innings data
            for inning_idx, inning in enumerate(innings):
                inning_record = {
                    'match_id': match_id,
                    'innings_number': inning_idx + 1,
                    'team': inning.get('team', ''),
                    'target_runs': inning.get('target', {}).get('runs', np.nan),
                    'target_overs': inning.get('target', {}).get('overs', np.nan),
                    'super_over': bool(inning.get('super_over', False))
                }
                innings_data.append(inning_record)
                
                # Process powerplay information
                if 'powerplays' in inning:
                    for powerplay in inning.get('powerplays', []):
                        powerplay_record = {
                            'match_id': match_id,
                            'innings_number': inning_idx + 1,
                            'team': inning.get('team', ''),
                            'powerplay_from': powerplay.get('from', ''),
                            'powerplay_to': powerplay.get('to', ''),
                            'powerplay_type': powerplay.get('type', '')
                        }
                        powerplay_data.append(powerplay_record)
                
                # Process overs data
                for over in inning.get('overs', []):
                    over_num = over.get('over', 0)
                    over_record = {
                        'match_id': match_id,
                        'innings_number': inning_idx + 1,
                        'over_number': over_num,
                        'team': inning.get('team', '')
                    }
                    over_data.append(over_record)
                    
                    # Process deliveries data
                    for delivery_idx, delivery in enumerate(over.get('deliveries', [])):
                        delivery_record = {
                            'match_id': match_id,
                            'innings_number': inning_idx + 1,
                            'over_number': over_num,
                            'ball_number': delivery_idx + 1,
                            'team': inning.get('team', ''),
                            'batter': delivery.get('batter', ''),
                            'bowler': delivery.get('bowler', ''),
                            'non_striker': delivery.get('non_striker', ''),
                            'runs_batter': delivery.get('runs', {}).get('batter', 0),
                            'runs_extras': delivery.get('runs', {}).get('extras', 0),
                            'runs_total': delivery.get('runs', {}).get('total', 0),
                            'extras_wides': delivery.get('extras', {}).get('wides', 0),
                            'extras_noballs': delivery.get('extras', {}).get('noballs', 0),
                            'extras_byes': delivery.get('extras', {}).get('byes', 0),
                            'extras_legbyes': delivery.get('extras', {}).get('legbyes', 0),
                            'extras_penalty': delivery.get('extras', {}).get('penalty', 0)
                        }
                        
                        # Process wicket information
                        if 'wickets' in delivery:
                            for wicket_idx, wicket in enumerate(delivery.get('wickets', [])):
                                if wicket_idx == 0:  # Only store the first wicket info in the delivery record
                                    delivery_record['wicket_player_out'] = wicket.get('player_out', '')
                                    delivery_record['wicket_kind'] = wicket.get('kind', '')
                                    if 'fielders' in wicket:
                                        fielders = [f.get('name', '') for f in wicket.get('fielders', [])]
                                        delivery_record['wicket_fielders'] = ', '.join(fielders)
                        
                        delivery_data.append(delivery_record)
    
    # Create DataFrames
    dataframes = {}
    
    if match_info:
        dataframes['ipl_matches'] = pd.DataFrame(match_info)
        print(f"Created IPL matches DataFrame: {len(match_info)} matches")
        
    if innings_data:
        dataframes['ipl_innings'] = pd.DataFrame(innings_data)
        print(f"Created IPL innings DataFrame: {len(innings_data)} innings")
    
    if powerplay_data:
        dataframes['ipl_powerplays'] = pd.DataFrame(powerplay_data)
        print(f"Created IPL powerplays DataFrame: {len(powerplay_data)} powerplays")
        
    if over_data:
        dataframes['ipl_overs'] = pd.DataFrame(over_data)
        print(f"Created IPL overs DataFrame: {len(over_data)} overs")
        
    if delivery_data:
        dataframes['ipl_deliveries'] = pd.DataFrame(delivery_data)
        print(f"Created IPL deliveries DataFrame: {len(delivery_data)} deliveries")
    
    return dataframes

if __name__ == "__main__":
    dataframes = read_ipl_data()
    
    # Show sample data from each DataFrame
    for name, df in dataframes.items():
        if not df.empty:
            print(f"\n--- Sample from {name} ---")
            print(df.head(2).to_string())