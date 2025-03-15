import os
import json
import pandas as pd
import numpy as np

# Directory containing JSON files
data_folder = "."  # Current directory for JSON files

def read_test_data():
    """Reads Test match JSON files and returns structured DataFrames."""
    # Data containers
    match_info = []
    innings_data = []
    over_data = []
    delivery_data = []
    
    # Look for Test JSON files
    json_files = [f for f in os.listdir(data_folder) if f.endswith('.json') and 'test' in f.lower()]
    
    if not json_files:
        print("No Test data JSON files found.")
        return {}
    
    print(f"Processing {len(json_files)} Test data files")
    
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
            
            # Extract match information (flattened) - Test match specific fields
            match_record = {
                'match_id': match_id,
                'data_version': meta_info.get('data_version', ''),
                'created': meta_info.get('created', ''),
                'city': info.get('city', ''),
                'venue': info.get('venue', ''),
                'date': info.get('dates', [''])[0] if info.get('dates') else '',
                'season': info.get('season', ''),
                'match_type': info.get('match_type', ''),
                'match_type_number': info.get('match_type_number', np.nan),  # Test specific
                'balls_per_over': info.get('balls_per_over', 6),
                'team1': info.get('teams', [''])[0] if info.get('teams') else '',
                'team2': info.get('teams', ['', ''])[1] if len(info.get('teams', [])) > 1 else '',
                'toss_winner': info.get('toss', {}).get('winner', ''),
                'toss_decision': info.get('toss', {}).get('decision', ''),
                'outcome_result': info.get('outcome', {}).get('result', ''),  # Test can have 'draw'
                'outcome_winner': info.get('outcome', {}).get('winner', ''),
                'outcome_by_innings': info.get('outcome', {}).get('by', {}).get('innings', np.nan),  # Test specific
                'outcome_by_runs': info.get('outcome', {}).get('by', {}).get('runs', np.nan),
                'outcome_by_wickets': info.get('outcome', {}).get('by', {}).get('wickets', np.nan),
                'player_of_match': ', '.join(info.get('player_of_match', [])),
                'event_name': info.get('event', {}).get('name', ''),
                'event_match_number': info.get('event', {}).get('match_number', np.nan)
            }
            match_info.append(match_record)
            
            # Process innings data - Test matches can have up to 4 innings
            for inning_idx, inning in enumerate(innings):
                inning_record = {
                    'match_id': match_id,
                    'innings_number': inning_idx + 1,
                    'team': inning.get('team', ''),
                    'declared': 'declared' in inning.get('declared', False),  # Test specific
                    'forfeited': 'forfeited' in inning.get('forfeited', False),  # Test specific
                    'follow_on': bool(inning.get('follow_on', False))  # Test specific
                }
                innings_data.append(inning_record)
                
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
                            'extras_penalty': delivery.get('extras', {}).get('penalty', 0)  # More common in Test
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
        dataframes['test_matches'] = pd.DataFrame(match_info)
        print(f"Created Test matches DataFrame: {len(match_info)} matches")
        
    if innings_data:
        dataframes['test_innings'] = pd.DataFrame(innings_data)
        print(f"Created Test innings DataFrame: {len(innings_data)} innings")
        
    if over_data:
        dataframes['test_overs'] = pd.DataFrame(over_data)
        print(f"Created Test overs DataFrame: {len(over_data)} overs")
        
    if delivery_data:
        dataframes['test_deliveries'] = pd.DataFrame(delivery_data)
        print(f"Created Test deliveries DataFrame: {len(delivery_data)} deliveries")
    
    return dataframes

if __name__ == "__main__":
    dataframes = read_test_data()
    
    # Show sample data from each DataFrame
    for name, df in dataframes.items():
        if not df.empty:
            print(f"\n--- Sample from {name} ---")
            print(df.head(2).to_string())