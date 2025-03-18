import os
import json
import pandas as pd
import numpy as np
import zipfile

class T20MatchReader:
    """Class to read and process T20 match data from ZIP files"""
    
    def __init__(self, data_folder="data"):
        """Initialize with the folder containing ZIP files"""
        self.data_folder = data_folder
    
    def read_data(self):
        """Reads T20 JSON files from ZIP archives and returns structured DataFrames."""
        # Data containers
        match_info = []
        innings_data = []
        over_data = []
        delivery_data = []
        
        # Look for T20 zip file
        t20_zip = None
        for f in os.listdir(self.data_folder):
            if f.endswith('.zip') and ('t20' in f.lower()):
                t20_zip = f
                break
        
        if not t20_zip:
            print("No T20 data ZIP file found in the data folder.")
            return {}
        
        print(f"Processing T20 data from {t20_zip}")
        
        with zipfile.ZipFile(os.path.join(self.data_folder, t20_zip)) as z:
            json_files = [f for f in z.namelist() if f.endswith('.json')]
            print(f"Found {len(json_files)} JSON files in the ZIP archive")
            
            for json_file in json_files:
                with z.open(json_file) as f:
                    try:
                        content = json.load(f)
                    except json.JSONDecodeError:
                        print(f"Error decoding {json_file}, skipping...")
                        continue
                
                # Process a list of matches or a single match
                match_list = content if isinstance(content, list) else [content]
                
                # Iterate through each match
                for match in match_list:
                    # Check if this is a T20 match (exclude IPL which is processed separately)
                    info = match.get('info', {})
                    match_type = info.get('match_type', '')
                    event_name = info.get('event', {}).get('name', '')
                    if match_type.upper() != 'T20' or ('Indian Premier League' in event_name):
                        continue
                    
                    # Generate a unique match ID if not present
                    match_id = match.get('id', '')
                    if not match_id:
                        # Create a pseudo ID based on teams and date
                        teams = info.get('teams', [])
                        date = info.get('dates', [''])[0] if info.get('dates') else ''
                        match_id = f"{'-'.join(teams)}-{date}" if teams and date else f"match-{len(match_info)}"
                        
                    meta_info = match.get('meta', {})
                    innings = match.get('innings', [])
                    
                    # Extract match information (flattened) - T20 specific fields
                    match_record = {
                        'match_id': match_id,
                        'data_version': meta_info.get('data_version', ''),
                        'created': meta_info.get('created', ''),
                        'city': info.get('city', ''),
                        'venue': info.get('venue', ''),
                        'date': info.get('dates', [''])[0] if info.get('dates') else '',
                        'season': info.get('season', ''),
                        'match_type': info.get('match_type', ''),
                        'match_type_number': info.get('match_type_number', np.nan),
                        'balls_per_over': info.get('balls_per_over', 6),
                        'overs': info.get('overs', 20),  # T20s are 20 overs
                        'team1': info.get('teams', [''])[0] if info.get('teams') else '',
                        'team2': info.get('teams', ['', ''])[1] if len(info.get('teams', [])) > 1 else '',
                        'team_type': info.get('team_type', ''),  # International, club, etc.
                        'toss_winner': info.get('toss', {}).get('winner', ''),
                        'toss_decision': info.get('toss', {}).get('decision', ''),
                        'winner': info.get('outcome', {}).get('winner', ''),
                        'result': info.get('outcome', {}).get('result', ''),  # For no-result or tied matches
                        'method': info.get('outcome', {}).get('method', ''),  # For DLS method
                        'win_by_runs': info.get('outcome', {}).get('by', {}).get('runs', np.nan),
                        'win_by_wickets': info.get('outcome', {}).get('by', {}).get('wickets', np.nan),
                        'player_of_match': ', '.join(info.get('player_of_match', [])),
                        'event_name': info.get('event', {}).get('name', ''),
                        'event_match_number': info.get('event', {}).get('match_number', np.nan)
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
                            'super_over': bool(inning.get('super_over', False))  # For super over in T20
                        }
                        innings_data.append(inning_record)
                        
                        # Process powerplay information if present
                        if 'powerplays' in inning:
                            pass  # Could process powerplay data here if needed
                        
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
            dataframes['t20_matches'] = pd.DataFrame(match_info)
            print(f"Created T20 matches DataFrame: {len(match_info)} matches")
            
        if innings_data:
            dataframes['t20_innings'] = pd.DataFrame(innings_data)
            print(f"Created T20 innings DataFrame: {len(innings_data)} innings")
            
        if over_data:
            dataframes['t20_overs'] = pd.DataFrame(over_data)
            print(f"Created T20 overs DataFrame: {len(over_data)} overs")
            
        if delivery_data:
            dataframes['t20_deliveries'] = pd.DataFrame(delivery_data)
            print(f"Created T20 deliveries DataFrame: {len(delivery_data)} deliveries")
        
        return dataframes