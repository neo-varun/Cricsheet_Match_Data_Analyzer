from web_scraping import JSONDownloader
from read_test_data import TestMatchReader
from read_odi_data import ODIMatchReader
from read_t20_data import T20MatchReader
from read_ipl_data import IPLMatchReader
from create_tables import DatabaseHandler

def main():
    """Main function to execute the cricket data pipeline"""
    print("=== Starting Cricket Match Data Processing Pipeline ===")
    
    # Step 1: Download cricket match data
    print("\n=== Downloading Cricket Match Data ===")
    downloader = JSONDownloader(download_dir="data")
    downloader.scrape_and_download()
    
    # Step 2: Process Test match data
    print("\n=== Processing Test Match Data ===")
    test_reader = TestMatchReader(data_folder="data")
    test_dataframes = test_reader.read_data()
    
    if test_dataframes:
        print("\n=== Storing Test Match Data to MySQL ===")
        db_handler = DatabaseHandler()
        db_handler.process_dataframes(test_dataframes)
        db_handler.close_connection()
    
    # Step 3: Process ODI match data
    print("\n=== Processing ODI Match Data ===")
    odi_reader = ODIMatchReader(data_folder="data")
    odi_dataframes = odi_reader.read_data()
    
    if odi_dataframes:
        print("\n=== Storing ODI Match Data to MySQL ===")
        db_handler = DatabaseHandler()
        db_handler.process_dataframes(odi_dataframes)
        db_handler.close_connection()
    
    # Step 4: Process T20 match data
    print("\n=== Processing T20 Match Data ===")
    t20_reader = T20MatchReader(data_folder="data")
    t20_dataframes = t20_reader.read_data()
    
    if t20_dataframes:
        print("\n=== Storing T20 Match Data to MySQL ===")
        db_handler = DatabaseHandler()
        db_handler.process_dataframes(t20_dataframes)
        db_handler.close_connection()
    
    # Step 5: Process IPL match data
    print("\n=== Processing IPL Match Data ===")
    ipl_reader = IPLMatchReader(data_folder="data")
    ipl_dataframes = ipl_reader.read_data()
    
    if ipl_dataframes:
        print("\n=== Storing IPL Match Data to MySQL ===")
        db_handler = DatabaseHandler()
        db_handler.process_dataframes(ipl_dataframes)
        db_handler.close_connection()
    
    print("\n=== Pipeline Completed ===")

if __name__ == "__main__":
    main()