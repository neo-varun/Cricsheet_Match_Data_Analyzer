from web_scraping import JSONDownloader
from read_ipl_data import JSONReader
from create_tables import DatabaseCreation

if __name__ == "__main__":
    # Read JSON files from ZIP

    downloader = JSONDownloader(download_dir="data")
    downloader.scrape_and_download()

    json_reader = JSONReader(zip_folder="data")
    dataframes = json_reader.read_json_from_zip()

    # Display each DataFrame
    for category, df in dataframes.items():
        print(f"\n📊 {category} DataFrame:")
        print(df.head())  # Display the first 5 rows for readability
        print(f"Shape: {df.shape}") 

    # Store into MySQL
    db = DatabaseCreation()
    db.store_dataframes_to_mysql(dataframes)
    db.close_connection()