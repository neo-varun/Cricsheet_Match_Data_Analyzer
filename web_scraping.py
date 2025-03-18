import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class JSONDownloader:
    """Class to handle downloading cricket match data from cricsheet.org"""
    
    def __init__(self, download_dir="data"):
        """Initialize with the directory to save downloaded files"""
        self.download_dir = download_dir
        self.base_url = "https://cricsheet.org"
        self.page_url = urljoin(self.base_url, "/matches/")
        # Categories to download
        self.categories = [
            "Test matches",
            "One-day internationals",
            "T20 internationals",
            "Indian Premier League"
        ]
        
        # Create download directory
        os.makedirs(self.download_dir, exist_ok=True)
    
    def download_file(self, url):
        """Download a file from a given URL and save it in the download directory."""
        filename = os.path.basename(url)
        file_path = os.path.join(self.download_dir, filename)
        
        response = requests.get(url)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {filename}")
        return file_path
    
    def scrape_and_download(self):
        """Main method to scrape cricket data from cricsheet.org and download ZIP files"""
        print("Starting download of cricket match data...")
        
        # Get the webpage content
        response = requests.get(self.page_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        downloaded_files = []
        
        # Process each category
        for category in self.categories:
            print(f"Processing category: {category}")
            
            # Find elements and download files
            dt_element = soup.find('dt', string=lambda text: category in text if text else False)
            if dt_element:
                dd_element = dt_element.find_next('dd')
                json_link = dd_element.find('a', string='JSON')
                if json_link:
                    relative_url = json_link['href']
                    
                    # Convert relative URL to absolute URL
                    absolute_url = urljoin(self.base_url, relative_url)
                    
                    print(f"Found JSON link: {absolute_url}")
                    downloaded_file = self.download_file(absolute_url)
                    downloaded_files.append(downloaded_file)
                else:
                    print(f"No JSON link found for {category}")
            else:
                print(f"Category {category} not found on the page")

        print("Download complete.")
        return downloaded_files