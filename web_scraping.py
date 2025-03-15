import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Create download directory
download_dir = "data"
os.makedirs(download_dir, exist_ok=True)

# Categories to download
categories = [
    "Test matches",
    "One-day internationals",
    "T20 internationals",
    "Indian Premier League"
]

def download_file(url):
    """Download a file from a given URL and save it in the download directory."""
    filename = os.path.basename(url)
    file_path = os.path.join(download_dir, filename)
    
    response = requests.get(url)
    with open(file_path, "wb") as f:
        f.write(response.content)
    print(f"Downloaded: {filename}")

# Main scraping function
def scrape_cricket_data():
    # Base URL and page URL
    base_url = "https://cricsheet.org"
    page_url = urljoin(base_url,"/matches/")
    
    # Get the webpage content
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text,'html.parser')
    
    # Process each category
    for category in categories:
        print(f"Processing category: {category}")
        
        # Find elements and download files
        dt_element = soup.find('dt', string=lambda text: category in text if text else False)
        dd_element = dt_element.find_next('dd')
        json_link = dd_element.find('a', string='JSON')
        relative_url = json_link['href']
        
        # Convert relative URL to absolute URL
        absolute_url = urljoin(base_url, relative_url)
        
        print(f"Found JSON link: {absolute_url}")
        download_file(absolute_url)

    print("Download complete.")

# Run the scraper
if __name__ == "__main__":
    scrape_cricket_data()