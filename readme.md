
# Cricsheet Match Data Analyzer

## Overview
This project is a **Cricket Match Data Analysis Pipeline** built with **Python** and **MySQL**. It downloads, processes, and analyzes cricket match data from different formats (Test, ODI, T20I, and IPL) using the Cricsheet dataset. The system automatically scrapes data, processes JSON files, creates database tables, and provides interactive visualizations.

## Features
- **Automated Data Collection** – Scrapes cricket match data from cricsheet.org
- **Multi-format Support** – Processes Test, ODI, T20I, and IPL cricket matches
- **Comprehensive Database** – Stores match details, innings, overs, and ball-by-ball data
- **Interactive Visualizations** – Analyze cricket statistics with matplotlib, seaborn, and plotly
- **SQL Analytics** – Pre-built queries for advanced cricket insights

## Prerequisites

### Install MySQL
Before running the application, you must have **MySQL** installed and running on your system.

- **Windows:** Download and install MySQL from [MySQL Official Site](https://dev.mysql.com/downloads/installer/)
- **Linux:**  
  ```bash
  sudo apt install mysql-server
  ```
- **macOS:**  
  ```bash
  brew install mysql
  ```  
- After installation, configure MySQL with your credentials (default user and password in the code are "root" and "2003").

## Installation & Setup

### Create a Virtual Environment (Recommended)
```bash
python -m venv venv
```
Activate the virtual environment:
- **Windows:**  
  ```bash
  venv\Scripts\activate
  ```
- **macOS/Linux:**  
  ```bash
  source venv/bin/activate
  ```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run the Data Pipeline
```bash
python app.py
```

## How the Program Works

### Data Collection
- The `JSONDownloader` class scrapes cricsheet.org to download cricket match data in JSON format
- It automatically creates a data directory and downloads ZIP files for Test, ODI, T20, and IPL matches

### Data Processing
- Specialized reader classes (`TestMatchReader`, `ODIMatchReader`, `T20MatchReader`, `IPLMatchReader`) extract structured data from JSON files
- Each format's unique characteristics are captured in separate DataFrame structures

### Database Storage
- The `DatabaseHandler` class manages MySQL connections and data storage
- It automatically creates a "cricketdata" database and appropriate tables for each cricket format
- Data is inserted in optimized batches for efficient performance

### Data Analysis
- The Jupyter notebook `cricket_data_insights.ipynb` provides interactive visualizations including:
  - Distribution of matches across formats
  - Toss decisions analysis
  - Win percentage after winning toss
  - Run distribution comparisons
  - Wicket type analysis
  - Extras comparison
  - Team performance trends

## Available Analytics

The project includes predefined SQL queries for in-depth cricket analysis:

### Test Cricket Insights
- Highest batting averages (min 20 innings)
- Best bowling figures in an innings
- Draw percentage by venue
- Declared innings analysis
- Longest partnerships

### ODI Cricket Insights
- Highest team totals
- Highest strike rates (min 500 runs)
- Best death bowlers (economy rate in last 10 overs)
- Toss impact on match results
- DLS method analysis

### T20 Cricket Insights
- Highest powerplay run rates
- Most boundaries hit
- Super over analysis
- Best venues for scoring
- Best death bowlers

### IPL Cricket Insights
- Most valuable players (batting and bowling combined)
- Team performance by season
- Powerplay analysis
- Best finishers in the last 5 overs
- Toss decisions impact

## Technologies Used
- **Python**
- **MySQL** (Database)
- **Pandas** (Data Processing)
- **Matplotlib, Seaborn, Plotly** (Data Visualization)
- **BeautifulSoup4** (Web Scraping)
- **Jupyter Notebook** (Interactive Analysis)

## License
This project is open-source and available under the MIT License.

## Author
Developed by **Varun**. For questions or feedback, please contact:
- **Email:** darklususnaturae@gmail.com