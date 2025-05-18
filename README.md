# MagnitudR - Earthquake Data Analysis

This repository contains a data analysis project for earthquake data from USGS, focusing on the Indonesian region.

## Environment and Technology

### System Requirements
- **Operating System**: Windows 10/11 (or Linux/MacOS)
- **Python Version**: 3.11.*
- **Package Manager**: pip 25.0.1

## Project Structure
```
magnitudr/
├── .gitignore
├── requirements.txt
├── notebook/
│   └── experiment.ipynb
```

## Installation and Setup

### Clone the Repository
```bash
git clone https://github.com/yourusername/magnitudr.git
cd magnitudr
```

### Virtual Environment Setup
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/MacOS:
# source venv/bin/activate
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

## Data Collection
The project collects earthquake data from the USGS API (https://earthquake.usgs.gov/fdsnws/event/1/query) in GeoJSON format.

## Data Processing
The data is processed to:
1. Filter earthquakes within Indonesian geographic coordinates (longitude: 95-141, latitude: -11-6)
2. Categorize earthquakes by depth (Shallow: 0-70km, Intermediate: 70-300km, Deep: 300-700km)
3. Convert timestamps to Asia/Jakarta timezone for local analysis

## Analysis Features
- Geographic distribution of earthquakes
- Magnitude analysis
- Depth categorization
- Time-based analysis

## Usage
Open the `notebook/experiment.ipynb` file in Jupyter Notebook or JupyterLab to see the data analysis process.

```bash
jupyter notebook notebook/etl_pipeline.ipynb
```

## Data Sources
- USGS Earthquake API: https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson

### Dependencies
- requests 2.32.3
- pandas 2.2.3
- sqlalchemy 2.0.40
- psycopg2-binary 2.9.10
- python-dotenv 1.1.0
- numpy 2.2.5
- python-dateutil 2.9.0.post0
- pytz 2025.2
- tzdata 2025.2
