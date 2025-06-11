from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import logging

class USGSDataOperator(BaseOperator):
    """
    Custom operator to extract earthquake data from USGS API
    """
    
    @apply_defaults
    def __init__(
        self,
        output_path: str,
        days_back: int = 7,
        min_magnitude: float = 3.0,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.output_path = output_path
        self.days_back = days_back
        self.min_magnitude = min_magnitude
    
    def execute(self, context):
        logging.info("🌍 Starting USGS earthquake data extraction...")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_back)
        
        # USGS API parameters for Indonesia region
        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            'format': 'geojson',
            'starttime': start_date.strftime('%Y-%m-%d'),
            'endtime': end_date.strftime('%Y-%m-%d'),
            'minmagnitude': self.min_magnitude,
            'maxlatitude': 6,      # North Indonesia
            'minlatitude': -11,    # South Indonesia  
            'maxlongitude': 141,   # East Indonesia
            'minlongitude': 95,    # West Indonesia
            'limit': 10000
        }
        
        try:
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()
            
            # Transform GeoJSON to flat structure
            earthquakes = []
            for feature in data.get("features", []):
                props = feature["properties"]
                coords = feature["geometry"]["coordinates"]
                
                earthquake = {
                    "id": feature["id"],
                    "place": props.get("place", ""),
                    "time": props.get("time"),
                    "magnitude": props.get("mag"),
                    "longitude": coords[0],
                    "latitude": coords[1], 
                    "depth": coords[2],
                    "magnitude_type": props.get("magType", ""),
                    "significance": props.get("sig", 0),
                    "alert": props.get("alert", ""),
                    "tsunami": props.get("tsunami", 0),
                    "extraction_timestamp": datetime.now().isoformat()
                }
                earthquakes.append(earthquake)
            
            # Save raw data
            df = pd.DataFrame(earthquakes)
            df.to_csv(self.output_path, index=False)
            
            logging.info(f"✅ Extracted {len(earthquakes)} earthquake records")
            logging.info(f"📁 Saved to: {self.output_path}")
            
            # Return count for downstream tasks
            return len(earthquakes)
            
        except Exception as e:
            logging.error(f"❌ USGS data extraction failed: {e}")
            raise
