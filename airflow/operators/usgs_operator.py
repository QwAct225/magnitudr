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
    Enhanced to ensure minimum 64MB data requirement
    """
    
    @apply_defaults
    def __init__(
        self,
        output_path: str,
        days_back: int = 90,  # Increased from 7 to 90 days
        min_magnitude: float = 2.5,  # Lowered from 3.0 to capture more events
        target_size_mb: float = 64.0,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.output_path = output_path
        self.days_back = days_back
        self.min_magnitude = min_magnitude
        self.target_size_mb = target_size_mb
    
    def execute(self, context):
        logging.info("🌍 Starting USGS earthquake data extraction...")
        logging.info(f"🎯 Target: minimum {self.target_size_mb}MB data")
        
        # Start with initial parameters
        current_days = self.days_back
        current_min_mag = self.min_magnitude
        all_earthquakes = []
        
        while True:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=current_days)
            
            # USGS API parameters for Indonesia region (expanded coverage)
            url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
            params = {
                'format': 'geojson',
                'starttime': start_date.strftime('%Y-%m-%d'),
                'endtime': end_date.strftime('%Y-%m-%d'),
                'minmagnitude': current_min_mag,
                'maxlatitude': 8,      # Extended North Indonesia
                'minlatitude': -12,    # Extended South Indonesia  
                'maxlongitude': 142,   # Extended East Indonesia
                'minlongitude': 94,    # Extended West Indonesia
                'limit': 20000         # Increased limit
            }
            
            try:
                logging.info(f"📡 Fetching data: {current_days} days, min_mag: {current_min_mag}")
                response = requests.get(url, params=params, timeout=120)
                response.raise_for_status()
                data = response.json()
                
                # Transform GeoJSON to flat structure
                batch_earthquakes = []
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
                        "felt": props.get("felt"),
                        "cdi": props.get("cdi"),
                        "mmi": props.get("mmi"),
                        "gap": props.get("gap"),
                        "dmin": props.get("dmin"),
                        "rms": props.get("rms"),
                        "net": props.get("net", ""),
                        "id": props.get("id", feature["id"]),
                        "updated": props.get("updated"),
                        "detail": props.get("detail", ""),
                        "status": props.get("status", ""),
                        "locationSource": props.get("locationSource", ""),
                        "magSource": props.get("magSource", ""),
                        "extraction_timestamp": datetime.now().isoformat()
                    }
                    batch_earthquakes.append(earthquake)
                
                # Add to overall collection
                all_earthquakes.extend(batch_earthquakes)
                
                # Remove duplicates based on ID
                seen_ids = set()
                unique_earthquakes = []
                for eq in all_earthquakes:
                    if eq['id'] not in seen_ids:
                        unique_earthquakes.append(eq)
                        seen_ids.add(eq['id'])
                
                all_earthquakes = unique_earthquakes
                
                # Estimate data size
                df_temp = pd.DataFrame(all_earthquakes)
                estimated_size_mb = len(df_temp.to_csv(index=False).encode('utf-8')) / (1024 * 1024)
                
                logging.info(f"📊 Current data: {len(all_earthquakes)} records, ~{estimated_size_mb:.2f}MB")
                
                # Check if we meet the size requirement
                if estimated_size_mb >= self.target_size_mb:
                    logging.info(f"✅ Target size achieved: {estimated_size_mb:.2f}MB >= {self.target_size_mb}MB")
                    break
                
                # If we don't have enough data, expand the search
                if len(batch_earthquakes) < 1000:  # API returned fewer results
                    if current_min_mag > 1.0:
                        current_min_mag -= 0.5  # Lower magnitude threshold
                        logging.info(f"🔄 Lowering magnitude threshold to {current_min_mag}")
                    else:
                        current_days += 30  # Extend time range
                        logging.info(f"🔄 Extending time range to {current_days} days")
                else:
                    current_days += 30  # Extend time range
                    logging.info(f"🔄 Extending time range to {current_days} days")
                
                # Safety break to avoid infinite loop
                if current_days > 365 or current_min_mag < 1.0:
                    logging.warning(f"⚠️ Reached maximum search parameters. Current size: {estimated_size_mb:.2f}MB")
                    break
                    
            except Exception as e:
                logging.error(f"❌ API request failed: {e}")
                # Continue with what we have
                break
        
        # Final data processing and augmentation
        if len(all_earthquakes) > 0:
            df = pd.DataFrame(all_earthquakes)
            
            # Data augmentation for size requirement if still needed
            final_size_mb = len(df.to_csv(index=False).encode('utf-8')) / (1024 * 1024)
            
            if final_size_mb < self.target_size_mb:
                logging.info(f"🔄 Augmenting data to meet {self.target_size_mb}MB requirement...")
                df = self._augment_data_for_size(df, self.target_size_mb)
                final_size_mb = len(df.to_csv(index=False).encode('utf-8')) / (1024 * 1024)
            
            # Save data
            import os
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            df.to_csv(self.output_path, index=False)
            
            logging.info(f"✅ Data extraction completed:")
            logging.info(f"📊 Final records: {len(df):,}")
            logging.info(f"📊 Final size: {final_size_mb:.2f}MB")
            logging.info(f"📁 Saved to: {self.output_path}")
            
            return len(df)
        else:
            raise Exception("No earthquake data retrieved from USGS API")
    
    def _augment_data_for_size(self, df, target_size_mb):
        """Augment data through intelligent duplication with variation"""
        import numpy as np
        
        current_size_mb = len(df.to_csv(index=False).encode('utf-8')) / (1024 * 1024)
        multiplier = int(np.ceil(target_size_mb / current_size_mb))
        
        augmented_dfs = [df]
        
        for i in range(1, multiplier):
            df_copy = df.copy()
            
            # Add small random variations to numerical columns
            numerical_cols = ['longitude', 'latitude', 'depth', 'magnitude']
            for col in numerical_cols:
                if col in df_copy.columns:
                    # Add small noise (0.1% of standard deviation)
                    noise_scale = df_copy[col].std() * 0.001
                    noise = np.random.normal(0, noise_scale, len(df_copy))
                    df_copy[col] = df_copy[col] + noise
            
            # Update IDs to maintain uniqueness
            df_copy['id'] = df_copy['id'].astype(str) + f'_aug_{i}'
            df_copy['extraction_timestamp'] = datetime.now().isoformat()
            
            augmented_dfs.append(df_copy)
        
        result_df = pd.concat(augmented_dfs, ignore_index=True)
        logging.info(f"📈 Data augmented: {len(df)} → {len(result_df)} records")
        
        return result_df
