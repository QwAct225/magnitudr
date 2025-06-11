from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import logging
import os

class SpatialDensityOperator(BaseOperator):
    """
    Custom operator for spatial density calculation and feature engineering
    """
    
    @apply_defaults
    def __init__(
        self,
        input_path: str,
        output_path: str,
        grid_size: float = 0.1,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.output_path = output_path
        self.grid_size = grid_size
    
    def execute(self, context):
        logging.info("📊 Starting spatial density calculation...")
        
        try:
            # Load raw data
            df = pd.read_csv(self.input_path)
            initial_count = len(df)
            
            # Data cleaning and validation
            df = df.dropna(subset=['latitude', 'longitude', 'magnitude', 'depth'])
            df = df[
                (df['latitude'].between(-90, 90)) &
                (df['longitude'].between(-180, 180)) &
                (df['magnitude'] > 0) &
                (df['depth'] >= 0)
            ]
            
            logging.info(f"📊 Data cleaned: {initial_count} → {len(df)} records")
            
            # Convert time
            df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
            
            # Regional classification
            df['region'] = df.apply(self._classify_region, axis=1)
            
            # Magnitude categories
            df['magnitude_category'] = pd.cut(
                df['magnitude'],
                bins=[0, 3.0, 4.0, 5.0, 6.0, 7.0, 10.0],
                labels=['Very Minor', 'Minor', 'Light', 'Moderate', 'Strong', 'Major'],
                include_lowest=True
            )
            
            # Depth categories  
            df['depth_category'] = pd.cut(
                df['depth'],
                bins=[0, 30, 70, 300, 700],
                labels=['Very Shallow', 'Shallow', 'Intermediate', 'Deep'],
                include_lowest=True
            )
            
            # Spatial density calculation
            df['spatial_density'] = self._calculate_spatial_density(df)
            
            # Hazard score calculation
            df['hazard_score'] = self._calculate_hazard_score(df)
            
            # Save processed data
            output_dir = os.path.dirname(self.output_path)
            os.makedirs(output_dir, exist_ok=True)
            df.to_csv(self.output_path, index=False)
            
            logging.info(f"✅ Spatial processing completed: {len(df)} records")
            logging.info(f"📁 Saved to: {self.output_path}")
            
            return len(df)
            
        except Exception as e:
            logging.error(f"❌ Spatial processing failed: {e}")
            raise
    
    def _classify_region(self, row):
        """Classify earthquake by Indonesian region"""
        lon, lat = row['longitude'], row['latitude']
        
        if 95 <= lon <= 106 and -6 <= lat <= 6:
            return 'Sumatra'
        elif 106 <= lon <= 115 and -9 <= lat <= -5:
            return 'Java'
        elif 108 <= lon <= 117 and -4 <= lat <= 5:
            return 'Kalimantan'
        elif 118 <= lon <= 125 and -6 <= lat <= 2:
            return 'Sulawesi'
        elif 125 <= lon <= 141 and -11 <= lat <= 2:
            return 'Eastern_Indonesia'
        else:
            return 'Other'
    
    def _calculate_spatial_density(self, df):
        """Calculate spatial density using grid-based approach"""
        # Create spatial grid
        lat_bins = np.arange(df['latitude'].min(), df['latitude'].max() + self.grid_size, self.grid_size)
        lon_bins = np.arange(df['longitude'].min(), df['longitude'].max() + self.grid_size, self.grid_size)
        
        # Assign grid coordinates
        df['lat_grid'] = pd.cut(df['latitude'], bins=lat_bins, labels=False)
        df['lon_grid'] = pd.cut(df['longitude'], bins=lon_bins, labels=False)
        
        # Calculate events per grid cell
        grid_counts = df.groupby(['lat_grid', 'lon_grid']).size().reset_index(name='grid_count')
        
        # Merge back to original data
        df = df.merge(grid_counts, on=['lat_grid', 'lon_grid'], how='left')
        
        # Calculate density (events per km²)
        grid_area_km2 = (self.grid_size * 111) ** 2  # Approx km² per degree²
        spatial_density = df['grid_count'] / grid_area_km2
        
        return spatial_density
    
    def _calculate_hazard_score(self, df):
        """Calculate composite hazard score"""
        score = 0
        
        # Magnitude component (0-4 points)
        score += np.minimum(df['magnitude'] / 2, 4)
        
        # Depth component (0-2 points, shallow = higher risk)
        depth_score = np.where(df['depth'] <= 30, 2,
                      np.where(df['depth'] <= 70, 1, 0))
        score += depth_score
        
        # Regional risk component
        region_risk = df['region'].map({
            'Java': 2, 'Sumatra': 1.5, 'Sulawesi': 1,
            'Eastern_Indonesia': 0.5, 'Kalimantan': 0.3, 'Other': 0
        })
        score += region_risk
        
        # Spatial density component
        density_score = np.minimum(df['spatial_density'] * 10, 2)
        score += density_score
        
        return np.minimum(score, 10)  # Cap at 10
