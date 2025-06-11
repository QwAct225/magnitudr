from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import psycopg2
from sqlalchemy import create_engine
import logging
import os

class DBSCANClusterOperator(BaseOperator):
    """
    Custom operator for DBSCAN clustering and hazard zone analysis
    """
    
    @apply_defaults
    def __init__(
        self,
        input_path: str,
        db_connection: str,
        eps: float = 0.1,
        min_samples: int = 5,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.db_connection = db_connection
        self.eps = eps
        self.min_samples = min_samples
    
    def execute(self, context):
        logging.info("🔬 Starting DBSCAN clustering analysis...")
        
        try:
            # Load processed data
            df = pd.read_csv(self.input_path)
            logging.info(f"📊 Loaded {len(df)} records for clustering")
            
            # Prepare features for clustering
            cluster_features = ['latitude', 'longitude', 'magnitude', 'depth']
            cluster_data = df[cluster_features].dropna()
            
            if len(cluster_data) < self.min_samples:
                logging.warning("⚠️ Insufficient data for clustering")
                return 0
            
            # Standardize features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(cluster_data)
            
            # Apply DBSCAN clustering
            dbscan = DBSCAN(eps=self.eps, min_samples=self.min_samples)
            cluster_labels = dbscan.fit_predict(scaled_features)
            
            # Add cluster results to dataframe
            df.loc[cluster_data.index, 'cluster_id'] = cluster_labels
            
            # Analyze clusters
            cluster_analysis = self._analyze_clusters(df)
            
            # Store results in database
            self._store_results(df, cluster_analysis)
            
            # Trigger DVC versioning
            self._trigger_dvc_versioning()
            
            n_clusters = len(cluster_analysis)
            logging.info(f"✅ DBSCAN completed: {n_clusters} clusters identified")
            
            return n_clusters
            
        except Exception as e:
            logging.error(f"❌ DBSCAN clustering failed: {e}")
            raise
    
    def _analyze_clusters(self, df):
        """Analyze cluster characteristics"""
        clusters = []
        
        # Filter out noise (cluster_id = -1)
        clustered_data = df[df['cluster_id'] != -1]
        
        for cluster_id in clustered_data['cluster_id'].unique():
            cluster_df = clustered_data[clustered_data['cluster_id'] == cluster_id]
            
            # Calculate cluster statistics
            cluster_stats = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_df),
                'centroid_lat': cluster_df['latitude'].mean(),
                'centroid_lon': cluster_df['longitude'].mean(),
                'avg_magnitude': cluster_df['magnitude'].mean(),
                'max_magnitude': cluster_df['magnitude'].max(),
                'avg_depth': cluster_df['depth'].mean(),
                'avg_hazard_score': cluster_df['hazard_score'].mean(),
                'dominant_region': cluster_df['region'].mode().iloc[0] if not cluster_df['region'].mode().empty else 'Unknown'
            }
            
            # Determine risk zone
            avg_hazard = cluster_stats['avg_hazard_score']
            if avg_hazard >= 8:
                risk_zone = 'Extreme'
            elif avg_hazard >= 6:
                risk_zone = 'High'
            elif avg_hazard >= 4:
                risk_zone = 'Moderate'
            else:
                risk_zone = 'Low'
            
            cluster_stats['risk_zone'] = risk_zone
            cluster_stats['cluster_label'] = f"Cluster_{cluster_id}_{risk_zone}"
            
            clusters.append(cluster_stats)
        
        return clusters
    
    def _store_results(self, df, cluster_analysis):
        """Store results in PostgreSQL database"""
        try:
            engine = create_engine(self.db_connection)
            
            # Store processed earthquake data
            processed_df = df[df['cluster_id'].notna()].copy()
            processed_df = processed_df.rename(columns={'time': 'time'})
            
            # Select columns that match database schema
            db_columns = [
                'id', 'magnitude', 'latitude', 'longitude', 'depth', 'time',
                'place', 'spatial_density', 'hazard_score', 'region',
                'magnitude_category', 'depth_category'
            ]
            
            # Filter existing columns
            available_columns = [col for col in db_columns if col in processed_df.columns]
            processed_df[available_columns].to_sql(
                'earthquakes_processed', 
                engine, 
                if_exists='append', 
                index=False
            )
            
            # Store cluster results
            cluster_records = []
            for cluster in cluster_analysis:
                cluster_df = processed_df[processed_df['cluster_id'] == cluster['cluster_id']]
                
                for _, row in cluster_df.iterrows():
                    cluster_record = {
                        'id': row['id'],
                        'cluster_id': cluster['cluster_id'],
                        'cluster_label': cluster['cluster_label'],
                        'risk_zone': cluster['risk_zone'],
                        'centroid_lat': cluster['centroid_lat'],
                        'centroid_lon': cluster['centroid_lon'],
                        'cluster_size': cluster['size'],
                        'avg_magnitude': cluster['avg_magnitude']
                    }
                    cluster_records.append(cluster_record)
            
            if cluster_records:
                pd.DataFrame(cluster_records).to_sql(
                    'earthquake_clusters',
                    engine,
                    if_exists='append',
                    index=False
                )
            
            # Store aggregated hazard zones
            hazard_zones = []
            for cluster in cluster_analysis:
                hazard_zone = {
                    'risk_level': cluster['risk_zone'],
                    'avg_magnitude': cluster['avg_magnitude'],
                    'event_count': cluster['size'],
                    'center_lat': cluster['centroid_lat'],
                    'center_lon': cluster['centroid_lon'],
                    'boundary_coordinates': f'{{"lat": {cluster["centroid_lat"]}, "lon": {cluster["centroid_lon"]}}}'
                }
                hazard_zones.append(hazard_zone)
            
            if hazard_zones:
                pd.DataFrame(hazard_zones).to_sql(
                    'hazard_zones',
                    engine,
                    if_exists='append',
                    index=False
                )
            
            logging.info(f"✅ Stored {len(cluster_analysis)} clusters in database")
            
        except Exception as e:
            logging.error(f"❌ Database storage failed: {e}")
            raise
    
    def _trigger_dvc_versioning(self):
        """Trigger DVC versioning for processed data"""
        try:
            import subprocess
            
            # DVC add and push
            dvc_commands = [
                "dvc add /opt/airflow/magnitudr/data/",
                "dvc push"
            ]
            
            for cmd in dvc_commands:
                result = subprocess.run(
                    cmd.split(),
                    cwd="/opt/airflow/magnitudr",
                    capture_output=True,
                    text=True
                )
                
                if result.returncode != 0:
                    logging.warning(f"⚠️ DVC command failed: {cmd}")
                    logging.warning(f"Error: {result.stderr}")
                else:
                    logging.info(f"✅ DVC command successful: {cmd}")
            
        except Exception as e:
            logging.warning(f"⚠️ DVC versioning failed: {e}")
            # Don't fail the whole task for DVC issues
