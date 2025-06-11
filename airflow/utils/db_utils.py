import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import logging

class DatabaseManager:
    """Database utilities for earthquake data management"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
    
    def test_connection(self):
        """Test database connectivity"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT 1")
                return True
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            return False
    
    def get_earthquake_summary(self):
        """Get summary statistics of earthquake data"""
        query = """
        SELECT 
            COUNT(*) as total_earthquakes,
            AVG(magnitude) as avg_magnitude,
            MAX(magnitude) as max_magnitude,
            COUNT(DISTINCT region) as unique_regions
        FROM earthquakes_processed
        """
        
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Failed to get earthquake summary: {e}")
            return pd.DataFrame()
    
    def get_cluster_summary(self):
        """Get summary of clustering results"""
        query = """
        SELECT 
            risk_zone,
            COUNT(*) as cluster_count,
            AVG(avg_magnitude) as avg_magnitude,
            SUM(cluster_size) as total_events
        FROM earthquake_clusters 
        GROUP BY risk_zone
        ORDER BY total_events DESC
        """
        
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Failed to get cluster summary: {e}")
            return pd.DataFrame()
