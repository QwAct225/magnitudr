from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add custom operators to path  
sys.path.append('/opt/airflow/operators')
from spatial_operator import SpatialDensityOperator

default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'earthquake_spatial_processing',
    default_args=default_args,
    description='Spatial processing and feature engineering',
    schedule_interval=None,  # Triggered by master DAG
    max_active_runs=1,
    tags=['earthquake', 'processing', 'spatial']
)

# Task 1: Apply spatial density and feature engineering
task_spatial_processing = SpatialDensityOperator(
    task_id='calculate_spatial_density',
    input_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
    output_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
    grid_size=0.1,  # 0.1 degree grid (~11km)
    dag=dag
)

def load_to_postgresql(**context):
    """Load processed data to PostgreSQL with proper transaction handling"""
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine, text
    import logging
    
    try:
        # Read processed data
        df = pd.read_csv('/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv')
        logging.info(f"📊 Read {len(df)} records from processed file")
        
        # Database connection
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        # Data type conversion and cleaning
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
        
        # Select columns matching database schema
        db_columns = [
            'id', 'magnitude', 'latitude', 'longitude', 'depth', 'time',
            'place', 'spatial_density', 'hazard_score', 'region',
            'magnitude_category', 'depth_category'
        ]
        
        # Filter to existing columns
        available_cols = [col for col in db_columns if col in df.columns]
        df_clean = df[available_cols].copy()
        
        logging.info(f"📊 Prepared {len(df_clean)} records with columns: {available_cols}")
        
        # SAFE DELETE: Handle foreign key constraints with proper transaction
        with engine.begin() as conn:  # begin() provides auto-commit transaction
            # Delete in proper order: child tables first
            result1 = conn.execute(text("DELETE FROM earthquake_clusters"))
            result2 = conn.execute(text("DELETE FROM hazard_zones"))
            result3 = conn.execute(text("DELETE FROM earthquakes_processed"))
            
            logging.info(f"✅ Cleared existing data: clusters, hazard_zones, earthquakes_processed")
        
        # Load new data to PostgreSQL
        rows_inserted = df_clean.to_sql(
            'earthquakes_processed',
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logging.info(f"✅ Loaded {len(df_clean)} records to earthquakes_processed")
        logging.info(f"📊 Columns loaded: {available_cols}")
        
        # Verify insertion
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM earthquakes_processed"))
            count = result.scalar()
            logging.info(f"✅ Verification: {count} records in database")
        
        return len(df_clean)
        
    except Exception as e:
        logging.error(f"❌ PostgreSQL loading failed: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        raise

task_load_to_db = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    dag=dag
)

# Dependencies
task_spatial_processing >> task_load_to_db
