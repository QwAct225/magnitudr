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
    description='🔄 Spatial processing and feature engineering',
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
    """Load processed data to PostgreSQL earthquakes_processed table"""
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine
    import logging
    
    try:
        # Read processed data
        df = pd.read_csv('/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv')
        
        # Database connection
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        # Data type conversion and cleaning
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
        
        # Remove any existing data to avoid duplicates
        with engine.connect() as conn:
            conn.execute("DELETE FROM earthquakes_processed")
            conn.commit()
        
        # Load to PostgreSQL
        df_clean.to_sql(
            'earthquakes_processed',
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logging.info(f"✅ Loaded {len(df_clean)} records to earthquakes_processed")
        logging.info(f"📊 Columns: {available_cols}")
        
        return len(df_clean)
        
    except Exception as e:
        logging.error(f"❌ PostgreSQL loading failed: {e}")
        raise

task_load_to_db = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    dag=dag
)

# Dependencies
task_spatial_processing >> task_load_to_db
