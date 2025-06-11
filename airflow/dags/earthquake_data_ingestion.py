from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add custom operators to path
sys.path.append('/opt/airflow/operators')
from usgs_operator import USGSDataOperator

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
    'earthquake_data_ingestion',
    default_args=default_args,
    description='🌍 USGS earthquake data ingestion for Indonesia',
    schedule_interval=None,  # Triggered by master DAG
    max_active_runs=1,
    tags=['earthquake', 'ingestion', 'usgs']
)

# Task 1: Extract raw data from USGS API
task_extract_usgs = USGSDataOperator(
    task_id='extract_usgs_earthquake_data',
    output_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
    days_back=90,           # 3 months untuk ensure 64MB+
    min_magnitude=2.5,      # Lower threshold untuk more data
    target_size_mb=64.0,    # Explicit 64MB requirement
    dag=dag
)

def validate_raw_data(**context):
    """Validate extracted data meets requirements"""
    import pandas as pd
    import logging
    
    file_path = '/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv'
    
    try:
        df = pd.read_csv(file_path)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        logging.info(f"📊 Raw data validation:")
        logging.info(f"📊 Records: {len(df):,}")
        logging.info(f"📊 File size: {file_size_mb:.2f}MB")
        logging.info(f"📊 Columns: {len(df.columns)}")
        
        # Validation checks
        assert len(df) > 1000, f"Insufficient records: {len(df)}"
        assert file_size_mb >= 64, f"File size too small: {file_size_mb}MB < 64MB"
        assert 'magnitude' in df.columns, "Missing magnitude column"
        assert 'latitude' in df.columns, "Missing latitude column"
        assert 'longitude' in df.columns, "Missing longitude column"
        
        logging.info("✅ Raw data validation passed")
        return len(df)
        
    except Exception as e:
        logging.error(f"❌ Raw data validation failed: {e}")
        raise

task_validate = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    dag=dag
)

# Dependencies
task_extract_usgs >> task_validate
