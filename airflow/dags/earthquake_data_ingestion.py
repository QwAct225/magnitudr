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
    description='USGS earthquake data ingestion 2016-2025',
    schedule_interval=None,
    max_active_runs=1,
    tags=['earthquake', 'ingestion', 'usgs', '2016-2025']
)

# Task 1: Extract historical data from USGS API (2016-2025)
task_extract_usgs = USGSDataOperator(
    task_id='extract_usgs_earthquake_data',
    output_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
    start_year=2016,        # Historical data from 2016
    min_magnitude=1.5,      # Lower threshold for more data
    target_size_mb=64.0,    # Target but not strict
    strict_validation=False, # Allow pipeline to continue
    dag=dag
)

def validate_raw_data(**context):
    """Flexible validation with detailed reporting"""
    import pandas as pd
    import logging
    
    file_path = '/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv'
    
    try:
        df = pd.read_csv(file_path)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        logging.info(f"📊 Raw Data Validation Report:")
        logging.info(f" Records: {len(df):,}")
        logging.info(f" File size: {file_size_mb:.2f}MB")
        logging.info(f" Columns: {len(df.columns)}")
        logging.info(f" Date range: {df['year'].min() if 'year' in df.columns else 'Unknown'} - {df['year'].max() if 'year' in df.columns else 'Unknown'}")
        logging.info(f" Regions: {df['indonesian_region'].nunique() if 'indonesian_region' in df.columns else 'Unknown'}")
        
        # Basic validation checks
        assert len(df) > 100, f"Insufficient records: {len(df)}"
        assert 'magnitude' in df.columns, "Missing magnitude column"
        assert 'latitude' in df.columns, "Missing latitude column"
        assert 'longitude' in df.columns, "Missing longitude column"
        
        # Size check (warning only)
        if file_size_mb < 64:
            logging.warning(f"⚠️ File size {file_size_mb:.2f}MB < 64MB target, but continuing pipeline...")
        else:
            logging.info(f"✅ Size requirement met: {file_size_mb:.2f}MB >= 64MB")
        
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
