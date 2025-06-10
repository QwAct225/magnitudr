"""
DAG 1: Earthquake Data Ingestion Pipeline
==========================================
Capstone 5: Penerapan Pipeline dengan Apache Airflow

Description:
- Automated data ingestion from USGS API
- Data validation and quality checks
- Transform raw data for processing pipeline
- Schedule: Daily at 06:00 WIB (Asia/Jakarta)

Author: [Your Name]
Date: 2025-06-09
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import os
import sys
import pandas as pd
import requests
import json
from pathlib import Path
import logging

# Setup paths
MAGNITUDR_PATH = os.getenv('MAGNITUDR_PROJECT_PATH', os.path.expanduser('~/magnitudr'))
sys.path.append(str(Path(MAGNITUDR_PATH) / 'scripts'))

# DAG configuration
default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9, tzinfo=None),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Initialize DAG
dag = DAG(
    'earthquake_data_ingestion',
    default_args=default_args,
    description='Automated earthquake data ingestion from USGS API',
    schedule_interval='0 6 * * *',  # Daily at 06:00 WIB
    max_active_runs=1,
    tags=['earthquake', 'ingestion', 'etl']
)

def check_api_connectivity(**context):
    """
    Task 1: Check USGS API connectivity and availability
    """
    logging.info("🔍 Checking USGS API connectivity...")
    
    try:
        # Test API endpoint
        test_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        test_params = {
            'format': 'geojson',
            'starttime': '2025-06-01',
            'endtime': '2025-06-02',
            'minmagnitude': 4.0,
            'limit': 1
        }
        
        response = requests.get(test_url, params=test_params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'features' in data:
            logging.info(f"✅ API connectivity OK - Found {len(data['features'])} test records")
            return True
        else:
            raise Exception("API response doesn't contain expected data structure")
            
    except Exception as e:
        logging.error(f"❌ API connectivity check failed: {e}")
        raise

def extract_raw_earthquake_data(**context):
    """
    Task 2: Extract raw earthquake data from USGS API
    Transformation 1: Data extraction with filtering
    """
    logging.info("📥 Extracting earthquake data from USGS API...")
    
    try:
        # Define extraction parameters
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)  # Last 7 days
        
        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            'format': 'geojson',
            'starttime': start_date.strftime('%Y-%m-%d'),
            'endtime': end_date.strftime('%Y-%m-%d'),
            'minmagnitude': 3.0,
            'maxlatitude': 6,
            'minlatitude': -11,
            'maxlongitude': 141,
            'minlongitude': 95,
            'limit': 10000
        }
        
        logging.info(f"Extracting data from {params['starttime']} to {params['endtime']}")
        
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract and flatten data
        earthquakes = []
        for feature in data.get("features", []):
            properties = feature["properties"]
            geometry = feature["geometry"]
            
            earthquake = {
                "id": feature["id"],
                "place": properties.get("place", ""),
                "time": properties.get("time"),
                "updated": properties.get("updated"),
                "longitude": geometry["coordinates"][0],
                "latitude": geometry["coordinates"][1],
                "depth": geometry["coordinates"][2],
                "magnitude": properties.get("mag"),
                "magnitude_type": properties.get("magType", ""),
                "felt": properties.get("felt"),
                "cdi": properties.get("cdi"),
                "mmi": properties.get("mmi"),
                "significance": properties.get("sig"),
                "alert": properties.get("alert", ""),
                "tsunami": properties.get("tsunami", 0),
                "status": properties.get("status", ""),
                "net": properties.get("net", ""),
                "type": properties.get("type", ""),
                "extraction_timestamp": datetime.now().isoformat()
            }
            earthquakes.append(earthquake)
        
        # Save raw data
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        raw_file = output_dir / 'raw_earthquake_data.json'
        with open(raw_file, 'w') as f:
            json.dump(earthquakes, f, indent=2)
        
        logging.info(f"✅ Extracted {len(earthquakes)} earthquake records")
        logging.info(f"📁 Raw data saved to: {raw_file}")
        
        # Store metadata for next tasks
        context['task_instance'].xcom_push(key='records_count', value=len(earthquakes))
        context['task_instance'].xcom_push(key='raw_file_path', value=str(raw_file))
        
        return len(earthquakes)
        
    except Exception as e:
        logging.error(f"❌ Data extraction failed: {e}")
        raise

def validate_and_clean_data(**context):
    """
    Task 3: Validate and clean extracted data
    Transformation 2: Data validation and cleaning
    """
    logging.info("🔍 Validating and cleaning earthquake data...")
    
    try:
        # Get raw data file path from previous task
        raw_file_path = context['task_instance'].xcom_pull(key='raw_file_path')
        
        # Load raw data
        with open(raw_file_path, 'r') as f:
            raw_data = json.load(f)
        
        df = pd.DataFrame(raw_data)
        initial_count = len(df)
        
        logging.info(f"Initial record count: {initial_count}")
        
        # Validation and cleaning steps
        
        # 1. Remove duplicates
        df = df.drop_duplicates(subset=['id'])
        logging.info(f"After duplicate removal: {len(df)} records")
        
        # 2. Validate coordinates
        df = df[
            (df['latitude'].between(-90, 90)) &
            (df['longitude'].between(-180, 180)) &
            (df['depth'] >= 0) &
            (df['magnitude'] > 0)
        ]
        logging.info(f"After coordinate validation: {len(df)} records")
        
        # 3. Handle missing values
        numeric_columns = ['magnitude', 'depth', 'felt', 'cdi', 'mmi', 'significance']
        for col in numeric_columns:
            if col in df.columns:
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
        
        # Fill categorical missing values
        categorical_columns = ['alert', 'status', 'magnitude_type', 'type', 'net']
        for col in categorical_columns:
            if col in df.columns:
                df[col] = df[col].fillna('unknown')
        
        # 4. Data type conversion
        df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
        df['updated'] = pd.to_datetime(df['updated'], unit='ms', errors='coerce')
        
        # 5. Add data quality score
        df['data_quality_score'] = 1.0
        df.loc[df['alert'] == '', 'data_quality_score'] -= 0.1
        df.loc[df['felt'].isna(), 'data_quality_score'] -= 0.1
        df.loc[df['significance'] < 100, 'data_quality_score'] -= 0.1
        
        final_count = len(df)
        
        # Save cleaned data
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        cleaned_file = output_dir / 'cleaned_earthquake_data.csv'
        df.to_csv(cleaned_file, index=False)
        
        logging.info(f"✅ Data validation completed")
        logging.info(f"📊 Records processed: {initial_count} → {final_count} ({final_count/initial_count*100:.1f}% retained)")
        logging.info(f"📁 Cleaned data saved to: {cleaned_file}")
        
        # Store metadata
        context['task_instance'].xcom_push(key='cleaned_records_count', value=final_count)
        context['task_instance'].xcom_push(key='cleaned_file_path', value=str(cleaned_file))
        context['task_instance'].xcom_push(key='data_quality_avg', value=df['data_quality_score'].mean())
        
        return final_count
        
    except Exception as e:
        logging.error(f"❌ Data validation failed: {e}")
        raise

def transform_and_enrich_data(**context):
    """
    Task 4: Transform and enrich earthquake data
    Transformation 3: Feature engineering and enrichment
    """
    logging.info("🔄 Transforming and enriching earthquake data...")
    
    try:
        # Get cleaned data file path
        cleaned_file_path = context['task_instance'].xcom_pull(key='cleaned_file_path')
        
        # Load cleaned data
        df = pd.read_csv(cleaned_file_path)
        df['time'] = pd.to_datetime(df['time'])
        
        # Feature engineering and enrichment
        
        # 1. Temporal features
        df['year'] = df['time'].dt.year
        df['month'] = df['time'].dt.month
        df['day_of_week'] = df['time'].dt.dayofweek
        df['hour'] = df['time'].dt.hour
        
        # 2. Magnitude categories
        df['magnitude_category'] = pd.cut(
            df['magnitude'],
            bins=[0, 3.0, 4.0, 5.0, 6.0, 7.0, 10.0],
            labels=['Very Minor', 'Minor', 'Light', 'Moderate', 'Strong', 'Major'],
            include_lowest=True
        )
        
        # 3. Depth categories
        df['depth_category'] = pd.cut(
            df['depth'],
            bins=[0, 30, 70, 300, 700],
            labels=['Very Shallow', 'Shallow', 'Intermediate', 'Deep'],
            include_lowest=True
        )
        
        # 4. Regional classification
        def classify_region(row):
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
        
        df['region'] = df.apply(classify_region, axis=1)
        
        # 5. Risk scoring
        def calculate_risk_score(row):
            score = 0
            
            # Magnitude component (0-4 points)
            score += min(row['magnitude'] / 2, 4)
            
            # Depth component (0-2 points, shallow = higher risk)
            if row['depth'] <= 30:
                score += 2
            elif row['depth'] <= 70:
                score += 1
            
            # Alert level component (0-2 points)
            alert_scores = {'red': 2, 'orange': 1.5, 'yellow': 1, 'green': 0.5, 'unknown': 0}
            score += alert_scores.get(row['alert'], 0)
            
            # Tsunami component (0-2 points)
            if row['tsunami'] == 1:
                score += 2
            
            # Population density component (regional)
            if row['region'] in ['Java', 'Sumatra']:
                score += 1
            
            return min(score, 10)  # Cap at 10
        
        df['risk_score'] = df.apply(calculate_risk_score, axis=1)
        
        # 6. Distance from major fault lines (simplified)
        df['fault_distance'] = ((df['longitude'] - 110) ** 2 + (df['latitude'] + 2) ** 2) ** 0.5
        
        # 7. Energy release estimation
        df['energy_joules'] = 10 ** (1.5 * df['magnitude'] + 4.8)
        
        # Save enriched data
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        enriched_file = output_dir / 'enriched_earthquake_data.csv'
        df.to_csv(enriched_file, index=False)
        
        # Generate summary statistics
        summary = {
            'total_records': len(df),
            'date_range': f"{df['time'].min()} to {df['time'].max()}",
            'magnitude_range': f"{df['magnitude'].min():.1f} to {df['magnitude'].max():.1f}",
            'regions': df['region'].value_counts().to_dict(),
            'magnitude_categories': df['magnitude_category'].value_counts().to_dict(),
            'average_risk_score': df['risk_score'].mean(),
            'high_risk_events': len(df[df['risk_score'] >= 7])
        }
        
        summary_file = output_dir / 'ingestion_summary.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logging.info("✅ Data transformation and enrichment completed")
        logging.info(f"📊 Total features: {len(df.columns)}")
        logging.info(f"📊 Average risk score: {summary['average_risk_score']:.2f}")
        logging.info(f"📊 High-risk events (≥7): {summary['high_risk_events']}")
        logging.info(f"📁 Enriched data saved to: {enriched_file}")
        
        # Store final metadata
        context['task_instance'].xcom_push(key='enriched_file_path', value=str(enriched_file))
        context['task_instance'].xcom_push(key='summary', value=summary)
        
        return str(enriched_file)
        
    except Exception as e:
        logging.error(f"❌ Data transformation failed: {e}")
        raise

def generate_ingestion_report(**context):
    """
    Task 5: Generate ingestion pipeline report
    """
    logging.info("📋 Generating ingestion pipeline report...")
    
    try:
        # Get summary from previous task
        summary = context['task_instance'].xcom_pull(key='summary')
        
        # Generate detailed report
        report = {
            'pipeline_name': 'Earthquake Data Ingestion',
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].dag_id,
            'status': 'SUCCESS',
            'summary': summary,
            'pipeline_stages': {
                'api_connectivity_check': 'PASSED',
                'data_extraction': f"Extracted {summary['total_records']} records",
                'data_validation': 'Data cleaned and validated',
                'data_enrichment': f"Added 7 new features, avg risk score: {summary['average_risk_score']:.2f}",
                'report_generation': 'Report generated successfully'
            },
            'next_steps': [
                'Data ready for processing pipeline',
                'Files available in data/airflow_output/',
                'Execute processing DAG to continue pipeline'
            ]
        }
        
        # Save report
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        report_file = output_dir / 'ingestion_pipeline_report.json'
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logging.info("✅ Ingestion pipeline report generated")
        logging.info(f"📁 Report saved to: {report_file}")
        
        return str(report_file)
        
    except Exception as e:
        logging.error(f"❌ Report generation failed: {e}")
        raise

# Define tasks
task_check_api = PythonOperator(
    task_id='check_api_connectivity',
    python_callable=check_api_connectivity,
    dag=dag
)

task_extract_data = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_raw_earthquake_data,
    dag=dag
)

task_validate_data = PythonOperator(
    task_id='validate_and_clean_data',
    python_callable=validate_and_clean_data,
    dag=dag
)

task_transform_data = PythonOperator(
    task_id='transform_and_enrich_data',
    python_callable=transform_and_enrich_data,
    dag=dag
)

task_generate_report = PythonOperator(
    task_id='generate_ingestion_report',
    python_callable=generate_ingestion_report,
    dag=dag
)

# Define task dependencies
task_check_api >> task_extract_data >> task_validate_data >> task_transform_data >> task_generate_report