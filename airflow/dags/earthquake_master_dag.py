from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging

# DAG configuration
default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Master DAG for orchestrating the complete pipeline
dag = DAG(
    'earthquake_master_pipeline',
    default_args=default_args,
    description='🌍 Master pipeline for earthquake analysis and hazard detection',
    schedule_interval='@daily',  # Run daily
    max_active_runs=1,
    tags=['earthquake', 'master', 'pipeline']
)

def check_system_health(**context):
    """Check system prerequisites and health"""
    logging.info("🔍 Checking system health...")
    
    # Database connectivity check
    import os
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host="postgres",
            port="5432", 
            database="magnitudr",
            user="postgres",
            password="earthquake123"
        )
        conn.close()
        logging.info("✅ Database connection: OK")
    except Exception as e:
        logging.error(f"❌ Database connection failed: {e}")
        raise
    
    # Check data directories
    data_paths = [
        "/opt/airflow/magnitudr/data",
        "/opt/airflow/magnitudr/data/airflow_output"
    ]
    
    for path in data_paths:
        os.makedirs(path, exist_ok=True)
        logging.info(f"📁 Directory ready: {path}")
    
    logging.info("🎉 System health check passed!")
    return True

def generate_pipeline_report(**context):
    """Generate comprehensive pipeline execution report"""
    logging.info("📋 Generating pipeline execution report...")
    
    import json
    from pathlib import Path
    
    # Collect execution metadata
    execution_date = context['execution_date']
    dag_run = context['dag_run']
    
    report = {
        'pipeline_name': 'Earthquake Master Pipeline',
        'execution_date': execution_date.isoformat(),
        'dag_run_id': dag_run.dag_id,
        'status': 'SUCCESS',
        'stages': [
            'System Health Check',
            'Data Ingestion', 
            'Spatial Processing',
            'DBSCAN Clustering',
            'API Data Availability'
        ],
        'next_steps': [
            'Check FastAPI endpoints at http://localhost:8000/docs',
            'Access Streamlit dashboard at http://localhost:8501',
            'Review cluster analysis and hazard zones'
        ],
        'timestamp': datetime.now().isoformat()
    }
    
    # Save report
    report_path = Path("/opt/airflow/magnitudr/data/airflow_output/master_pipeline_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    logging.info(f"✅ Pipeline report saved: {report_path}")
    return str(report_path)

# Define tasks
task_health_check = PythonOperator(
    task_id='check_system_health',
    python_callable=check_system_health,
    dag=dag
)

task_trigger_ingestion = TriggerDagRunOperator(
    task_id='trigger_data_ingestion',
    trigger_dag_id='earthquake_data_ingestion',
    wait_for_completion=True,
    dag=dag
)

task_trigger_processing = TriggerDagRunOperator(
    task_id='trigger_spatial_processing', 
    trigger_dag_id='earthquake_spatial_processing',
    wait_for_completion=True,
    dag=dag
)

task_trigger_clustering = TriggerDagRunOperator(
    task_id='trigger_dbscan_clustering',
    trigger_dag_id='earthquake_dbscan_clustering', 
    wait_for_completion=True,
    dag=dag
)

task_generate_report = PythonOperator(
    task_id='generate_pipeline_report',
    python_callable=generate_pipeline_report,
    dag=dag
)

# Define dependencies
task_health_check >> task_trigger_ingestion >> task_trigger_processing >> task_trigger_clustering >> task_generate_report
