from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add custom operators to path
sys.path.append('/opt/airflow/operators')
sys.path.append('/opt/airflow/utils')
from dbscan_operator import DBSCANClusterOperator
from dvc_utils import DVCManager

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

dag = DAG(
    'earthquake_dbscan_clustering',
    default_args=default_args,
    description='🔬 DBSCAN clustering and hazard zone analysis',
    schedule_interval=None,  # Triggered by master DAG
    max_active_runs=1,
    tags=['earthquake', 'clustering', 'dbscan', 'dvc']
)

# Task 1: DBSCAN clustering analysis
task_dbscan = DBSCANClusterOperator(
    task_id='run_dbscan_clustering',
    input_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
    db_connection='postgresql://postgres:earthquake123@postgres:5432/magnitudr',
    eps=0.1,            # 0.1 degree radius (~11km)
    min_samples=5,      # Minimum 5 events per cluster
    dag=dag
)

def trigger_dvc_versioning(**context):
    """Trigger DVC add and push for data versioning"""
    import logging
    
    try:
        dvc_manager = DVCManager()
        
        # Add and push processed data
        data_paths = [
            '/opt/airflow/magnitudr/data/airflow_output/',
        ]
        
        for path in data_paths:
            if os.path.exists(path):
                success = dvc_manager.add_and_push_data(path)
                if success:
                    logging.info(f"✅ DVC versioning successful: {path}")
                else:
                    logging.warning(f"⚠️ DVC versioning failed: {path}")
        
        return True
        
    except Exception as e:
        logging.warning(f"⚠️ DVC versioning error: {e}")
        # Don't fail the whole pipeline for DVC issues
        return True

task_dvc_version = PythonOperator(
    task_id='trigger_dvc_versioning',
    python_callable=trigger_dvc_versioning,
    dag=dag
)

def generate_clustering_summary(**context):
    """Generate clustering analysis summary"""
    import psycopg2
    import pandas as pd
    import json
    import logging
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="magnitudr", 
            user="postgres",
            password="earthquake123"
        )
        
        # Get clustering statistics
        query = """
        SELECT 
            risk_zone,
            COUNT(DISTINCT cluster_id) as cluster_count,
            COUNT(*) as total_events,
            AVG(avg_magnitude) as avg_magnitude,
            MAX(avg_magnitude) as max_magnitude
        FROM earthquake_clusters
        GROUP BY risk_zone
        ORDER BY total_events DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Generate summary report
        summary = {
            'clustering_timestamp': datetime.now().isoformat(),
            'total_clusters': len(df),
            'risk_zones': df.to_dict('records'),
            'high_risk_zones': len(df[df['risk_zone'].isin(['High', 'Extreme'])]),
            'total_clustered_events': df['total_events'].sum()
        }
        
        # Save summary
        summary_path = '/opt/airflow/magnitudr/data/airflow_output/clustering_summary.json'
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logging.info(f"✅ Clustering summary generated: {summary_path}")
        logging.info(f"📊 Total clusters: {summary['total_clusters']}")
        logging.info(f"🚨 High-risk zones: {summary['high_risk_zones']}")
        
        return summary['total_clusters']
        
    except Exception as e:
        logging.error(f"❌ Clustering summary failed: {e}")
        raise

task_generate_summary = PythonOperator(
    task_id='generate_clustering_summary',
    python_callable=generate_clustering_summary,
    dag=dag
)

# Dependencies
task_dbscan >> task_dvc_version >> task_generate_summary
