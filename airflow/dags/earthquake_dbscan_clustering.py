from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add custom operators to path
sys.path.append('/opt/airflow')
from operators.dbscan_operator import DBSCANClusterOperator

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
    description='DBSCAN clustering and hazard zone analysis',
    schedule_interval=None,  # Triggered by master DAG
    max_active_runs=1,
    tags=['earthquake', 'clustering', 'dbscan']
)

# Task 1: DBSCAN clustering analysis
task_dbscan = DBSCANClusterOperator(
    task_id='run_dbscan_clustering',
    input_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
    db_connection='postgresql://postgres:earthquake123@postgres:5432/magnitudr',
    model_path='/opt/airflow/magnitudr/data/models/earthquake_model.pkl',
    scaler_path='/opt/airflow/magnitudr/data/models/earthquake_model_scaler.pkl',
    label_encoder_path='/opt/airflow/magnitudr/data/models/earthquake_model_label_encoder.pkl',
    eps=0.1,
    min_samples=5,
    dag=dag
)

def generate_clustering_summary(**context):
    """Generate clustering analysis summary with proper JSON serialization"""
    from sqlalchemy import create_engine, text
    import pandas as pd
    import json
    import logging
    import numpy as np
    
    try:
        # Create engine
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        # Get clustering statistics with proper data types
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    risk_zone,
                    COUNT(DISTINCT cluster_id) as cluster_count,
                    COUNT(*) as total_events,
                    AVG(avg_magnitude) as avg_magnitude,
                    MAX(avg_magnitude) as max_magnitude
                FROM earthquake_clusters
                GROUP BY risk_zone
                ORDER BY total_events DESC
            """))

            risk_zones = []
            for row in result:
                risk_zone_data = {
                    'risk_zone': str(row[0]) if row[0] else 'Unknown',
                    'cluster_count': int(row[1]) if row[1] else 0,
                    'total_events': int(row[2]) if row[2] else 0,
                    'avg_magnitude': float(row[3]) if row[3] else 0.0,
                    'max_magnitude': float(row[4]) if row[4] else 0.0
                }
                risk_zones.append(risk_zone_data)
        
        # Get total clusters count
        with engine.connect() as conn:
            total_clusters_result = conn.execute(text("""
                SELECT COUNT(DISTINCT cluster_id) as total_clusters
                FROM earthquake_clusters
                WHERE cluster_id IS NOT NULL
            """))
            total_clusters = total_clusters_result.scalar() or 0
        
        # Generate summary report with JSON-safe data types
        summary = {
            'clustering_timestamp': datetime.now().isoformat(),
            'total_clusters': int(total_clusters),
            'risk_zones': risk_zones,
            'high_risk_zones': len([rz for rz in risk_zones if rz['risk_zone'] in ['High', 'Extreme']]),
            'total_clustered_events': sum(rz['total_events'] for rz in risk_zones),
            'pipeline_status': 'completed',
            'data_quality': {
                'risk_zone_diversity': len(risk_zones),
                'largest_cluster_size': max([rz['total_events'] for rz in risk_zones]) if risk_zones else 0,
                'magnitude_range': {
                    'max': max([rz['max_magnitude'] for rz in risk_zones]) if risk_zones else 0.0,
                    'avg': sum([rz['avg_magnitude'] for rz in risk_zones]) / len(risk_zones) if risk_zones else 0.0
                }
            }
        }
        
        # Save summary with error handling
        summary_path = '/opt/airflow/magnitudr/data/airflow_output/clustering_summary.json'
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)  # default=str handles remaining type issues
        
        logging.info(f"✅ Clustering summary generated: {summary_path}")
        logging.info(f" Total clusters: {summary['total_clusters']}")
        logging.info(f" High-risk zones: {summary['high_risk_zones']}")
        logging.info(f" Total events clustered: {summary['total_clustered_events']}")
        
        # Log detailed breakdown
        for rz in risk_zones:
            logging.info(f"   {rz['risk_zone']}: {rz['cluster_count']} clusters, {rz['total_events']} events")
        
        return summary['total_clusters']
        
    except Exception as e:
        logging.error(f"❌ Clustering summary failed: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        raise

task_generate_summary = PythonOperator(
    task_id='generate_clustering_summary',
    python_callable=generate_clustering_summary,
    dag=dag
)

def trigger_dvc_versioning(**context):
    """Optional DVC versioning trigger"""
    import logging
    import subprocess
    
    try:
        # Check if DVC is available
        result = subprocess.run(
            ["dvc", "status"],
            cwd="/opt/airflow/magnitudr",
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            # DVC add and push
            subprocess.run(
                ["dvc", "add", "data/airflow_output/"],
                cwd="/opt/airflow/magnitudr",
                check=True,
                timeout=60
            )
            
            subprocess.run(
                ["dvc", "push"],
                cwd="/opt/airflow/magnitudr", 
                check=True,
                timeout=120
            )

            logging.info("✅ DVC versioning completed")
        else:
            logging.warning("⚠️ DVC not properly configured - skipping versioning")
            
    except Exception as e:
        logging.warning(f"⚠️ DVC versioning failed: {e} - continuing pipeline")
    
    return True

task_dvc_version = PythonOperator(
    task_id='trigger_dvc_versioning',
    python_callable=trigger_dvc_versioning,
    dag=dag
)

# Dependencies
task_dbscan >> task_generate_summary >> task_dvc_version
