from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import logging

# Add operators to path
sys.path.append('/opt/airflow/operators')

default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'earthquake_master_pipeline',
    default_args=default_args,
    description='🌍 Master earthquake analysis pipeline with ML - production ready',
    schedule_interval='0 19 * * *',  # Daily at 02:00 WIB (19:00 UTC) - Best practice
    max_active_runs=1,
    tags=['earthquake', 'master', 'production', 'ml']
)

def run_usgs_ingestion(**context):
    """Step 1: USGS Data Ingestion"""
    logging.info("🌍 Starting USGS data ingestion...")
    
    try:
        from usgs_operator import USGSDataOperator
        
        usgs_operator = USGSDataOperator(
            task_id='usgs_ingestion',
            output_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
            start_year=2014,
            min_magnitude=1.0,
            target_size_mb=64.0,
            strict_validation=False
        )
        
        result = usgs_operator.execute(context)
        logging.info(f"✅ USGS ingestion completed: {result} records")
        return result
        
    except Exception as e:
        logging.error(f"❌ USGS ingestion failed: {e}")
        raise

def run_spatial_processing(**context):
    """Step 2: Spatial Processing and Feature Engineering"""
    logging.info("🔄 Starting spatial processing...")
    
    try:
        from spatial_operator import SpatialDensityOperator
        
        spatial_operator = SpatialDensityOperator(
            task_id='spatial_processing',
            input_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
            output_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
            grid_size=0.1
        )
        
        result = spatial_operator.execute(context)
        logging.info(f"✅ Spatial processing completed: {result} records")
        return result
        
    except Exception as e:
        logging.error(f"❌ Spatial processing failed: {e}")
        raise

def load_to_database(**context):
    """Step 3: Load processed data to PostgreSQL"""
    logging.info("📊 Loading data to PostgreSQL...")
    
    try:
        import pandas as pd
        from sqlalchemy import create_engine, text
        
        df = pd.read_csv('/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv')
        logging.info(f"📊 Read {len(df)} records from processed file")
        
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
        
        db_columns = [
            'id', 'magnitude', 'latitude', 'longitude', 'depth', 'time',
            'place', 'spatial_density', 'hazard_score', 'region',
            'magnitude_category', 'depth_category'
        ]
        
        available_cols = [col for col in db_columns if col in df.columns]
        df_clean = df[available_cols].copy()
        
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM earthquake_clusters"))
            conn.execute(text("DELETE FROM hazard_zones"))
            conn.execute(text("DELETE FROM earthquakes_processed"))
            logging.info("✅ Cleared existing data")
        
        df_clean.to_sql(
            'earthquakes_processed',
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logging.info(f"✅ Loaded {len(df_clean)} records to database")
        return len(df_clean)
        
    except Exception as e:
        logging.error(f"❌ Database loading failed: {e}")
        raise

def run_dbscan_clustering(**context):
    """Step 4: DBSCAN Clustering Analysis"""
    logging.info("🔬 Starting DBSCAN clustering...")
    
    try:
        from dbscan_operator import DBSCANClusterOperator
        
        dbscan_operator = DBSCANClusterOperator(
            task_id='dbscan_clustering',
            input_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
            db_connection='postgresql://postgres:earthquake123@postgres:5432/magnitudr',
            eps=0.1,
            min_samples=5
        )
        
        result = dbscan_operator.execute(context)
        logging.info(f"✅ DBSCAN clustering completed: {result} clusters")
        return result
        
    except Exception as e:
        logging.error(f"❌ DBSCAN clustering failed: {e}")
        raise

def run_data_visualization(**context):
    """Step 5: Data Visualization and EDA"""
    logging.info("📊 Starting data visualization...")
    
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        import seaborn as sns
        from sqlalchemy import create_engine
        import os
        
        # Load data for visualization
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        # Load processed earthquake data
        earthquake_query = "SELECT * FROM earthquakes_processed LIMIT 10000"
        df_earthquakes = pd.read_sql(earthquake_query, engine)
        
        # Load cluster data  
        cluster_query = "SELECT * FROM earthquake_clusters"
        df_clusters = pd.read_sql(cluster_query, engine)
        
        # Create output directory
        viz_dir = '/opt/airflow/magnitudr/data/plots'
        os.makedirs(viz_dir, exist_ok=True)
        
        # Set style
        plt.style.use('dark_background')
        sns.set_palette("husl")
        
        # 1. Magnitude distribution
        plt.figure(figsize=(10, 6))
        plt.hist(df_earthquakes['magnitude'], bins=30, alpha=0.7, color='orange')
        plt.title('Earthquake Magnitude Distribution', fontsize=14)
        plt.xlabel('Magnitude')
        plt.ylabel('Frequency')
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/magnitude_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Risk zone distribution
        if not df_clusters.empty:
            plt.figure(figsize=(8, 6))
            risk_counts = df_clusters['risk_zone'].value_counts()
            colors = ['#E74C3C', '#FF6B35', '#F39C12', '#27AE60']
            plt.pie(risk_counts.values, labels=risk_counts.index, autopct='%1.1f%%', colors=colors)
            plt.title('Risk Zone Distribution', fontsize=14)
            plt.tight_layout()
            plt.savefig(f'{viz_dir}/risk_zone_distribution.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 3. Geographic scatter plot
        plt.figure(figsize=(12, 8))
        scatter = plt.scatter(df_earthquakes['longitude'], df_earthquakes['latitude'], 
                            c=df_earthquakes['magnitude'], cmap='Reds', alpha=0.6)
        plt.colorbar(scatter, label='Magnitude')
        plt.title('Earthquake Geographic Distribution', fontsize=14)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/geographic_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logging.info(f"✅ Generated 3 visualization plots in {viz_dir}")
        
        # Create visualization summary
        viz_summary = {
            'visualization_timestamp': datetime.now().isoformat(),
            'plots_generated': 3,
            'earthquake_samples': len(df_earthquakes),
            'cluster_samples': len(df_clusters),
            'output_directory': viz_dir,
            'plots': [
                'magnitude_distribution.png',
                'risk_zone_distribution.png', 
                'geographic_distribution.png'
            ]
        }
        
        import json
        with open(f'{viz_dir}/visualization_summary.json', 'w') as f:
            json.dump(viz_summary, f, indent=2)
        
        logging.info("✅ Data visualization completed")
        return len(viz_summary['plots'])
        
    except Exception as e:
        logging.error(f"❌ Data visualization failed: {e}")
        raise

def generate_master_report(**context):
    """Step 6: Generate master pipeline report with ML metrics"""
    logging.info("📋 Generating master pipeline report...")
    
    try:
        import psycopg2
        import json
        from pathlib import Path
        
        conn = psycopg2.connect(
            host="postgres", port="5432", database="magnitudr",
            user="postgres", password="earthquake123"
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM earthquakes_processed")
        total_earthquakes = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT cluster_id) FROM earthquake_clusters WHERE cluster_id IS NOT NULL")
        total_clusters = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM hazard_zones WHERE risk_level IN ('High', 'Extreme')")
        high_risk_zones = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT risk_zone, COUNT(*) as count 
            FROM earthquake_clusters 
            GROUP BY risk_zone 
            ORDER BY count DESC
        """)
        risk_distribution = dict(cursor.fetchall())
        
        cursor.close()
        conn.close()
        
        # Get ML model metrics if available
        try:
            metrics_path = '/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_metrics.json'
            if os.path.exists(metrics_path):
                with open(metrics_path, 'r') as f:
                    ml_metrics = json.load(f)
                report_ml_metrics = {
                    'accuracy': ml_metrics.get('accuracy', 0),
                    'f1_score': ml_metrics.get('f1_score', 0),
                    'precision': ml_metrics.get('precision', 0),
                    'recall': ml_metrics.get('recall', 0)
                }
            else:
                report_ml_metrics = {'status': 'model_not_found'}
        except Exception as e:
            report_ml_metrics = {'status': 'error', 'message': str(e)}
        
        report = {
            'pipeline_name': 'Magnitudr Master Production Pipeline with ML',
            'execution_timestamp': datetime.now().isoformat(),
            'status': 'SUCCESS',
            'schedule': 'Daily at 02:00 WIB',
            'data_statistics': {
                'total_earthquakes': total_earthquakes,
                'total_clusters': total_clusters,
                'high_risk_zones': high_risk_zones,
                'risk_distribution': risk_distribution,
                'data_coverage': '2014-2025 (11 years)'
            },
            'ml_model_performance': report_ml_metrics,
            'system_endpoints': {
                'api_docs': 'http://localhost:8000/docs',
                'api_health': 'http://localhost:8000/health',
                'ml_predictions': 'http://localhost:8000/ml/predictions',
                'ml_metrics': 'http://localhost:8000/ml/model-metrics',
                'dashboard': 'http://localhost:8501',
                'airflow': 'http://localhost:8080'
            },
            'production_ready': True
        }
        
        report_path = Path("/opt/airflow/magnitudr/data/airflow_output/master_pipeline_report.json")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logging.info(f"🎉 MASTER PIPELINE WITH ML COMPLETED!")
        logging.info(f"📊 Earthquakes: {total_earthquakes:,}")
        logging.info(f"🔬 Clusters: {total_clusters}")
        logging.info(f"🚨 High-risk zones: {high_risk_zones}")
        logging.info(f"🤖 ML Model: {report_ml_metrics.get('status', 'Available')}")
        logging.info(f"🎯 Dashboard: http://localhost:8501")
        logging.info(f"🤖 ML API: http://localhost:8000/ml/predictions")
        
        return str(report_path)
        
    except Exception as e:
        logging.error(f"❌ Master report failed: {e}")
        raise

# Define tasks
task_ingestion = PythonOperator(
    task_id='usgs_data_ingestion',
    python_callable=run_usgs_ingestion,
    dag=dag
)

task_processing = PythonOperator(
    task_id='spatial_processing',
    python_callable=run_spatial_processing,
    dag=dag
)

task_database = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag
)

task_clustering = PythonOperator(
    task_id='dbscan_clustering',
    python_callable=run_dbscan_clustering,
    dag=dag
)

task_visualization = PythonOperator(
    task_id='data_visualization',
    python_callable=run_data_visualization,
    dag=dag
)

task_report = PythonOperator(
    task_id='generate_master_report',
    python_callable=generate_master_report,
    dag=dag
)

# Daily production pipeline flow (without ML)
task_ingestion >> task_processing >> task_database >> task_clustering >> task_visualization >> task_report
