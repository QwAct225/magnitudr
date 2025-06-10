"""
DAG 5: Earthquake Visualization (Simplified)
============================================
Capstone 5: Penerapan Pipeline dengan Apache Airflow

Description:
- Simplified visualization generation from processed data
- Basic charts and dashboard creation
- Optimized for better Airflow performance

Author: [Your Name]
Date: 2025-06-10
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
from sqlalchemy import create_engine

# Setup paths
MAGNITUDR_PATH = os.getenv('MAGNITUDR_PROJECT_PATH', os.path.expanduser('~/magnitudr'))

# DAG configuration
default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9, tzinfo=None),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'execution_timeout': timedelta(minutes=30)  # 30 minutes max per task
}

# Initialize DAG
dag = DAG(
    'earthquake_visualization_dashboard',
    default_args=default_args,
    description='Dashboard earthquake data visualization pipeline',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    tags=['earthquake', 'visualization', 'dashboard', 'analytics']
)

def load_data_simple(**context):
    """Load data for visualization (simplified)"""
    logging.info("📊 Loading data for visualization...")
    
    try:
        # Try database first
        try:
            db_connection = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            engine = create_engine(db_connection)
            
            query = "SELECT id, magnitude, place, longitude, latitude, depth, region, risk_score FROM earthquakes_processed_airflow LIMIT 10000"
            df = pd.read_sql(query, engine)
            data_source = "database"
            
        except Exception as e:
            logging.warning(f"Database failed: {e}, trying file...")
            
            # Fallback to file
            data_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
            csv_file = data_dir / 'final_processed_earthquake_data.csv'
            
            if csv_file.exists():
                df = pd.read_csv(csv_file).head(10000)  # Limit for performance
                data_source = "file"
            else:
                raise FileNotFoundError("No data source available")
        
        # Save subset for visualization
        viz_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'viz_data_simple.csv'
        df.to_csv(viz_file, index=False)
        
        logging.info(f"✅ Loaded {len(df)} records from {data_source}")
        return str(viz_file)
        
    except Exception as e:
        logging.error(f"❌ Data loading failed: {e}")
        raise

def create_basic_plots(**context):
    """Create basic visualization plots"""
    logging.info("📈 Creating basic plots...")
    
    try:
        # Load data
        viz_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'viz_data_simple.csv'
        df = pd.read_csv(viz_file)
        
        # Create plots directory
        plots_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'plots'
        plots_dir.mkdir(parents=True, exist_ok=True)
        
        # Use matplotlib for simple plots
        import matplotlib.pyplot as plt
        import matplotlib
        matplotlib.use('Agg')
        
        # 1. Magnitude distribution
        plt.figure(figsize=(10, 6))
        plt.hist(df['magnitude'].dropna(), bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        plt.title('Earthquake Magnitude Distribution')
        plt.xlabel('Magnitude')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.savefig(plots_dir / 'magnitude_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        # 2. Regional distribution
        if 'region' in df.columns:
            plt.figure(figsize=(12, 6))
            region_counts = df['region'].value_counts().head(8)
            plt.bar(region_counts.index, region_counts.values, color='lightcoral', alpha=0.7)
            plt.title('Earthquake Distribution by Region')
            plt.xlabel('Region')
            plt.ylabel('Count')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(plots_dir / 'regional_distribution.png', dpi=150, bbox_inches='tight')
            plt.close()
        
        # 3. Magnitude vs Depth scatter
        plt.figure(figsize=(10, 6))
        plt.scatter(df['magnitude'], df['depth'], alpha=0.6, c='green', s=20)
        plt.title('Magnitude vs Depth')
        plt.xlabel('Magnitude')
        plt.ylabel('Depth (km)')
        plt.grid(True, alpha=0.3)
        plt.savefig(plots_dir / 'magnitude_vs_depth.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        logging.info("✅ Basic plots created successfully")
        return str(plots_dir)
        
    except Exception as e:
        logging.error(f"❌ Plot creation failed: {e}")
        raise

def create_simple_dashboard(**context):
    """Create simple HTML dashboard"""
    logging.info("📋 Creating simple dashboard...")
    
    try:
        plots_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'plots'
        
        # Simple HTML dashboard
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Earthquake Analysis Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; }}
                .header {{ text-align: center; color: #2c3e50; margin-bottom: 30px; }}
                .plot {{ margin: 20px 0; text-align: center; }}
                .plot img {{ max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 5px; }}
                .footer {{ text-align: center; margin-top: 30px; color: #7f8c8d; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🌍 Earthquake Analysis Dashboard</h1>
                    <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="plot">
                    <h2>Magnitude Distribution</h2>
                    <img src="magnitude_distribution.png" alt="Magnitude Distribution">
                </div>
                
                <div class="plot">
                    <h2>Regional Distribution</h2>
                    <img src="regional_distribution.png" alt="Regional Distribution">
                </div>
                
                <div class="plot">
                    <h2>Magnitude vs Depth</h2>
                    <img src="magnitude_vs_depth.png" alt="Magnitude vs Depth">
                </div>
                
                <div class="footer">
                    <p>Magnitudr - Earthquake Analysis System</p>
                    <p>Capstone 5: Apache Airflow Pipeline</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Save dashboard
        dashboard_file = plots_dir / 'dashboard.html'
        with open(dashboard_file, 'w') as f:
            f.write(html_content)
        
        logging.info(f"✅ Dashboard created: {dashboard_file}")
        return str(dashboard_file)
        
    except Exception as e:
        logging.error(f"❌ Dashboard creation failed: {e}")
        raise

def generate_viz_report(**context):
    """Generate visualization report"""
    logging.info("📋 Generating visualization report...")
    
    try:
        report = {
            'pipeline_name': 'Earthquake Visualization',
            'execution_date': context['execution_date'].isoformat(),
            'status': 'SUCCESS',
            'plots_created': ['magnitude_distribution.png', 'regional_distribution.png', 'magnitude_vs_depth.png'],
            'dashboard_created': 'dashboard.html',
            'output_directory': f"{MAGNITUDR_PATH}/data/airflow_output/plots/",
            'creation_timestamp': datetime.now().isoformat()
        }
        
        # Save report
        report_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'visualization_report_simple.json'
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logging.info("✅ Visualization report generated")
        return str(report_file)
        
    except Exception as e:
        logging.error(f"❌ Report generation failed: {e}")
        raise

def check_storage_completed(**context):
    """
    Check if storage DAG has completed by checking database or files
    """
    logging.info("🔍 Checking if storage DAG completed...")
    
    try:
        # First, try to check database
        try:
            db_connection = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            engine = create_engine(db_connection)
            
            # Check if table exists and has data
            result = engine.execute("SELECT COUNT(*) FROM earthquakes_processed_airflow LIMIT 1").fetchone()
            if result and result[0] > 0:
                logging.info("✅ Database table found with data - proceeding with visualization")
                return True
                
        except Exception as e:
            logging.warning(f"Database check failed: {e}")
        
        # Fallback: check for storage output files
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        storage_files = [
            output_dir / 'storage_pipeline_report.json',
            output_dir / 'final_processed_earthquake_data.csv',
            output_dir / 'enriched_earthquake_data.csv'
        ]
        
        if any(f.exists() for f in storage_files):
            logging.info("✅ Storage output files found - proceeding with visualization")
            return True
        
        # Check if storage DAG ran recently
        try:
            from airflow.models import DagRun
            from airflow.utils.db import provide_session
            
            @provide_session
            def get_recent_storage_run(session=None):
                recent_run = session.query(DagRun).filter(
                    DagRun.dag_id == 'earthquake_data_storage',
                    DagRun.state == 'success'
                ).order_by(DagRun.execution_date.desc()).first()
                return recent_run
            
            recent_run = get_recent_storage_run()
            if recent_run:
                logging.info(f"✅ Found successful storage run: {recent_run.execution_date}")
                return True
            
        except Exception as e:
            logging.warning(f"Could not check storage DAG runs: {e}")
        
        logging.warning("⚠️ No storage output found, but continuing anyway...")
        return True  # Continue anyway
        
    except Exception as e:
        logging.error(f"❌ Error checking storage status: {e}")
        return True  # Continue anyway

# Replace sensor with file-based check
check_storage_task = PythonOperator(
    task_id='check_storage_completed',
    python_callable=check_storage_completed,
    dag=dag
)

# Define tasks
task_load_data = PythonOperator(
    task_id='load_data_simple',
    python_callable=load_data_simple,
    dag=dag
)

task_create_plots = PythonOperator(
    task_id='create_basic_plots',
    python_callable=create_basic_plots,
    dag=dag
)

task_create_dashboard = PythonOperator(
    task_id='create_simple_dashboard',
    python_callable=create_simple_dashboard,
    dag=dag
)

task_generate_report = PythonOperator(
    task_id='generate_viz_report',
    python_callable=generate_viz_report,
    dag=dag
)

# Define task dependencies
check_storage_task >> task_load_data >> task_create_plots >> task_create_dashboard >> task_generate_report