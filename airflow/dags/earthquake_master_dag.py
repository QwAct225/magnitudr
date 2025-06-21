from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import logging
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import json
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append('/opt/airflow')

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
    description='🌍 Master earthquake analysis pipeline with Spark integration - production ready',
    schedule_interval='0 19 * * *',
    max_active_runs=1,
    tags=['earthquake', 'master', 'production', 'spark']
)

DB_CONNECTION_STRING = 'postgresql://postgres:earthquake123@postgres:5432/magnitudr'


def run_spark_usgs_ingestion(**context):
    logging.info("🌍 Starting Spark-enhanced USGS data ingestion...")
    try:
        from operators.spark_usgs_operator import SparkUSGSDataOperator
        spark_usgs_operator = SparkUSGSDataOperator(
            task_id='spark_usgs_ingestion',
            output_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
            start_year=2016, min_magnitude=1.0, target_size_mb=64.0, strict_validation=False
        )
        result = spark_usgs_operator.execute(context)
        logging.info(f"✅ Spark USGS ingestion completed: {result} records")
        return result
    except Exception as e:
        logging.error(f"❌ Spark USGS ingestion failed: {e}", exc_info=True)
        raise


def run_spatial_processing(**context):
    logging.info("Starting spatial processing...")
    try:
        from operators.spatial_operator import SpatialDensityOperator
        spatial_operator = SpatialDensityOperator(
            task_id='spatial_processing',
            input_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
            output_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
            grid_size=0.1, enable_validation=True
        )
        result = spatial_operator.execute(context)
        logging.info(f"✅ Spatial processing completed: {result} records")
        return result
    except Exception as e:
        logging.error(f"❌ Spatial processing failed: {e}", exc_info=True)
        raise


def load_to_database(**context):
    logging.info("📊 Loading data to PostgreSQL...")
    try:
        input_path = '/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv'
        df = pd.read_csv(input_path)
        logging.info(f"📊 Read {len(df)} records from {input_path}")
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')

        db_columns = [
            'id', 'magnitude', 'latitude', 'longitude', 'depth', 'time',
            'place', 'spatial_density', 'hazard_score', 'region',
            'magnitude_category', 'depth_category'
        ]
        available_cols = [col for col in db_columns if col in df.columns]
        df_clean = df[available_cols].copy()
        df_final = df_clean.replace({np.nan: None, pd.NaT: None})
        conn = None
        try:
            conn = psycopg2.connect(DB_CONNECTION_STRING)
            with conn.cursor() as cursor:
                logging.info("Clearing existing data from target tables...")
                cursor.execute("DELETE FROM earthquake_clusters")
                cursor.execute("DELETE FROM hazard_zones")
                cursor.execute("DELETE FROM earthquakes_processed")
                logging.info("✅ Cleared existing data")

                insert_query = f"INSERT INTO earthquakes_processed ({', '.join(df_final.columns)}) VALUES %s"
                data_tuples = [tuple(row) for row in df_final.itertuples(index=False)]
                psycopg2.extras.execute_values(cursor, insert_query, data_tuples, page_size=1000)
                conn.commit()
                logging.info(f"✅ Loaded {len(df_final)} records to database")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"❌ Database operation failed: {error}", exc_info=True)
            if conn: conn.rollback()
            raise
        finally:
            if conn: conn.close()
        return len(df_final)
    except Exception as e:
        logging.error(f"❌ Database loading failed: {e}", exc_info=True)
        raise


def run_hybrid_dbscan_clustering(**context):
    logging.info("🔬 Starting Hybrid DBSCAN clustering with ML labeling...")
    try:
        from operators.dbscan_operator import DBSCANClusterOperator
        dbscan_operator = DBSCANClusterOperator(
            task_id='hybrid_dbscan_clustering',
            input_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
            db_connection=DB_CONNECTION_STRING,
            model_path='/opt/airflow/magnitudr/data/models/earthquake_model.pkl',
            scaler_path='/opt/airflow/magnitudr/data/models/earthquake_model_scaler.pkl',
            label_encoder_path='/opt/airflow/magnitudr/data/models/earthquake_model_label_encoder.pkl',
            eps=0.1, min_samples=5
        )
        result = dbscan_operator.execute(context)
        logging.info(f"✅ Hybrid DBSCAN clustering completed: {result} clusters with ML labeling")
        return result
    except Exception as e:
        logging.error(f"❌ Hybrid DBSCAN clustering failed: {e}", exc_info=True)
        raise


def run_data_visualization(**context):
    logging.info("📊 Starting data visualization...")
    conn = None
    try:
        # PERBAIKAN: Menggunakan psycopg2 secara langsung
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        logging.info("✅ Database connection for visualization established using psycopg2.")

        earthquake_query = "SELECT * FROM earthquakes_processed LIMIT 10000"
        df_earthquakes = pd.read_sql(earthquake_query, conn)
        cluster_query = "SELECT * FROM earthquake_clusters"
        df_clusters = pd.read_sql(cluster_query, conn)

    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"❌ Data loading for visualization failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()

    try:
        viz_dir = Path('/opt/airflow/magnitudr/data/plots')
        viz_dir.mkdir(parents=True, exist_ok=True)

        plt.style.use('dark_background')
        sns.set_palette("husl")

        plt.figure(figsize=(10, 6))
        plt.hist(df_earthquakes['magnitude'], bins=30, alpha=0.7, color='orange')
        plt.title('Earthquake Magnitude Distribution', fontsize=14)
        plt.xlabel('Magnitude')
        plt.ylabel('Frequency')
        plt.tight_layout()
        plt.savefig(viz_dir / 'magnitude_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        if not df_clusters.empty and 'risk_zone' in df_clusters.columns:
            plt.figure(figsize=(8, 6))
            risk_counts = df_clusters['risk_zone'].value_counts()
            colors = ['#E74C3C', '#FF6B35', '#F39C12', '#27AE60']
            plt.pie(risk_counts.values, labels=risk_counts.index, autopct='%1.1f%%', colors=colors)
            plt.title('Risk Zone Distribution (ML-Labeled)', fontsize=14)
            plt.tight_layout()
            plt.savefig(viz_dir / 'risk_zone_distribution.png', dpi=300, bbox_inches='tight')
            plt.close()

        plt.figure(figsize=(12, 8))
        scatter = plt.scatter(df_earthquakes['longitude'], df_earthquakes['latitude'], c=df_earthquakes['magnitude'],
                              cmap='Reds', alpha=0.6)
        plt.colorbar(scatter, label='Magnitude')
        plt.title('Earthquake Geographic Distribution', fontsize=14)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.tight_layout()
        plt.savefig(viz_dir / 'geographic_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logging.info(f"✅ Generated 3 visualization plots in {viz_dir}")
        return str(viz_dir)

    except Exception as e:
        logging.error(f"❌ Data visualization failed: {e}", exc_info=True)
        raise


def generate_master_report(**context):
    logging.info("📋 Generating master pipeline report...")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM earthquakes_processed")
        total_earthquakes = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT cluster_id) FROM earthquake_clusters WHERE cluster_id IS NOT NULL")
        total_clusters = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM hazard_zones WHERE risk_level IN ('High', 'Extreme')")
        high_risk_zones = cursor.fetchone()[0]
        cursor.execute(
            "SELECT risk_zone, COUNT(*) as count FROM earthquake_clusters GROUP BY risk_zone ORDER BY count DESC")
        risk_distribution = dict(cursor.fetchall())
        cursor.execute("SELECT COUNT(*) FROM ml_model_metadata")
        ml_models_trained = cursor.fetchone()[0]

        cursor.close()

        report_path = Path("/opt/airflow/magnitudr/data/airflow_output/master_pipeline_report.json")
        report = {
            'pipeline_name': 'Magnitudr Master Production Pipeline',
            'execution_timestamp': datetime.now().isoformat(),
            'data_statistics': {'total_earthquakes': total_earthquakes, 'total_clusters': total_clusters},
        }

        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        logging.info("🎉 MASTER PIPELINE COMPLETED!")
        return str(report_path)
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"❌ Master report failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()


task_spark_ingestion = PythonOperator(task_id='spark_usgs_data_ingestion', python_callable=run_spark_usgs_ingestion,
                                      dag=dag)
task_processing = PythonOperator(task_id='spatial_processing', python_callable=run_spatial_processing, dag=dag)
task_database = PythonOperator(task_id='load_to_database', python_callable=load_to_database, dag=dag)
task_clustering = PythonOperator(task_id='hybrid_dbscan_clustering', python_callable=run_hybrid_dbscan_clustering,
                                 dag=dag)
task_visualization = PythonOperator(task_id='data_visualization', python_callable=run_data_visualization, dag=dag)
task_report = PythonOperator(task_id='generate_master_report', python_callable=generate_master_report, dag=dag)

task_spark_ingestion >> task_processing >> task_database >> task_clustering >> task_visualization >> task_report