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

# Menambahkan direktori root proyek ke path Python (CARA YANG BENAR)
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


def run_spark_usgs_ingestion(**context):
    """Step 1: Spark-Enhanced USGS Data Ingestion"""
    logging.info("🌍 Starting Spark-enhanced USGS data ingestion...")
    try:
        # Impor absolut dari direktori root
        from operators.spark_usgs_operator import SparkUSGSDataOperator

        spark_usgs_operator = SparkUSGSDataOperator(
            task_id='spark_usgs_ingestion',
            output_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
            start_year=2016,
            min_magnitude=1.0,
            target_size_mb=64.0,
            strict_validation=False
        )
        result = spark_usgs_operator.execute(context)
        logging.info(f"✅ Spark USGS ingestion completed: {result} records")
        return result
    except Exception as e:
        logging.error(f"❌ Spark USGS ingestion failed: {e}")
        raise


def run_spatial_processing(**context):
    """Step 2: Spatial Processing and Feature Engineering"""
    logging.info("Starting spatial processing...")
    try:
        # Impor absolut dari direktori root
        from operators.spatial_operator import SpatialDensityOperator

        spatial_operator = SpatialDensityOperator(
            task_id='spatial_processing',
            input_path='/opt/airflow/magnitudr/data/airflow_output/raw_earthquake_data.csv',
            output_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
            grid_size=0.1,
            enable_validation=True
        )
        result = spatial_operator.execute(context)
        logging.info(f"✅ Spatial processing completed: {result} records")
        return result
    except Exception as e:
        logging.error(f"❌ Spatial processing failed: {e}")
        raise


def load_to_database(**context):
    """Step 3: Load processed data to PostgreSQL (FIXED VERSION)"""
    logging.info("📊 Loading data to PostgreSQL...")
    try:
        input_path = '/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv'
        df = pd.read_csv(input_path)
        logging.info(f"📊 Read {len(df)} records from {input_path}")

        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce', utc=True) 
            
        db_columns = [
            'id', 'magnitude', 'latitude', 'longitude', 'depth', 'time',
            'place', 'spatial_density', 'hazard_score', 'region',
            'magnitude_category', 'depth_category', 'cluster_id', 'risk_zone'  
        ]

        if 'cluster_id' not in df.columns:
            df['cluster_id'] = None 
        if 'risk_zone' not in df.columns:
            df['risk_zone'] = 'Unknown' 

        available_cols = [col for col in db_columns if col in df.columns]
        df_clean = df[available_cols].copy()

        df_final = df_clean.replace({np.nan: None, pd.NaT: None})

        conn = None
        try:
            conn = psycopg2.connect(
                host="postgres", port="5432", dbname="magnitudr",
                user="postgres", password="earthquake123"
            )
            with conn.cursor() as cursor:
                logging.info("Clearing existing data from target tables...")
                cursor.execute("DELETE FROM earthquake_clusters CASCADE") 
                cursor.execute("DELETE FROM hazard_zones")
                cursor.execute("DELETE FROM earthquake_predictions CASCADE")
                cursor.execute("DELETE FROM ml_model_metadata")
                cursor.execute("DELETE FROM earthquakes_processed") 


                logging.info("✅ Cleared existing data")

                logging.info(f"Inserting {len(df_final)} records into earthquakes_processed...")
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
    """Step 4: Hybrid DBSCAN Clustering with ML Labeling"""
    logging.info("🔬 Starting Hybrid DBSCAN clustering with ML labeling...")
    try:
        from operators.dbscan_operator import DBSCANClusterOperator

        # Panggil operator dengan parameter yang benar dan lengkap
        dbscan_operator = DBSCANClusterOperator(
            task_id='hybrid_dbscan_clustering_logic',
            input_path='/opt/airflow/magnitudr/data/airflow_output/processed_earthquake_data.csv',
            db_connection='postgresql://postgres:earthquake123@postgres:5432/magnitudr',
            model_path='/opt/airflow/magnitudr/data/models/earthquake_model.pkl',
            scaler_path='/opt/airflow/magnitudr/data/models/earthquake_model_scaler.pkl',
            label_encoder_path='/opt/airflow/magnitudr/data/models/earthquake_model_label_encoder.pkl',
            eps=0.1,
            min_samples=5,
            max_rows_to_load=40000
        )

        result = dbscan_operator.execute(context)
        logging.info(f"✅ Hybrid DBSCAN clustering completed: {result} clusters with ML labeling")
        return result

    except Exception as e:
        logging.error(f"❌ Hybrid DBSCAN clustering failed: {e}", exc_info=True)
        raise

def run_data_visualization(**context):
    """Step 5: Data Visualization and EDA"""
    logging.info("📊 Starting data visualization...")
    conn = None 
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        import json
        import psycopg2 

        db_connection_str = 'postgresql://postgres:earthquake123@postgres:5432/magnitudr'
        conn = psycopg2.connect(db_connection_str) 

        earthquake_query = "SELECT * FROM earthquakes_processed LIMIT 10000"
        df_earthquakes = pd.read_sql(earthquake_query, conn)
        
        cluster_query = "SELECT * FROM earthquake_clusters"
        df_clusters = pd.read_sql(cluster_query, conn)

        hazard_query = "SELECT * FROM hazard_zones"
        df_hazard_zones = pd.read_sql(hazard_query, conn)

        viz_dir = '/opt/airflow/magnitudr/data/plots'
        os.makedirs(viz_dir, exist_ok=True)

        plt.style.use('dark_background')
        sns.set_palette("husl")

        # Plot 1: Magnitude Distribution (sama seperti sebelumnya)
        plt.figure(figsize=(10, 6))
        plt.hist(df_earthquakes['magnitude'], bins=30, alpha=0.7, color='orange')
        plt.title('Earthquake Magnitude Distribution (Spark-Processed)', fontsize=14)
        plt.xlabel('Magnitude')
        plt.ylabel('Frequency')
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/magnitude_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        # Plot 2: Risk Zone Distribution (Diagram Lingkaran)
        plt.figure(figsize=(8, 6))
        if not df_clusters.empty and 'risk_zone' in df_clusters.columns:
            risk_counts = df_clusters['risk_zone'].value_counts()
            if not risk_counts.empty:
                colors = {'High': '#E74C3C', 'Extreme': '#8E44AD', 'Medium': '#F39C12', 'Low': '#27AE60', 'Unknown': '#BDC3C7'}
                pie_colors = [colors.get(zone, '#BDC3C7') for zone in risk_counts.index]
                plt.pie(risk_counts.values, labels=risk_counts.index, autopct='%1.1f%%', colors=pie_colors, startangle=90)
                plt.title('Risk Zone Distribution (ML-Labeled)', fontsize=14)
            else:
                plt.text(0.5, 0.5, "No clusters with risk zones found.", horizontalalignment='center', verticalalignment='center', transform=plt.gca().transAxes, fontsize=12, color='gray')
                plt.title('Risk Zone Distribution (ML-Labeled) - No Data', fontsize=14)
        else:
            plt.text(0.5, 0.5, "No cluster data available for risk zone distribution.", horizontalalignment='center', verticalalignment='center', transform=plt.gca().transAxes, fontsize=12, color='gray')
            plt.title('Risk Zone Distribution (ML-Labeled) - No Data', fontsize=14)
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/risk_zone_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()


        # Plot 3: Earthquake Geographic Distribution (Scatter Plot Asli)
        plt.figure(figsize=(12, 8))
        scatter = plt.scatter(df_earthquakes['longitude'], df_earthquakes['latitude'], c=df_earthquakes['magnitude'],
                              cmap='Reds', alpha=0.6, s=df_earthquakes['magnitude']*10) # Ukuran titik berdasarkan magnitudo
        plt.colorbar(scatter, label='Magnitude')
        plt.title('Earthquake Geographic Distribution (Spark+ML Pipeline)', fontsize=14)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/geographic_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        # Plot 4 (BARU): Hazard Zone Map - Menyerupai Peta Risiko Streamlit
        plt.figure(figsize=(12, 8))
        if not df_hazard_zones.empty:
            plt.scatter(df_earthquakes['longitude'], df_earthquakes['latitude'], color='grey', alpha=0.1, s=1)

            if 'risk_level' in df_hazard_zones.columns and 'center_lon' in df_hazard_zones.columns and 'center_lat' in df_hazard_zones.columns:
                risk_colors = {
                    'Extreme': 'darkred',
                    'High': 'red',
                    'Medium': 'orange',
                    'Low': 'lightgreen',
                    'Unknown': 'gray'
                }
                scatter_hazard = plt.scatter(df_hazard_zones['center_lon'], df_hazard_zones['center_lat'],
                                            s=df_hazard_zones['event_count'].fillna(1)*5 if 'event_count' in df_hazard_zones.columns else 100,
                                            c=df_hazard_zones['risk_level'].map(risk_colors),
                                            alpha=0.8, edgecolors='black', linewidth=0.5,
                                            label='Hazard Zones')
                
                handles = [plt.Line2D([0], [0], marker='o', color='w', label=label, 
                                      markerfacecolor=risk_colors[label], markersize=10) 
                           for label in risk_colors if label in df_hazard_zones['risk_level'].unique()]
                plt.legend(handles=handles, title="Risk Level")
                
            plt.title('Hazard Zones (ML-Labeled) Geographic Distribution', fontsize=14)
            plt.xlabel('Longitude')
            plt.ylabel('Latitude')
            plt.grid(True, linestyle='--', alpha=0.7)
        else:
            plt.text(0.5, 0.5, "No hazard zone data available.", horizontalalignment='center', verticalalignment='center', transform=plt.gca().transAxes, fontsize=12, color='gray')
            plt.title('Hazard Zones - No Data', fontsize=14)
        plt.tight_layout()
        plt.savefig(f'{viz_dir}/hazard_zone_map.png', dpi=300, bbox_inches='tight')
        plt.close()


        logging.info(f"✅ Generated visualization plots in {viz_dir}")

        viz_summary = {
            'visualization_timestamp': datetime.now().isoformat(), 
            'plots_generated': 4,
            'earthquake_samples': len(df_earthquakes), 
            'cluster_samples': len(df_clusters),
            'hazard_zone_samples': len(df_hazard_zones), 
            'output_directory': viz_dir, 
            'processing_method': 'Spark + ML Hybrid',
            'plots': ['magnitude_distribution.png', 'risk_zone_distribution.png', 'geographic_distribution.png', 'hazard_zone_map.png'] # Tambahkan plot baru
        }

        with open(f'{viz_dir}/visualization_summary.json', 'w') as f:
            json.dump(viz_summary, f, indent=2)

        logging.info("✅ Data visualization completed")
        return len(viz_summary['plots'])
    except Exception as e:
        logging.error(f"❌ Data visualization failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close() 


def generate_master_report(**context):
    """Step 6: Generate master pipeline report with Spark and ML integration"""
    logging.info("📋 Generating master pipeline report...")
    try:
        from pathlib import Path
        import json

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
        cursor.execute(
            "SELECT risk_zone, COUNT(*) as count FROM earthquake_clusters GROUP BY risk_zone ORDER BY count DESC")
        risk_distribution = dict(cursor.fetchall())
        cursor.execute("SELECT COUNT(*) FROM ml_model_metadata")
        ml_models_trained = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        try:
            metrics_path = '/opt/airflow/magnitudr/data/airflow_output/model_comparison_report.json'
            if os.path.exists(metrics_path):
                with open(metrics_path, 'r') as f:
                    ml_metrics = json.load(f)
                best_model = ml_metrics.get('best_model', 'Unknown')
                best_model_metrics = ml_metrics.get('model_comparison', {}).get(best_model, {})
                report_ml_metrics = {
                    'best_model': best_model, 'accuracy': best_model_metrics.get('test_accuracy', 0),
                    'f1_score': best_model_metrics.get('f1_score', 0),
                    'precision': best_model_metrics.get('precision', 0),
                    'recall': best_model_metrics.get('recall', 0), 'models_trained': ml_models_trained
                }
            else:
                report_ml_metrics = {'status': 'model_not_found', 'models_trained': ml_models_trained}
        except Exception as e:
            report_ml_metrics = {'status': 'error', 'message': str(e)}

        report = {
            'pipeline_name': 'Magnitudr Master Production Pipeline with Spark + ML',
            'execution_timestamp': datetime.now().isoformat(), 'status': 'SUCCESS',
            'schedule': 'Daily at 02:00 WIB', 'architecture': 'Hybrid Spark + DBSCAN + ML',
            'data_statistics': {
                'total_earthquakes': total_earthquakes, 'total_clusters': total_clusters,
                'high_risk_zones': high_risk_zones, 'risk_distribution': risk_distribution,
                'data_coverage': '2016-2025 (9 years)', 'processing_method': 'Apache Spark + ML Classification'
            },
            'ml_model_performance': report_ml_metrics,
            'system_endpoints': {
                'api_docs': 'http://localhost:8000/docs', 'api_health': 'http://localhost:8000/health',
                'ml_comparison': 'http://localhost:8000/ml/model-comparison',
                'pipeline_status': 'http://localhost:8000/pipeline/status', 'dashboard': 'http://localhost:8501',
                'airflow': 'http://localhost:8080'
            },
            'production_ready': True, 'data_volume_compliant': total_earthquakes > 10000
        }

        report_path = Path("/opt/airflow/magnitudr/data/airflow_output/master_pipeline_report.json")
        report_path.parent.mkdir(parents=True, exist_ok=True)

        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        logging.info("🎉 MASTER PIPELINE WITH SPARK + ML COMPLETED!")
        logging.info(
            f"📊 Earthquakes: {total_earthquakes:,}, 🔬 Clusters: {total_clusters}, 🚨 High-risk zones: {high_risk_zones}")
        logging.info(f"🚀 Processed with: Apache Spark, 🤖 ML Models: {ml_models_trained} trained")
        logging.info(f"🎯 Dashboard: http://localhost:8501, 🤖 ML API: http://localhost:8000/ml/model-comparison")

        return str(report_path)
    except Exception as e:
        logging.error(f"❌ Master report failed: {e}")
        raise


# Define tasks
task_spark_ingestion = PythonOperator(task_id='spark_usgs_data_ingestion', python_callable=run_spark_usgs_ingestion,
                                      dag=dag)
task_processing = PythonOperator(task_id='spatial_processing', python_callable=run_spatial_processing, dag=dag)
task_database = PythonOperator(task_id='load_to_database', python_callable=load_to_database, dag=dag)
task_clustering = PythonOperator(task_id='hybrid_dbscan_clustering', python_callable=run_hybrid_dbscan_clustering,
                                dag=dag)
task_visualization = PythonOperator(task_id='data_visualization', python_callable=run_data_visualization, dag=dag)
task_report = PythonOperator(task_id='generate_master_report', python_callable=generate_master_report, dag=dag)

# Enhanced pipeline flow with Spark integration
task_spark_ingestion >> task_processing >> task_database >> task_clustering >> task_visualization >> task_report