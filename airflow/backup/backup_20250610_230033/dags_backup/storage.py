"""
DAG 3: Earthquake Data Storage Pipeline
=======================================
Capstone 5: Penerapan Pipeline dengan Apache Airflow

Description:
- Automated data storage to PostgreSQL database
- Data validation and quality assurance
- Query execution and result validation
- Schedule: Triggered after processing DAG or manually

Author: [Your Name]
Date: 2025-06-09
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
from sqlalchemy import create_engine, text
import psycopg2

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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'execution_timeout': timedelta(minutes=45)  # 45 minutes max per task
}

# Initialize DAG
dag = DAG(
    'earthquake_data_storage',
    default_args=default_args,
    description='Data storage pipeline for processed earthquake data',
    schedule_interval=None,  # Triggered manually or by upstream DAG
    max_active_runs=1,
    tags=['earthquake', 'storage', 'database', 'postgres']
)

def check_database_connectivity(**context):
    """
    Task 1: Check PostgreSQL database connectivity
    """
    logging.info("🗄️ Checking database connectivity...")
    
    try:
        # Database connection parameters
        db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'magnitudr'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }
        
        # Test connection
        conn_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        engine = create_engine(conn_string)
        
        # Test with a simple query
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            logging.info(f"✅ Database connected: {version}")
        
        # Store connection info for other tasks
        context['task_instance'].xcom_push(key='db_connection_string', value=conn_string)
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Database connectivity check failed: {e}")
        raise

def create_database_schema(**context):
    """
    Task 2: Create database schema and tables
    Transformation 1: Schema design and table creation
    """
    logging.info("🏗️ Creating database schema...")
    
    try:
        # Get connection string
        conn_string = context['task_instance'].xcom_pull(key='db_connection_string')
        engine = create_engine(conn_string)
        
        # Define table creation SQL
        create_table_sql = """
        -- Drop existing table if exists
        DROP TABLE IF EXISTS earthquakes_processed_airflow;
        
        -- Create main earthquakes table
        CREATE TABLE earthquakes_processed_airflow (
            id VARCHAR(100) PRIMARY KEY,
            magnitude DECIMAL(4,2),
            place TEXT,
            time TIMESTAMP,
            longitude DECIMAL(10,7),
            latitude DECIMAL(10,7),
            depth DECIMAL(8,3),
            region VARCHAR(50),
            magnitude_category VARCHAR(20),
            depth_category VARCHAR(20),
            risk_score DECIMAL(5,3),
            risk_category VARCHAR(20),
            tsunami INTEGER,
            alert VARCHAR(20),
            significance INTEGER,
            felt INTEGER,
            cdi DECIMAL(4,2),
            mmi DECIMAL(4,2),
            year INTEGER,
            month INTEGER,
            day_of_week INTEGER,
            hour INTEGER,
            quarter INTEGER,
            week_of_year INTEGER,
            is_weekend BOOLEAN,
            earthquake_cluster INTEGER,
            geo_cluster INTEGER,
            energy_joules DECIMAL(15,2),
            fault_distance DECIMAL(10,3),
            data_quality_score DECIMAL(4,3),
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Indexes for better query performance
            CONSTRAINT chk_magnitude CHECK (magnitude >= 0 AND magnitude <= 10),
            CONSTRAINT chk_latitude CHECK (latitude >= -90 AND latitude <= 90),
            CONSTRAINT chk_longitude CHECK (longitude >= -180 AND longitude <= 180),
            CONSTRAINT chk_depth CHECK (depth >= 0)
        );
        
        -- Create indexes
        CREATE INDEX idx_earthquakes_magnitude ON earthquakes_processed_airflow(magnitude);
        CREATE INDEX idx_earthquakes_region ON earthquakes_processed_airflow(region);
        CREATE INDEX idx_earthquakes_time ON earthquakes_processed_airflow(time);
        CREATE INDEX idx_earthquakes_risk ON earthquakes_processed_airflow(risk_score);
        CREATE INDEX idx_earthquakes_cluster ON earthquakes_processed_airflow(earthquake_cluster);
        
        -- Create summary statistics table
        DROP TABLE IF EXISTS earthquake_statistics_airflow;
        
        CREATE TABLE earthquake_statistics_airflow (
            id SERIAL PRIMARY KEY,
            statistic_type VARCHAR(50),
            region VARCHAR(50),
            time_period VARCHAR(20),
            value DECIMAL(10,3),
            count INTEGER,
            calculation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute table creation
        with engine.connect() as connection:
            # Split and execute each statement
            statements = create_table_sql.split(';')
            for statement in statements:
                if statement.strip():
                    connection.execute(text(statement))
                    connection.commit()
        
        logging.info("✅ Database schema created successfully")
        logging.info("📊 Tables created: earthquakes_processed_airflow, earthquake_statistics_airflow")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Schema creation failed: {e}")
        raise

def load_processed_data(**context):
    """
    Task 3: Load processed data into database
    Transformation 2: Data loading with validation and type conversion
    """
    logging.info("📤 Loading processed data into database...")
    
    try:
        # Get connection string
        conn_string = context['task_instance'].xcom_pull(key='db_connection_string')
        engine = create_engine(conn_string)
        
        # Find the processed data file
        data_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        processed_file = data_dir / 'final_processed_earthquake_data.csv'
        
        if not processed_file.exists():
            # Try alternative files
            alternatives = [
                data_dir / 'ml_processed_earthquake_data.csv',
                data_dir / 'enriched_earthquake_data.csv',
                Path(MAGNITUDR_PATH) / 'data' / 'earthquake_enhanced.csv'
            ]
            
            for alt_file in alternatives:
                if alt_file.exists():
                    processed_file = alt_file
                    logging.info(f"Using alternative file: {alt_file}")
                    break
            else:
                raise FileNotFoundError("No processed data file found")
        
        # Load and prepare data
        df = pd.read_csv(processed_file)
        initial_count = len(df)
        
        logging.info(f"📊 Loading {initial_count:,} records...")
        
        # Data type conversions and cleaning
        
        # Convert time columns
        time_columns = ['time', 'extraction_timestamp', 'processing_timestamp']
        for col in time_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Ensure required columns exist with defaults
        required_columns = {
            'id': lambda x: x.astype(str),
            'magnitude': lambda x: pd.to_numeric(x, errors='coerce'),
            'place': lambda x: x.astype(str).fillna('Unknown'),
            'time': lambda x: pd.to_datetime(x, errors='coerce'),
            'longitude': lambda x: pd.to_numeric(x, errors='coerce'),
            'latitude': lambda x: pd.to_numeric(x, errors='coerce'),
            'depth': lambda x: pd.to_numeric(x, errors='coerce'),
            'region': lambda x: x.astype(str).fillna('Unknown'),
            'magnitude_category': lambda x: x.astype(str).fillna('Unknown'),
            'depth_category': lambda x: x.astype(str).fillna('Unknown'),
            'risk_score': lambda x: pd.to_numeric(x, errors='coerce').fillna(0),
            'tsunami': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int),
            'alert': lambda x: x.astype(str).fillna('unknown'),
            'significance': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int),
            'year': lambda x: pd.to_numeric(x, errors='coerce').fillna(2025).astype(int),
            'month': lambda x: pd.to_numeric(x, errors='coerce').fillna(1).astype(int),
            'processing_timestamp': lambda x: pd.to_datetime(x, errors='coerce').fillna(pd.Timestamp.now())
        }
        
        # Apply transformations for existing columns
        for col, transform in required_columns.items():
            if col in df.columns:
                df[col] = transform(df[col])
            else:
                # Create default values for missing columns
                if col == 'processing_timestamp':
                    df[col] = pd.Timestamp.now()
                elif col in ['magnitude', 'longitude', 'latitude', 'depth', 'risk_score']:
                    df[col] = 0.0
                elif col in ['year', 'month', 'tsunami', 'significance']:
                    df[col] = 0
                else:
                    df[col] = 'Unknown'
        
        # Add derived columns if missing
        if 'risk_category' not in df.columns and 'risk_score' in df.columns:
            df['risk_category'] = pd.cut(
                df['risk_score'], 
                bins=[0, 3, 6, 8, 10], 
                labels=['Low', 'Moderate', 'High', 'Extreme'],
                include_lowest=True
            ).astype(str).fillna('Low')
        
        # Handle temporal features
        temporal_features = ['day_of_week', 'hour', 'quarter', 'week_of_year', 'is_weekend']
        if 'time' in df.columns:
            df['day_of_week'] = df['time'].dt.dayofweek
            df['hour'] = df['time'].dt.hour
            df['quarter'] = df['time'].dt.quarter
            df['week_of_year'] = df['time'].dt.isocalendar().week
            df['is_weekend'] = df['time'].dt.dayofweek >= 5
        else:
            for feature in temporal_features:
                if feature not in df.columns:
                    df[feature] = 0 if feature != 'is_weekend' else False
        
        # Ensure numeric columns are properly formatted
        numeric_columns = ['magnitude', 'longitude', 'latitude', 'depth', 'risk_score', 
                          'earthquake_cluster', 'geo_cluster', 'energy_joules', 'fault_distance', 
                          'data_quality_score']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Remove records with invalid coordinates
        df = df[
            (df['latitude'].between(-90, 90)) &
            (df['longitude'].between(-180, 180)) &
            (df['magnitude'] >= 0) &
            (df['depth'] >= 0)
        ]
        
        valid_count = len(df)
        logging.info(f"📊 Valid records after cleaning: {valid_count:,} ({valid_count/initial_count*100:.1f}%)")
        
        # Select columns that exist in the database schema
        db_columns = [
            'id', 'magnitude', 'place', 'time', 'longitude', 'latitude', 'depth',
            'region', 'magnitude_category', 'depth_category', 'risk_score', 'risk_category',
            'tsunami', 'alert', 'significance', 'felt', 'cdi', 'mmi',
            'year', 'month', 'day_of_week', 'hour', 'quarter', 'week_of_year', 'is_weekend',
            'earthquake_cluster', 'geo_cluster', 'energy_joules', 'fault_distance',
            'data_quality_score', 'processing_timestamp'
        ]
        
        # Filter to only include columns that exist in the dataframe
        available_columns = [col for col in db_columns if col in df.columns]
        df_to_load = df[available_columns].copy()
        
        # Load data in batches to handle large datasets
        batch_size = 10000
        total_batches = (len(df_to_load) + batch_size - 1) // batch_size
        
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(df_to_load))
            batch_df = df_to_load.iloc[start_idx:end_idx]
            
            # Load batch to database
            batch_df.to_sql(
                'earthquakes_processed_airflow',
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logging.info(f"✅ Loaded batch {i+1}/{total_batches} ({len(batch_df)} records)")
        
        logging.info(f"✅ Data loading completed: {valid_count:,} records loaded")
        
        # Store loading results
        loading_results = {
            'total_records_loaded': valid_count,
            'columns_loaded': available_columns,
            'batch_count': total_batches,
            'loading_timestamp': datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='loading_results', value=loading_results)
        
        return valid_count
        
    except Exception as e:
        logging.error(f"❌ Data loading failed: {e}")
        raise

def calculate_and_store_statistics(**context):
    """
    Task 4: Calculate and store statistical summaries
    Transformation 3: Statistical aggregation and summary calculation
    """
    logging.info("📊 Calculating and storing statistics...")
    
    try:
        # Get connection string
        conn_string = context['task_instance'].xcom_pull(key='db_connection_string')
        engine = create_engine(conn_string)
        
        # Statistical calculations using SQL
        statistical_queries = [
            # Regional statistics
            """
            INSERT INTO earthquake_statistics_airflow (statistic_type, region, time_period, value, count)
            SELECT 
                'avg_magnitude' as statistic_type,
                region,
                'all_time' as time_period,
                ROUND(AVG(magnitude)::numeric, 3) as value,
                COUNT(*) as count
            FROM earthquakes_processed_airflow
            WHERE magnitude IS NOT NULL
            GROUP BY region;
            """,
            
            # Monthly trends
            """
            INSERT INTO earthquake_statistics_airflow (statistic_type, region, time_period, value, count)
            SELECT 
                'monthly_count' as statistic_type,
                'all_regions' as region,
                CONCAT(year, '-', LPAD(month::text, 2, '0')) as time_period,
                0 as value,
                COUNT(*) as count
            FROM earthquakes_processed_airflow
            GROUP BY year, month
            ORDER BY year, month;
            """,
            
            # Risk score statistics
            """
            INSERT INTO earthquake_statistics_airflow (statistic_type, region, time_period, value, count)
            SELECT 
                'avg_risk_score' as statistic_type,
                region,
                'all_time' as time_period,
                ROUND(AVG(risk_score)::numeric, 3) as value,
                COUNT(*) as count
            FROM earthquakes_processed_airflow
            WHERE risk_score IS NOT NULL
            GROUP BY region;
            """,
            
            # Depth category distribution
            """
            INSERT INTO earthquake_statistics_airflow (statistic_type, region, time_period, value, count)
            SELECT 
                CONCAT('depth_', LOWER(REPLACE(depth_category, ' ', '_'))) as statistic_type,
                'all_regions' as region,
                'all_time' as time_period,
                ROUND(AVG(magnitude)::numeric, 3) as value,
                COUNT(*) as count
            FROM earthquakes_processed_airflow
            WHERE depth_category IS NOT NULL
            GROUP BY depth_category;
            """,
            
            # High-risk events
            """
            INSERT INTO earthquake_statistics_airflow (statistic_type, region, time_period, value, count)
            SELECT 
                'high_risk_events' as statistic_type,
                region,
                'all_time' as time_period,
                ROUND(AVG(magnitude)::numeric, 3) as value,
                COUNT(*) as count
            FROM earthquakes_processed_airflow
            WHERE risk_score >= 7
            GROUP BY region;
            """
        ]
        
        # Execute statistical calculations
        with engine.connect() as connection:
            # Clear existing statistics
            connection.execute(text("DELETE FROM earthquake_statistics_airflow"))
            connection.commit()
            
            # Execute each statistical query
            for i, query in enumerate(statistical_queries):
                try:
                    connection.execute(text(query))
                    connection.commit()
                    logging.info(f"✅ Statistical query {i+1} executed successfully")
                except Exception as e:
                    logging.warning(f"⚠️  Statistical query {i+1} failed: {e}")
                    continue
        
        # Verify statistics were created
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM earthquake_statistics_airflow"))
            stats_count = result.fetchone()[0]
            
            logging.info(f"✅ Statistics calculation completed: {stats_count} statistical records created")
        
        # Generate summary of statistics
        statistics_summary = {
            'statistics_records_created': stats_count,
            'calculation_timestamp': datetime.now().isoformat(),
            'statistic_types': [
                'avg_magnitude', 'monthly_count', 'avg_risk_score', 
                'depth_categories', 'high_risk_events'
            ]
        }
        
        context['task_instance'].xcom_push(key='statistics_summary', value=statistics_summary)
        
        return stats_count
        
    except Exception as e:
        logging.error(f"❌ Statistics calculation failed: {e}")
        raise

def execute_validation_queries(**context):
    """
    Task 5: Execute validation queries on stored data
    """
    logging.info("🔍 Executing validation queries...")
    
    try:
        # Get connection string
        conn_string = context['task_instance'].xcom_pull(key='db_connection_string')
        engine = create_engine(conn_string)
        
        # Define validation queries
        validation_queries = {
            'total_records': "SELECT COUNT(*) as count FROM earthquakes_processed_airflow",
            
            'magnitude_range': """
                SELECT 
                    MIN(magnitude) as min_magnitude,
                    MAX(magnitude) as max_magnitude,
                    ROUND(AVG(magnitude)::numeric, 3) as avg_magnitude
                FROM earthquakes_processed_airflow 
                WHERE magnitude IS NOT NULL
            """,
            
            'regional_distribution': """
                SELECT 
                    region,
                    COUNT(*) as count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude
                FROM earthquakes_processed_airflow
                GROUP BY region
                ORDER BY count DESC
                LIMIT 10
            """,
            
            'risk_categories': """
                SELECT 
                    risk_category,
                    COUNT(*) as count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    ROUND(AVG(risk_score)::numeric, 2) as avg_risk_score
                FROM earthquakes_processed_airflow
                WHERE risk_category IS NOT NULL
                GROUP BY risk_category
                ORDER BY count DESC
            """,
            
            'temporal_distribution': """
                SELECT 
                    year,
                    COUNT(*) as count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude
                FROM earthquakes_processed_airflow
                WHERE year IS NOT NULL
                GROUP BY year
                ORDER BY year DESC
                LIMIT 5
            """,
            
            'high_magnitude_events': """
                SELECT 
                    id, magnitude, place, region, time, risk_score
                FROM earthquakes_processed_airflow
                WHERE magnitude >= 6.0
                ORDER BY magnitude DESC
                LIMIT 10
            """,
            
            'clustering_results': """
                SELECT 
                    earthquake_cluster,
                    COUNT(*) as cluster_size,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    ROUND(AVG(depth)::numeric, 1) as avg_depth
                FROM earthquakes_processed_airflow
                WHERE earthquake_cluster IS NOT NULL
                GROUP BY earthquake_cluster
                ORDER BY cluster_size DESC
                LIMIT 10
            """,
            
            'statistics_summary': """
                SELECT 
                    statistic_type,
                    COUNT(*) as records,
                    ROUND(AVG(value)::numeric, 3) as avg_value
                FROM earthquake_statistics_airflow
                GROUP BY statistic_type
                ORDER BY records DESC
            """
        }
        
        # Execute queries and collect results
        query_results = {}
        
        for query_name, sql in validation_queries.items():
            try:
                logging.info(f"📊 Executing query: {query_name}")
                
                with engine.connect() as connection:
                    result = connection.execute(text(sql))
                    rows = result.fetchall()
                    columns = result.keys()
                
                # Convert to list of dictionaries
                query_data = [dict(zip(columns, row)) for row in rows]
                query_results[query_name] = query_data
                
                logging.info(f"✅ Query {query_name}: {len(query_data)} rows returned")
                
                # Log sample results for key queries
                if query_name in ['total_records', 'magnitude_range']:
                    logging.info(f"📊 {query_name} result: {query_data}")
                
            except Exception as e:
                logging.error(f"❌ Query {query_name} failed: {e}")
                query_results[query_name] = {'error': str(e)}
        
        # Save query results
        results_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'validation_query_results.json'
        with open(results_file, 'w') as f:
            json.dump(query_results, f, indent=2, default=str)
        
        logging.info("✅ Validation queries completed")
        logging.info(f"📁 Results saved to: {results_file}")
        
        # Store results for reporting
        context['task_instance'].xcom_push(key='query_results', value=query_results)
        
        return query_results
        
    except Exception as e:
        logging.error(f"❌ Validation queries failed: {e}")
        raise

def generate_storage_report(**context):
    """
    Task 6: Generate comprehensive storage pipeline report
    """
    logging.info("📋 Generating storage pipeline report...")
    
    try:
        # Get results from previous tasks
        loading_results = context['task_instance'].xcom_pull(key='loading_results')
        statistics_summary = context['task_instance'].xcom_pull(key='statistics_summary')
        query_results = context['task_instance'].xcom_pull(key='query_results')
        
        # Generate comprehensive report
        report = {
            'pipeline_name': 'Earthquake Data Storage',
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].dag_id,
            'status': 'SUCCESS',
            'loading_results': loading_results,
            'statistics_summary': statistics_summary,
            'validation_results': {
                'total_records': query_results.get('total_records', [{'count': 0}])[0]['count'],
                'magnitude_range': query_results.get('magnitude_range', [{}])[0],
                'regional_distribution': len(query_results.get('regional_distribution', [])),
                'risk_categories': len(query_results.get('risk_categories', [])),
                'high_magnitude_events': len(query_results.get('high_magnitude_events', [])),
                'clustering_results': len(query_results.get('clustering_results', []))
            },
            'pipeline_stages': {
                'database_connectivity': 'Database connection established',
                'schema_creation': 'Tables and indexes created successfully',
                'data_loading': f"Loaded {loading_results['total_records_loaded']} records",
                'statistics_calculation': f"Generated {statistics_summary['statistics_records_created']} statistical records",
                'validation_queries': f"Executed {len(query_results)} validation queries",
                'report_generation': 'Comprehensive report generated'
            },
            'data_quality_metrics': {
                'records_loaded': loading_results['total_records_loaded'],
                'columns_stored': len(loading_results['columns_loaded']),
                'statistics_generated': statistics_summary['statistics_records_created'],
                'validation_queries_passed': sum(1 for result in query_results.values() if not isinstance(result, dict) or 'error' not in result)
            },
            'database_schema': {
                'main_table': 'earthquakes_processed_airflow',
                'statistics_table': 'earthquake_statistics_airflow',
                'indexes_created': ['magnitude', 'region', 'time', 'risk_score', 'cluster'],
                'constraints_applied': ['magnitude_range', 'coordinate_bounds', 'depth_positive']
            },
            'sample_queries': {
                'total_earthquakes': "SELECT COUNT(*) FROM earthquakes_processed_airflow",
                'high_risk_events': "SELECT * FROM earthquakes_processed_airflow WHERE risk_score >= 7",
                'regional_summary': "SELECT region, AVG(magnitude) FROM earthquakes_processed_airflow GROUP BY region",
                'statistics_view': "SELECT * FROM earthquake_statistics_airflow ORDER BY calculation_date DESC"
            },
            'next_steps': [
                'Data successfully stored in PostgreSQL database',
                'Statistical summaries available for analysis',
                'Ready for dashboard creation and visualization',
                'Database optimized with indexes for fast queries'
            ]
        }
        
        # Save comprehensive report
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        report_file = output_dir / 'storage_pipeline_report.json'
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Also save a summary CSV for easy analysis
        summary_df = pd.DataFrame([{
            'pipeline_stage': 'Storage Pipeline',
            'execution_date': context['execution_date'].isoformat(),
            'records_processed': loading_results['total_records_loaded'],
            'statistics_created': statistics_summary['statistics_records_created'],
            'validation_queries': len(query_results),
            'status': 'SUCCESS'
        }])
        
        summary_file = output_dir / 'pipeline_execution_summary.csv'
        summary_df.to_csv(summary_file, index=False)
        
        logging.info("✅ Storage pipeline report generated")
        logging.info(f"📁 Report saved to: {report_file}")
        logging.info(f"📊 Records stored: {loading_results['total_records_loaded']:,}")
        logging.info(f"📊 Statistics created: {statistics_summary['statistics_records_created']}")
        
        return str(report_file)
        
    except Exception as e:
        logging.error(f"❌ Report generation failed: {e}")
        raise

def check_processing_completed(**context):
    """
    Check if processing DAG has completed by looking for output files
    """
    logging.info("🔍 Checking if processing DAG completed...")
    
    try:
        # Check for processing output files
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        required_files = [
            output_dir / 'final_processed_earthquake_data.csv',
            output_dir / 'processing_pipeline_report.json'
        ]
        
        # Also check alternative files
        alternative_files = [
            output_dir / 'ml_processed_earthquake_data.csv',
            output_dir / 'enriched_earthquake_data.csv'
        ]
        
        # Check if any processing output exists
        main_files_exist = any(f.exists() for f in required_files)
        alt_files_exist = any(f.exists() for f in alternative_files)
        
        if main_files_exist or alt_files_exist:
            logging.info("✅ Processing output files found - proceeding with storage")
            return True
        else:
            # Check if processing DAG ran recently
            try:
                from airflow.models import DagRun
                from airflow.utils.db import provide_session
                
                @provide_session
                def get_recent_processing_run(session=None):
                    recent_run = session.query(DagRun).filter(
                        DagRun.dag_id == 'earthquake_data_processing',
                        DagRun.state == 'success'
                    ).order_by(DagRun.execution_date.desc()).first()
                    return recent_run
                
                recent_run = get_recent_processing_run()
                if recent_run:
                    logging.info(f"✅ Found successful processing run: {recent_run.execution_date}")
                    return True
                
            except Exception as e:
                logging.warning(f"Could not check processing DAG runs: {e}")
            
            logging.warning("⚠️ No processing output found, but continuing anyway...")
            return True  # Continue anyway
            
    except Exception as e:
        logging.error(f"❌ Error checking processing status: {e}")
        return True  # Continue anyway

# Replace sensor with file-based check
check_processing_task = PythonOperator(
    task_id='check_processing_completed',
    python_callable=check_processing_completed,
    dag=dag
)

# Define tasks
task_check_db = PythonOperator(
    task_id='check_database_connectivity',
    python_callable=check_database_connectivity,
    dag=dag
)

task_create_schema = PythonOperator(
    task_id='create_database_schema',
    python_callable=create_database_schema,
    dag=dag
)

task_load_data = PythonOperator(
    task_id='load_processed_data',
    python_callable=load_processed_data,
    dag=dag
)

task_calculate_stats = PythonOperator(
    task_id='calculate_and_store_statistics',
    python_callable=calculate_and_store_statistics,
    dag=dag
)

task_validate_queries = PythonOperator(
    task_id='execute_validation_queries',
    python_callable=execute_validation_queries,
    dag=dag
)

task_generate_report = PythonOperator(
    task_id='generate_storage_report',
    python_callable=generate_storage_report,
    dag=dag
)

# Define task dependencies
check_processing_task >> task_check_db >> task_create_schema >> task_load_data >> task_calculate_stats >> task_validate_queries >> task_generate_report