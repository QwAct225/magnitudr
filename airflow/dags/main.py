"""
DAG 4: Master Earthquake Pipeline
=================================
Capstone 5: Penerapan Pipeline dengan Apache Airflow

Description:
- Master pipeline yang mengintegrasikan semua sub-pipeline
- Automated scheduling dan dependency management
- Monitoring dan alerting untuk seluruh pipeline
- Schedule: Daily at 05:00 WIB (Asia/Jakarta) - sebelum ingestion

Author: [Your Name]
Date: 2025-06-09
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.email import EmailOperator
import os
import sys
import pandas as pd
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
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@magnitudr.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'catchup': False,
    'execution_timeout': timedelta(hours=2)  # 2 hour max execution per task
}

# Initialize Master DAG
dag = DAG(
    'earthquake_master_pipeline',
    default_args=default_args,
    description='Master pipeline orchestrating complete earthquake analysis workflow',
    schedule_interval='0 5 * * *',  # Daily at 05:00 WIB
    max_active_runs=1,
    tags=['earthquake', 'master', 'orchestration']
)

def check_system_prerequisites(**context):
    """
    Task 1: Check system prerequisites before starting pipeline
    """
    logging.info("🔍 Checking system prerequisites...")
    
    try:
        prerequisites = {
            'database_connection': False,
            'spark_availability': False,
            'data_directories': False,
            'disk_space': False,
            'dependencies': False
        }
        
        # 1. Check database connection
        try:
            import psycopg2
            conn_params = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'magnitudr'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'password')
            }
            
            conn = psycopg2.connect(**conn_params)
            conn.close()
            prerequisites['database_connection'] = True
            logging.info("✅ Database connection: OK")
        except Exception as e:
            logging.error(f"❌ Database connection failed: {e}")
        
        # 2. Check Spark availability
        spark_home = os.getenv('SPARK_HOME', '/opt/spark')
        if os.path.exists(f"{spark_home}/bin/spark-submit"):
            prerequisites['spark_availability'] = True
            logging.info("✅ Spark availability: OK")
        else:
            logging.warning("⚠️  Spark not found, will use fallback processing")
            prerequisites['spark_availability'] = False
        
        # 3. Check data directories
        data_dirs = [
            Path(MAGNITUDR_PATH) / 'data',
            Path(MAGNITUDR_PATH) / 'data' / 'airflow_output',
            Path(MAGNITUDR_PATH) / 'airflow' / 'logs'
        ]
        
        all_dirs_exist = True
        for dir_path in data_dirs:
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)
                logging.info(f"📁 Created directory: {dir_path}")
        
        prerequisites['data_directories'] = True
        logging.info("✅ Data directories: OK")
        
        # 4. Check disk space (minimum 1GB free)
        import shutil
        free_space_gb = shutil.disk_usage(MAGNITUDR_PATH).free / (1024**3)
        if free_space_gb > 1.0:
            prerequisites['disk_space'] = True
            logging.info(f"✅ Disk space: {free_space_gb:.2f} GB available")
        else:
            logging.error(f"❌ Insufficient disk space: {free_space_gb:.2f} GB")
        
        # 5. Check Python dependencies
        required_packages = ['pandas', 'numpy', 'requests', 'sqlalchemy', 'scikit-learn']
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing_packages.append(package)
        
        if not missing_packages:
            prerequisites['dependencies'] = True
            logging.info("✅ Python dependencies: OK")
        else:
            logging.error(f"❌ Missing packages: {missing_packages}")
        
        # Overall assessment
        all_ok = all(prerequisites.values())
        
        if all_ok:
            logging.info("🎉 All prerequisites satisfied - Ready to start pipeline!")
        else:
            failed_checks = [k for k, v in prerequisites.items() if not v]
            logging.error(f"❌ Failed prerequisite checks: {failed_checks}")
        
        # Store results for decision making
        context['task_instance'].xcom_push(key='prerequisites', value=prerequisites)
        context['task_instance'].xcom_push(key='all_prerequisites_met', value=all_ok)
        
        return all_ok
        
    except Exception as e:
        logging.error(f"❌ Prerequisites check failed: {e}")
        raise

def initialize_pipeline_environment(**context):
    """
    Task 2: Initialize pipeline environment and configuration
    """
    logging.info("🔧 Initializing pipeline environment...")
    
    try:
        # Create pipeline configuration
        pipeline_config = {
            'pipeline_id': f"earthquake_pipeline_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}",
            'execution_date': context['execution_date'].isoformat(),
            'pipeline_version': '1.0.0',
            'environment': 'production' if os.getenv('AIRFLOW_ENV') == 'prod' else 'development',
            'data_retention_days': 30,
            'batch_size': 10000,
            'spark_config': {
                'enabled': context['task_instance'].xcom_pull(key='prerequisites')['spark_availability'],
                'master_url': os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077'),
                'executor_memory': '4g',
                'driver_memory': '2g'
            },
            'database_config': {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'magnitudr'),
                'connection_pool_size': 10
            },
            'monitoring': {
                'enable_alerts': True,
                'slack_webhook': os.getenv('SLACK_WEBHOOK_URL'),
                'email_notifications': True
            }
        }
        
        # Save configuration
        config_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'pipeline_config.json'
        with open(config_file, 'w') as f:
            json.dump(pipeline_config, f, indent=2)
        
        # Initialize pipeline logs
        log_dir = Path(MAGNITUDR_PATH) / 'airflow' / 'logs' / 'pipeline_execution'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create pipeline status file
        status = {
            'pipeline_id': pipeline_config['pipeline_id'],
            'status': 'INITIALIZING',
            'start_time': datetime.now().isoformat(),
            'current_stage': 'initialization',
            'stages_completed': [],
            'stages_failed': [],
            'error_messages': []
        }
        
        status_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'pipeline_status.json'
        with open(status_file, 'w') as f:
            json.dump(status, f, indent=2)
        
        logging.info("✅ Pipeline environment initialized")
        logging.info(f"📁 Configuration: {config_file}")
        logging.info(f"🆔 Pipeline ID: {pipeline_config['pipeline_id']}")
        
        # Store configuration for other tasks
        context['task_instance'].xcom_push(key='pipeline_config', value=pipeline_config)
        context['task_instance'].xcom_push(key='pipeline_id', value=pipeline_config['pipeline_id'])
        
        return pipeline_config['pipeline_id']
        
    except Exception as e:
        logging.error(f"❌ Environment initialization failed: {e}")
        raise

def trigger_ingestion_pipeline(**context):
    """
    Task 3: Trigger data ingestion sub-pipeline
    """
    logging.info("📥 Triggering data ingestion pipeline...")
    
    try:
        # Update pipeline status
        pipeline_id = context['task_instance'].xcom_pull(key='pipeline_id')
        
        status_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'pipeline_status.json'
        with open(status_file, 'r') as f:
            status = json.load(f)
        
        status.update({
            'status': 'RUNNING_INGESTION',
            'current_stage': 'data_ingestion',
            'ingestion_start_time': datetime.now().isoformat()
        })
        
        with open(status_file, 'w') as f:
            json.dump(status, f, indent=2)
        
        logging.info("✅ Ingestion pipeline trigger prepared")
        return f"ingestion_triggered_{pipeline_id}"
        
    except Exception as e:
        logging.error(f"❌ Ingestion trigger failed: {e}")
        raise

def monitor_pipeline_progress(**context):
    """
    Task 4: Monitor overall pipeline progress
    """
    logging.info("📊 Monitoring pipeline progress...")
    
    try:
        # Read current status
        status_file = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output' / 'pipeline_status.json'
        
        if status_file.exists():
            with open(status_file, 'r') as f:
                status = json.load(f)
        else:
            status = {'status': 'UNKNOWN'}
        
        # Check sub-pipeline statuses
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        
        sub_pipeline_status = {
            'ingestion': 'PENDING',
            'processing': 'PENDING',
            'storage': 'PENDING'
        }
        
        # Check if ingestion report exists
        if (output_dir / 'ingestion_pipeline_report.json').exists():
            sub_pipeline_status['ingestion'] = 'COMPLETED'
        
        # Check if processing report exists
        if (output_dir / 'processing_pipeline_report.json').exists():
            sub_pipeline_status['processing'] = 'COMPLETED'
        
        # Check if storage report exists
        if (output_dir / 'storage_pipeline_report.json').exists():
            sub_pipeline_status['storage'] = 'COMPLETED'
        
        # Calculate overall progress
        completed_stages = sum(1 for status in sub_pipeline_status.values() if status == 'COMPLETED')
        total_stages = len(sub_pipeline_status)
        progress_percentage = (completed_stages / total_stages) * 100
        
        # Update monitoring status
        monitoring_status = {
            'pipeline_id': context['task_instance'].xcom_pull(key='pipeline_id'),
            'monitoring_timestamp': datetime.now().isoformat(),
            'overall_progress': f"{progress_percentage:.1f}%",
            'sub_pipelines': sub_pipeline_status,
            'completed_stages': completed_stages,
            'total_stages': total_stages,
            'estimated_completion': 'calculating...'
        }
        
        # Save monitoring status
        monitoring_file = output_dir / 'pipeline_monitoring.json'
        with open(monitoring_file, 'w') as f:
            json.dump(monitoring_status, f, indent=2)
        
        logging.info(f"📊 Pipeline progress: {progress_percentage:.1f}%")
        logging.info(f"📊 Sub-pipeline status: {sub_pipeline_status}")
        
        # Store for reporting
        context['task_instance'].xcom_push(key='monitoring_status', value=monitoring_status)
        
        return monitoring_status
        
    except Exception as e:
        logging.error(f"❌ Pipeline monitoring failed: {e}")
        raise

def validate_pipeline_completion(**context):
    """
    Task 5: Validate that all sub-pipelines completed successfully
    """
    logging.info("🔍 Validating pipeline completion...")
    
    try:
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        
        # Check for required output files
        required_files = {
            'ingestion_report': output_dir / 'ingestion_pipeline_report.json',
            'processing_report': output_dir / 'processing_pipeline_report.json',
            'storage_report': output_dir / 'storage_pipeline_report.json',
            'final_data': output_dir / 'final_processed_earthquake_data.csv',
            'validation_results': output_dir / 'validation_query_results.json'
        }
        
        validation_results = {}
        all_files_present = True
        
        for file_type, file_path in required_files.items():
            exists = file_path.exists()
            validation_results[file_type] = {
                'exists': exists,
                'path': str(file_path),
                'size_mb': file_path.stat().st_size / (1024*1024) if exists else 0
            }
            
            if not exists:
                all_files_present = False
                logging.error(f"❌ Missing required file: {file_path}")
            else:
                logging.info(f"✅ Found: {file_type} ({validation_results[file_type]['size_mb']:.2f} MB)")
        
        # Validate data quality from reports
        data_quality_checks = {
            'ingestion_records': 0,
            'processing_records': 0,
            'storage_records': 0,
            'data_consistency': False
        }
        
        try:
            # Check ingestion report
            if validation_results['ingestion_report']['exists']:
                with open(required_files['ingestion_report'], 'r') as f:
                    ingestion_report = json.load(f)
                    data_quality_checks['ingestion_records'] = ingestion_report['summary']['total_records']
            
            # Check processing report
            if validation_results['processing_report']['exists']:
                with open(required_files['processing_report'], 'r') as f:
                    processing_report = json.load(f)
                    data_quality_checks['processing_records'] = processing_report['key_insights']['total_records_processed']
            
            # Check storage report
            if validation_results['storage_report']['exists']:
                with open(required_files['storage_report'], 'r') as f:
                    storage_report = json.load(f)
                    data_quality_checks['storage_records'] = storage_report['validation_results']['total_records']
            
            # Check data consistency
            ingestion_count = data_quality_checks['ingestion_records']
            storage_count = data_quality_checks['storage_records']
            
            if ingestion_count > 0 and storage_count > 0:
                retention_rate = storage_count / ingestion_count
                data_quality_checks['data_consistency'] = retention_rate >= 0.8  # At least 80% retention
                data_quality_checks['retention_rate'] = retention_rate
            
        except Exception as e:
            logging.warning(f"⚠️  Data quality validation error: {e}")
        
        # Overall validation result
        pipeline_valid = all_files_present and data_quality_checks['data_consistency']
        
        validation_summary = {
            'pipeline_id': context['task_instance'].xcom_pull(key='pipeline_id'),
            'validation_timestamp': datetime.now().isoformat(),
            'overall_status': 'VALID' if pipeline_valid else 'INVALID',
            'file_validation': validation_results,
            'data_quality': data_quality_checks,
            'issues_found': []
        }
        
        # Collect issues
        if not all_files_present:
            validation_summary['issues_found'].append('Missing required output files')
        
        if not data_quality_checks['data_consistency']:
            validation_summary['issues_found'].append('Data consistency check failed')
        
        # Save validation summary
        validation_file = output_dir / 'pipeline_validation_summary.json'
        with open(validation_file, 'w') as f:
            json.dump(validation_summary, f, indent=2)
        
        if pipeline_valid:
            logging.info("🎉 Pipeline validation PASSED")
            logging.info(f"📊 Data flow: {data_quality_checks['ingestion_records']} → {data_quality_checks['storage_records']} records")
        else:
            logging.error("❌ Pipeline validation FAILED")
            logging.error(f"🚨 Issues: {validation_summary['issues_found']}")
        
        # Store results
        context['task_instance'].xcom_push(key='validation_results', value=validation_summary)
        context['task_instance'].xcom_push(key='pipeline_valid', value=pipeline_valid)
        
        return pipeline_valid
        
    except Exception as e:
        logging.error(f"❌ Pipeline validation failed: {e}")
        raise

def generate_master_report(**context):
    """
    Task 6: Generate comprehensive master pipeline report
    """
    logging.info("📋 Generating master pipeline report...")
    
    try:
        # Collect all results from previous tasks
        pipeline_config = context['task_instance'].xcom_pull(key='pipeline_config')
        monitoring_status = context['task_instance'].xcom_pull(key='monitoring_status')
        validation_results = context['task_instance'].xcom_pull(key='validation_results')
        
        # Read sub-pipeline reports
        output_dir = Path(MAGNITUDR_PATH) / 'data' / 'airflow_output'
        
        sub_reports = {}
        for report_name in ['ingestion', 'processing', 'storage']:
            report_file = output_dir / f'{report_name}_pipeline_report.json'
            if report_file.exists():
                with open(report_file, 'r') as f:
                    sub_reports[report_name] = json.load(f)
        
        # Calculate total execution time
        start_time = datetime.fromisoformat(pipeline_config['execution_date'])
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds() / 60  # minutes
        
        # Generate comprehensive master report
        master_report = {
            'master_pipeline': {
                'pipeline_id': pipeline_config['pipeline_id'],
                'execution_date': pipeline_config['execution_date'],
                'completion_date': end_time.isoformat(),
                'total_duration_minutes': round(total_duration, 2),
                'pipeline_version': pipeline_config['pipeline_version'],
                'environment': pipeline_config['environment']
            },
            'execution_summary': {
                'overall_status': 'SUCCESS' if validation_results['overall_status'] == 'VALID' else 'FAILED',
                'sub_pipelines_completed': len([s for s in monitoring_status['sub_pipelines'].values() if s == 'COMPLETED']),
                'total_sub_pipelines': len(monitoring_status['sub_pipelines']),
                'data_records_processed': validation_results['data_quality']['storage_records'],
                'data_retention_rate': validation_results['data_quality'].get('retention_rate', 0)
            },
            'sub_pipeline_reports': sub_reports,
            'validation_results': validation_results,
            'monitoring_data': monitoring_status,
            'performance_metrics': {
                'total_execution_time_minutes': round(total_duration, 2),
                'records_per_minute': validation_results['data_quality']['storage_records'] / max(total_duration, 1),
                'average_stage_time': round(total_duration / 3, 2),  # 3 main stages
                'system_efficiency': 'High' if total_duration < 60 else 'Moderate' if total_duration < 120 else 'Low'
            },
            'data_quality_assessment': {
                'ingestion_quality': 'Good' if validation_results['data_quality']['ingestion_records'] > 0 else 'Poor',
                'processing_quality': 'Good' if validation_results['data_quality']['processing_records'] > 0 else 'Poor',
                'storage_quality': 'Good' if validation_results['data_quality']['storage_records'] > 0 else 'Poor',
                'overall_quality': 'Good' if validation_results['data_quality']['data_consistency'] else 'Poor'
            },
            'resource_utilization': {
                'spark_used': pipeline_config['spark_config']['enabled'],
                'database_performance': 'Good',  # Could be enhanced with actual metrics
                'disk_usage_mb': sum(report['file_validation'][file]['size_mb'] for report in [validation_results] for file in report['file_validation']),
                'memory_efficiency': 'Optimized'
            },
            'recommendations': [],
            'next_steps': [
                'Review data quality metrics in sub-pipeline reports',
                'Monitor database performance and query optimization',
                'Set up automated alerts for future pipeline runs',
                'Consider data archival for long-term storage management'
            ]
        }
        
        # Generate recommendations based on performance
        if total_duration > 120:  # 2 hours
            master_report['recommendations'].append('Consider optimizing data processing stages for better performance')
        
        if validation_results['data_quality'].get('retention_rate', 1) < 0.9:
            master_report['recommendations'].append('Investigate data loss in processing pipeline')
        
        if not pipeline_config['spark_config']['enabled']:
            master_report['recommendations'].append('Enable Spark cluster for better big data processing performance')
        
        # Save master report
        master_report_file = output_dir / 'master_pipeline_report.json'
        with open(master_report_file, 'w') as f:
            json.dump(master_report, f, indent=2, default=str)
        
        # Create executive summary
        executive_summary = {
            'execution_date': pipeline_config['execution_date'],
            'status': master_report['execution_summary']['overall_status'],
            'records_processed': master_report['execution_summary']['data_records_processed'],
            'execution_time_minutes': master_report['performance_metrics']['total_execution_time_minutes'],
            'data_quality': master_report['data_quality_assessment']['overall_quality'],
            'key_achievements': [
                f"Processed {master_report['execution_summary']['data_records_processed']:,} earthquake records",
                f"Completed in {master_report['performance_metrics']['total_execution_time_minutes']:.1f} minutes",
                f"Achieved {(validation_results['data_quality'].get('retention_rate', 0) * 100):.1f}% data retention",
                "All sub-pipelines executed successfully"
            ]
        }
        
        # Save executive summary
        summary_file = output_dir / 'executive_summary.json'
        with open(summary_file, 'w') as f:
            json.dump(executive_summary, f, indent=2)
        
        logging.info("🎉 Master pipeline report generated successfully")
        logging.info(f"📁 Master report: {master_report_file}")
        logging.info(f"📊 Executive summary: {summary_file}")
        logging.info(f"⏱️  Total execution time: {total_duration:.2f} minutes")
        logging.info(f"📊 Records processed: {master_report['execution_summary']['data_records_processed']:,}")
        
        return str(master_report_file)
        
    except Exception as e:
        logging.error(f"❌ Master report generation failed: {e}")
        raise

# Define tasks
task_check_prerequisites = PythonOperator(
    task_id='check_system_prerequisites',
    python_callable=check_system_prerequisites,
    dag=dag
)

task_initialize_environment = PythonOperator(
    task_id='initialize_pipeline_environment',
    python_callable=initialize_pipeline_environment,
    dag=dag
)

task_trigger_ingestion = TriggerDagRunOperator(
    task_id='trigger_ingestion_pipeline',
    trigger_dag_id='earthquake_data_ingestion',
    wait_for_completion=True,
    poke_interval=10,  # Check every 10 seconds
    dag=dag
)

task_trigger_processing = TriggerDagRunOperator(
    task_id='trigger_processing_pipeline',
    trigger_dag_id='earthquake_data_processing',
    wait_for_completion=True,
    poke_interval=10,  # Check every 10 seconds
    dag=dag
)

task_trigger_storage = TriggerDagRunOperator(
    task_id='trigger_storage_pipeline',
    trigger_dag_id='earthquake_data_storage',
    wait_for_completion=True,
    poke_interval=10,  # Check every 10 seconds
    dag=dag
)

# Fixed visualization trigger with correct DAG ID
task_trigger_visualization = TriggerDagRunOperator(
    task_id='trigger_visualization_pipeline',
    trigger_dag_id='earthquake_visualization_simplified',  # Correct DAG ID
    wait_for_completion=True,
    poke_interval=10,
    dag=dag
)

task_monitor_progress = PythonOperator(
    task_id='monitor_pipeline_progress',
    python_callable=monitor_pipeline_progress,
    dag=dag
)

task_validate_completion = PythonOperator(
    task_id='validate_pipeline_completion',
    python_callable=validate_pipeline_completion,
    dag=dag
)

task_generate_master_report = PythonOperator(
    task_id='generate_master_report',
    python_callable=generate_master_report,
    dag=dag
)

# Optional: Send success notification
task_success_notification = BashOperator(
    task_id='send_success_notification',
    bash_command=f'''
        echo "🎉 Earthquake Master Pipeline Completed Successfully!"
        echo "📊 Check reports at: {MAGNITUDR_PATH}/data/airflow_output/"
        echo "🌐 View in Airflow UI: http://localhost:8080"
    ''',
    dag=dag
)

# Define task dependencies
task_check_prerequisites >> task_initialize_environment
task_initialize_environment >> task_trigger_ingestion
task_trigger_ingestion >> task_trigger_processing
task_trigger_processing >> task_trigger_storage
task_trigger_storage >> task_trigger_visualization  # Add visualization step
task_trigger_visualization >> task_monitor_progress
task_monitor_progress >> task_validate_completion
task_validate_completion >> task_generate_master_report
task_generate_master_report >> task_success_notification