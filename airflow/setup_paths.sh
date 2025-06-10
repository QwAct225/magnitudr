#!/bin/bash

echo "🚀 SETTING UP APACHE AIRFLOW ENVIRONMENT"
echo "=========================================="

# 1. Auto-detect magnitudr project path
MAGNITUDR_PATH=$(pwd)
echo "✅ Detected project path: $MAGNITUDR_PATH"

# 2. Set environment variables
export AIRFLOW_HOME=~/airflow
export MAGNITUDR_PROJECT_PATH="$MAGNITUDR_PATH"

echo "📁 Creating directory structure..."
# 3. Create necessary directories
mkdir -p airflow/dags
mkdir -p airflow/plugins
mkdir -p airflow/logs
mkdir -p data/airflow_output
mkdir -p data/airflow_input

# 4. Create .env file with dynamic paths
cat > .env << EOL
# Database Configuration (existing)
DB_USER=postgres
DB_PASSWORD=password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=magnitudr

# Airflow Configuration - Dynamic Paths
AIRFLOW_HOME=$AIRFLOW_HOME
MAGNITUDR_PROJECT_PATH=$MAGNITUDR_PATH

# Airflow Environment Variables
AIRFLOW__CORE__DAGS_FOLDER=$MAGNITUDR_PATH/airflow/dags
AIRFLOW__CORE__PLUGINS_FOLDER=$MAGNITUDR_PATH/airflow/plugins
AIRFLOW__WEBSERVER__SECRET_KEY=earthquake-pipeline-secret-key-2025
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow_user:airflow_pass@localhost:5432/airflow_db
AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Jakarta
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.session
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=False
AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False

# Pipeline Configuration - Dynamic Paths
PIPELINE_DATA_DIR=$MAGNITUDR_PATH/data
PIPELINE_OUTPUT_DIR=$MAGNITUDR_PATH/data/airflow_output
PIPELINE_LOG_DIR=$MAGNITUDR_PATH/airflow/logs
PIPELINE_SCRIPTS_DIR=$MAGNITUDR_PATH/scripts

# Spark Configuration
SPARK_HOME=/opt/spark
SPARK_MASTER_URL=spark://localhost:7077
EOL

# 5. Copy airflow.cfg to AIRFLOW_HOME
mkdir -p ~/airflow
cat > ~/airflow/airflow.cfg << EOL
[core]
# Set timezone
default_timezone = Asia/Jakarta
executor = LocalExecutor

# Dynamic paths (will be set via environment variables)
dags_folder = $MAGNITUDR_PATH/airflow/dags
plugins_folder = $MAGNITUDR_PATH/airflow/plugins

# Performance optimizations
load_examples = False
execute_tasks_new_python_interpreter = False
parallelism = 32
max_active_tasks_per_dag = 16
max_active_runs_per_dag = 1
dagbag_import_timeout = 30.0

# DAG parsing optimization
dag_dir_list_interval = 300
dag_file_parsing_timeout = 30
min_file_process_interval = 30
store_serialized_dags = True
min_serialized_dag_update_interval = 30
compress_serialized_dags = True

[webserver]
# Web server settings
web_server_port = 8080
base_url = http://localhost:8080
secret_key = earthquake-pipeline-secret-key-2025

# Security settings - Simplified for development
expose_config = True
authenticate = False
auth_backend = airflow.api.auth.backend.default

# Performance settings
workers = 4
worker_refresh_batch_size = 1
worker_refresh_interval = 6000
reload_on_plugin_change = False

[scheduler]
# Scheduler optimization for better performance
dag_dir_list_interval = 60
dag_file_parsing_timeout = 30
min_file_process_interval = 10
catchup_by_default = False

# Performance tuning
max_tis_per_query = 512
parsing_processes = 2
scheduler_heartbeat_sec = 5
job_heartbeat_sec = 5
num_runs = -1

# Task instance optimization
max_dagruns_to_create_per_loop = 10
max_dagruns_per_loop_to_schedule = 20

[database]
# Use PostgreSQL instead of SQLite (production ready)
sql_alchemy_conn = postgresql://airflow_user:airflow_pass@localhost:5432/airflow_db
sql_alchemy_pool_size = 10
sql_alchemy_pool_recycle = 3600
sql_alchemy_pool_pre_ping = True

[logging]
# Logging configuration
base_log_folder = $MAGNITUDR_PATH/airflow/logs
remote_logging = False
logging_level = INFO
fab_logging_level = WARN
logging_config_class = 
colored_console_log = True
colored_log_format = [%%(blue)s%%(asctime)s%%(reset)s] {%%(blue)s%%(filename)s:%%(reset)s%%(lineno)d} %%(log_color)s%%(levelname)s%%(reset)s - %%(log_color)s%%(message)s%%(reset)s
colored_formatter_class = airflow.utils.log.colored_log.CustomTTYColoredFormatter

[api]
# Enable API
auth_backends = airflow.api.auth.backend.default
maximum_page_limit = 100

[celery]
# Celery configuration (if needed)
worker_concurrency = 4
task_always_eager = False

[sensors]
# Sensor optimization
default_timeout = 60 * 60 * 24 * 7
sensor_default_timeout = 60 * 60 * 24 * 7
default_retry_delay = 60
default_email_on_retry = False
default_email_on_failure = False

[taskflow]
# TaskFlow API settings
decorated_operator_class = airflow.operators.python.PythonOperator

[operators]
# Operator defaults
default_owner = magnitudr-team
default_cpus = 1
default_ram = 512
default_disk = 512
default_gpus = 0
EOL

echo "✅ Environment variables configured with dynamic paths"
echo "✅ Project path: $MAGNITUDR_PATH"
echo "✅ .env file created"
echo "✅ airflow.cfg configured"
echo ""
echo "🎯 Next steps:"
echo "1. Run: source .env"
echo "2. Run: ./setup_airflow_database.sh"
echo "3. Run: ./start_airflow.sh"