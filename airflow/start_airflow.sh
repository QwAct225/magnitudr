#!/bin/bash

echo "🚀 STARTING APACHE AIRFLOW (FULL SCRIPT)"
echo "========================================"

# Load environment variables
source .env

# Set Airflow Home
export AIRFLOW_HOME=~/airflow

# Pastikan direktori log tersedia
mkdir -p $AIRFLOW_HOME/logs

# Check if PostgreSQL is running
if ! sudo service postgresql status > /dev/null 2>&1; then
    echo "📊 Starting PostgreSQL..."
    sudo service postgresql start
    sleep 3
else
    echo "✅ PostgreSQL is already running."
fi

# Initialize or upgrade database
echo "🛠️ Checking Airflow metadata database..."
if ! psql -U $DB_USER -d $DB_NAME -c '\q' 2>/dev/null; then
    echo "🔧 Database not found. Initializing Airflow metadata database..."
    airflow db init
else
    echo "🔄 Database found. Running upgrade..."
    airflow db upgrade
fi

# Start Spark Master on port 7077, Web UI on 8081 (NOT 8080)
if command -v $SPARK_HOME/sbin/start-master.sh &> /dev/null; then
    echo "⚡ Starting Spark cluster on port 8081..."
    $SPARK_HOME/sbin/start-master.sh --host localhost --port 7077 --webui-port 8081
    sleep 5
    $SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
else
    echo "⚠️  Spark not found, continuing without Spark..."
fi

# Suppress Python warnings
export PYTHONWARNINGS="ignore"

# Start Airflow Webserver in background (on port 8080)
echo "🌐 Starting Airflow Webserver..."
nohup airflow webserver --port 8080 > webserver.log 2>&1 &

# Start Airflow Scheduler in background
echo "📅 Starting Airflow Scheduler..."
nohup airflow scheduler > scheduler.log 2>&1 &

# Wait for webserver to start (maximum 30 attempts with 5s interval)
echo "⏳ Waiting for Airflow Web UI to become available..."
for i in {1..30}; do
    if curl -s http://localhost:8080 | grep -q "Airflow"; then
        echo "✅ Airflow Web UI is now available!"
        break
    fi
    echo "  - Attempt: $i/30"
    sleep 5
done

# Wait briefly to let scheduler settle
sleep 10

# Check if scheduler crashed
if grep -q "Traceback" scheduler.log; then
    echo ""
    echo "❌ Scheduler appears to have crashed. Here's the last 50 lines of log:"
    tail -n 50 scheduler.log
else
    echo ""
    echo "✅ Scheduler is running normally (no critical errors detected in logs)."
fi

# Final status summary
echo ""
echo "📊 STATUS SUMMARY"
echo "----------------------"
echo "🌐 Web UI: http://localhost:8080"
echo "🔑 Login: admin / admin123 (or your configured user)"
echo "🗂️  Logs: webserver.log / scheduler.log"
echo "📁 DAGs folder: $MAGNITUDR_PATH/airflow/dags"
echo ""
echo "💡 Running Services:"
echo "   - PostgreSQL: $(sudo service postgresql status | grep -o 'online\|active' || echo 'inactive')"
echo "   - Webserver PID: $(pgrep -f 'airflow webserver' || echo 'Not running')"
echo "   - Scheduler PID: $(pgrep -f 'airflow scheduler' || echo 'Not running')"
if command -v $SPARK_HOME/sbin/start-master.sh &> /dev/null; then
    echo "   - Spark Master: Running on port 8081"
    echo "   - Spark Worker: Connected to master"
fi
echo ""
echo "🛑 To stop all services, run: ./stop_airflow.sh"
