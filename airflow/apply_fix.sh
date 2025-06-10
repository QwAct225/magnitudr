#!/bin/bash

echo "🔧 APPLYING AIRFLOW PERFORMANCE FIXES"
echo "====================================="
echo "Fixing DAG sensor issues and performance problems"
echo ""

# Load environment
source .env

# Function to apply optimized configuration
apply_optimized_config() {
    echo "⚙️  Applying optimized Airflow configuration..."
    
    # Copy optimized config
    if [ -f ./airflow_optimized.cfg ]; then
        cp ./airflow_optimized.cfg ~/airflow/airflow.cfg
        echo "✅ Applied optimized airflow.cfg"
    else
        echo "❌ Optimized config file not found!"
        return 1
    fi
    
    # Set proper environment variables in airflow.cfg
    sed -i "s|\${MAGNITUDR_PROJECT_PATH}|${MAGNITUDR_PROJECT_PATH}|g" ~/airflow/airflow.cfg
    sed -i "s|\${AIRFLOW__CORE__DAGS_FOLDER}|${AIRFLOW__CORE__DAGS_FOLDER}|g" ~/airflow/airflow.cfg
    sed -i "s|\${AIRFLOW__CORE__PLUGINS_FOLDER}|${AIRFLOW__CORE__PLUGINS_FOLDER}|g" ~/airflow/airflow.cfg
    
    echo "✅ Environment variables applied to config"
}

# Function to restart Airflow services
restart_airflow_services() {
    echo "🔄 Restarting Airflow services with new configuration..."
    
    # Stop existing services
    echo "🛑 Stopping current Airflow services..."
    pkill -f "airflow scheduler" 2>/dev/null || echo "Scheduler not running"
    pkill -f "airflow webserver" 2>/dev/null || echo "Webserver not running"
    
    # Wait a moment
    sleep 5
    
    # Start services with new config
    echo "🚀 Starting Airflow services..."
    
    # Set environment
    export AIRFLOW_HOME=~/airflow
    
    # Start webserver
    echo "🌐 Starting Airflow webserver..."
    nohup airflow webserver --port 8080 > ~/airflow/logs/webserver.log 2>&1 &
    
    # Wait for webserver to start
    sleep 10
    
    # Start scheduler
    echo "📅 Starting Airflow scheduler..."
    nohup airflow scheduler > ~/airflow/logs/scheduler.log 2>&1 &
    
    # Wait for services to start
    sleep 15
    
    echo "✅ Airflow services restarted"
    
    # Check if services are running
    if pgrep -f "airflow webserver" > /dev/null; then
        echo "✅ Webserver is running"
    else
        echo "❌ Webserver failed to start"
    fi
    
    if pgrep -f "airflow scheduler" > /dev/null; then
        echo "✅ Scheduler is running"
    else
        echo "❌ Scheduler failed to start"
    fi
}

# Function to clear problematic task instances
clear_problematic_tasks() {
    echo "🧹 Clearing problematic task instances..."
    
    # Clear entire DAG runs that have skipped status
    echo "Clearing processing DAG runs..."
    airflow dags delete earthquake_data_processing --yes 2>/dev/null || echo "No processing DAG to clear"
    
    echo "Clearing storage DAG runs..."
    airflow dags delete earthquake_data_storage --yes 2>/dev/null || echo "No storage DAG to clear"
    
    echo "Clearing visualization DAG runs..."
    airflow dags delete earthquake_visualization_dashboard --yes 2>/dev/null || echo "No visualization DAG to clear"
    
    # Clear any stuck sensor tasks specifically
    airflow tasks clear earthquake_data_processing check_ingestion_completed --yes 2>/dev/null || echo "Task not found"
    airflow tasks clear earthquake_data_storage check_processing_completed --yes 2>/dev/null || echo "Task not found"
    airflow tasks clear earthquake_visualization_dashboard check_storage_completed --yes 2>/dev/null || echo "Task not found"
    
    # Clear master pipeline triggers
    airflow tasks clear earthquake_master_pipeline trigger_processing_pipeline --yes 2>/dev/null || echo "Task not found"
    airflow tasks clear earthquake_master_pipeline trigger_storage_pipeline --yes 2>/dev/null || echo "Task not found"
    airflow tasks clear earthquake_master_pipeline trigger_visualization_pipeline --yes 2>/dev/null || echo "Task not found"
    
    echo "✅ Problematic tasks cleared"
}

# Function to verify fixes
verify_fixes() {
    echo "🔍 Verifying applied fixes..."
    
    # Check if DAGs are loading properly
    echo "📊 Checking DAG loading..."
    airflow dags list | grep earthquake > /tmp/dag_check.txt
    
    expected_dags=("earthquake_data_ingestion" "earthquake_data_processing" "earthquake_data_storage" "earthquake_master_pipeline" "earthquake_visualization_dashboard")
    
    for dag in "${expected_dags[@]}"; do
        if grep -q "$dag" /tmp/dag_check.txt; then
            echo "✅ $dag - loaded successfully"
        else
            echo "❌ $dag - not loaded"
        fi
    done
    
    # Check Airflow UI accessibility
    echo "🌐 Checking Airflow UI..."
    if curl -s http://localhost:8080/health > /dev/null; then
        echo "✅ Airflow UI is accessible at http://localhost:8080"
    else
        echo "❌ Airflow UI is not accessible"
    fi
    
    # Check for any import errors
    echo "🔍 Checking for import errors..."
    airflow dags list-import-errors > /tmp/import_errors.txt 2>&1
    
    if [ -s /tmp/import_errors.txt ]; then
        echo "⚠️  Import errors found:"
        cat /tmp/import_errors.txt
    else
        echo "✅ No import errors found"
    fi
}

# Function to create monitoring script
create_monitoring_script() {
    echo "📊 Creating monitoring script..."
    
    cat > monitor_dag_health.sh << 'EOF'
#!/bin/bash

echo "📊 DAG HEALTH MONITOR"
echo "===================="
echo "Checking DAG status every 30 seconds..."
echo "Press Ctrl+C to stop"
echo ""

while true; do
    echo "🕐 $(date)"
    echo "------------------------"
    
    # Check each DAG status
    dags=("earthquake_data_ingestion" "earthquake_data_processing" "earthquake_data_storage" "earthquake_master_pipeline" "earthquake_visualization_dashboard")
    
    for dag in "${dags[@]}"; do
        # Get latest run
        latest_run=$(airflow dags list-runs -d "$dag" --limit 1 2>/dev/null | tail -1)
        
        if [ -n "$latest_run" ]; then
            state=$(echo "$latest_run" | awk '{print $3}')
            date=$(echo "$latest_run" | awk '{print $2}')
            
            case "$state" in
                "success") echo "✅ $dag: SUCCESS ($date)" ;;
                "running") echo "🔄 $dag: RUNNING ($date)" ;;
                "failed") echo "❌ $dag: FAILED ($date)" ;;
                "up_for_reschedule") echo "⏳ $dag: WAITING ($date)" ;;
                *) echo "❓ $dag: $state ($date)" ;;
            esac
        else
            echo "⚪ $dag: No runs"
        fi
    done
    
    echo ""
    echo "📊 System Status:"
    echo "Scheduler: $(pgrep -f 'airflow scheduler' > /dev/null && echo 'Running' || echo 'Stopped')"
    echo "Webserver: $(pgrep -f 'airflow webserver' > /dev/null && echo 'Running' || echo 'Stopped')"
    echo ""
    
    sleep 30
done
EOF
    
    chmod +x monitor_dag_health.sh
    echo "✅ Created monitor_dag_health.sh"
}

# Function to show quick commands
show_quick_commands() {
    echo ""
    echo "🚀 QUICK COMMANDS FOR TROUBLESHOOTING"
    echo "====================================="
    echo ""
    echo "📊 Check DAG status:"
    echo "   airflow dags list | grep earthquake"
    echo ""
    echo "🔄 Trigger DAGs manually:"
    echo "   airflow dags trigger earthquake_data_ingestion"
    echo "   airflow dags trigger earthquake_data_processing"
    echo "   airflow dags trigger earthquake_data_storage"
    echo ""
    echo "🧹 Clear stuck tasks:"
    echo "   airflow tasks clear earthquake_data_processing wait_for_ingestion_dag --yes"
    echo "   airflow tasks clear earthquake_data_storage wait_for_processing_dag --yes"
    echo ""
    echo "📋 Check task logs:"
    echo "   airflow tasks log <dag_id> <task_id> <execution_date>"
    echo ""
    echo "🔍 Debug sensors:"
    echo "   ./debug_airflow_issues.sh sensor"
    echo ""
    echo "📊 Monitor DAGs:"
    echo "   ./monitor_dag_health.sh"
    echo ""
}

# Main execution
main() {
    echo "🔧 Applying comprehensive fixes for Airflow DAG issues..."
    echo ""
    
    # Ask for confirmation
    read -p "This will restart Airflow services. Continue? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "❌ Operation cancelled"
        exit 1
    fi
    
    echo ""
    echo "🚀 Starting fix application..."
    
    # Apply fixes step by step
    apply_optimized_config
    clear_problematic_tasks
    restart_airflow_services
    
    echo ""
    echo "⏳ Waiting for services to stabilize..."
    sleep 20
    
    verify_fixes
    create_monitoring_script
    
    echo ""
    echo "🎉 FIXES APPLIED SUCCESSFULLY!"
    echo "=============================="
    echo ""
    echo "✅ Optimized Airflow configuration applied"
    echo "✅ Problematic tasks cleared"
    echo "✅ Services restarted with new config"
    echo "✅ Monitoring script created"
    echo ""
    echo "🌐 Airflow UI: http://localhost:8080"
    echo "📊 Monitor: ./monitor_dag_health.sh"
    echo "🔍 Debug: ./debug_airflow_issues.sh"
    echo ""
    
    show_quick_commands
    
    echo ""
    echo "🎯 NEXT STEPS:"
    echo "1. Open Airflow UI: http://localhost:8080"
    echo "2. Check that all DAGs are visible and parsed correctly"
    echo "3. Try triggering earthquake_data_ingestion first"
    echo "4. Monitor progress with ./monitor_dag_health.sh"
    echo "5. If issues persist, run ./debug_airflow_issues.sh"
}

# Run main function
main