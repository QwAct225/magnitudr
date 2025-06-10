#!/bin/bash

echo "🔧 FIXING SKIPPED TASKS ISSUE"
echo "============================="
echo "Clearing skipped tasks and resetting DAG states"
echo ""

# Load environment
source .env

# Function to clear all task instances for a DAG
clear_dag_tasks() {
    local dag_id="$1"
    echo "🧹 Clearing all tasks for DAG: $dag_id"
    
    # Clear all task instances
    airflow tasks clear "$dag_id" --yes 2>/dev/null || echo "No tasks to clear for $dag_id"
    
    # Clear DAG runs
    echo "🗑️ Clearing DAG runs for: $dag_id"
    airflow dags delete "$dag_id" --yes 2>/dev/null || echo "DAG not found: $dag_id"
    
    echo "✅ Cleared $dag_id"
}

# Function to mark tasks as success (bypass skipped status)
mark_tasks_success() {
    local dag_id="$1"
    local execution_date="$2"
    
    echo "✅ Marking all tasks as success for $dag_id"
    
    # Get current date if not provided
    if [ -z "$execution_date" ]; then
        execution_date=$(date -Iseconds)
    fi
    
    # Mark entire DAG run as success
    airflow dags state "$dag_id" "$execution_date" 2>/dev/null || echo "Could not get DAG state"
    
    # Try to mark the DAG run as success
    airflow tasks mark "$dag_id" --yes --execution_date "$execution_date" 2>/dev/null || echo "Could not mark DAG tasks"
}

# Function to reset DAG configurations
reset_dag_configs() {
    echo "⚙️ Resetting DAG configurations..."
    
    # Stop Airflow services
    echo "🛑 Stopping Airflow services..."
    pkill -f "airflow scheduler" 2>/dev/null || echo "Scheduler not running"
    pkill -f "airflow webserver" 2>/dev/null || echo "Webserver not running"
    
    sleep 5
    
    # Clear Airflow metadata
    echo "🗑️ Clearing Airflow metadata..."
    
    # Reset the metadata database
    airflow db reset --yes 2>/dev/null || echo "Could not reset DB"
    
    # Reinitialize database
    echo "🔄 Reinitializing Airflow database..."
    airflow db init
    
    # Recreate admin user
    echo "👤 Recreating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@magnitudr.com \
        --password admin123
    
    echo "✅ DAG configurations reset"
}

# Function to restart services
restart_services() {
    echo "🚀 Restarting Airflow services..."
    
    # Set environment
    export AIRFLOW_HOME=~/airflow
    
    # Start webserver
    echo "🌐 Starting webserver..."
    nohup airflow webserver --port 8080 > ~/airflow/logs/webserver.log 2>&1 &
    
    sleep 10
    
    # Start scheduler
    echo "📅 Starting scheduler..."
    nohup airflow scheduler > ~/airflow/logs/scheduler.log 2>&1 &
    
    sleep 15
    
    # Check services
    if pgrep -f "airflow webserver" > /dev/null; then
        echo "✅ Webserver running"
    else
        echo "❌ Webserver failed to start"
    fi
    
    if pgrep -f "airflow scheduler" > /dev/null; then
        echo "✅ Scheduler running"  
    else
        echo "❌ Scheduler failed to start"
    fi
}

# Function to verify DAGs are loaded
verify_dags() {
    echo "🔍 Verifying DAGs are loaded properly..."
    
    expected_dags=(
        "earthquake_data_ingestion"
        "earthquake_data_processing" 
        "earthquake_data_storage"
        "earthquake_master_pipeline"
        "earthquake_visualization_dashboard"
    )
    
    sleep 10  # Wait for DAGs to load
    
    for dag in "${expected_dags[@]}"; do
        if airflow dags list | grep -q "$dag"; then
            echo "✅ $dag - loaded"
        else
            echo "❌ $dag - not loaded"
        fi
    done
}

# Function to create test triggers
create_test_triggers() {
    echo "🧪 Creating test trigger script..."
    
    cat > test_individual_dags.sh << 'EOF'
#!/bin/bash

echo "🧪 TESTING INDIVIDUAL DAGS"
echo "=========================="

echo "1. Testing Ingestion DAG..."
airflow dags trigger earthquake_data_ingestion
echo "   ✅ Ingestion triggered"

echo ""
echo "2. Wait 5 minutes, then test Processing DAG..."
echo "   Run: airflow dags trigger earthquake_data_processing"

echo ""
echo "3. Wait for processing to complete, then test Storage DAG..."  
echo "   Run: airflow dags trigger earthquake_data_storage"

echo ""
echo "4. Finally test Visualization DAG..."
echo "   Run: airflow dags trigger earthquake_visualization_dashboard"

echo ""
echo "📊 Monitor progress:"
echo "   airflow dags list-runs -d <dag_id> --limit 5"
echo "   Or use: ./monitor_dag_health.sh"

EOF
    
    chmod +x test_individual_dags.sh
    echo "✅ Created test_individual_dags.sh"
}

# Main function
main() {
    echo "🔧 Choose fix option:"
    echo "1. Clear all skipped tasks (Quick fix)"
    echo "2. Full reset (Clear DB + Restart services)"  
    echo "3. Clear specific DAG"
    echo "4. Create test scripts only"
    echo ""
    
    read -p "Enter choice (1-4): " choice
    
    case $choice in
        1)
            echo "🧹 Quick fix - clearing skipped tasks..."
            
            # Clear problematic DAGs
            clear_dag_tasks "earthquake_data_processing"
            clear_dag_tasks "earthquake_data_storage"
            clear_dag_tasks "earthquake_visualization_dashboard"
            
            echo ""
            echo "✅ Quick fix completed!"
            echo "🚀 Try triggering DAGs individually now:"
            echo "   1. airflow dags trigger earthquake_data_ingestion"
            echo "   2. airflow dags trigger earthquake_data_processing" 
            echo "   3. airflow dags trigger earthquake_data_storage"
            echo "   4. airflow dags trigger earthquake_visualization_dashboard"
            ;;
            
        2)
            echo "🔄 Full reset - this will restart everything..."
            read -p "This will reset Airflow database. Continue? (y/N): " confirm
            
            if [[ "$confirm" =~ ^[Yy]$ ]]; then
                reset_dag_configs
                restart_services
                verify_dags
                create_test_triggers
                
                echo ""
                echo "🎉 Full reset completed!"
                echo "🌐 Access Airflow: http://localhost:8080"
                echo "🔑 Login: admin / admin123"
                echo "🧪 Test with: ./test_individual_dags.sh"
            else
                echo "❌ Full reset cancelled"
            fi
            ;;
            
        3)
            echo "🎯 Clear specific DAG"
            echo "Available DAGs:"
            echo "  - earthquake_data_processing"
            echo "  - earthquake_data_storage" 
            echo "  - earthquake_visualization_dashboard"
            echo "  - earthquake_master_pipeline"
            echo ""
            
            read -p "Enter DAG ID to clear: " dag_id
            
            if [ -n "$dag_id" ]; then
                clear_dag_tasks "$dag_id"
                echo "✅ Cleared $dag_id"
                echo "🚀 Try triggering: airflow dags trigger $dag_id"
            else
                echo "❌ No DAG ID provided"
            fi
            ;;
            
        4)
            echo "📝 Creating test scripts only..."
            create_test_triggers
            echo "✅ Test scripts created"
            echo "🧪 Run: ./test_individual_dags.sh"
            ;;
            
        *)
            echo "❌ Invalid choice"
            ;;
    esac
}

# Check if running with arguments
if [ $# -eq 0 ]; then
    main
else
    case $1 in
        "clear") 
            clear_dag_tasks "earthquake_data_processing"
            clear_dag_tasks "earthquake_data_storage" 
            clear_dag_tasks "earthquake_visualization_dashboard"
            ;;
        "reset") 
            reset_dag_configs
            restart_services
            ;;
        "verify") verify_dags ;;
        *) echo "Usage: $0 [clear|reset|verify]" ;;
    esac
fi