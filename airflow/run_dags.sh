#!/bin/bash

echo "🚀 SEQUENTIAL DAG RUNNER"
echo "========================"
echo "Running DAGs independently without sensors"
echo ""

# Function to wait for DAG completion
wait_for_dag_completion() {
    local dag_id="$1"
    local max_wait="$2"
    local wait_time=0
    
    echo "⏳ Waiting for $dag_id to complete (max ${max_wait}min)..."
    
    while [ $wait_time -lt $((max_wait * 60)) ]; do
        # Get latest DAG run state using airflow CLI
        latest_run=$(airflow dags list-runs -d "$dag_id" --limit 1 --output table 2>/dev/null | tail -n +4 | head -1)
        
        if [ -z "$latest_run" ]; then
            echo "❓ No DAG runs found for $dag_id"
            sleep 30
            wait_time=$((wait_time + 30))
            continue
        fi
        
        # Extract state from the run (state is typically in the last column)
        latest_state=$(echo "$latest_run" | awk '{print $NF}' | tr -d '|' | xargs)
        
        case "$latest_state" in
            "success")
                echo "✅ $dag_id completed successfully!"
                return 0
                ;;
            "failed"|"upstream_failed")
                echo "❌ $dag_id failed!"
                echo "📋 Recent run details:"
                echo "$latest_run"
                return 1
                ;;
            "running")
                echo "🔄 $dag_id still running... (${wait_time}s elapsed)"
                ;;
            "queued")
                echo "⏳ $dag_id in queue... (${wait_time}s elapsed)"
                ;;
            *)
                echo "❓ $dag_id state: $latest_state (${wait_time}s elapsed)"
                ;;
        esac
        
        # Show active tasks if DAG is running
        if [ "$latest_state" = "running" ] || [ "$latest_state" = "queued" ]; then
            active_tasks=$(airflow tasks list-instances -d "$dag_id" --state running,queued 2>/dev/null | wc -l)
            if [ "$active_tasks" -gt 0 ]; then
                echo "   📊 Active tasks: $active_tasks"
            fi
        fi
        
        sleep 30
        wait_time=$((wait_time + 30))
    done
    
    echo "⚠️ $dag_id timed out after ${max_wait} minutes"
    return 1
}

# Function to check if DAG exists and is ready
check_dag_ready() {
    local dag_id="$1"
    
    echo "🔍 Checking if $dag_id is ready..."
    
    # Check if DAG is in the list
    if airflow dags list | grep -q "$dag_id"; then
        echo "✅ $dag_id found and ready"
        return 0
    else
        echo "❌ $dag_id not found in DAG list"
        return 1
    fi
}

# Function to trigger a DAG and wait
trigger_and_wait() {
    local dag_id="$1"
    local max_wait="$2"
    local description="$3"
    
    echo ""
    echo "🎯 $description"
    echo "=================================="
    
    # Check if DAG is ready
    if ! check_dag_ready "$dag_id"; then
        echo "❌ Cannot proceed with $dag_id - DAG not ready"
        return 1
    fi
    
    echo "🚀 Triggering $dag_id..."
    
    # Trigger the DAG
    if airflow dags trigger "$dag_id"; then
        echo "✅ $dag_id triggered successfully"
        
        # Wait for completion
        if wait_for_dag_completion "$dag_id" "$max_wait"; then
            echo "🎉 $dag_id completed successfully!"
            return 0
        else
            echo "❌ $dag_id failed or timed out"
            return 1
        fi
    else
        echo "❌ Failed to trigger $dag_id"
        return 1
    fi
}

# Function to show current status
show_status() {
    echo ""
    echo "📊 CURRENT DAG STATUS"
    echo "===================="
    
    dags=("earthquake_data_ingestion" "earthquake_data_processing" "earthquake_data_storage" "earthquake_visualization_dashboard")
    
    for dag in "${dags[@]}"; do
        latest_state=$(airflow dags list-runs -d "$dag" --limit 1 2>/dev/null | tail -1 | awk '{print $3}' || echo "no-runs")
        
        case "$latest_state" in
            "success") echo "✅ $dag: SUCCESS" ;;
            "running") echo "🔄 $dag: RUNNING" ;;
            "failed") echo "❌ $dag: FAILED" ;;
            "no-runs") echo "⚪ $dag: NO RUNS" ;;
            *) echo "❓ $dag: $latest_state" ;;
        esac
    done
    echo ""
}

# Function to run complete pipeline
run_complete_pipeline() {
    echo "🌍 RUNNING COMPLETE EARTHQUAKE PIPELINE"
    echo "======================================="
    echo "This will run all DAGs sequentially:"
    echo "1. Data Ingestion (15 min)"
    echo "2. Data Processing (30 min)" 
    echo "3. Data Storage (15 min)"
    echo "4. Data Visualization (10 min)"
    echo ""
    echo "Total estimated time: ~70 minutes"
    echo ""
    
    read -p "Continue with complete pipeline? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "❌ Pipeline cancelled"
        return 1
    fi
    
    local start_time=$(date +%s)
    
    # Step 1: Data Ingestion
    if trigger_and_wait "earthquake_data_ingestion" 15 "STEP 1: Data Ingestion"; then
        echo "✅ Ingestion completed - proceeding to processing"
    else
        echo "❌ Ingestion failed - stopping pipeline"
        return 1
    fi
    
    # Step 2: Data Processing  
    if trigger_and_wait "earthquake_data_processing" 30 "STEP 2: Data Processing"; then
        echo "✅ Processing completed - proceeding to storage"
    else
        echo "❌ Processing failed - stopping pipeline"
        return 1
    fi
    
    # Step 3: Data Storage
    if trigger_and_wait "earthquake_data_storage" 15 "STEP 3: Data Storage"; then
        echo "✅ Storage completed - proceeding to visualization"
    else
        echo "❌ Storage failed - stopping pipeline"
        return 1
    fi
    
    # Step 4: Data Visualization
    if trigger_and_wait "earthquake_visualization_dashboard" 10 "STEP 4: Data Visualization"; then
        echo "✅ Visualization completed - pipeline finished!"
    else
        echo "⚠️ Visualization failed - but core pipeline is complete"
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local duration_min=$((duration / 60))
    
    echo ""
    echo "🎉 PIPELINE EXECUTION COMPLETED!"
    echo "================================"
    echo "⏱️ Total execution time: ${duration_min} minutes"
    echo "📁 Check results in: data/airflow_output/"
    echo "🌐 Dashboard: data/airflow_output/plots/dashboard.html"
    echo ""
}

# Function to run individual DAG
run_individual_dag() {
    echo "🎯 INDIVIDUAL DAG RUNNER"
    echo "========================"
    echo "Available DAGs:"
    echo "1. earthquake_data_ingestion"
    echo "2. earthquake_data_processing"
    echo "3. earthquake_data_storage" 
    echo "4. earthquake_visualization_dashboard"
    echo ""
    
    read -p "Enter DAG name: " dag_id
    
    if [ -z "$dag_id" ]; then
        echo "❌ No DAG name provided"
        return 1
    fi
    
    # Set appropriate timeout based on DAG
    local timeout=15
    case "$dag_id" in
        "earthquake_data_processing") timeout=30 ;;
        "earthquake_data_ingestion") timeout=15 ;;
        "earthquake_data_storage") timeout=15 ;;
        "earthquake_visualization_dashboard") timeout=10 ;;
    esac
    
    trigger_and_wait "$dag_id" "$timeout" "Running Individual DAG: $dag_id"
}

# Function to monitor active DAGs
monitor_active_dags() {
    echo "📊 MONITORING ACTIVE DAGS"
    echo "========================="
    echo "Press Ctrl+C to stop monitoring"
    echo ""
    
    while true; do
        clear
        echo "🔄 Live DAG Monitor - $(date)"
        echo "=================================="
        
        # Show running tasks
        echo "🔄 Currently Running Tasks:"
        running_tasks=$(airflow tasks list-instances --state running 2>/dev/null)
        if [ -n "$running_tasks" ]; then
            echo "$running_tasks" | head -10
        else
            echo "   No tasks currently running"
        fi
        
        echo ""
        show_status
        
        # Show recent logs
        echo "📋 Recent Activity:"
        airflow dags list-runs --limit 8 2>/dev/null | head -8
        
        sleep 15
    done
}

# Function to clear and restart
clear_and_restart() {
    echo "🧹 CLEAR AND RESTART"
    echo "===================="
    echo "This will:"
    echo "1. Clear all DAG runs"
    echo "2. Clear any stuck tasks"
    echo "3. Reset DAG states"
    echo ""
    
    read -p "Continue? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "❌ Operation cancelled"
        return 1
    fi
    
    echo "🧹 Clearing DAG runs..."
    
    # Clear all problematic DAGs
    airflow dags delete earthquake_data_processing --yes 2>/dev/null || echo "No processing runs to clear"
    airflow dags delete earthquake_data_storage --yes 2>/dev/null || echo "No storage runs to clear"
    airflow dags delete earthquake_visualization_dashboard --yes 2>/dev/null || echo "No visualization runs to clear"
    airflow dags delete earthquake_master_pipeline --yes 2>/dev/null || echo "No master runs to clear"
    
    echo "✅ All DAG runs cleared"
    echo ""
    echo "🎯 Ready for fresh start!"
    echo "You can now:"
    echo "1. Run individual DAGs"
    echo "2. Run complete pipeline"
    echo "3. Use Airflow UI to trigger manually"
}

# Main menu
main_menu() {
    echo "🚀 MAGNITUDR DAG RUNNER"
    echo "======================="
    echo "Choose an option:"
    echo ""
    echo "1. Run Complete Pipeline (Sequential)"
    echo "2. Run Individual DAG"
    echo "3. Show Current Status"
    echo "4. Monitor Active DAGs (Live)"
    echo "5. Clear and Restart"
    echo "6. Exit"
    echo ""
    
    read -p "Enter choice (1-6): " choice
    
    case $choice in
        1) run_complete_pipeline ;;
        2) run_individual_dag ;;
        3) show_status ;;
        4) monitor_active_dags ;;
        5) clear_and_restart ;;
        6) echo "👋 Goodbye!"; exit 0 ;;
        *) echo "❌ Invalid choice" ;;
    esac
}

# Check if running with arguments
if [ $# -eq 0 ]; then
    # Interactive mode
    while true; do
        main_menu
        echo ""
        read -p "Press Enter to continue..."
        clear
    done
else
    # Command line mode
    case $1 in
        "pipeline") run_complete_pipeline ;;
        "status") show_status ;;
        "monitor") monitor_active_dags ;;
        "clear") clear_and_restart ;;
        "ingestion") trigger_and_wait "earthquake_data_ingestion" 15 "Manual Ingestion Trigger" ;;
        "processing") trigger_and_wait "earthquake_data_processing" 30 "Manual Processing Trigger" ;;
        "storage") trigger_and_wait "earthquake_data_storage" 15 "Manual Storage Trigger" ;;
        "visualization") trigger_and_wait "earthquake_visualization_dashboard" 10 "Manual Visualization Trigger" ;;
        *) 
            echo "Usage: $0 [pipeline|status|monitor|clear|ingestion|processing|storage|visualization]"
            echo ""
            echo "Examples:"
            echo "  $0 pipeline     # Run complete pipeline"
            echo "  $0 status      # Show current status"
            echo "  $0 ingestion   # Run ingestion DAG only"
            echo "  $0 monitor     # Live monitoring"
            ;;
    esac
fi