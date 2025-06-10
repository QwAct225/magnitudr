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
