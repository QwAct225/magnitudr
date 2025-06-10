#!/bin/bash

echo "🛑 STOPPING APACHE AIRFLOW (Enhanced Version)"
echo "=============================================="

# Load environment variables if exists
if [ -f .env ]; then
    source .env
fi

# Function to kill processes with timeout
kill_with_timeout() {
    local process_name="$1"
    local timeout=10
    
    echo "🔄 Stopping $process_name..."
    
    # Get PIDs
    pids=$(pgrep -f "$process_name" 2>/dev/null)
    
    if [ -z "$pids" ]; then
        echo "   ✅ No $process_name processes found"
        return 0
    fi
    
    echo "   📋 Found PIDs: $pids"
    
    # Try SIGTERM first
    echo "   📤 Sending SIGTERM..."
    pkill -TERM -f "$process_name" 2>/dev/null
    
    # Wait for graceful shutdown
    for i in {1..10}; do
        if ! pgrep -f "$process_name" >/dev/null 2>&1; then
            echo "   ✅ $process_name stopped gracefully"
            return 0
        fi
        echo "   ⏳ Waiting... ($i/10)"
        sleep 1
    done
    
    # Force kill if still running
    echo "   💥 Force killing $process_name..."
    pkill -KILL -f "$process_name" 2>/dev/null
    sleep 2
    
    # Check if still running
    if pgrep -f "$process_name" >/dev/null 2>&1; then
        echo "   ❌ Failed to stop $process_name"
        return 1
    else
        echo "   ✅ $process_name force stopped"
        return 0
    fi
}

# Function to clean zombie processes
clean_zombies() {
    echo "🧹 Cleaning zombie processes..."
    zombies=$(ps aux | grep '[airflow worker ] <defunct>' | awk '{print $2}' 2>/dev/null)
    if [ -n "$zombies" ]; then
        echo "   📋 Found zombie PIDs: $zombies"
        # Kill parent processes to clean zombies
        for pid in $zombies; do
            ppid=$(ps -o ppid= -p $pid 2>/dev/null | tr -d ' ')
            if [ -n "$ppid" ] && [ "$ppid" != "1" ]; then
                echo "   💀 Killing parent PID: $ppid"
                kill -KILL $ppid 2>/dev/null
            fi
        done
        sleep 2
    else
        echo "   ✅ No zombie processes found"
    fi
}

# Stop Airflow Task Supervisors and Runners first
echo "📋 Stopping running tasks..."
kill_with_timeout "airflow task supervisor"
kill_with_timeout "airflow task runner"

# Stop Airflow Workers
echo "👷 Stopping Airflow Workers..."
kill_with_timeout "airflow worker"

# Stop Airflow Executor
echo "⚙️ Stopping Airflow Executor..."
kill_with_timeout "airflow executor"

# Stop Airflow Scheduler & DagFileProcessor
echo "📅 Stopping Airflow Scheduler..."
kill_with_timeout "airflow scheduler"
kill_with_timeout "DagFileProcessor"

# Stop Airflow Webserver
echo "🌐 Stopping Airflow Webserver..."
kill_with_timeout "airflow webserver"
kill_with_timeout "gunicorn.*airflow"

# Clean zombie processes
clean_zombies

# Stop all remaining airflow processes
echo "🔄 Final cleanup - stopping any remaining Airflow processes..."
remaining_pids=$(pgrep -f "airflow" 2>/dev/null)
if [ -n "$remaining_pids" ]; then
    echo "   📋 Remaining PIDs: $remaining_pids"
    echo "   💥 Force killing remaining processes..."
    pkill -KILL -f "airflow" 2>/dev/null
    sleep 2
fi

# Stop Spark if running
if [[ -n "$SPARK_HOME" ]] && command -v $SPARK_HOME/sbin/stop-master.sh &> /dev/null; then
    echo "⚡ Stopping Spark cluster..."
    $SPARK_HOME/sbin/stop-worker.sh 2>/dev/null
    $SPARK_HOME/sbin/stop-master.sh 2>/dev/null
fi

# Final verification
echo ""
echo "🔍 VERIFICATION"
echo "==============="
remaining=$(ps aux | grep airflow | grep -v grep | wc -l)
if [ "$remaining" -eq 0 ]; then
    echo "✅ All Airflow processes stopped successfully!"
else
    echo "❌ Warning: $remaining Airflow processes still running:"
    ps aux | grep airflow | grep -v grep
    echo ""
    echo "💡 You may need to manually kill these processes:"
    echo "   sudo kill -9 \$(pgrep -f airflow)"
fi

echo ""
echo "📊 Service Status:"
echo "   - Airflow Webserver: Stopped"
echo "   - Airflow Scheduler: Stopped"
echo "   - Airflow Executor: Stopped"
echo "   - Airflow Workers: Stopped"
if [[ -n "$SPARK_HOME" ]] && command -v $SPARK_HOME/sbin/stop-master.sh &> /dev/null; then
    echo "   - Spark Master: Stopped"
    echo "   - Spark Worker: Stopped"
fi
echo "   - PostgreSQL: Still running (use 'sudo service postgresql stop' to stop)"
echo ""
echo "🚀 To restart services, run: ./start_airflow.sh"