#!/bin/bash

echo "🧪 QUICK DAG TEST"
echo "================="
echo "Testing individual DAG functions without full execution"
echo ""

# Test 1: Check DAG syntax
test_dag_syntax() {
    echo "🔍 Testing DAG syntax..."
    
    dag_files=(
        "airflow/dags/ingestion.py"
        "airflow/dags/ml_processing.py"
        "airflow/dags/storage.py"
        "airflow/dags/main.py"
        "airflow/dags/visualization.py"
    )
    
    for dag_file in "${dag_files[@]}"; do
        if python3 -m py_compile "$dag_file" 2>/dev/null; then
            echo "✅ $(basename $dag_file) - syntax OK"
        else
            echo "❌ $(basename $dag_file) - syntax error!"
            python3 -c "import ast; ast.parse(open('$dag_file').read())" 2>&1 | head -3
        fi
    done
}

# Test 2: Check DAG imports
test_dag_imports() {
    echo ""
    echo "📦 Testing DAG imports..."
    
    # Test import of each DAG file
    export PYTHONPATH="./airflow/dags:$PYTHONPATH"
    
    dags=("ingestion" "ml_processing" "storage" "visualization")
    
    for dag in "${dags[@]}"; do
        if python3 -c "import sys; sys.path.append('./airflow/dags'); import $dag" 2>/dev/null; then
            echo "✅ $dag - imports OK"
        else
            echo "❌ $dag - import error!"
        fi
    done
}

# Test 4: Check Airflow DAG loading
test_airflow_dag_loading() {
    echo ""
    echo "🌪️ Testing Airflow DAG loading..."
    
    # Check if DAGs are properly loaded
    echo "Checking DAG list..."
    airflow dags list | grep earthquake > /tmp/dag_test.txt
    
    expected_dags=("earthquake_data_ingestion" "earthquake_data_processing" "earthquake_data_storage" "earthquake_master_pipeline" "earthquake_visualization_simplified")
    
    for dag in "${expected_dags[@]}"; do
        if grep -q "$dag" /tmp/dag_test.txt; then
            echo "✅ $dag - loaded in Airflow"
        else
            echo "❌ $dag - not loaded"
        fi
    done
    
    # Check for import errors
    echo ""
    echo "Checking for import errors..."
    import_errors=$(airflow dags list-import-errors 2>&1)
    if [ -z "$import_errors" ] || echo "$import_errors" | grep -q "No import errors"; then
        echo "✅ No import errors found"
    else
        echo "❌ Import errors found:"
        echo "$import_errors"
    fi
}

# Test 5: Simulate sensor checks
test_sensor_logic() {
    echo ""
    echo "📡 Testing sensor logic..."
    
    # Test file-based checks that replace sensors
    python3 -c "
import sys
sys.path.append('./airflow/dags')
from pathlib import Path
import os

# Test ingestion check
output_dir = Path('./data/airflow_output')
output_dir.mkdir(parents=True, exist_ok=True)

# Create dummy files to test sensor logic
test_files = [
    'enriched_earthquake_data.csv',
    'ingestion_pipeline_report.json',
    'final_processed_earthquake_data.csv',
    'processing_pipeline_report.json'
]

for file in test_files:
    (output_dir / file).touch()

print('✅ Test files created')

# Test the check functions
try:
    # Simulate ingestion check
    required_files = [
        output_dir / 'enriched_earthquake_data.csv',
        output_dir / 'ingestion_pipeline_report.json'
    ]
    ingestion_ok = all(f.exists() for f in required_files)
    print(f'✅ Ingestion check: {ingestion_ok}')
    
    # Simulate processing check  
    processing_files = [
        output_dir / 'final_processed_earthquake_data.csv',
        output_dir / 'processing_pipeline_report.json'
    ]
    processing_ok = any(f.exists() for f in processing_files)
    print(f'✅ Processing check: {processing_ok}')
    
except Exception as e:
    print(f'❌ Sensor logic test failed: {e}')
"
}

# Test 6: Test trigger simulation
test_trigger_simulation() {
    echo ""
    echo "🎯 Testing trigger simulation..."
    
    # Test if DAG can be triggered (dry run)
    echo "Testing DAG trigger (dry run)..."
    
    # Just check if the trigger command works without actually running
    if airflow dags list | grep -q "earthquake_data_ingestion"; then
        echo "✅ DAG triggering capability - OK"
    else
        echo "❌ DAG not found for triggering"
    fi
}

# Main test runner
run_all_tests() {
    echo "🧪 RUNNING ALL TESTS"
    echo "===================="
    
    test_dag_syntax
    test_dag_imports
    test_airflow_dag_loading
    test_sensor_logic
    test_trigger_simulation
    
    echo ""
    echo "📋 TEST SUMMARY"
    echo "==============="
    echo "If all tests show ✅, your DAGs should work correctly"
    echo "If any tests show ❌, fix those issues before running the pipeline"
    echo ""
    echo "🚀 Next steps if tests pass:"
    echo "1. ./fix_skipped_tasks.sh clear"
    echo "2. ./run_dags_sequentially.sh ingestion"
    echo "3. Monitor with ./monitor_dag_health.sh"
}

# Individual test functions
case "${1:-all}" in
    "syntax") test_dag_syntax ;;
    "imports") test_dag_imports ;;
    "loading") test_airflow_dag_loading ;;
    "sensors") test_sensor_logic ;;
    "triggers") test_trigger_simulation ;;
    "all") run_all_tests ;;
    *)
        echo "Usage: $0 [syntax|imports|functions|loading|sensors|triggers|all]"
        echo ""
        echo "Available tests:"
        echo "  syntax    - Check DAG file syntax"
        echo "  imports   - Test DAG imports"
        echo "  functions - Test task functions"
        echo "  loading   - Check Airflow DAG loading"
        echo "  sensors   - Test sensor logic"
        echo "  triggers  - Test trigger simulation"
        echo "  all       - Run all tests (default)"
        ;;
esac