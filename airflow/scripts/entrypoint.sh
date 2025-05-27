#!/bin/bash
set -e

# Initialize the Airflow database
airflow db init

# Create the admin user if it doesn't exist
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start the Airflow webserver in the background
airflow webserver -p 8080 &

# Start the Airflow scheduler
exec airflow scheduler