#!/bin/bash

# Create directories
mkdir -p /opt/airflow/data/raw
mkdir -p /opt/airflow/data/processed

# Set up Airflow connection to PostgreSQL
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host "$DB_HOST" \
    --conn-port "$DB_PORT" \
    --conn-login "$DB_USER" \
    --conn-password "$DB_PASSWORD" \
    --conn-schema "$DB_NAME"

psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "
CREATE TABLE IF NOT EXISTS $DB_PIPELINE_TABLE (
    id VARCHAR(50) PRIMARY KEY,
    magnitude FLOAT,
    place TEXT,
    time TIMESTAMP WITH TIME ZONE,
    longitude FLOAT,
    latitude FLOAT,
    depth FLOAT,
    depth_category VARCHAR(20)
);"

echo "Initialization complete!"