#!/bin/bash

echo "🗄️ SETTING UP AIRFLOW DATABASE"
echo "==============================="

# Load environment variables
source .env

# 1. Create Airflow database and user in PostgreSQL
echo "📊 Creating Airflow database..."

# Drop existing roles/databases to avoid conflicts
sudo -u postgres psql << EOL
REASSIGN OWNED BY airflow_user TO postgres;
DROP OWNED BY airflow_user;
DROP DATABASE airflow_db;
DROP USER airflow_user;
EOL

# Create database and user
sudo -u postgres psql << EOL
-- Create Airflow user
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';

-- Create Airflow database
CREATE DATABASE airflow_db OWNER airflow_user;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

-- Connect to airflow_db and grant schema privileges
\c airflow_db
GRANT ALL ON SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow_user;

\q
EOL

echo "✅ Airflow database created"

# 2. Initialize Airflow database
echo "🔧 Initializing Airflow database..."
export AIRFLOW_HOME=~/airflow
airflow db init

# Fix invalid sensor timeout configuration
echo "🔧 Fixing sensor timeout configuration..."
sed -i "s/default_timeout = .*/default_timeout = 604800/" ~/airflow/airflow.cfg

echo "✅ Airflow database initialized"

# 3. Create Airflow admin user
echo "👤 Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@magnitudr.com \
    --password admin123

echo "✅ Airflow admin user created"
echo ""
echo "🔑 Login credentials:"
echo "   Username: admin"
echo "   Password: admin123"
echo ""
echo "🎯 Next step: Run ./start_airflow.sh"