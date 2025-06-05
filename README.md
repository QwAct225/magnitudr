# MagnitudR - Earthquake Data Analysis

This repository contains a data analysis project for earthquake data from USGS, focusing on the Indonesian region.

## Environment and Technology

### System Requirements
- **Operating System**: Windows 10/11 (or Linux/MacOS)
- **Python Version**: 3.11.*
- **Package Manager**: pip 25.0.1

## Project Structure
```
magnitudr/
│
├── data/                      # Folder data hasil ETL, input, output, dan laporan
│   ├── cluster_statistics.csv
│   ├── clustering_data.csv
│   ├── earthquake_clusters.csv
│   ├── earthquake_enhanced.csv
│   ├── hazard_zone_centers.csv
│   ├── bigdata/               # (subfolder untuk data bigdata)
│   ├── plots/                 # (subfolder untuk hasil visualisasi)
│   ├── reports/               # (subfolder untuk laporan validasi, dsb)
│   └── spark_output/          # (subfolder untuk output Spark)
│
├── lib/                       # Library eksternal/manual
│   ├── postgresql-42.7.5.jar  # JDBC driver PostgreSQL
│   └── spark-3.4.0-bin-hadoop3.tgz # Distribusi Spark
│
├── notebook/                  # Notebook Jupyter untuk eksplorasi & prototipe
│   └── crud_operations.ipynb
│
├── scripts/                   # Script Python utama (ETL, Spark, validasi, dsb)
│   ├── run_etl_pipeline.py
│   ├── run_spark_evaluation.py
│   ├── run_big_data_collection.py
│   ├── run_spark_ml_processing.py
│   └── run_dbscan_clustering.py
│
├── .env                       # Konfigurasi environment (DB, API key, dsb)
├── .gitignore                 # File/folder yang diabaikan git
├── README.md                  # Dokumentasi utama proyek
└── requirements.txt           # Daftar dependencies Python
```

## Installation and Setup

### Clone the Repository
```bash
git clone https://github.com/yourusername/magnitudr.git
cd magnitudr
```

### Virtual Environment Setup
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/MacOS:
# source venv/bin/activate
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Data Collection
The project collects earthquake data from the USGS API (https://earthquake.usgs.gov/fdsnws/event/1/query) in GeoJSON format.

### Data Processing
The data is processed to:
1. Filter earthquakes within Indonesian geographic coordinates (longitude: 95-141, latitude: -11-6)
2. Categorize earthquakes by depth (Shallow: 0-70km, Intermediate: 70-300km, Deep: 300-700km)
3. Convert timestamps to Asia/Jakarta timezone for local analysis

### Analysis Features
- Geographic distribution of earthquakes
- Magnitude analysis
- Depth categorization
- Time-based analysis

### Usage
Open the `notebook/experiment.ipynb` file in Jupyter Notebook or JupyterLab to see the data analysis process.

```bash
# Locate directory
cd magnitudr

# Exceute ETL Implementation (Extract, Transform, Load) and Batching
python scripts/run_etl_pipeline.py
```

```bash
# Visualization Procesed Data With DBSCAN Clustering
python scripts/run_dbscan_clustering.py
```

### Data Sources
- USGS Earthquake API: https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson

### Dependencies
- requests 2.32.3
- pandas 2.2.3
- sqlalchemy 2.0.40
- psycopg2-binary 2.9.10
- python-dotenv 1.1.0
- numpy 2.2.5
- python-dateutil 2.9.0.post0
- pytz 2025.2
- tzdata 2025.2

## Spark Earthquake Data Analysis Setup Guide

This guide provides step-by-step instructions for setting up Apache Spark for big data earthquake analysis with the Magnitudr project.

### Prerequisites

- Operating System: Ubuntu/Debian (Recommended), Windows, or macOS
- RAM: Minimum 8GB (16GB recommended)
- Disk Space: At least 10GB free space
- Internet connection for downloading dependencies (Hadoop, PySpark, winutils)

### STEP 1: Environment Setup

#### 1.1 Java Installation (Required for Spark)

#### Ubuntu/Debian (Recommended):
```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

#### Windows:
```powershell
# Using Chocolatey
choco install openjdk17
```

#### macOS:
```bash
brew install openjdk@17
```

#### Verify Java Installation:
```bash
java -version
# Expected output: openjdk version "17.x.x"
```

#### 1.2 Spark Installation

```bash
# Download Spark 3.4.0
wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz

# Extract and move to /opt
tar -xzf spark-3.4.0-bin-hadoop3.tgz
sudo mv spark-3.4.0-bin-hadoop3 /opt/spark

# Set ownership (Linux/Mac)
sudo chown -R $USER:$USER /opt/spark
```

#### 1.3 Hadoop Environment Configuration 

1. Lakukan instlasi [hadoop](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz) dan [winutils](https://github.com/steveloughran/winutils)
2. Extract tar hadoop dan simpan di directory yang praktis
3. Simpan winutils.exe
    ```
    C:\hadoop\bin\winutils.exe
    ```
4. Tekan "win + r" dan search "SystemPropertiesAdvanced" 
5. Environment Veriables > User Variables + System Variables
   ```bash
   # New Variable
   Variable name: HADOOP_HOME
   Variable value: C:\hadoop

   # Path System Variables
   %HADOOP_HOME%\bin
   ``` 
6. Masuk ke Path
   ```   
   C:\hadoop\etc\hadoop\hadoop-env.cmd
   ```
7. Edit pada baris 'set JAVA_HOME=%JAVA_HOME%' ubah path ke lokasi instalasi openJDK Java 
    ```bash
    set JAVA_HOME=C:\Progra~1\Java\jdk-19

    # C:\Program Files -> C:\Progra~1
    ```
8. Open CMD mode admid dan ketik
    ```
    hadoop version
    ```


#### 1.4 Database Configuration

Create `.env` file in project root:
```bash
# PostgreSQL Configuration
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=earthquake_db
```

### STEP 2: Spark Cluster Configuration
**Note**: Penggunaan terminal sangat disarankan untuk menggunakan linux, terutama dalam melakukan testing pengelolaan data menggunakan Spark.
#### 2.1 Configure Spark Master

```bash
cd /opt/spark/conf

# Copy configuration templates
cp spark-env.sh.template spark-env.sh
cp spark-defaults.conf.template spark-defaults.conf

# Configure spark-env.sh
nano spark-env.sh

# Taruh atau paste di paling ahir file
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_MASTER_HOST=localhost
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081

source spark-env.sh

# Configure spark-defaults.conf
nano spark-defaults.conf

# Tarus atau paste di paling akhir file
spark.master spark://localhost:7077
spark.executor.memory 4g
spark.executor.cores 2
spark.driver.memory 4g
spark.sql.adaptive.enabled true

source spark-defaults.conf
```

#### 2.2 Start Spark Cluster
```bash
# Start Master
export SPARK_HOME=/opt/spark
$SPARK_HOME/sbin/start-master.sh

# Start Worker(s)
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

# Verify cluster
# Access Web UI: http://localhost:8080
curl http://localhost:8080 # Spark Master
curl http://localhost:8081 # Spark Worker

# Stop
$SPARK_HOME/sbin/stop-master.sh # Stop master
$SPARK_HOME/sbin/stop-worker.sh # Stop worker
``` 
### STEP 3: Big Data Collection

#### 3.1 Execute Data Collection
```bash
# Run big data collection script
cd magnitudr

python scripts/run_big_data_collection.py # Windows
python3 scripts/run_big_data_collection.py # Linux

# Expected output:
# - earthquake_bigdata.csv (64MB+)
# - earthquake_bigdata.parquet
# - earthquake_bigdata.json
# - earthquake_bigdata_metadata.json
```

#### 3.2 Verify Data Collection
```bash
# Check file sizes
ls -lh ./data/bigdata/
# Should show files >= 64MB

# Verify record count
head -1 ./data/bigdata/earthquake_bigdata.csv | tr ',' '\n' | wc -l
# Should show ~30+ columns with enhanced features
```

### STEP 4: Spark ETL Execution

#### 4.1 Run Batch Processing
```bash
# Execute Spark ETL pipeline
cd magnitudr

python3 scripts/run_spark_ml_processing.py # Linux (Recommended)

# Monitor progress:
# - Spark UI: http://localhost:4040
# - Master UI: http://localhost:8080

# Bila terjadi error midway coba lakukan ini
sudo service postgresql status
# Bila down maka bisa melakukan ini
sudo service postgresql start

# Lakukan ini untuk pembuatan database
# Masuk ke postgresql sebagai superuser
psql -U postgres
# Ganti password user
ALTER USER postgres WITH PASSWORD 'password_baru_anda';
# Keluar dari psql
\q

# Setelah reset bisa langsung masuk sebagai User
psql -U postgres -h localhost -W
# Buat database
CREATE DATABASE magnitudr;

# CREATE DATABASE nama_database;    -- Membuat database baru
# \l                -- Melihat daftar database
# \c earthquake     -- Masuk ke database earthquake
# \dt               -- Melihat tabel di database tersebut
# SELECT * FROM nama_table;         -- Menampilkan seluruh isi data dalam table
# DROP TABLE nama_table;            -- Menghapus table
# \q                -- Keluar shell
```

### STEP 5: Validation & Testing

#### 5.1 Run Complete Validation
```bash
# Execute validation suite
python3 scripts/run_spark_evaluation.py

# Generates:
# - Batching with PySpark and Hadoop
# - Data quality validation report
# - Performance metrics
# - Query execution results
# - Visualization dashboard

```