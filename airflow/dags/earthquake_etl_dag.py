import os
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_unixtime
import sys

# Add the scripts directory to the path
sys.path.append('/opt/airflow/scripts')
from transform_utils import filter_indonesia_region, categorize_depth, convert_timezone, clean_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 1),
}

# Create the DAG
dag = DAG(
    'earthquake_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for earthquake data from USGS API',
    schedule_interval=timedelta(days=1),
    catchup=False,
)


# Task 1: Extract data from USGS API
def extract_data(**kwargs):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
    response = requests.get(url)
    data = response.json()

    # Save raw data
    raw_data_path = f"/opt/airflow/data/raw/earthquake_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(raw_data_path, 'w') as f:
        json.dump(data, f)

    # Return the file path for the next task
    return raw_data_path


# Task 2: Transform data with Pandas
def transform_data_pandas(**kwargs):
    ti = kwargs['ti']
    raw_data_path = ti.xcom_pull(task_ids='extract_data')

    # Load data
    with open(raw_data_path, 'r') as f:
        data = json.load(f)

    # Extract earthquake data
    earthquakes = []
    for feature in data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]
        earthquakes.append({
            "id": feature["id"],
            "magnitude": properties["mag"],
            "place": properties["place"],
            "time": properties["time"],
            "longitude": geometry["coordinates"][0],
            "latitude": geometry["coordinates"][1],
            "depth": geometry["coordinates"][2]
        })

    # Convert to DataFrame
    df = pd.DataFrame(earthquakes)

    # Apply transformations
    df = filter_indonesia_region(df)
    df = categorize_depth(df)
    df = convert_timezone(df)
    df = clean_data(df)

    # Save transformed data
    transformed_data_path = f"/opt/airflow/data/processed/transformed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(transformed_data_path, index=False)

    return transformed_data_path


# Task 3: Transform data with PySpark
def transform_data_spark(**kwargs):
    ti = kwargs['ti']
    raw_data_path = ti.xcom_pull(task_ids='extract_data')

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("EarthquakeTransformation") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.5.jar") \
        .getOrCreate()

    # Load data
    with open(raw_data_path, 'r') as f:
        data = json.load(f)

    # Extract earthquake data
    earthquakes = []
    for feature in data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]
        earthquakes.append({
            "id": str(feature["id"]),
            "magnitude": float(properties["mag"]) if properties["mag"] is not None else None,
            "place": str(properties["place"]),
            "time": int(properties["time"]),
            "longitude": float(geometry["coordinates"][0]),
            "latitude": float(geometry["coordinates"][1]),
            "depth": float(geometry["coordinates"][2])
        })

    # Create Spark DataFrame
    df = spark.createDataFrame(earthquakes)

    # Apply transformations
    # 1. Filter Indonesia region
    df = df.filter(
        (col("longitude").between(95, 141)) &
        (col("latitude").between(-11, 6))
    )

    # 2. Categorize depth
    df = df.withColumn(
        "depth_category",
        when(col("depth") <= 70, "Shallow")
        .when((col("depth") > 70) & (col("depth") <= 300), "Intermediate")
        .otherwise("Deep")
    )

    # 3. Convert to Pandas for timezone conversion
    # (PySpark doesn't handle timezones as easily)
    pandas_df = df.toPandas()
    pandas_df["time"] = pd.to_datetime(pandas_df["time"], unit="ms")
    pandas_df["time"] = pandas_df["time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Jakarta")

    # Save transformed data
    transformed_spark_path = f"/opt/airflow/data/processed/transformed_spark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    pandas_df.to_csv(transformed_spark_path, index=False)

    # Stop Spark session
    spark.stop()

    return transformed_spark_path


# Task 4: Create database table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS earthquakes_batch_pipeline (
        id VARCHAR(50) PRIMARY KEY,
        magnitude FLOAT,
        place TEXT,
        time TIMESTAMP WITH TIME ZONE,
        longitude FLOAT,
        latitude FLOAT,
        depth FLOAT,
        depth_category VARCHAR(20)
    )
    """,
    dag=dag,
)


# Task 5: Load data to PostgreSQL
def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    transformed_data_path = ti.xcom_pull(task_ids='transform_data_spark')

    # Read CSV
    df = pd.read_csv(transformed_data_path)

    # Convert time to datetime
    df['time'] = pd.to_datetime(df['time'])

    # Get PostgreSQL hook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()

    # Load data
    table_name = os.environ.get('DB_BATCH_TABLE', 'earthquakes_batch_pipeline')
    df.to_sql(
        table_name,
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )

    # Count rows
    row_count = len(df)
    print(f"Loaded {row_count} rows to PostgreSQL table {table_name}")

    return row_count


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_pandas_task = PythonOperator(
    task_id='transform_data_pandas',
    python_callable=transform_data_pandas,
    dag=dag,
)

transform_spark_task = PythonOperator(
    task_id='transform_data_spark',
    python_callable=transform_data_spark,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# Define task dependencies
extract_task >> [transform_pandas_task, transform_spark_task]
transform_spark_task >> create_table >> load_task