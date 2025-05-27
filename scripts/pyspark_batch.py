import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import findspark

load_dotenv()
findspark.init()


def extract_data():
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
    response = requests.get(url)
    data = response.json()

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
    return earthquakes


def create_spark_df(earthquakes):
    spark = SparkSession.builder.appName("EarthquakeData").getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("magnitude", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("depth", DoubleType(), True)
    ])

    return spark.createDataFrame(earthquakes, schema=schema)


def transform_data(df):
    # Filter out earthquakes outside the specified region (Indonesia)
    df = df.filter(
        (col("longitude").between(95, 141)) &
        (col("latitude").between(-11, 6))
    )

    # Convert depth to category
    df = df.withColumn(
        "depth_category",
        when(col("depth") <= 70, "Shallow")
        .when((col("depth") > 70) & (col("depth") <= 300), "Intermediate")
        .otherwise("Deep")
    )

    # Convert time to datetime and localize to Asia/Jakarta timezone
    df = df.withColumn("time_utc", to_timestamp(from_unixtime(col("time") / 1000)))

    df_pd = df.toPandas()
    df_pd["time"] = df_pd["time_utc"].dt.tz_localize("UTC").dt.tz_convert("Asia/Jakarta")
    df_pd.drop(columns=["time_utc"], inplace=True)

    return df_pd


def load_data(df_pd):
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    df_pd.to_sql(os.getenv("DB_BATCH_TABLE"), engine, if_exists="append", index=False)

def show_head(df_pd):
    print(df_pd.head(15))

def main():
    try:
        earthquakes = extract_data()
        df = create_spark_df(earthquakes)
        df_pd = transform_data(df)
        load_data(df_pd)
        show_head(df_pd)
        print("✅ Data berhasil disimpan ke PostgreSQL.")
    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")


if __name__ == "__main__":
    main()