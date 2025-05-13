import os
import subprocess
from pathlib import Path
import sys
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

requirements_path = Path.cwd() / "requirements.txt"

try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", str(requirements_path)])
    print("✅ Semua paket berhasil diinstal.")
except subprocess.CalledProcessError as e:
    print(f"❌ Instalasi gagal: {e}")

print("Password:", os.getenv("DB_PASSWORD"))

os.environ["DB_PASSWORD"] = "chandrapiw2406"

print("User:", os.getenv("DB_USER"))
print("Password:", os.getenv("DB_PASSWORD"))
print("Host:", os.getenv("DB_HOST"))
print("Port:", os.getenv("DB_PORT"))
print("Database:", os.getenv("DB_NAME"))    

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


spark = SparkSession.builder \
    .appName("EarthquakeData") \
    .getOrCreate()

# Definisikan schema secara eksplisit
schema = StructType([
    StructField("id", StringType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("depth", DoubleType(), True)
])

# Buat Spark DataFrame
df = spark.createDataFrame(earthquakes, schema=schema)

df.show(5, truncate=False)

# Asumsikan `df` sudah berupa Spark DataFrame dari hasil API
# Filtering lokasi Indonesia (longitude & latitude)
df = df.filter(
    (col("longitude").between(95, 141)) &
    (col("latitude").between(-11, 6))
)

# Menambahkan kolom kategori kedalaman
df = df.withColumn(
    "depth_category",
    when(col("depth") <= 70, "Shallow")
    .when((col("depth") > 70) & (col("depth") <= 300), "Intermediate")
    .otherwise("Deep")
)

# Konversi waktu dari epoch milisecond ke timestamp dan ubah ke zona waktu Jakarta
# from_unixtime di Spark pakai detik, jadi bagi 1000
df = df.withColumn("time_utc", to_timestamp(from_unixtime(col("time") / 1000)))

# Spark tidak punya fungsi built-in `tz_convert`, jadi convert ke pandas lalu simpan
df_pd = df.toPandas()
df_pd["time"] = df_pd["time_utc"].dt.tz_localize("UTC").dt.tz_convert("Asia/Jakarta")
df_pd.drop(columns=["time_utc"], inplace=True)

# Simpan ke PostgreSQL
try:
    # Tentukan koneksi ke PostgreSQL
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:chandrapiw2406@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    # Simpan DataFrame ke tabel 'earthquakes' di PostgreSQL
    df_pd.to_sql("earthquakes", engine, if_exists="append", index=False)
    print("✅ Data berhasil disimpan ke PostgreSQL.")

except Exception as e:
    print(f"❌ Terjadi kesalahan: {e}")

# Ambil kembali semua data untuk pengecekan
df_all = pd.read_sql("SELECT * FROM earthquakes", engine)
df_all.head(30)