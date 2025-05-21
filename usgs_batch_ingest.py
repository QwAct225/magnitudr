import os
import requests
import psycopg2
from dotenv import load_dotenv

# Load environment variable dari file .env
load_dotenv()

# Ambil variabel dari environment
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("DB_BATCH_TABLE")

# Ambil data dari USGS
url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
response = requests.get(url)
data = response.json()

# Buka koneksi ke PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# Buat tabel jika belum ada
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id TEXT PRIMARY KEY,
    place TEXT,
    magnitude FLOAT,
    time BIGINT,
    longitude FLOAT,
    latitude FLOAT,
    depth FLOAT
);
"""
cur.execute(create_table_query)

# Insert data
for feature in data["features"]:
    quake_id = feature["id"]
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]

    cur.execute(
        f"""INSERT INTO {TABLE_NAME} (id, place, magnitude, time, longitude, latitude, depth)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING""",
        (
            quake_id,
            props.get("place"),
            props.get("mag"),
            props.get("time"),
            coords[0],
            coords[1],
            coords[2]
        )
    )

# Commit dan tutup koneksi
conn.commit()
cur.close()
conn.close()

print(f"✅ Data gempa berhasil disimpan ke tabel '{TABLE_NAME}'")
