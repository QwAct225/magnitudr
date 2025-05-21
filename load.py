# load.py
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("DB_BATCH_TABLE")

def load_to_postgres(df: pd.DataFrame):
    try:
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        df.to_sql(TABLE_NAME, engine, index=False, if_exists='replace')
        print(f"✅ Data berhasil dimuat ke tabel '{TABLE_NAME}' di database '{DB_NAME}'")
    except Exception as e:
        print("❌ Gagal menyimpan ke database:", e)
