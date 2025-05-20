# etl/transform.py
import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Bersihkan nilai null dan ubah waktu ke datetime
    df_clean = df.dropna(subset=["mag", "time"])
    df_clean["time"] = pd.to_datetime(df_clean["time"], unit="ms")
    df_clean = df_clean[df_clean["mag"] >= 1.0]  # contoh filter

    return df_clean
