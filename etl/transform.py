# etl/transform.py

def transform_data(df: pd.DataFrame) -> pd.DataFrame: # type: ignore
    # 1. Filter gempa dengan magnitudo > 4.0
    df = df[df['Magnitude'] > 4.0]

    # 2. Hapus duplikat (berdasarkan tanggal dan lokasi)
    df = df.drop_duplicates(subset=['Date', 'Location'])

    # 3. Buat kolom baru 'Region' dari lokasi (misal split by comma)
    df['Region'] = df['Location'].apply(lambda x: x.split(',')[-1].strip() if ',' in x else 'Unknown')

    return df
