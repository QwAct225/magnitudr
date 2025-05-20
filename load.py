etl/load.py # type: ignore
def save_to_csv(df, filepath="data/target/earthquakes.csv"):
    df.to_csv(filepath, index=False)