# etl/extract.py
import requests
import pandas as pd

def extract_earthquake_data():
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson"
    response = requests.get(url)
    data = response.json()

    features = data["features"]
    records = []
    for quake in features:
        prop = quake["properties"]
        geometry = quake["geometry"]
        records.append({
            "place": prop["place"],
            "mag": prop["mag"],
            "time": prop["time"],
            "felt": prop["felt"],
            "tsunami": prop["tsunami"],
            "longitude": geometry["coordinates"][0],
            "latitude": geometry["coordinates"][1],
            "depth": geometry["coordinates"][2],
        })

    df = pd.DataFrame(records)
    return df
