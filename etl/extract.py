# etl/extract.py

import requests
import pandas as pd

def extract_earthquake_data():
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": "2024-01-01",
        "minmagnitude": 4
    }

    response = requests.get(url, params=params)
    data = response.json()

    records = []
    for feature in data["features"]:
        props = feature["properties"]
        records.append({
            "Date": pd.to_datetime(props["time"], unit="ms"),
            "Location": props["place"],
            "Magnitude": props["mag"]
        })

    df = pd.DataFrame(records)
    return df
