import pandas as pd
import pytz
from datetime import datetime


def filter_indonesia_region(df):
    """
    Filter earthquakes within Indonesian region (longitude: 95-141, latitude: -11-6)
    """
    return df[(df["longitude"].between(95, 141)) & (df["latitude"].between(-11, 6))]


def categorize_depth(df):
    """
    Categorize earthquake depths:
    - Shallow: 0-70 km
    - Intermediate: 70-300 km
    - Deep: 300-700 km
    """
    df["depth_category"] = pd.cut(
        df["depth"],
        bins=[0, 70, 300, 700],
        labels=["Shallow", "Intermediate", "Deep"]
    )
    return df


def convert_timezone(df):
    """
    Convert UTC timestamp to Asia/Jakarta timezone
    """
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df["time"] = df["time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Jakarta")
    return df


def clean_data(df):
    """
    Perform basic data cleaning operations
    """
    # Drop duplicates based on id
    df = df.drop_duplicates(subset=["id"])

    # Handle missing values in magnitude
    df["magnitude"] = df["magnitude"].fillna(0)

    return df