import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime
import warnings
from dateutil.relativedelta import relativedelta
warnings.filterwarnings('ignore')

def extract_earthquake_data(start_date, end_date, batch_days=180):
    """
    Extract earthquake data from USGS API in batches per 6 months (180 days).
    Returns list of all earthquakes.
    """
    earthquakes = []
    current_start = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    while current_start < end_date:
        current_end = min(current_start + pd.Timedelta(days=batch_days), end_date)

        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            'format': 'geojson',
            'starttime': current_start.strftime('%Y-%m-%d'),
            'endtime': current_end.strftime('%Y-%m-%d'),
            'minmagnitude': 3.0,
            'maxlatitude': 6,
            'minlatitude': -11,
            'maxlongitude': 141,
            'minlongitude': 95
        }
        print(f"📥 Fetching data: {params['starttime']} to {params['endtime']}")
        response = requests.get(url, params=params)
        data = response.json()

        earthquakes = []
        for feature in data.get("features", []):
            properties = feature["properties"]
            geometry = feature["geometry"]
            earthquake = {
                "id": feature["id"],
                "place": properties.get("place", ""),
                "time": properties.get("time"),
                "updated": properties.get("updated"),
                "longitude": geometry["coordinates"][0],
                "latitude": geometry["coordinates"][1],
                "depth": geometry["coordinates"][2],
                "magnitude": properties.get("mag"),
                "magnitude_type": properties.get("magType", ""),
                "felt": properties.get("felt"),
                "cdi": properties.get("cdi"),
                "mmi": properties.get("mmi"),
                "significance": properties.get("sig"),
                "alert": properties.get("alert", ""),
                "tsunami": properties.get("tsunami", 0),
                "status": properties.get("status", ""),
                "gap": properties.get("gap"),
                "rms": properties.get("rms"),
                "dmin": properties.get("dmin"),
                "nst": properties.get("nst"),
                "net": properties.get("net", ""),
                "type": properties.get("type", "")
            }
            earthquakes.append(earthquake)

        if earthquakes:
            print(f"🔄 Processing {len(earthquakes)} records for {params['starttime']} to {params['endtime']}")
            df_batch = transform_earthquake_data(earthquakes)
            # Batching DB per bulan:
            df_batch['month_batch'] = df_batch['datetime_local'].dt.to_period('M')
            for period, batch in df_batch.groupby('month_batch'):
                print(f"🚚 Saving batch for month: {period}")
                load_enhanced_data(batch)  # Simpan per bulan
        else:
            print(f"⚠️  No data for {params['starttime']} to {params['endtime']}")

        earthquakes.extend(earthquakes)
        current_start = current_end

    return earthquakes

def transform_earthquake_data(earthquakes):
    """
    Transform earthquake data for seismic hazard analysis
    Based on seismological research parameters
    """
    df = pd.DataFrame(earthquakes)
    
    # 1. TEMPORAL TRANSFORMATIONS
    df['datetime'] = pd.to_datetime(df['time'], unit='ms', utc=True)
    df['datetime_local'] = df['datetime'].dt.tz_convert('Asia/Jakarta')
    df['year'] = df['datetime_local'].dt.year
    df['month'] = df['datetime_local'].dt.month
    df['hour'] = df['datetime_local'].dt.hour
    
    # 2. ENHANCED DEPTH CATEGORIZATION (Based on Indonesian seismology)
    # Reference: Indonesian seismic zones typically classified as:
    # - Very Shallow: 0-30km (most dangerous for surface damage)
    # - Shallow: 30-70km 
    # - Intermediate: 70-300km
    # - Deep: 300-700km
    df['depth_category'] = pd.cut(
        df['depth'],
        bins=[0, 30, 70, 300, 700],
        labels=['Very Shallow', 'Shallow', 'Intermediate', 'Deep'],
        include_lowest=True
    )
    
    # 3. MAGNITUDE-BASED HAZARD CLASSIFICATION
    # Based on Richter scale and Indonesian building codes
    df['magnitude_hazard'] = pd.cut(
        df['magnitude'],
        bins=[0, 4.0, 5.0, 6.0, 7.0, 10.0],
        labels=['Low', 'Moderate', 'Strong', 'Major', 'Great'],
        include_lowest=True
    )
    
    # 4. INTENSITY-BASED RISK ASSESSMENT
    # Combine multiple intensity measures for comprehensive risk score
    df['intensity_score'] = 0
    
    # Add scores based on different intensity measures
    # Felt reports (population impact)
    df.loc[df['felt'] >= 1000, 'intensity_score'] += 3
    df.loc[(df['felt'] >= 100) & (df['felt'] < 1000), 'intensity_score'] += 2
    df.loc[(df['felt'] >= 10) & (df['felt'] < 100), 'intensity_score'] += 1
    
    # Community Decimal Intensity
    df.loc[df['cdi'] >= 8, 'intensity_score'] += 4
    df.loc[(df['cdi'] >= 6) & (df['cdi'] < 8), 'intensity_score'] += 3
    df.loc[(df['cdi'] >= 4) & (df['cdi'] < 6), 'intensity_score'] += 2
    df.loc[(df['cdi'] >= 2) & (df['cdi'] < 4), 'intensity_score'] += 1
    
    # Modified Mercalli Intensity
    df.loc[df['mmi'] >= 8, 'intensity_score'] += 4
    df.loc[(df['mmi'] >= 6) & (df['mmi'] < 8), 'intensity_score'] += 3
    df.loc[(df['mmi'] >= 4) & (df['mmi'] < 6), 'intensity_score'] += 2
    df.loc[(df['mmi'] >= 2) & (df['mmi'] < 4), 'intensity_score'] += 1
    
    # 5. ALERT LEVEL ENCODING
    alert_mapping = {'green': 1, 'yellow': 2, 'orange': 3, 'red': 4, '': 0}
    df['alert_level'] = df['alert'].map(alert_mapping).fillna(0)
    
    # 6. REGIONAL CLASSIFICATION
    # Classify earthquakes by Indonesian geological regions
    def classify_region(row):
        lon, lat = row['longitude'], row['latitude']
        
        # Sumatra region (high seismic activity)
        if 95 <= lon <= 106 and -6 <= lat <= 6:
            return 'Sumatra'
        # Java region (densely populated, high risk)
        elif 106 <= lon <= 115 and -9 <= lat <= -5:
            return 'Java'
        # Kalimantan (lower seismic activity)
        elif 108 <= lon <= 117 and -4 <= lat <= 5:
            return 'Kalimantan'
        # Sulawesi (complex tectonics)
        elif 118 <= lon <= 125 and -6 <= lat <= 2:
            return 'Sulawesi'
        # Eastern Indonesia (high seismic activity)
        elif 125 <= lon <= 141 and -11 <= lat <= 2:
            return 'Eastern_Indonesia'
        else:
            return 'Other'
    
    df['region'] = df.apply(classify_region, axis=1)
    
    # 7. COMPOSITE HAZARD SCORE
    # Create comprehensive hazard score for clustering analysis
    df['hazard_score'] = (
        (df['magnitude'].fillna(0) * 2) +  # Magnitude weight: 2
        (df['intensity_score'] * 1.5) +   # Intensity weight: 1.5
        (df['alert_level'] * 1) +         # Alert weight: 1
        (df['tsunami'] * 3) +             # Tsunami weight: 3
        (np.where(df['depth'] <= 30, 2, 0))  # Very shallow bonus: 2
    )
    
    # 8. DATA QUALITY INDICATORS
    df['data_quality'] = 'Good'
    df.loc[df['gap'] > 180, 'data_quality'] = 'Poor'  # Large azimuthal gap
    df.loc[df['rms'] > 1.0, 'data_quality'] = 'Poor'  # High RMS error
    df.loc[df['nst'] < 4, 'data_quality'] = 'Poor'    # Few stations
    
    # 9. CLUSTERING PREPARATION FEATURES
    # Features specifically for DBSCAN clustering
    clustering_features = [
        'longitude', 'latitude', 'depth', 
        'magnitude', 'hazard_score', 'intensity_score'
    ]
    
    # Normalize features for clustering (will be used in next step)
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    df[['lon_norm', 'lat_norm', 'depth_norm', 'mag_norm', 'hazard_norm', 'intensity_norm']] = \
        scaler.fit_transform(df[clustering_features].fillna(0))
    
    # 10. FILTER AND CLEAN DATA
    # Remove records with poor data quality for clustering analysis
    df_clean = df[df['data_quality'] == 'Good'].copy()
    
    # Remove outliers (optional)
    Q1 = df_clean['magnitude'].quantile(0.25)
    Q3 = df_clean['magnitude'].quantile(0.75)
    IQR = Q3 - Q1
    df_clean = df_clean[
        (df_clean['magnitude'] >= Q1 - 1.5 * IQR) & 
        (df_clean['magnitude'] <= Q3 + 1.5 * IQR)
    ]
    
    return df_clean

def load_enhanced_data(df):
    """
    Load enhanced earthquake data to PostgreSQL (append mode)
    """
    load_dotenv()
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    table_name = "earthquakes_enhanced"

    important_cols = [
        'id', 'place', 'datetime_local', 'latitude', 'longitude', 'region', 'depth', 'magnitude', 
        'tsunami', 'depth_category', 'alert_level', 'magnitude_hazard', 'hazard_score', 'intensity_score'
    ]
    df_to_save = df[important_cols].copy()
    df_to_save['datetime_local'] = pd.to_datetime(df_to_save['datetime_local'])

    df_to_save.to_sql(
        table_name, engine,
        if_exists="append",  # Selalu append!
        index=False
    )

    print(f"\n✅ Enhanced earthquake data loaded to table: {table_name} (batched per 1 months)")
    print(f"📊 Total records: {len(df_to_save)}")
    print(f"📅 Date range: {df_to_save['datetime_local'].min()} to {df_to_save['datetime_local'].max()}")
    print(f"🌏 Regions covered: {df_to_save['region'].value_counts().to_dict()}")
    print(f"⚠️  Hazard distribution: {df_to_save['magnitude_hazard'].value_counts().to_dict()}")

    return engine, table_name

def prepare_clustering_data(df):
    """
    Prepare data specifically for DBSCAN clustering analysis
    """
    # Select features for clustering
    clustering_features = [
        'lon_norm', 'lat_norm', 'depth_norm', 
        'mag_norm', 'hazard_norm', 'intensity_norm'
    ]
    
    clustering_data = df[clustering_features].fillna(0)
    
    print("\n🔍 Clustering Data Preparation:")
    print(f"Features for clustering: {clustering_features}")
    print(f"Data shape: {clustering_data.shape}")
    print(f"Data range after normalization:")
    print(clustering_data.describe())
    
    return clustering_data, clustering_features

def main():
    """
    Main ETL pipeline for enhanced earthquake hazard analysis
    """
    print("🌍 Starting Enhanced Earthquake ETL Pipeline...")
    print("=" * 60)
    
    try:
        # 1. EXTRACT
        print("\n📥 EXTRACTING earthquake data from USGS (6-month API batching, monthly DB batching)...")
        earthquakes = extract_earthquake_data("2025-01-01", "2025-06-30", batch_days=180)
        print(f"✅ Extracted {len(earthquakes)} earthquake records in total")
        
        # 2. TRANSFORM  
        print("\n🔄 TRANSFORMING data for seismic hazard analysis...")
        df_transformed = transform_earthquake_data(earthquakes)
        print(f"✅ Transformed data: {df_transformed.shape}")
        
        # 3. LOAD
        print("\n📤 LOADING enhanced data to PostgreSQL...")
        engine, table_name = load_enhanced_data(df_transformed)
        
        # 4. PREPARE FOR CLUSTERING
        print("\n🎯 PREPARING data for DBSCAN clustering...")
        clustering_data, features = prepare_clustering_data(df_transformed)
        
        # Save clustering data for next step
        clustering_data.to_csv('./data/clustering_data.csv', index=False)
        
        # Hanya simpan kolom penting ke earthquake_enhanced.csv
        important_cols = [
            'id', 'place', 'datetime_local', 'latitude', 'longitude', 'region', 'depth', 'magnitude', 
            'tsunami', 'depth_category', 'alert_level', 'magnitude_hazard', 'hazard_score', 'intensity_score'
        ]
        df_transformed[important_cols].to_csv('./data/earthquake_enhanced.csv', index=False)
        
        print("\n🎉 ETL Pipeline completed successfully!")
        print("📁 Files saved:")
        print("   - earthquake_enhanced.csv (selected columns only)")
        print("   - clustering_data.csv (normalized features for DBSCAN)")
        print(f"   - PostgreSQL table: {table_name}")
        
        # Display sample of enhanced data
        print("\n📋 Sample of enhanced earthquake data:")
        print(df_transformed.head(10))
        
        return df_transformed, clustering_data
        
    except Exception as e:
        print(f"❌ ERROR in ETL Pipeline: {e}")
        raise e

if __name__ == "__main__":
    df_enhanced, clustering_data = main()