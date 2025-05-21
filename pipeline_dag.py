# pipeline_dag.py

from etl.extract import extract_earthquake_data

def run_pipeline():
    print("🔄 Starting ETL pipeline...")

    # Extract
    print("📥 Extracting data...")
    df = extract_earthquake_data()
    print(f"✅ Extracted {len(df)} earthquake records")

    # Display the first 5 rows
    print(df.head())

if __name__ == "__main__":
    run_pipeline()