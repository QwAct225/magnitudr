# pipeline_dag.py
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from etl.extract import extract_earthquake_data  # type: ignore
from etl.transform import transform_data         # type: ignore

def main():
    print("🔍 Extracting data...")
    df_raw = extract_earthquake_data()
    print(f"✅ Extracted {len(df_raw)} rows")

    print("🛠 Transforming data...")
    df_clean = transform_data(df_raw)
    print(f"✅ Transformed into {len(df_clean)} cleaned rows")

    print("💾 Saving to 'earthquake_clean.csv'...")
    df_clean.to_csv("earthquake_clean.csv", index=False)
    print("🎉 Done!")

if __name__ == "__main__":
    main()
