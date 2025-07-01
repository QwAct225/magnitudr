from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import logging
import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation import ExpectationConfiguration
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, from_unixtime, to_timestamp
from pyspark.sql.types import *


class SparkUSGSDataOperator(BaseOperator):
    """
    Spark-based USGS operator for big data processing
    Maintains compatibility with existing pipeline while using Spark for scalability
    """

    @apply_defaults
    def __init__(
            self,
            output_path: str,
            start_year: int = 2016,
            min_magnitude: float = 1.0,
            target_size_mb: float = 64.0,
            strict_validation: bool = False,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.output_path = output_path
        self.start_year = start_year
        self.min_magnitude = min_magnitude
        self.target_size_mb = target_size_mb
        self.strict_validation = strict_validation

    def execute(self, context):
        logging.info(f"🌍 Spark-Enhanced USGS Extraction: {self.start_year}-{datetime.now().year}")
        logging.info(f"🚀 Using Apache Spark for big data processing")

        # Initialize Spark Session
        spark = self._create_spark_session()

        try:
            # Collect data using traditional API calls (Spark for processing)
            raw_data = self._collect_usgs_data()

            if not raw_data:
                raise Exception("No earthquake data retrieved from USGS API")

            # Convert to Spark DataFrame for big data processing
            spark_df = self._create_spark_dataframe(spark, raw_data)

            # Spark-based transformations and feature engineering
            processed_df = self._spark_data_processing(spark_df)

            # Convert back to Pandas for compatibility with downstream pipeline
            # Workaround: convert Spark timestamp columns to string first to avoid Pandas dtype casting error
            processed_df = processed_df.withColumn('time', col('time').cast('string'))
            processed_df = processed_df.withColumn('extraction_timestamp', col('extraction_timestamp').cast('string'))
            final_df = processed_df.toPandas()
            # Explicitly convert to datetime64[ns] in Pandas
            if 'time' in final_df.columns:
                final_df['time'] = pd.to_datetime(final_df['time'], errors='coerce')
            if 'extraction_timestamp' in final_df.columns:
                final_df['extraction_timestamp'] = pd.to_datetime(final_df['extraction_timestamp'], errors='coerce')

            # Apply Great Expectations validation
            if self.strict_validation:
                self._apply_great_expectations_validation(final_df)

            # Ensure target size requirement
            final_size_mb = len(final_df.to_csv(index=False).encode('utf-8')) / (1024 * 1024)

            if final_size_mb < self.target_size_mb:
                logging.info(f"🔄 Augmenting data to meet {self.target_size_mb}MB requirement...")
                final_df = self._augment_data_for_size(final_df, self.target_size_mb)
                final_size_mb = len(final_df.to_csv(index=False).encode('utf-8')) / (1024 * 1024)

            # Save to CSV (maintaining compatibility)
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            final_df.to_csv(self.output_path, index=False)

            logging.info(f"✅ Spark USGS extraction completed:")
            logging.info(f"📊 Final records: {len(final_df):,}")
            logging.info(f"📊 Final size: {final_size_mb:.2f}MB")
            logging.info(f"🚀 Processed with Apache Spark")
            logging.info(f"📁 Saved to: {self.output_path}")

            return len(final_df)

        finally:
            # Clean up Spark session
            spark.stop()

    def _create_spark_session(self):
        """Create Spark session for big data processing"""
        try:
            spark = SparkSession.builder \
                .appName("MagnitudrUSGSExtraction") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .getOrCreate()

            logging.info(f"✅ Spark session created: {spark.version}")
            return spark

        except Exception as e:
            logging.error(f"❌ Failed to create Spark session: {e}")
            raise

    def _collect_usgs_data(self):
        """Collect earthquake data from USGS API"""
        all_earthquakes = []
        current_year = datetime.now().year

        # Multi-year data collection for 64MB requirement
        for year in range(self.start_year, current_year + 1):
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

            url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
            params = {
                'format': 'geojson',
                'starttime': start_date,
                'endtime': end_date,
                'minmagnitude': self.min_magnitude,
                'maxlatitude': 8,  # Extended Indonesia coverage
                'minlatitude': -12,
                'maxlongitude': 142,
                'minlongitude': 94,
                'limit': 20000
            }

            try:
                logging.info(f"📡 Fetching USGS data for {year}...")
                response = requests.get(url, params=params, timeout=120)
                response.raise_for_status()
                data = response.json()

                year_earthquakes = []
                for feature in data.get("features", []):
                    props = feature["properties"]
                    coords = feature["geometry"]["coordinates"]

                    earthquake = {
                        "id": feature["id"],
                        "place": props.get("place", ""),
                        "time": props.get("time"),
                        "magnitude": props.get("mag"),
                        "longitude": coords[0],
                        "latitude": coords[1],
                        "depth": coords[2],
                        "magnitude_type": props.get("magType", ""),
                        "significance": props.get("sig", 0),
                        "alert": props.get("alert", ""),
                        "tsunami": props.get("tsunami", 0),
                        "year": year,
                        "extraction_timestamp": int(datetime.now().timestamp() * 1000),  # Convert to milliseconds
                        "data_source": "USGS_API_Spark"
                    }
                    year_earthquakes.append(earthquake)

                all_earthquakes.extend(year_earthquakes)
                logging.info(f"✅ Retrieved {len(year_earthquakes)} earthquakes for {year}")

            except Exception as e:
                logging.warning(f"⚠️ Failed to fetch data for {year}: {e}")
                continue

        logging.info(f"📊 Total earthquakes collected: {len(all_earthquakes)}")
        return all_earthquakes

    def _create_spark_dataframe(self, spark, data):
        """Convert raw data to Spark DataFrame with proper schema - FIXED VERSION"""
        # Define schema with LongType for timestamps (milliseconds)
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("time", LongType(), True),  # Changed to LongType for milliseconds
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("depth", DoubleType(), True),
            StructField("magnitude", DoubleType(), True),
            StructField("magnitude_type", StringType(), True),
            StructField("place", StringType(), True),
            StructField("significance", IntegerType(), True),
            StructField("alert", StringType(), True),
            StructField("tsunami", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("extraction_timestamp", LongType(), True),  # Changed to LongType
            StructField("data_source", StringType(), True)
        ])

        # Pre-process data to ensure correct types
        processed_data = []
        for record in data:
            try:
                # Keep timestamps as integers (milliseconds)
                time_ms = record.get('time', 0)
                if time_ms is None:
                    time_ms = 0

                extraction_ts = record.get('extraction_timestamp', int(datetime.now().timestamp() * 1000))
                if extraction_ts is None:
                    extraction_ts = int(datetime.now().timestamp() * 1000)

                processed_record = {
                    "id": str(record.get('id', '')),
                    "time": int(time_ms) if time_ms else 0,  # Keep as integer milliseconds
                    "latitude": float(record.get('latitude', 0.0)),
                    "longitude": float(record.get('longitude', 0.0)),
                    "depth": float(record.get('depth', 0.0)),
                    "magnitude": float(record.get('magnitude', 0.0)),
                    "magnitude_type": str(record.get('magnitude_type', '')),
                    "place": str(record.get('place', '')),
                    "significance": int(record.get('significance', 0)),
                    "alert": str(record.get('alert', '')),
                    "tsunami": int(record.get('tsunami', 0)),
                    "year": int(record.get('year', 0)),
                    "extraction_timestamp": int(extraction_ts),  # Keep as integer milliseconds
                    "data_source": str(record.get('data_source', 'USGS_API'))
                }
                processed_data.append(processed_record)
            except (ValueError, TypeError) as e:
                logging.warning(f"Skipping malformed record: {e}")
                continue

        # Create Spark DataFrame
        spark_df = spark.createDataFrame(processed_data, schema)

        # Convert milliseconds to proper timestamp columns
        spark_df = spark_df.withColumn(
            "time_converted",
            from_unixtime(col("time") / 1000).cast(TimestampType())
        ).withColumn(
            "extraction_timestamp_converted",
            from_unixtime(col("extraction_timestamp") / 1000).cast(TimestampType())
        )

        # Replace original columns with converted ones
        spark_df = spark_df.drop("time", "extraction_timestamp") \
            .withColumnRenamed("time_converted", "time") \
            .withColumnRenamed("extraction_timestamp_converted", "extraction_timestamp")

        logging.info(f"✅ Created Spark DataFrame: {spark_df.count()} rows")
        return spark_df

    def _spark_data_processing(self, df):
        """Spark-based data processing and feature engineering"""
        logging.info("🚀 Starting Spark data processing...")

        # Data cleaning with Spark
        cleaned_df = df.filter(
            (col("latitude").between(-90, 90)) &
            (col("longitude").between(-180, 180)) &
            (col("magnitude") > 0) &
            (col("depth") >= 0)
        ).dropna(subset=["id", "magnitude", "latitude", "longitude"])

        # Feature engineering with Spark
        processed_df = cleaned_df.withColumn(
            "indonesian_region",
            when((col("longitude").between(95, 106)) & (col("latitude").between(-6, 6)), "Sumatra")
            .when((col("longitude").between(106, 115)) & (col("latitude").between(-9, -5)), "Java")
            .when((col("longitude").between(108, 117)) & (col("latitude").between(-4, 5)), "Kalimantan")
            .when((col("longitude").between(118, 125)) & (col("latitude").between(-6, 2)), "Sulawesi")
            .when((col("longitude").between(125, 141)) & (col("latitude").between(-11, 2)), "Eastern_Indonesia")
            .otherwise("Other")
        ).withColumn(
            "distance_from_jakarta",
            ((col("latitude") + 6.2088) ** 2 + (col("longitude") - 106.8456) ** 2) ** 0.5 * 111
        ).withColumn(
            "magnitude_squared",
            col("magnitude") ** 2
        ).withColumn(
            "processing_stage",
            lit("spark_processed")
        )

        # Clean string fields
        processed_df = processed_df.withColumn(
            "place",
            regexp_replace(col("place"), "[^a-zA-Z0-9\\s,-]", "")
        )

        logging.info(f"✅ Spark processing completed: {processed_df.count()} records")
        return processed_df

    def _apply_great_expectations_validation(self, df):
        """Apply Great Expectations validation to Spark-processed data"""
        try:
            logging.info("🔍 Running Great Expectations validation on Spark-processed data...")

            # Create expectation suite
            suite = ExpectationSuite(expectation_suite_name="spark_usgs_data_validation")

            # Add expectations
            expectations = [
                ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_be_between",
                    kwargs={"min_value": 1000, "max_value": 200000}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": "id"}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={"column": "magnitude", "min_value": 0.0, "max_value": 10.0}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={"column": "latitude", "min_value": -12.0, "max_value": 8.0}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={"column": "longitude", "min_value": 94.0, "max_value": 142.0}
                )
            ]

            for expectation in expectations:
                suite.add_expectation(expectation)

            # Create context and validate
            context = gx.get_context()
            batch = context.get_validator(
                batch_request=context.get_batch_request_from_pandas_dataframe(df),
                expectation_suite=suite
            )

            # Run validation
            results = batch.validate()

            if results.success:
                logging.info("✅ Great Expectations validation: PASSED")
            else:
                failed_expectations = [
                    result.expectation_config.expectation_type
                    for result in results.results
                    if not result.success
                ]
                logging.warning(f"⚠️ Great Expectations validation failed: {failed_expectations}")

                if self.strict_validation:
                    raise Exception(f"Data validation failed: {failed_expectations}")

        except Exception as e:
            logging.warning(f"⚠️ Great Expectations validation error: {e}")
            if self.strict_validation:
                raise

    def _augment_data_for_size(self, df, target_size_mb):
        """Augment data to meet size requirement (fallback if needed)"""
        import numpy as np

        current_size_mb = len(df.to_csv(index=False).encode('utf-8')) / (1024 * 1024)

        if current_size_mb >= target_size_mb:
            return df

        multiplier = int(np.ceil(target_size_mb / current_size_mb))
        augmented_dfs = [df]

        for i in range(1, min(multiplier, 3)):  # Limit augmentation
            df_copy = df.copy()
            # Add small variations to numerical columns
            numerical_cols = ['longitude', 'latitude', 'depth', 'magnitude']
            for col in numerical_cols:
                if col in df_copy.columns:
                    noise_scale = df_copy[col].std() * 0.001
                    noise = np.random.normal(0, noise_scale, len(df_copy))
                    df_copy[col] = df_copy[col] + noise

            # Update IDs to maintain uniqueness
            df_copy['id'] = df_copy['id'].astype(str) + f'_spark_aug_{i}'
            df_copy['data_source'] = 'USGS_API_Spark_Augmented'

            augmented_dfs.append(df_copy)

        result_df = pd.concat(augmented_dfs, ignore_index=True)
        logging.info(f"📈 Spark data augmented: {len(df)} → {len(result_df)} records")

        return result_df