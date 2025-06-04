"""
STEP 3: Spark ETL Pipeline for Big Data Earthquake Processing
Includes: Batch Processing, Stream Processing, ML Preprocessing
Requirements: 3+ transformations, Load to database, ML-ready output
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd
import numpy as np

# Spark imports
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, isnan, isnull, regexp_replace, split,
    from_unixtime, to_timestamp, year, month, dayofmonth, hour,
    sqrt, pow, log, exp, sin, cos, radians, degrees,
    mean, stddev, min as spark_min, max as spark_max,
    percentile_approx, count, sum as spark_sum,
    udf, broadcast, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, LongType, BooleanType, TimestampType
)

# ML imports
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, MinMaxScaler, 
    PCA, StringIndexer, OneHotEncoder, Bucketizer,
    QuantileDiscretizer
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation

# Database imports
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SparkEarthquakeETL:
    """
    Big Data ETL Pipeline for Earthquake Analysis using Spark
    Supports both Batch and Stream processing
    """
    
    def __init__(self, master_url="spark://localhost:7077", app_name="EarthquakeETL"):
        """Initialize Spark ETL Pipeline"""
        self.master_url = master_url
        self.app_name = app_name
        self.spark = None
        
        # Paths
        self.data_dir = Path('./data/bigdata')
        self.output_dir = Path('./data/spark_output')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # ML configurations
        self.feature_columns = []
        self.ml_pipeline = None
        
        # Load environment variables
        load_dotenv()
        
    def initialize_spark(self, executor_memory="4g", executor_cores="2", driver_memory="4g"):
        """Initialize Spark Session with cluster configuration"""
        logger.info(f"Initializing Spark session: {self.app_name}")
        logger.info(f"Master URL: {self.master_url}")
        
        try:
            # Path ke JDBC driver PostgreSQL (pastikan file ada di lokasi ini)
            postgres_jar = str(Path("lib/postgresql-42.7.5.jar").resolve())
            
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master_url) \
                .config("spark.executor.memory", executor_memory) \
                .config("spark.executor.cores", executor_cores) \
                .config("spark.driver.memory", driver_memory) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.jars", postgres_jar) \
                .getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("✅ Spark session initialized successfully")
            logger.info(f"Spark UI: {self.spark.sparkContext.uiWebUrl}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark: {e}")
            return False
    
    def extract_data(self, file_path=None, file_format='parquet'):
        """
        EXTRACT: Load big data from various sources
        Supports CSV, Parquet, JSON formats
        """
        logger.info("🔄 EXTRACT: Loading earthquake big data...")
        
        if file_path is None:
            file_path = self.data_dir / f'earthquake_bigdata.{file_format}'
        
        try:
            if file_format.lower() == 'parquet':
                df = self.spark.read.parquet(str(file_path))
            elif file_format.lower() == 'csv':
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(str(file_path))
            elif file_format.lower() == 'json':
                df = self.spark.read.json(str(file_path))
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            logger.info(f"✅ Data loaded successfully from {file_path}")
            logger.info(f"📊 Schema:")
            df.printSchema()
            logger.info(f"📊 Record count: {df.count():,}")
            logger.info(f"📊 Partition count: {df.rdd.getNumPartitions()}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading data from {file_path}: {e}")
            return None
    
    def transform_data_cleaning(self, df):
        """
        TRANSFORM 1: Data Cleaning and Quality Improvement
        """
        logger.info("🔄 TRANSFORM 1: Data Cleaning...")
        
        # Count initial records
        initial_count = df.count()
        logger.info(f"Initial record count: {initial_count:,}")
        
        # 1. Remove duplicates based on id
        df_clean = df.dropDuplicates(['id'])
        logger.info(f"Removed {initial_count - df_clean.count():,} duplicate records")
        
        # 2. Filter out invalid coordinates
        df_clean = df_clean.filter(
            (col("latitude").between(-90, 90)) &
            (col("longitude").between(-180, 180)) &
            (col("depth") >= 0) &
            (col("magnitude") > 0)
        )
        
        # 3. Handle missing values
        # Fill missing numerical values with median
        numeric_columns = ['magnitude', 'depth', 'felt', 'cdi', 'mmi', 'sig', 'nst', 'dmin', 'rms', 'gap']
        for column in numeric_columns:
            if column in df_clean.columns:
                median_val = df_clean.approxQuantile(column, [0.5], 0.01)[0]
                df_clean = df_clean.fillna({column: median_val})
        
        # Fill missing categorical values
        categorical_columns = ['alert', 'status', 'magType', 'type', 'net']
        for column in categorical_columns:
            if column in df_clean.columns:
                df_clean = df_clean.fillna({column: 'unknown'})
        
        # 4. Clean string columns
        string_columns = ['place', 'alert', 'status', 'magType', 'type']
        for column in string_columns:
            if column in df_clean.columns:
                df_clean = df_clean.withColumn(
                    column, 
                    regexp_replace(col(column), "[^a-zA-Z0-9\\s,.-]", "")
                )
        
        # 5. Standardize alert levels
        df_clean = df_clean.withColumn(
            "alert_standardized",
            when(col("alert").isin(["green", "GREEN"]), "green")
            .when(col("alert").isin(["yellow", "YELLOW"]), "yellow")
            .when(col("alert").isin(["orange", "ORANGE"]), "orange")
            .when(col("alert").isin(["red", "RED"]), "red")
            .otherwise("unknown")
        )
        
        final_count = df_clean.count()
        logger.info(f"✅ Data cleaning completed. Records: {final_count:,} (removed: {initial_count - final_count:,})")
        
        return df_clean
    
    def transform_feature_engineering(self, df):
        """
        TRANSFORM 2: Feature Engineering for ML
        """
        logger.info("🔄 TRANSFORM 2: Feature Engineering...")
        
        # 1. Temporal features from timestamp
        df_features = df.withColumn("timestamp_dt", to_timestamp(col("time") / 1000))
        df_features = df_features.withColumn("year", year("timestamp_dt"))
        df_features = df_features.withColumn("month", month("timestamp_dt"))
        df_features = df_features.withColumn("day", dayofmonth("timestamp_dt"))
        df_features = df_features.withColumn("hour", hour("timestamp_dt"))
        
        # 2. Magnitude categories
        df_features = df_features.withColumn(
            "magnitude_category",
            when(col("magnitude") < 3.0, "micro")
            .when(col("magnitude") < 4.0, "minor")
            .when(col("magnitude") < 5.0, "light")
            .when(col("magnitude") < 6.0, "moderate")
            .when(col("magnitude") < 7.0, "strong")
            .when(col("magnitude") < 8.0, "major")
            .otherwise("great")
        )
        
        # 3. Depth categories (enhanced)
        df_features = df_features.withColumn(
            "depth_category_detailed",
            when(col("depth") <= 10, "very_shallow")
            .when(col("depth") <= 30, "shallow")
            .when(col("depth") <= 70, "intermediate_shallow")
            .when(col("depth") <= 150, "intermediate")
            .when(col("depth") <= 300, "intermediate_deep")
            .when(col("depth") <= 500, "deep")
            .otherwise("very_deep")
        )
        
        # 4. Geographic zones
        df_features = df_features.withColumn(
            "geographic_zone",
            when((col("longitude").between(95, 141)) & (col("latitude").between(-11, 6)), "indonesia")
            .when((col("longitude").between(129, 146)) & (col("latitude").between(24, 46)), "japan")
            .when((col("longitude").between(116, 127)) & (col("latitude").between(5, 19)), "philippines")
            .when((col("longitude").between(-76, -66)) & (col("latitude").between(-56, -17)), "chile")
            .otherwise("other")
        )
        
        # 5. Distance from major fault lines (simplified calculation)
        # Ring of Fire proximity
        df_features = df_features.withColumn(
            "ring_of_fire_distance",
            sqrt(
                pow(col("longitude") - 140.0, 2) + 
                pow(col("latitude") - 35.0, 2)
            )
        )
        
        # 6. Energy release estimation
        df_features = df_features.withColumn(
            "energy_release_log",
            1.5 * col("magnitude") + 4.8
        )
        
        # 7. Risk score computation
        df_features = df_features.withColumn(
            "risk_score",
            (col("magnitude") * 0.4) +
            (when(col("depth") <= 50, 1.0).otherwise(0.5) * 0.3) +
            (when(col("alert_standardized") == "red", 1.0)
             .when(col("alert_standardized") == "orange", 0.7)
             .when(col("alert_standardized") == "yellow", 0.4)
             .otherwise(0.1) * 0.3)
        )
        
        # 8. Population impact estimation (simplified)
        df_features = df_features.withColumn(
            "population_impact_estimate",
            when(col("geographic_zone") == "indonesia", col("magnitude") * 1000)
            .when(col("geographic_zone") == "japan", col("magnitude") * 800)
            .when(col("geographic_zone") == "philippines", col("magnitude") * 600)
            .otherwise(col("magnitude") * 200)
        )
        
        # 9. Seasonal patterns
        df_features = df_features.withColumn(
            "season",
            when(col("month").isin([12, 1, 2]), "winter")
            .when(col("month").isin([3, 4, 5]), "spring")
            .when(col("month").isin([6, 7, 8]), "summer")
            .otherwise("autumn")
        )
        
        # 10. Tectonic activity indicators
        df_features = df_features.withColumn(
            "tectonic_activity_score",
            when(col("geographic_zone") == "indonesia", 0.95)
            .when(col("geographic_zone") == "japan", 0.90)
            .when(col("geographic_zone") == "philippines", 0.85)
            .when(col("geographic_zone") == "chile", 0.80)
            .otherwise(0.30)
        )
        
        logger.info("✅ Feature engineering completed")
        logger.info(f"📊 New features added: 10+ features")
        logger.info(f"📊 Total columns: {len(df_features.columns)}")
        
        return df_features
    
    def transform_ml_preprocessing(self, df):
        """
        TRANSFORM 3: Machine Learning Preprocessing
        """
        logger.info("🔄 TRANSFORM 3: ML Preprocessing...")
        
        # 1. Select features for ML
        numerical_features = [
            'magnitude', 'depth', 'latitude', 'longitude',
            'felt', 'cdi', 'mmi', 'sig', 'nst', 'dmin', 'rms', 'gap',
            'year', 'month', 'day', 'hour',
            'ring_of_fire_distance', 'energy_release_log', 'risk_score',
            'population_impact_estimate', 'tectonic_activity_score'
        ]
        
        categorical_features = [
            'alert_standardized', 'magnitude_category', 'depth_category_detailed',
            'geographic_zone', 'season', 'magType', 'type'
        ]
        
        # Filter existing columns
        numerical_features = [col for col in numerical_features if col in df.columns]
        categorical_features = [col for col in categorical_features if col in df.columns]
        
        logger.info(f"Numerical features: {len(numerical_features)}")
        logger.info(f"Categorical features: {len(categorical_features)}")
        
        # 2. Handle categorical variables with StringIndexer and OneHotEncoder
        indexers = []
        encoders = []
        encoded_cols = []

        for cat_col in categorical_features:
            # Cek jumlah nilai unik
            unique_count = df.select(cat_col).distinct().count()
            if unique_count < 2:
                logger.warning(f"Skipping categorical feature '{cat_col}' (only one unique value)")
                continue
            indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_index")
            encoder = OneHotEncoder(inputCol=f"{cat_col}_index", outputCol=f"{cat_col}_encoded")
            indexers.append(indexer)
            encoders.append(encoder)
            encoded_cols.append(f"{cat_col}_encoded")
        
        # 3. Create feature vector
        all_feature_cols = numerical_features + encoded_cols
        vector_assembler = VectorAssembler(
            inputCols=all_feature_cols,
            outputCol="features_raw"
        )
        
        # 4. Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features_scaled",
            withStd=True,
            withMean=True
        )
        
        # 5. Principal Component Analysis (optional, for dimensionality reduction)
        pca = PCA(
            k=10,  # Reduce to 10 principal components
            inputCol="features_scaled",
            outputCol="features_pca"
        )
        
        # 6. Create ML Pipeline
        pipeline_stages = indexers + encoders + [vector_assembler, scaler, pca]
        
        self.ml_pipeline = Pipeline(stages=pipeline_stages)
        
        # Fit and transform
        logger.info("Fitting ML preprocessing pipeline...")
        ml_model = self.ml_pipeline.fit(df)
        df_ml = ml_model.transform(df)
        
        # 7. Create bucketized features for certain analyses
        magnitude_bucketizer = Bucketizer(
            splits=[0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, float('inf')],
            inputCol="magnitude",
            outputCol="magnitude_bucket"
        )
        
        depth_bucketizer = Bucketizer(
            splits=[0, 10, 30, 70, 150, 300, 500, float('inf')],
            inputCol="depth",
            outputCol="depth_bucket"
        )
        
        df_ml = magnitude_bucketizer.transform(df_ml)
        df_ml = depth_bucketizer.transform(df_ml)
        
        # 8. Store feature column names for later use
        self.feature_columns = all_feature_cols
        
        logger.info("✅ ML preprocessing completed")
        logger.info(f"📊 Feature vector size: {len(all_feature_cols)}")
        logger.info(f"📊 PCA components: 10")
        
        return df_ml
    
    def transform_aggregations(self, df):
        """
        TRANSFORM 4: Statistical Aggregations and Windowing
        """
        logger.info("🔄 TRANSFORM 4: Statistical Aggregations...")
        
        # 1. Regional statistics
        regional_stats = df.groupBy("geographic_zone").agg(
            count("*").alias("earthquake_count"),
            mean("magnitude").alias("avg_magnitude"),
            stddev("magnitude").alias("std_magnitude"),
            spark_max("magnitude").alias("max_magnitude"),
            spark_min("magnitude").alias("min_magnitude"),
            mean("depth").alias("avg_depth"),
            mean("risk_score").alias("avg_risk_score")
        )
        
        # 2. Temporal aggregations
        temporal_stats = df.groupBy("year", "month").agg(
            count("*").alias("monthly_count"),
            mean("magnitude").alias("monthly_avg_magnitude"),
            spark_max("magnitude").alias("monthly_max_magnitude"),
            mean("risk_score").alias("monthly_avg_risk")
        )
        
        # 3. Magnitude category distribution
        magnitude_dist = df.groupBy("magnitude_category").agg(
            count("*").alias("category_count"),
            mean("depth").alias("avg_depth_by_magnitude")
        )
        
        # 4. Join aggregated statistics back to main dataframe
        df_with_regional = df.join(
            broadcast(regional_stats.select(
                "geographic_zone",
                col("avg_magnitude").alias("regional_avg_magnitude"),
                col("earthquake_count").alias("regional_earthquake_count")
            )),
            on="geographic_zone",
            how="left"
        )
        
        df_with_temporal = df_with_regional.join(
            temporal_stats.select(
                "year", "month",
                col("monthly_avg_magnitude").alias("monthly_regional_avg_magnitude")
            ),
            on=["year", "month"],
            how="left"
        )
        
        # 5. Create relative features
        df_final = df_with_temporal.withColumn(
            "magnitude_vs_regional_avg",
            col("magnitude") - col("regional_avg_magnitude")
        ).withColumn(
            "magnitude_vs_monthly_avg",
            col("magnitude") - col("monthly_regional_avg_magnitude")
        )
        
        logger.info("✅ Statistical aggregations completed")
        
        # Save intermediate statistics for analysis
        self._save_statistics(regional_stats, temporal_stats, magnitude_dist)
        
        return df_final
    
    def _save_statistics(self, regional_stats, temporal_stats, magnitude_dist):
        """Save aggregated statistics"""
        stats_dir = self.output_dir / 'statistics'
        stats_dir.mkdir(exist_ok=True)
        
        # Save as CSV files
        regional_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(stats_dir / "regional_stats"))
        temporal_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(stats_dir / "temporal_stats"))
        magnitude_dist.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(stats_dir / "magnitude_distribution"))
        
        logger.info(f"📊 Statistics saved to {stats_dir}")
    
    def load_to_database(self, df, table_name="earthquakes_spark_processed"):
        """
        LOAD: Save processed data to PostgreSQL database
        """
        logger.info("🔄 LOAD: Saving to database...")
        
        try:
            # Database connection parameters
            db_params = {
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'host': os.getenv('DB_HOST'),
                'port': os.getenv('DB_PORT'),
                'database': os.getenv('DB_NAME')
            }
            
            # JDBC URL for Spark
            jdbc_url = f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}"
            
            # Connection properties
            connection_properties = {
                "user": db_params['user'],
                "password": db_params['password'],
                "driver": "org.postgresql.Driver",
                "stringtype": "unspecified"
            }
            
            # Select columns for database (exclude complex types)
            simple_columns = [
                'id', 'magnitude', 'place', 'time', 'longitude', 'latitude', 'depth',
                'felt', 'cdi', 'mmi', 'alert_standardized', 'tsunami', 'sig',
                'year', 'month', 'day', 'hour',
                'magnitude_category', 'depth_category_detailed', 'geographic_zone',
                'risk_score', 'energy_release_log', 'population_impact_estimate',
                'tectonic_activity_score', 'ring_of_fire_distance',
                'magnitude_vs_regional_avg', 'magnitude_vs_monthly_avg'
            ]
            
            # Filter existing columns
            available_columns = [col for col in simple_columns if col in df.columns]
            df_to_save = df.select(*available_columns)
            
            # Write to database
            df_to_save.write \
                .jdbc(jdbc_url, table_name, "overwrite", connection_properties)
            
            record_count = df_to_save.count()
            logger.info(f"✅ Data saved to database table: {table_name}")
            logger.info(f"📊 Records saved: {record_count:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving to database: {e}")
            # Fallback: save as Parquet
            self._save_as_parquet(df, table_name)
            return False
    
    def _save_as_parquet(self, df, table_name):
        """Fallback: save as Parquet files"""
        output_path = self.output_dir / f"{table_name}.parquet"
        
        df.write.mode("overwrite").parquet(str(output_path))
        logger.info(f"💾 Data saved as Parquet: {output_path}")
    
    def run_ml_clustering(self, df):
        """
        Run K-Means clustering for earthquake pattern analysis
        """
        logger.info("🔄 Running ML Clustering Analysis...")
        
        # Use scaled features for clustering
        feature_col = "features_scaled"
        
        if feature_col not in df.columns:
            logger.warning("Features not found, skipping clustering")
            return df
        
        # Determine optimal number of clusters
        silhouette_scores = []
        K_values = range(2, 11)
        
        for k in K_values:
            kmeans = KMeans(k=k, featuresCol=feature_col, predictionCol="cluster")
            model = kmeans.fit(df)
            predictions = model.transform(df)
            
            evaluator = ClusteringEvaluator(featuresCol=feature_col, predictionCol="cluster")
            silhouette = evaluator.evaluate(predictions)
            silhouette_scores.append(silhouette)
            logger.info(f"K={k}, Silhouette Score: {silhouette:.4f}")
        
        # Choose best K
        optimal_k = K_values[np.argmax(silhouette_scores)]
        logger.info(f"Optimal number of clusters: {optimal_k}")
        
        # Final clustering
        kmeans_final = KMeans(k=optimal_k, featuresCol=feature_col, predictionCol="earthquake_cluster")
        clustering_model = kmeans_final.fit(df)
        df_clustered = clustering_model.transform(df)
        
        # Analyze clusters
        cluster_analysis = df_clustered.groupBy("earthquake_cluster").agg(
            count("*").alias("cluster_size"),
            mean("magnitude").alias("avg_magnitude"),
            mean("depth").alias("avg_depth"),
            mean("risk_score").alias("avg_risk_score")
        )
        
        cluster_analysis.show()
        
        logger.info("✅ ML clustering completed")
        return df_clustered
    
    def run_batch_processing(self, input_file=None):
        """
        Run complete batch processing pipeline
        """
        logger.info("🚀 Starting BATCH PROCESSING pipeline...")
        
        # Extract
        df_raw = self.extract_data(input_file)
        if df_raw is None:
            return None
        
        # Transform 1: Data Cleaning
        df_clean = self.transform_data_cleaning(df_raw)
        
        # Transform 2: Feature Engineering
        df_features = self.transform_feature_engineering(df_clean)
        
        # Transform 3: ML Preprocessing
        df_ml = self.transform_ml_preprocessing(df_features)
        
        # Transform 4: Aggregations
        df_final = self.transform_aggregations(df_ml)
        
        # ML Analysis
        df_clustered = self.run_ml_clustering(df_final)
        
        # Load
        success = self.load_to_database(df_clustered)
        
        # Save final processed data
        self._save_as_parquet(df_clustered, "earthquakes_final_processed")
        
        logger.info("✅ Batch processing completed successfully!")
        
        return df_clustered
    
    def run_stream_processing(self, input_path, checkpoint_location=None):
        """
        Run stream processing pipeline (for continuous data)
        """
        logger.info("🚀 Starting STREAM PROCESSING pipeline...")
        
        if checkpoint_location is None:
            checkpoint_location = str(self.output_dir / "checkpoints")
        
        try:
            # Read streaming data
            df_stream = self.spark.readStream \
                .format("json") \
                .option("path", input_path) \
                .load()
            
            # Apply transformations (simplified for streaming)
            df_transformed = df_stream.select(
                col("id"),
                col("properties.mag").alias("magnitude"),
                col("properties.place").alias("place"),
                col("properties.time").alias("time"),
                col("geometry.coordinates")[0].alias("longitude"),
                col("geometry.coordinates")[1].alias("latitude"),
                col("geometry.coordinates")[2].alias("depth")
            ).filter(
                (col("magnitude").isNotNull()) &
                (col("magnitude") > 0) &
                (col("latitude").between(-90, 90)) &
                (col("longitude").between(-180, 180))
            )
            
            # Add processing timestamp
            df_transformed = df_transformed.withColumn("processed_at", lit(datetime.now()))
            
            # Write stream to console (for monitoring)
            query = df_transformed.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .option("checkpointLocation", checkpoint_location) \
                .start()
            
            logger.info("✅ Stream processing started")
            logger.info(f"Monitor at: {self.spark.sparkContext.uiWebUrl}")
            
            return query
            
        except Exception as e:
            logger.error(f"❌ Stream processing error: {e}")
            return None
    
    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("🛑 Spark session stopped")

def main_batch():
    """Main function for batch processing"""
    print("🌍 SPARK ETL PIPELINE - BATCH PROCESSING")
    print("🎯 Big Data Earthquake Analysis with ML")
    print("=" * 60)
    
    # Initialize ETL pipeline
    etl = SparkEarthquakeETL()
    
    try:
        # Initialize Spark
        if not etl.initialize_spark():
            print("❌ Failed to initialize Spark!")
            return None
        
        # Run batch processing
        result = etl.run_batch_processing()
        
        if result:
            print("\n🎉 Batch processing completed successfully!")
            print(f"📊 Final record count: {result.count():,}")
            print(f"📁 Output saved to: {etl.output_dir}")
            print(f"🔍 Spark UI: {etl.spark.sparkContext.uiWebUrl}")
        else:
            print("❌ Batch processing failed!")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in batch processing: {e}")
        return None
    finally:
        etl.stop_spark()

def main_stream():
    """Main function for stream processing"""
    print("🌍 SPARK ETL PIPELINE - STREAM PROCESSING")
    print("🎯 Real-time Earthquake Data Processing")
    print("=" * 60)
    
    # Initialize ETL pipeline
    etl = SparkEarthquakeETL()
    
    try:
        # Initialize Spark
        if not etl.initialize_spark():
            print("❌ Failed to initialize Spark!")
            return None
        
        # Run stream processing
        input_path = "./data/streaming_input/"  # Directory for streaming files
        query = etl.run_stream_processing(input_path)
        
        if query:
            print("✅ Stream processing started!")
            print("📊 Monitoring streaming data...")
            print("Press Ctrl+C to stop")
            
            # Wait for termination
            query.awaitTermination()
        else:
            print("❌ Stream processing failed!")
        
        return query
        
    except KeyboardInterrupt:
        print("\n🛑 Stream processing stopped by user")
        return None
    except Exception as e:
        logger.error(f"❌ Error in stream processing: {e}")
        return None
    finally:
        etl.stop_spark()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "stream":
        # Run stream processing
        main_stream()
    else:
        # Run batch processing (default)
        main_batch()