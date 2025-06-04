"""
STEP 4: Testing, Monitoring & Validation for Spark ETL Pipeline
Includes: Performance monitoring, Data validation, Query examples
"""

import os
import time
import psutil
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, when, current_timestamp, year, month

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SparkBatchProcessor:
    """Handles batch processing of earthquake data"""
    
    def __init__(self, batch_size=10000):
        self.batch_size = batch_size
        self.input_path = './data/bigdata/earthquake_bigdata.csv'
        load_dotenv()
        
        # Database connection settings
        self.db_props = {
            "driver": "org.postgresql.Driver",
            "url": f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASSWORD')
        }
        
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("EarthquakeBatchProcessor") \
            .config("spark.jars", "./lib/postgresql-42.7.5.jar") \
            .getOrCreate()
            
        logger.info("✅ Spark session initialized")

    def read_csv_in_batches(self):
        """Read CSV file in batches"""
        try:
            # Define schema for better performance
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("time", TimestampType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("depth", DoubleType(), True),
                StructField("magnitude", DoubleType(), True),
                StructField("place", StringType(), True)
            ])

            # Read the CSV file
            df = self.spark.read \
                .format("csv") \
                .option("header", "true") \
                .schema(schema) \
                .load(self.input_path)

            # Calculate number of batches
            total_count = df.count()
            num_batches = (total_count + self.batch_size - 1) // self.batch_size

            logger.info(f"📦 Total records: {total_count}")
            logger.info(f"📦 Number of batches: {num_batches}")

            return df, num_batches

        except Exception as e:
            logger.error(f"❌ Error reading CSV: {e}")
            raise

    def process_and_load_batch(self, batch_df, batch_num):
        """Process a single batch and load to PostgreSQL"""
        try:
            # Add processing timestamp
            batch_df = batch_df.withColumn("processed_at", current_timestamp())
            
            # Add derived columns
            batch_df = batch_df \
                .withColumn("year", year("time")) \
                .withColumn("month", month("time")) \
                .withColumn("magnitude_category", when(col("magnitude") >= 7, "Major")
                    .when(col("magnitude") >= 5, "Moderate")
                    .otherwise("Minor")) \
                .withColumn("geographic_zone", when(col("longitude") < 0, "Western Hemisphere").otherwise("Eastern Hemisphere")) \
                .withColumn("risk_score", (col("magnitude") / 10 + col("depth") / 700)) \
                .withColumn("depth_category_detailed", when(col("depth") < 70, "Shallow")
                    .when(col("depth") < 300, "Intermediate")
                    .otherwise("Deep")) \
                .withColumn("population_impact_estimate", (col("magnitude") * 1000 - col("depth") * 10)) \
                .withColumn("earthquake_cluster", (col("latitude") * 100 + col("longitude")).cast("int"))

            # Write batch to PostgreSQL
            batch_df.write \
                .format("jdbc") \
                .option("driver", self.db_props["driver"]) \
                .option("url", self.db_props["url"]) \
                .option("dbtable", "earthquakes_spark_processed") \
                .option("user", self.db_props["user"]) \
                .option("password", self.db_props["password"]) \
                .mode("append") \
                .save()

            logger.info(f"✅ Batch {batch_num} processed and loaded successfully")

        except Exception as e:
            logger.error(f"❌ Error processing batch {batch_num}: {e}")
            raise

    def run_batch_processing(self):
        """Run the complete batch processing pipeline"""
        try:
            logger.info("🚀 Starting batch processing pipeline")
            
            # Read data in batches
            df, num_batches = self.read_csv_in_batches()
            
            # Process each batch
            for i in range(num_batches):
                logger.info(f"Processing batch {i+1}/{num_batches}")
                
                # Get current batch
                start_idx = i * self.batch_size
                batch_df = df.limit(self.batch_size).offset(start_idx)
                
                # Process and load batch
                self.process_and_load_batch(batch_df, i+1)
                
            logger.info("✅ Batch processing completed successfully")
            
            # Stop Spark session
            self.spark.stop()
            
            return True

        except Exception as e:
            logger.error(f"❌ Batch processing failed: {e}")
            self.spark.stop()
            return False

class SparkETLValidator:
    """Validation and monitoring tools for Spark ETL pipeline"""
    
    def __init__(self):
        load_dotenv()
        self.output_dir = Path('./data/spark_output')
        self.reports_dir = Path('./data/reports')
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Database connection
        self.engine = None
        self._setup_database_connection()
        
        # Performance metrics
        self.performance_metrics = {}
        
    def _setup_database_connection(self):
        """Setup database connection for validation"""
        try:
            self.engine = create_engine(
                f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
    
    def monitor_spark_performance(self, spark_session, operation_name="spark_operation"):
        """Monitor Spark application performance"""
        logger.info(f"📊 Monitoring performance for: {operation_name}")
        
        start_time = time.time()
        start_memory = psutil.virtual_memory().percent
        start_cpu = psutil.cpu_percent()
        
        # Get Spark context metrics
        sc = spark_session.sparkContext
        
        metrics = {
            'operation_name': operation_name,
            'start_time': datetime.now().isoformat(),
            'spark_app_id': sc.applicationId,
            'spark_app_name': sc.appName,
            'master_url': sc.master,
            'executor_memory': sc.getConf().get('spark.executor.memory', 'default'),
            'executor_cores': sc.getConf().get('spark.executor.cores', 'default'),
            'driver_memory': sc.getConf().get('spark.driver.memory', 'default'),
            'num_executors': len(sc.statusTracker().getExecutorInfos()),
            'start_memory_percent': start_memory,
            'start_cpu_percent': start_cpu
        }
        
        self.performance_metrics[operation_name] = metrics
        
        def finalize_metrics():
            end_time = time.time()
            end_memory = psutil.virtual_memory().percent
            end_cpu = psutil.cpu_percent()
            
            metrics.update({
                'end_time': datetime.now().isoformat(),
                'duration_seconds': end_time - start_time,
                'end_memory_percent': end_memory,
                'end_cpu_percent': end_cpu,
                'memory_increase': end_memory - start_memory,
                'avg_cpu_usage': (start_cpu + end_cpu) / 2
            })
            
            logger.info(f"✅ Performance monitoring completed for {operation_name}")
            logger.info(f"Duration: {metrics['duration_seconds']:.2f} seconds")
            logger.info(f"Memory usage: {metrics['memory_increase']:+.1f}%")
            
            return metrics
        
        return finalize_metrics
    
    def validate_data_quality(self, table_name="earthquakes_spark_processed"):
        """Validate data quality in processed dataset"""
        logger.info(f"🔍 Validating data quality for table: {table_name}")
        
        validation_results = {
            'table_name': table_name,
            'validation_timestamp': datetime.now().isoformat(),
            'tests': {}
        }
        
        try:
            # Test 1: Record count
            count_query = f"SELECT COUNT(*) as record_count FROM {table_name}"
            record_count = pd.read_sql(count_query, self.engine).iloc[0]['record_count']
            validation_results['tests']['record_count'] = {
                'value': record_count,
                'status': 'PASS' if record_count > 0 else 'FAIL',
                'threshold': '> 0'
            }
            
            # Test 2: Null value checks
            null_checks = {
                'magnitude': 'magnitude IS NULL',
                'latitude': 'latitude IS NULL OR latitude < -90 OR latitude > 90',
                'longitude': 'longitude IS NULL OR longitude < -180 OR longitude > 180',
                'depth': 'depth IS NULL OR depth < 0'
            }
            
            for field, condition in null_checks.items():
                query = f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {condition}"
                null_count = pd.read_sql(query, self.engine).iloc[0]['null_count']
                null_percentage = (null_count / record_count) * 100 if record_count > 0 else 0
                
                validation_results['tests'][f'{field}_quality'] = {
                    'null_count': null_count,
                    'null_percentage': null_percentage,
                    'status': 'PASS' if null_percentage < 5 else 'FAIL',
                    'threshold': '< 5% null values'
                }
            
            # Test 3: Magnitude range validation
            mag_query = f"""
                SELECT 
                    MIN(magnitude) as min_mag,
                    MAX(magnitude) as max_mag,
                    AVG(magnitude) as avg_mag,
                    COUNT(*) FILTER (WHERE magnitude BETWEEN 2.5 AND 9.5) as valid_mag_count
                FROM {table_name}
            """
            mag_stats = pd.read_sql(mag_query, self.engine).iloc[0]
            valid_mag_percentage = (mag_stats['valid_mag_count'] / record_count) * 100
            
            validation_results['tests']['magnitude_range'] = {
                'min_magnitude': mag_stats['min_mag'],
                'max_magnitude': mag_stats['max_mag'],
                'avg_magnitude': mag_stats['avg_mag'],
                'valid_range_percentage': valid_mag_percentage,
                'status': 'PASS' if valid_mag_percentage > 95 else 'FAIL',
                'threshold': '> 95% in valid range (2.5-9.5)'
            }
            
            # Test 4: Geographic distribution
            geo_query = f"""
                SELECT 
                    geographic_zone,
                    COUNT(*) as zone_count,
                    AVG(magnitude) as avg_magnitude
                FROM {table_name}
                WHERE geographic_zone IS NOT NULL
                GROUP BY geographic_zone
                ORDER BY zone_count DESC
            """
            geo_distribution = pd.read_sql(geo_query, self.engine)
            
            validation_results['tests']['geographic_distribution'] = {
                'zones_found': len(geo_distribution),
                'distribution': geo_distribution.to_dict('records'),
                'status': 'PASS' if len(geo_distribution) > 0 else 'FAIL',
                'threshold': '> 0 geographic zones identified'
            }
            
            # Test 5: Temporal coverage
            temporal_query = f"""
                SELECT 
                    MIN(year) as min_year,
                    MAX(year) as max_year,
                    COUNT(DISTINCT year) as year_coverage,
                    COUNT(DISTINCT month) as month_coverage
                FROM {table_name}
                WHERE year IS NOT NULL
            """
            temporal_stats = pd.read_sql(temporal_query, self.engine).iloc[0]

            def safe_int(val):
                return int(val) if val is not None else None

            min_year = safe_int(temporal_stats['min_year'])
            max_year = safe_int(temporal_stats['max_year'])
            year_coverage = safe_int(temporal_stats['year_coverage'])
            month_coverage = safe_int(temporal_stats['month_coverage'])
            year_span = (max_year - min_year) if (min_year is not None and max_year is not None) else None

            validation_results['tests']['temporal_coverage'] = {
                'min_year': min_year,
                'max_year': max_year,
                'year_span': year_span,
                'unique_years': year_coverage,
                'unique_months': month_coverage,
                'status': 'PASS' if year_coverage and year_coverage > 1 else 'FAIL',
                'threshold': '> 1 year of data coverage'
            }
            
            # Overall validation status
            all_tests_passed = all(
                test.get('status') == 'PASS' 
                for test in validation_results['tests'].values()
            )
            validation_results['overall_status'] = 'PASS' if all_tests_passed else 'FAIL'
            
            logger.info(f"✅ Data validation completed: {validation_results['overall_status']}")
            
        except Exception as e:
            logger.error(f"❌ Data validation error: {e}")
            validation_results['error'] = str(e)
            validation_results['overall_status'] = 'ERROR'
        
        # Save validation report
        self._save_validation_report(validation_results)
        
        return validation_results
    
    def run_database_queries(self, table_name="earthquakes_spark_processed"):
        """Run example queries on processed data"""
        logger.info(f"📊 Running database queries on: {table_name}")
        
        queries = {
            # Query 1: Top 10 highest magnitude earthquakes
            'top_magnitude_earthquakes': f"""
                SELECT 
                    id, magnitude, place, 
                    CAST(latitude AS numeric) as latitude,
                    CAST(longitude AS numeric) as longitude,
                    depth, geographic_zone, risk_score
                FROM {table_name}
                WHERE magnitude IS NOT NULL
                ORDER BY magnitude DESC
                LIMIT 10
            """,

            # Query 2: Earthquake count by geographic zone
            'earthquakes_by_zone': f"""
                SELECT 
                    geographic_zone,
                    COUNT(*) as earthquake_count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    ROUND(MAX(magnitude)::numeric, 2) as max_magnitude,
                    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score
                FROM {table_name}
                WHERE geographic_zone IS NOT NULL
                GROUP BY geographic_zone
                ORDER BY earthquake_count DESC
            """,

            # Query 3: Monthly earthquake trends
            'monthly_trends': f"""
                SELECT 
                    year, month,
                    COUNT(*) as monthly_count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    COUNT(*) FILTER (WHERE magnitude >= 5.0) as major_earthquakes
                FROM {table_name}
                WHERE year IS NOT NULL AND month IS NOT NULL
                GROUP BY year, month
                ORDER BY year, month
            """,

            # Query 4: Depth category analysis
            'depth_analysis': f"""
                SELECT 
                    depth_category_detailed,
                    COUNT(*) as count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    ROUND(AVG(depth)::numeric, 1) as avg_depth,
                    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score
                FROM {table_name}
                WHERE depth_category_detailed IS NOT NULL
                GROUP BY depth_category_detailed
                ORDER BY avg_depth
            """,

            # Query 5: High-risk earthquakes
            'high_risk_analysis': f"""
                SELECT 
                    geographic_zone,
                    magnitude_category,
                    COUNT(*) as count,
                    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
                    ROUND(AVG(population_impact_estimate)::numeric, 0) as avg_population_impact
                FROM {table_name}
                WHERE risk_score > 0.7
                GROUP BY geographic_zone, magnitude_category
                ORDER BY avg_risk_score DESC
            """,

            # Query 6: Clustering analysis results
            'clustering_analysis': f"""
                SELECT 
                    earthquake_cluster,
                    COUNT(*) as cluster_size,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    ROUND(AVG(depth)::numeric, 1) as avg_depth,
                    ROUND(AVG(risk_score)::numeric, 3) as avg_risk,
                    geographic_zone,
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
                FROM {table_name}
                WHERE earthquake_cluster IS NOT NULL
                GROUP BY earthquake_cluster, geographic_zone
                ORDER BY cluster_size DESC
            """
        }
        
        query_results = {}
        
        for query_name, sql in queries.items():
            try:
                logger.info(f"Running query: {query_name}")
                result = pd.read_sql(sql, self.engine)
                query_results[query_name] = {
                    'sql': sql,
                    'result': result,
                    'row_count': len(result),
                    'status': 'SUCCESS'
                }
                
                # Display first few rows
                print(f"\n📊 {query_name.replace('_', ' ').title()}:")
                print("=" * 60)
                print(result.head().to_string(index=False))
                
            except Exception as e:
                logger.error(f"❌ Query failed - {query_name}: {e}")
                query_results[query_name] = {
                    'sql': sql,
                    'error': str(e),
                    'status': 'FAILED'
                }
        
        # Save query results
        self._save_query_results(query_results)
        
        return query_results
    
    def generate_performance_report(self):
        """Generate comprehensive performance report"""
        logger.info("📋 Generating performance report...")
        
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'system_info': {
                'cpu_count': psutil.cpu_count(),
                'total_memory_gb': round(psutil.virtual_memory().total / (1024**3), 2),
                'available_memory_gb': round(psutil.virtual_memory().available / (1024**3), 2),
                'disk_usage_percent': psutil.disk_usage('.').percent
            },
            'spark_performance': self.performance_metrics,
            'recommendations': []
        }
        
        # Generate recommendations based on performance
        for operation, metrics in self.performance_metrics.items():
            duration = metrics.get('duration_seconds', 0)
            memory_increase = metrics.get('memory_increase', 0)
            
            if duration > 300:  # 5 minutes
                report['recommendations'].append({
                    'type': 'PERFORMANCE',
                    'operation': operation,
                    'issue': 'Long execution time',
                    'recommendation': 'Consider increasing executor cores or memory'
                })
            
            if memory_increase > 20:  # 20% memory increase
                report['recommendations'].append({
                    'type': 'MEMORY',
                    'operation': operation,
                    'issue': 'High memory usage',
                    'recommendation': 'Optimize data partitioning or increase driver memory'
                })
        
        # Save performance report
        report_path = self.reports_dir / 'performance_report.json'
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"📊 Performance report saved: {report_path}")
        
        return report
    
    def create_visualization_dashboard(self, table_name="earthquakes_spark_processed"):
        """Create visualization dashboard for processed data"""
        logger.info("📊 Creating visualization dashboard...")
        
        try:
            # Load data for visualization
            viz_query = f"""
                SELECT 
                    magnitude, depth, latitude, longitude,
                    geographic_zone, magnitude_category, 
                    depth_category_detailed, risk_score,
                    year, month, earthquake_cluster
                FROM {table_name}
                WHERE magnitude IS NOT NULL
                LIMIT 10000
            """
            
            df = pd.read_sql(viz_query, self.engine)
            
            # Create dashboard with multiple plots
            fig, axes = plt.subplots(2, 3, figsize=(20, 12))
            fig.suptitle('Spark ETL Pipeline - Earthquake Analysis Dashboard', fontsize=16, fontweight='bold')
            
            # Plot 1: Magnitude distribution
            axes[0, 0].hist(df['magnitude'].dropna(), bins=30, alpha=0.7, color='skyblue', edgecolor='black')
            axes[0, 0].set_title('Magnitude Distribution')
            axes[0, 0].set_xlabel('Magnitude')
            axes[0, 0].set_ylabel('Frequency')
            axes[0, 0].grid(True, alpha=0.3)
            
            # Plot 2: Geographic distribution
            if 'geographic_zone' in df.columns:
                zone_counts = df['geographic_zone'].value_counts()
                axes[0, 1].bar(zone_counts.index, zone_counts.values, color='lightcoral', alpha=0.7)
                axes[0, 1].set_title('Earthquakes by Geographic Zone')
                axes[0, 1].set_xlabel('Geographic Zone')
                axes[0, 1].set_ylabel('Count')
                axes[0, 1].tick_params(axis='x', rotation=45)
            
            # Plot 3: Magnitude vs Depth
            axes[0, 2].scatter(df['magnitude'], df['depth'], alpha=0.6, c='green', s=20)
            axes[0, 2].set_title('Magnitude vs Depth')
            axes[0, 2].set_xlabel('Magnitude')
            axes[0, 2].set_ylabel('Depth (km)')
            axes[0, 2].grid(True, alpha=0.3)
            
            # Plot 4: Risk score distribution
            if 'risk_score' in df.columns:
                axes[1, 0].hist(df['risk_score'].dropna(), bins=25, alpha=0.7, color='red', edgecolor='black')
                axes[1, 0].set_title('Risk Score Distribution')
                axes[1, 0].set_xlabel('Risk Score')
                axes[1, 0].set_ylabel('Frequency')
                axes[1, 0].grid(True, alpha=0.3)
            
            # Plot 5: Temporal trends
            if 'year' in df.columns and 'month' in df.columns:
                monthly_counts = df.groupby(['year', 'month']).size().reset_index(name='count')
                monthly_counts['date'] = pd.to_datetime(monthly_counts[['year', 'month']].assign(day=1))
                axes[1, 1].plot(monthly_counts['date'], monthly_counts['count'], marker='o', linewidth=2)
                axes[1, 1].set_title('Monthly Earthquake Frequency')
                axes[1, 1].set_xlabel('Date')
                axes[1, 1].set_ylabel('Count')
                axes[1, 1].tick_params(axis='x', rotation=45)
                axes[1, 1].grid(True, alpha=0.3)
            
            # Plot 6: Clustering results
            if 'earthquake_cluster' in df.columns:
                cluster_counts = df['earthquake_cluster'].value_counts().sort_index()
                axes[1, 2].bar(cluster_counts.index, cluster_counts.values, color='purple', alpha=0.7)
                axes[1, 2].set_title('Earthquake Clusters')
                axes[1, 2].set_xlabel('Cluster ID')
                axes[1, 2].set_ylabel('Count')
                axes[1, 2].grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            # Save dashboard
            dashboard_path = self.reports_dir / 'spark_etl_dashboard.png'
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"📊 Dashboard saved: {dashboard_path}")
            
            return dashboard_path
            
        except Exception as e:
            logger.error(f"❌ Dashboard creation failed: {e}")
            return None
    
    def _save_validation_report(self, validation_results):
        """Save validation report to file"""
        import numpy as np

        def convert_types(obj):
            if isinstance(obj, dict):
                return {k: convert_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_types(i) for i in obj]
            elif isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            else:
                return obj

        report_path = self.reports_dir / 'data_validation_report.json'
        with open(report_path, 'w') as f:
            json.dump(convert_types(validation_results), f, indent=2)

        logger.info(f"📋 Validation report saved: {report_path}")
    
    def _save_query_results(self, query_results):
        """Save query results to files"""
        for query_name, result_data in query_results.items():
            if result_data['status'] == 'SUCCESS':
                # Save as CSV
                csv_path = self.reports_dir / f'{query_name}_results.csv'
                result_data['result'].to_csv(csv_path, index=False)
                
                # Save SQL query
                sql_path = self.reports_dir / f'{query_name}_query.sql'
                with open(sql_path, 'w') as f:
                    f.write(result_data['sql'])
        
        # Save summary
        summary = {
            'execution_timestamp': datetime.now().isoformat(),
            'queries_executed': len(query_results),
            'successful_queries': sum(1 for r in query_results.values() if r['status'] == 'SUCCESS'),
            'failed_queries': sum(1 for r in query_results.values() if r['status'] == 'FAILED'),
            'query_list': list(query_results.keys())
        }
        
        summary_path = self.reports_dir / 'query_execution_summary.json'
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"📊 Query results saved to: {self.reports_dir}")

def run_complete_validation(table_name="earthquakes_spark_processed"):
    """Run complete validation and testing suite"""
    print("🔍 SPARK ETL VALIDATION & TESTING SUITE")
    print("=" * 60)
    
    validator = SparkETLValidator()
    
    try:
        # 1. Data Quality Validation
        print("\n📋 1. Running Data Quality Validation...")
        validation_results = validator.validate_data_quality(table_name)
        print(f"   Status: {validation_results['overall_status']}")
        
        # 2. Database Query Testing
        print("\n📊 2. Running Database Query Tests...")
        query_results = validator.run_database_queries(table_name)
        successful_queries = sum(1 for r in query_results.values() if r['status'] == 'SUCCESS')
        print(f"   Successful queries: {successful_queries}/{len(query_results)}")
        
        # 3. Performance Report
        print("\n⚡ 3. Generating Performance Report...")
        performance_report = validator.generate_performance_report()
        print(f"   Recommendations: {len(performance_report['recommendations'])}")
        
        # 4. Visualization Dashboard
        print("\n📊 4. Creating Visualization Dashboard...")
        dashboard_path = validator.create_visualization_dashboard(table_name)
        if dashboard_path:
            print(f"   Dashboard saved: {dashboard_path}")
        
        print("\n🎉 Validation and testing completed successfully!")
        print(f"📁 Reports saved to: {validator.reports_dir}")
        
        # Summary
        print("\n📋 VALIDATION SUMMARY:")
        print(f"   • Data Quality: {validation_results['overall_status']}")
        print(f"   • Query Tests: {successful_queries}/{len(query_results)} passed")
        print(f"   • Performance Recommendations: {len(performance_report['recommendations'])}")
        print(f"   • Visualization: {'✅' if dashboard_path else '❌'}")
        
        return {
            'validation': validation_results,
            'queries': query_results,
            'performance': performance_report,
            'dashboard': dashboard_path
        }
        
    except Exception as e:
        logger.error(f"❌ Validation suite error: {e}")
        return None

if __name__ == "__main__":
    # First run batch processing
    batch_processor = SparkBatchProcessor(batch_size=10000)
    success = batch_processor.run_batch_processing()
    
    if success:
        # Then run validation suite
        print("\n🔍 Running validation suite...")
        results = run_complete_validation()
    else:
        print("❌ Batch processing failed. Validation suite not executed.")