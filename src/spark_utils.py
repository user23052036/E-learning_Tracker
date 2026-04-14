"""
E-Learning Progress Tracker - Spark Configuration & Utilities
Provides Spark session configuration and common utility functions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os

class SparkConfig:
    """Spark Session Configuration"""
    
    @staticmethod
    def create_spark_session(app_name="ELearningTracker", master="local[*]"):
        """Create and configure Spark session"""
        print("\n" + "="*60)
        print(f"Initializing Spark Session: {app_name}")
        print("="*60 + "\n")
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "4") \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        print(f"✅ Spark {spark.version} session created successfully!")
        print(f"   App Name: {app_name}")
        print(f"   Master: {master}")
        print(f"   Driver Memory: 2g")
        print(f"   Executor Memory: 2g")
        print("="*60 + "\n")
        
        return spark
    
    @staticmethod
    def stop_spark_session(spark):
        """Stop Spark session"""
        print("\n🛑 Stopping Spark session...")
        spark.stop()
        print("✅ Spark session stopped\n")


class SparkDataLoader:
    """Load data from various sources"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def load_csv_from_hdfs(self, hdfs_path, schema=None):
        """Load CSV from HDFS"""
        print(f"📂 Loading: {hdfs_path}")
        
        if schema:
            df = self.spark.read.csv(hdfs_path, header=True, schema=schema)
        else:
            df = self.spark.read.csv(hdfs_path, header=True, inferSchema=True)
        
        print(f"✅ Loaded {df.count()} records")
        return df
    
    def load_csv_from_local(self, local_path, schema=None):
        """Load CSV from local filesystem"""
        print(f"📂 Loading: {local_path}")
        
        if schema:
            df = self.spark.read.csv(local_path, header=True, schema=schema)
        else:
            df = self.spark.read.csv(local_path, header=True, inferSchema=True)
        
        print(f"✅ Loaded {df.count()} records")
        return df
    
    def load_all_datasets(self, base_path, source='local'):
        """Load all E-Learning datasets"""
        print("\n" + "="*60)
        print("Loading All E-Learning Datasets")
        print("="*60 + "\n")
        
        datasets = {}
        files = ['students', 'courses', 'modules', 'enrollments', 'progress', 'assessments']
        
        for file in files:
            if source == 'hdfs':
                path = f"{base_path}/{file}.csv"
                datasets[file] = self.load_csv_from_hdfs(path)
            else:
                path = f"{base_path}/{file}.csv"
                datasets[file] = self.load_csv_from_local(path)
        
        print("\n" + "="*60)
        print(f"✅ Loaded {len(datasets)} datasets successfully!")
        print("="*60 + "\n")
        
        return datasets


class SparkUtils:
    """Common Spark utility functions"""
    
    @staticmethod
    def display_dataframe_info(df, name="DataFrame"):
        """Display comprehensive DataFrame information"""
        print("\n" + "="*60)
        print(f"📊 {name} Information")
        print("="*60 + "\n")
        
        print(f"Total Records: {df.count()}")
        print(f"Total Columns: {len(df.columns)}\n")
        
        print("Schema:")
        df.printSchema()
        
        print("\nSample Data (First 5 rows):")
        df.show(5, truncate=False)
        
        print("="*60 + "\n")
    
    @staticmethod
    def get_basic_stats(df):
        """Get basic statistics for numeric columns"""
        print("\n📈 Basic Statistics:")
        print("-" * 60)
        df.describe().show()
    
    @staticmethod
    def check_null_values(df):
        """Check for null values in DataFrame"""
        print("\n🔍 Null Value Check:")
        print("-" * 60)
        
        null_counts = df.select([
            count(when(col(c).isNull(), c)).alias(c) 
            for c in df.columns
        ])
        null_counts.show(vertical=True)
    
    @staticmethod
    def save_to_csv(df, output_path, mode='overwrite'):
        """Save DataFrame to CSV"""
        print(f"\n💾 Saving to: {output_path}")
        df.coalesce(1).write.mode(mode).option('header', 'true').csv(output_path)
        print("✅ Saved successfully!")
    
    @staticmethod
    def save_to_parquet(df, output_path, mode='overwrite', partition_by=None):
        """Save DataFrame to Parquet format"""
        print(f"\n💾 Saving to Parquet: {output_path}")
        
        if partition_by:
            df.write.mode(mode).partitionBy(partition_by).parquet(output_path)
        else:
            df.write.mode(mode).parquet(output_path)
        
        print("✅ Saved successfully!")
    
    @staticmethod
    def cache_dataframe(df, name="DataFrame"):
        """Cache DataFrame in memory"""
        print(f"\n🔄 Caching {name}...")
        df.cache()
        count = df.count()  # Trigger caching
        print(f"✅ Cached {count} records")
        return df
    
    @staticmethod
    def create_temp_view(df, view_name):
        """Create temporary SQL view"""
        print(f"\n📋 Creating temporary view: {view_name}")
        df.createOrReplaceTempView(view_name)
        print("✅ View created!")
    
    @staticmethod
    def execute_sql(spark, query, show_result=True):
        """Execute SQL query and return result"""
        print(f"\n🔍 Executing SQL Query:")
        print("-" * 60)
        print(query)
        print("-" * 60)
        
        result = spark.sql(query)
        
        if show_result:
            result.show(truncate=False)
        
        return result
    
    @staticmethod
    def add_date_features(df, date_column):
        """Add date-based features (year, month, day, day_of_week)"""
        print(f"\n📅 Adding date features from: {date_column}")
        
        df = df.withColumn('year', year(col(date_column))) \
               .withColumn('month', month(col(date_column))) \
               .withColumn('day', dayofmonth(col(date_column))) \
               .withColumn('day_of_week', dayofweek(col(date_column))) \
               .withColumn('quarter', quarter(col(date_column)))
        
        print("✅ Date features added!")
        return df
    
    @staticmethod
    def get_column_distribution(df, column_name, top_n=10):
        """Get value distribution for a column"""
        print(f"\n📊 Distribution of '{column_name}' (Top {top_n}):")
        print("-" * 60)
        
        df.groupBy(column_name) \
          .count() \
          .orderBy(desc('count')) \
          .limit(top_n) \
          .show(truncate=False)
    
    @staticmethod
    def filter_date_range(df, date_column, start_date, end_date):
        """Filter DataFrame by date range"""
        print(f"\n🗓️  Filtering {date_column} between {start_date} and {end_date}")
        
        filtered_df = df.filter(
            (col(date_column) >= start_date) & 
            (col(date_column) <= end_date)
        )
        
        print(f"✅ Filtered to {filtered_df.count()} records")
        return filtered_df


class PerformanceMonitor:
    """Monitor Spark job performance"""
    
    @staticmethod
    def show_execution_plan(df):
        """Show DataFrame execution plan"""
        print("\n📋 Execution Plan:")
        print("-" * 60)
        df.explain(extended=True)
    
    @staticmethod
    def get_partition_info(df):
        """Get partition information"""
        print("\n🔢 Partition Information:")
        print("-" * 60)
        print(f"Number of Partitions: {df.rdd.getNumPartitions()}")
        
        # Show records per partition
        partition_sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
        print(f"Records per partition: {partition_sizes}")
        print(f"Total records: {sum(partition_sizes)}")


if __name__ == "__main__":
    # Test Spark configuration
    spark = SparkConfig.create_spark_session()
    
    # Test data loading
    loader = SparkDataLoader(spark)
    
    # Try to load sample data if exists
    try:
        datasets = loader.load_all_datasets('data/raw', source='local')
        
        # Display info for students dataset
        SparkUtils.display_dataframe_info(datasets['students'], 'Students')
        
        # Check null values
        SparkUtils.check_null_values(datasets['students'])
        
    except Exception as e:
        print(f"⚠️  Could not load data: {e}")
        print("Please run data_generator.py first to create datasets")
    
    # Stop Spark
    SparkConfig.stop_spark_session(spark)