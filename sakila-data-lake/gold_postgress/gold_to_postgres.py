"""
Load Gold Layer to PostgreSQL for fast querying
"""
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldToPostgres:
    def __init__(self):
        # Spark session for reading from S3
        self.spark = SparkSession.builder \
            .appName("GoldToPostgres") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # PostgreSQL connection
        self.pg_engine = create_engine('postgresql://analytics:analytics123@localhost:5432/analytics')
        
        self.gold_bucket = "s3a://datalake-gold"
        self.gold_tables = ['customer_summary', 'film_performance', 'daily_revenue', 'rental_trends']
    
    def load_table_to_postgres(self, table_name):
        """Load a Gold table to PostgreSQL"""
        logger.info(f"Loading {table_name}...")
        
        try:
            # Read from S3
            path = f"{self.gold_bucket}/gold/{table_name}/"
            df = self.spark.read.parquet(path)
            
            # Convert to Pandas
            pdf = df.toPandas()
            
            # Write to PostgreSQL
            pdf.to_sql(
                table_name,
                self.pg_engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✓ {table_name}: {len(pdf)} rows loaded")
            
        except Exception as e:
            logger.error(f"✗ Error loading {table_name}: {e}")
    
    def load_all_gold_tables(self):
        """Load all Gold layer tables to PostgreSQL"""
        logger.info("=" * 60)
        logger.info("Loading Gold Layer to PostgreSQL")
        logger.info("=" * 60)
        
        for table in self.gold_tables:
            self.load_table_to_postgres(table)
        
        logger.info("=" * 60)
        logger.info("✓ Gold layer loaded to PostgreSQL!")
        logger.info("Connect: postgresql://analytics:analytics123@localhost:5432/analytics")
        logger.info("=" * 60)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    loader = GoldToPostgres()
    try:
        loader.load_all_gold_tables()
    finally:
        loader.stop()