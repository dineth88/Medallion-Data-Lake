"""
Silver to Gold Layer Processing
Creates business-ready aggregated datasets
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverToGold:
    def __init__(self, s3_config):
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("SilverToGold") \
            .config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint_url']) \
            .config("spark.hadoop.fs.s3a.access.key", s3_config['access_key']) \
            .config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key']) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .getOrCreate()
        
        self.silver_bucket = f"s3a://{s3_config['silver_bucket']}"
        self.gold_bucket = f"s3a://{s3_config['gold_bucket']}"
    
    def read_silver_data(self, table_name):
        """Read data from silver layer"""
        try:
            path = f"{self.silver_bucket}/silver/{table_name}/"
            df = self.spark.read.parquet(path)
            logger.info(f"✓ Read {df.count()} records from silver/{table_name}")
            return df
        except Exception as e:
            logger.error(f"✗ Error reading silver data for {table_name}: {e}")
            return None
    
    def create_customer_summary(self):
        """Create customer analytics summary"""
        logger.info("Creating customer summary...")
        
        df_customer = self.read_silver_data('customer')
        df_payment = self.read_silver_data('payment')
        df_rental = self.read_silver_data('rental')
        
        if df_customer is None or df_payment is None or df_rental is None:
            return None
        
        # Calculate customer metrics
        customer_summary = df_customer.alias('c') \
            .join(df_payment.alias('p'), col('c.customer_id') == col('p.customer_id'), 'left') \
            .join(df_rental.alias('r'), col('c.customer_id') == col('r.customer_id'), 'left') \
            .groupBy(
                col('c.customer_id'),
                col('c.email'),
                col('c.active')
            ) \
            .agg(
                count(col('p.payment_id')).alias('total_payments'),
                sum(col('p.amount')).alias('total_spent'),
                avg(col('p.amount')).alias('avg_payment_amount'),
                count(col('r.rental_id')).alias('total_rentals'),
                max(col('p.payment_date')).alias('last_payment_date'),
                max(col('r.rental_date')).alias('last_rental_date')
            ) \
            .withColumn('customer_value_tier', 
                when(col('total_spent') >= 200, 'Premium')
                .when(col('total_spent') >= 100, 'Gold')
                .when(col('total_spent') >= 50, 'Silver')
                .otherwise('Bronze')
            ) \
            .withColumn('created_at', current_timestamp())
        
        return customer_summary
    
    def create_film_performance(self):
        """Create film performance analytics"""
        logger.info("Creating film performance...")
        
        df_film = self.read_silver_data('film')
        df_rental = self.read_silver_data('rental')
        df_payment = self.read_silver_data('payment')
        
        if df_film is None or df_rental is None or df_payment is None:
            return None
        
        # Calculate film metrics
        film_performance = df_film.alias('f') \
            .join(df_rental.alias('r'), col('f.film_id') == col('r.inventory_id'), 'left') \
            .join(df_payment.alias('p'), col('r.rental_id') == col('p.rental_id'), 'left') \
            .groupBy(
                col('f.film_id'),
                col('f.title'),
                col('f.release_year'),
                col('f.rental_rate'),
                col('f.rental_duration')
            ) \
            .agg(
                count(col('r.rental_id')).alias('total_rentals'),
                sum(col('p.amount')).alias('total_revenue'),
                avg(col('p.amount')).alias('avg_revenue_per_rental'),
                countDistinct(col('r.customer_id')).alias('unique_customers')
            ) 
        
        # --- FIX APPLIED HERE ---
        # 1. Convert 'release_year' (INT) to a date string (e.g., '2006')
        # 2. Convert the string to a DATE object (e.g., '2006-01-01')
        # 3. Calculate DATEDIFF (in days) between current date and release date.
        # 4. Use `when` to ensure we divide by at least 1 day to prevent DivisionByZeroError.
        
        datediff_expr = datediff(
            current_date(), 
            to_date(col('release_year').cast('string'), 'yyyy')
        )

        film_performance = film_performance.withColumn('revenue_per_day', 
                col('total_revenue') / when(datediff_expr > 0, datediff_expr).otherwise(lit(1))
            ) \
            .withColumn('popularity_rank',
                row_number().over(Window.orderBy(desc('total_rentals')))
            ) \
            .withColumn('created_at', current_timestamp())
        
        return film_performance
    
    def create_daily_revenue(self):
        """Create daily revenue analytics"""
        logger.info("Creating daily revenue...")
        
        df_payment = self.read_silver_data('payment')
        
        if df_payment is None:
            return None
        
        # Calculate daily metrics
        daily_revenue = df_payment \
            .withColumn('payment_date', to_date(col('payment_date'))) \
            .groupBy('payment_date') \
            .agg(
                count('payment_id').alias('total_transactions'),
                sum('amount').alias('total_revenue'),
                avg('amount').alias('avg_transaction_amount'),
                min('amount').alias('min_transaction'),
                max('amount').alias('max_transaction')
            ) \
            .withColumn('revenue_growth',
                (col('total_revenue') - lag('total_revenue', 1).over(Window.orderBy('payment_date'))) / 
                lag('total_revenue', 1).over(Window.orderBy('payment_date')) * 100
            ) \
            .withColumn('created_at', current_timestamp()) \
            .orderBy('payment_date')
        
        return daily_revenue
    
    def create_rental_trends(self):
        """Create rental trends analytics"""
        logger.info("Creating rental trends...")
        
        df_rental = self.read_silver_data('rental')
        
        if df_rental is None:
            return None
        
        # Calculate rental trends
        rental_trends = df_rental \
            .withColumn('rental_date', to_date(col('rental_date'))) \
            .withColumn('year', year(col('rental_date'))) \
            .withColumn('month', month(col('rental_date'))) \
            .withColumn('day_of_week', dayofweek(col('rental_date'))) \
            .groupBy('year', 'month', 'day_of_week') \
            .agg(
                count('rental_id').alias('total_rentals'),
                countDistinct('customer_id').alias('unique_customers'),
                avg(datediff(col('return_date'), col('rental_date'))).alias('avg_rental_duration')
            ) \
            .withColumn('created_at', current_timestamp()) \
            .orderBy('year', 'month', 'day_of_week')
        
        return rental_trends
    
    def write_gold_data(self, df, table_name):
        """Write aggregated data to gold layer"""
        try:
            path = f"{self.gold_bucket}/gold/{table_name}/"
            df.write \
                .mode("overwrite") \
                .parquet(path)
            
            logger.info(f"✓ Written {df.count()} records to gold/{table_name}")
        except Exception as e:
            logger.error(f"✗ Error writing gold data for {table_name}: {e}")
    
    def run_pipeline(self):
        """Run the complete silver to gold pipeline"""
        logger.info("Starting Silver to Gold pipeline")
        
        # Create all gold tables
        analytics = {
            'customer_summary': self.create_customer_summary(),
            'film_performance': self.create_film_performance(),
            'daily_revenue': self.create_daily_revenue(),
            'rental_trends': self.create_rental_trends()
        }
        
        # Write to gold layer
        for table_name, df in analytics.items():
            if df is not None:
                self.write_gold_data(df, table_name)
        
        logger.info("Silver to Gold pipeline completed!")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    # Configuration
    s3_config = {
        'endpoint_url': os.getenv('ENDPOINT_URL', 'http://localhost:9000'),
        'access_key': os.getenv('ACCESS_KEY'),
        'secret_key': os.getenv('SECRET_KEY'),
        'silver_bucket': 'datalake-silver',
        'gold_bucket': 'datalake-gold'
    }
    
    # Run pipeline
    processor = SilverToGold(s3_config)
    try:
        processor.run_pipeline()
    finally:
        processor.stop()