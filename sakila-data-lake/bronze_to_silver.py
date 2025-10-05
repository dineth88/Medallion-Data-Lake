"""
Bronze to Silver Layer Processing
Cleans and transforms raw data using PySpark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper, when, current_timestamp, to_date
from pyspark.sql.types import *
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeToSilver:
    def __init__(self, s3_config):
        # Ensure Windows-safe local temp dir exists
        local_tmp = r"C:\spark-temp"
        os.makedirs(local_tmp, exist_ok=True)

        # Initialize Spark session with Windows-safe and S3A settings
        self.spark = (
            SparkSession.builder
            .appName("BronzeToSilver")
            # Local temp directories
            .config("spark.local.dir", local_tmp)
            .config("spark.hadoop.fs.s3a.buffer.dir", local_tmp)

            # Disable Hadoop native IO (fixes UnsatisfiedLinkError on Windows)
            .config("spark.hadoop.io.native.lib.available", "false")
            .config("spark.hadoop.io.nativeio.enabled", "false")
            .config("spark.driver.extraJavaOptions", "-Dorg.apache.hadoop.io.native.lib.available=false -Dhadoop.native.lib=false")
            .config("spark.executor.extraJavaOptions", "-Dorg.apache.hadoop.io.native.lib.available=false -Dhadoop.native.lib=false")

            # S3A configurations
            .config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint_url'])
            .config("spark.hadoop.fs.s3a.access.key", s3_config['access_key'])
            .config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key'])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.committer.name", "directory")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

            # Hadoop + AWS dependencies
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")

            .getOrCreate()
        )

        # Double-check disabling native IO
        self.spark.conf.set("spark.hadoop.io.nativeio.enabled", "false")

        logger.info("✅ Spark session initialized successfully on Windows")
        logger.info(f"⚙ Local temp dir: {local_tmp}")

        self.bronze_bucket = f"s3a://{s3_config['bronze_bucket']}"
        self.silver_bucket = f"s3a://{s3_config['silver_bucket']}"

    def read_bronze_data(self, table_name):
        try:
            path = f"{self.bronze_bucket}/bronze/{table_name}/"
            df = self.spark.read.json(path)
            logger.info(f"✓ Read {df.count()} records from bronze/{table_name}")
            return df
        except Exception as e:
            logger.error(f"✗ Error reading bronze data for {table_name}: {e}")
            return None

    def _add_processed_date(self, df):
        return df.withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_date", to_date(col("processed_at")))

    def clean_customer_data(self, df):
        df_clean = (
            df.select(col("data.*"))
              .withColumn("email", upper(trim(col("email"))))
              .withColumn("create_date", to_timestamp(col("create_date")))
              .withColumn("last_update", to_timestamp(col("last_update")))
              .withColumn("active", col("active").cast(IntegerType()))
              .dropDuplicates(["customer_id"])
              .na.drop(subset=["customer_id", "email"])
        )
        return self._add_processed_date(df_clean)

    def clean_film_data(self, df):
        df_clean = (
            df.select(col("data.*"))
              .withColumn("title", trim(col("title")))
              .withColumn("release_year", col("release_year").cast(IntegerType()))
              .withColumn("rental_duration", col("rental_duration").cast(IntegerType()))
              .withColumn("rental_rate", col("rental_rate").cast(DoubleType()))
              .withColumn("length", col("length").cast(IntegerType()))
              .withColumn("replacement_cost", col("replacement_cost").cast(DoubleType()))
              .withColumn("last_update", to_timestamp(col("last_update")))
              .dropDuplicates(["film_id"])
              .na.drop(subset=["film_id", "title"])
        )
        return self._add_processed_date(df_clean)

    def clean_payment_data(self, df):
        df_clean = (
            df.select(col("data.*"))
              .withColumn("amount", when(col("amount") < 0, 0).otherwise(col("amount").cast(DoubleType())))
              .withColumn("payment_date", to_timestamp(col("payment_date")))
              .withColumn("last_update", to_timestamp(col("last_update")))
              .dropDuplicates(["payment_id"])
              .na.drop(subset=["payment_id", "amount"])
        )
        return self._add_processed_date(df_clean)

    def clean_rental_data(self, df):
        df_clean = (
            df.select(col("data.*"))
              .withColumn("rental_date", to_timestamp(col("rental_date")))
              .withColumn("return_date", to_timestamp(col("return_date")))
              .withColumn("last_update", to_timestamp(col("last_update")))
              .dropDuplicates(["rental_id"])
              .na.drop(subset=["rental_id", "rental_date"])
        )
        return self._add_processed_date(df_clean)

    def write_silver_data(self, df, table_name):
        try:
            path = f"{self.silver_bucket}/silver/{table_name}/"
            df.write.mode("overwrite").partitionBy("processed_date").parquet(path)
            logger.info(f"✓ Written {df.count()} records to silver/{table_name}")
        except Exception as e:
            logger.error(f"✗ Error writing silver data for {table_name}: {e}")

    def process_table(self, table_name, cleaning_function):
        logger.info(f"Processing table: {table_name}")
        df_bronze = self.read_bronze_data(table_name)
        if df_bronze is None:
            return
        df_silver = cleaning_function(df_bronze)
        self.write_silver_data(df_silver, table_name)

    def run_pipeline(self):
        logger.info("Starting Bronze to Silver pipeline")
        tables_to_process = {
            'customer': self.clean_customer_data,
            'film': self.clean_film_data,
            'payment': self.clean_payment_data,
            'rental': self.clean_rental_data
        }
        for table_name, func in tables_to_process.items():
            self.process_table(table_name, func)
        logger.info("Bronze to Silver pipeline completed!")

    def stop(self):
        self.spark.stop()


if __name__ == "__main__":
    s3_config = {
        'endpoint_url': os.getenv('ENDPOINT_URL', 'http://localhost:9000'),
        'access_key': os.getenv('ACCESS_KEY'),
        'secret_key': os.getenv('SECRET_KEY'),
        'bronze_bucket': 'datalake-bronze',
        'silver_bucket': 'datalake-silver'
    }

    processor = BronzeToSilver(s3_config)
    try:
        processor.run_pipeline()
    finally:
        processor.stop()
 