"""
Data Lake Query Interface
Query Bronze, Silver, and Gold layers with SQL
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class DataLakeQuery:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataLakeQuery") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        self.bronze_bucket = "s3a://datalake-bronze"
        self.silver_bucket = "s3a://datalake-silver"
        self.gold_bucket = "s3a://datalake-gold"
        
        self._load_all_layers()
    
    def _load_bronze_tables(self):
        """Load Bronze layer tables (raw JSON)"""
        bronze_tables = [
            'actor', 'address', 'category', 'city', 'country', 
            'customer', 'film', 'film_actor', 'film_category',
            'inventory', 'language', 'payment', 'rental', 'staff', 'store'
        ]
        
        print("\nLoading Bronze Layer (Raw Data)...")
        for table in bronze_tables:
            try:
                path = f"{self.bronze_bucket}/bronze/{table}/"
                df = self.spark.read.json(path)
                
                # Extract data from nested structure
                if 'data' in df.columns:
                    df = df.select("data.*", "timestamp", "operation")
                
                df.createOrReplaceTempView(f"bronze_{table}")
                count = df.count()
                print(f"  ✓ bronze_{table}: {count} records")
            except Exception as e:
                print(f"  ✗ bronze_{table}: {str(e)[:50]}")
    
    def _load_silver_tables(self):
        """Load Silver layer tables (cleaned Parquet)"""
        silver_tables = ['customer', 'film', 'payment', 'rental']
        
        print("\nLoading Silver Layer (Cleaned Data)...")
        for table in silver_tables:
            try:
                path = f"{self.silver_bucket}/silver/{table}/"
                df = self.spark.read.parquet(path)
                df.createOrReplaceTempView(f"silver_{table}")
                count = df.count()
                print(f"  ✓ silver_{table}: {count} records")
            except Exception as e:
                print(f"  ✗ silver_{table}: {str(e)[:50]}")
    
    def _load_gold_tables(self):
        """Load Gold layer tables (analytics)"""
        gold_tables = ['customer_summary', 'film_performance', 'daily_revenue', 'rental_trends']
        
        print("\nLoading Gold Layer (Analytics)...")
        for table in gold_tables:
            try:
                path = f"{self.gold_bucket}/gold/{table}/"
                df = self.spark.read.parquet(path)
                df.createOrReplaceTempView(f"gold_{table}")
                count = df.count()
                print(f"  ✓ gold_{table}: {count} records")
            except Exception as e:
                print(f"  ✗ gold_{table}: {str(e)[:50]}")
    
    def _load_all_layers(self):
        """Load all layers"""
        print("=" * 70)
        print("LOADING DATA LAKE LAYERS")
        print("=" * 70)
        self._load_bronze_tables()
        self._load_silver_tables()
        self._load_gold_tables()
        print("\n" + "=" * 70)
    
    def show_tables(self):
        """Show all available tables grouped by layer"""
        print("\n" + "=" * 70)
        print("AVAILABLE TABLES")
        print("=" * 70)
        
        tables = self.spark.catalog.listTables()
        
        bronze = [t.name for t in tables if t.name.startswith('bronze_')]
        silver = [t.name for t in tables if t.name.startswith('silver_')]
        gold = [t.name for t in tables if t.name.startswith('gold_')]
        
        print("\nBRONZE LAYER (Raw Data):")
        for t in sorted(bronze):
            print(f"  - {t}")
        
        print("\nSILVER LAYER (Cleaned Data):")
        for t in sorted(silver):
            print(f"  - {t}")
        
        print("\nGOLD LAYER (Analytics):")
        for t in sorted(gold):
            print(f"  - {t}")
        
        print("\n" + "=" * 70)
    
    def describe_table(self, table_name):
        """Show table schema and sample data"""
        try:
            df = self.spark.table(table_name)
            
            print(f"\n{'=' * 70}")
            print(f"TABLE: {table_name}")
            print(f"{'=' * 70}")
            print(f"Row Count: {df.count()}")
            print("\nSchema:")
            df.printSchema()
            print("\nSample Data (5 rows):")
            df.show(5, truncate=False)
            print("=" * 70)
        except Exception as e:
            print(f"Error: {e}")
    
    def query(self, sql):
        """Execute SQL query"""
        try:
            result = self.spark.sql(sql)
            return result
        except Exception as e:
            print(f"Query Error: {e}")
            return None
    
    def sample_queries(self):
        """Run sample queries across all layers"""
        print("\n" + "=" * 70)
        print("SAMPLE QUERIES - ALL LAYERS")
        print("=" * 70)
        
        # Bronze Layer Query
        print("\n1. BRONZE - Recent Customer Records (Raw):")
        print("-" * 70)
        q1 = """
        SELECT 
            data.customer_id,
            data.first_name,
            data.last_name,
            data.email,
            timestamp
        FROM bronze_customer
        LIMIT 5
        """
        self.query(q1).show(truncate=False)
        
        # Silver Layer Query
        print("\n2. SILVER - Cleaned Customer Data:")
        print("-" * 70)
        q2 = """
        SELECT 
            customer_id,
            email,
            active,
            create_date
        FROM silver_customer
        LIMIT 5
        """
        self.query(q2).show(truncate=False)
        
        # Gold Layer Query
        print("\n3. GOLD - Customer Value Analysis:")
        print("-" * 70)
        q3 = """
        SELECT 
            customer_value_tier,
            COUNT(*) as customers,
            ROUND(AVG(total_spent), 2) as avg_spent,
            ROUND(SUM(total_spent), 2) as total_revenue
        FROM gold_customer_summary
        GROUP BY customer_value_tier
        ORDER BY total_revenue DESC
        """
        self.query(q3).show()
        
        # Cross-Layer Analysis
        print("\n4. CROSS-LAYER - Payment Comparison:")
        print("-" * 70)
        q4 = """
        SELECT 
            'Bronze (Raw)' as layer,
            COUNT(*) as record_count,
            ROUND(SUM(CAST(data.amount AS DOUBLE)), 2) as total_amount
        FROM bronze_payment
        UNION ALL
        SELECT 
            'Silver (Cleaned)' as layer,
            COUNT(*) as record_count,
            ROUND(SUM(amount), 2) as total_amount
        FROM silver_payment
        """
        self.query(q4).show()
        
        # Gold - Film Performance
        print("\n5. GOLD - Top 10 Films by Revenue:")
        print("-" * 70)
        q5 = """
        SELECT 
            title,
            release_year,
            total_rentals,
            ROUND(total_revenue, 2) as revenue
        FROM gold_film_performance
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        self.query(q5).show(truncate=False)
    
    def interactive_mode(self):
        """Interactive SQL mode"""
        print("\n" + "=" * 70)
        print("INTERACTIVE SQL MODE")
        print("=" * 70)
        print("Commands:")
        print("  tables      - Show all tables")
        print("  describe X  - Show schema for table X")
        print("  exit        - Quit")
        print("=" * 70 + "\n")
        
        while True:
            try:
                query = input("SQL> ").strip()
                
                if query.lower() == 'exit':
                    break
                elif query.lower() == 'tables':
                    self.show_tables()
                elif query.lower().startswith('describe '):
                    table = query.split(maxsplit=1)[1]
                    self.describe_table(table)
                elif query:
                    result = self.query(query)
                    if result:
                        result.show(100, truncate=False)
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except EOFError:
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()


def main():
    import sys
    
    querier = DataLakeQuery()
    
    try:
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == 'interactive':
                querier.interactive_mode()
            elif command == 'tables':
                querier.show_tables()
            elif command == 'describe' and len(sys.argv) > 2:
                querier.describe_table(sys.argv[2])
            elif command == 'samples':
                querier.sample_queries()
            else:
                # Execute SQL query
                sql = ' '.join(sys.argv[1:])
                result = querier.query(sql)
                if result:
                    result.show(100, truncate=False)
        else:
            # Default: run sample queries
            querier.sample_queries()
            
    finally:
        querier.stop()


if __name__ == "__main__":
    main()