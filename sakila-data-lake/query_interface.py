import os
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
import time
import logging

# Configure logging to suppress verbose Spark messages in the console/UI
# The Spark session will still print warnings about JARS on initial run
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class DataLakeQuery:
    """
    Manages Spark Session and loads Data Lake tables.
    This class is instantiated once and cached by Streamlit.
    """
    def __init__(self):
        # Notify user that the slow initialization is starting
        st.info("⏳ Initializing Spark Session and loading Data Lake layers... (This heavy load only happens once.)")
        
        # 1. Initialize Spark Session (The major performance bottleneck initially)
        self.spark = SparkSession.builder \
            .appName("DataLakeQueryStreamlit") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('ACCESS_KEY')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('SECRET_KEY')) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        self.bronze_bucket = "s3a://datalake-bronze"
        self.silver_bucket = "s3a://datalake-silver"
        self.gold_bucket = "s3a://datalake-gold"
        
        # 2. Load all layers and register temporary tables
        self._load_all_layers()
        st.success("✅ Spark Session ready and all tables registered!")
    
    def _get_tables_to_load(self):
        """Defines the tables for each layer based on your original script."""
        return {
            'bronze': ['actor', 'address', 'category', 'city', 'country', 
                       'customer', 'film', 'film_actor', 'film_category',
                       'inventory', 'language', 'payment', 'rental', 'staff', 'store'],
            'silver': ['customer', 'film', 'payment', 'rental'],
            'gold': ['customer_summary', 'film_performance', 'daily_revenue', 'rental_trends']
        }

    def _load_all_layers(self):
        """Load all layers and register temp views."""
        tables_map = self._get_tables_to_load()
        
        # Load Bronze Layer (JSON - less efficient format)
        for table in tables_map['bronze']:
            try:
                path = f"{self.bronze_bucket}/bronze/{table}/"
                df = self.spark.read.json(path)
                
                # Handling nested 'data' field
                if 'data' in df.columns:
                    df = df.select("data.*", "timestamp", "operation")
                    
                df.createOrReplaceTempView(f"bronze_{table}")
            except Exception:
                continue

        # Load Silver/Gold Layers (Parquet - Optimized columnar format)
        for layer in ['silver', 'gold']:
            bucket = getattr(self, f"{layer}_bucket")
            for table in tables_map[layer]:
                try:
                    path = f"{bucket}/{layer}/{table}/"
                    df = self.spark.read.parquet(path)
                    df.createOrReplaceTempView(f"{layer}_{table}")
                except Exception:
                    continue
    
    def query(self, sql):
        """Execute SQL query and return results."""
        start_time = time.time()
        
        try:
            # Execute the query against the registered temporary views
            result_df = self.spark.sql(sql)
            
            # Convert to Pandas DataFrame for display in Streamlit
            data = result_df.toPandas() 
            end_time = time.time()
            return data, end_time - start_time
        except Exception as e:
            end_time = time.time()
            return str(e), end_time - start_time


# --- Streamlit Application Logic ---

# Use Streamlit caching to prevent re-running this function on every interaction
@st.cache_resource(ttl=None)
def get_datalake_query_engine():
    """Cache the DataLakeQuery instance to keep Spark session alive."""
    try:
        engine = DataLakeQuery()
        return engine
    except Exception as e:
        st.error(f"🚨 Failed to initialize Spark or load data. Check MinIO/S3 connection/paths. Error: {e}")
        return None

def main_app():
    st.set_page_config(layout="wide", page_title="Data Lake Query Interface")

    st.title("💡 Data Lake Query Interface (Optimized)")
    st.markdown("Use this interactive tool to query your data lake layers. The Spark session is cached for **fast subsequent queries**.")

    # Get or initialize the cached query engine
    engine = get_datalake_query_engine()

    if engine is None:
        return # Stop execution if initialization fails

    # Get available tables for the predefined query dropdown
    available_tables = sorted([t.name for t in engine.spark.catalog.listTables()])
    
    # Predefined Query Options
    predefined_queries = {
        "Select an option or write custom SQL...": "",
        "Bronze: Top 10 Raw Films (bronze_film)": "SELECT title, length, rating, timestamp, operation FROM bronze_film LIMIT 10",
        "Silver: Top 10 Cleaned Customers (silver_customer)": "SELECT customer_id, first_name, email, create_date FROM silver_customer LIMIT 10",
        "Gold: Top 10 Films by Revenue (gold_film_performance)": "SELECT title, total_revenue, total_rentals FROM gold_film_performance ORDER BY total_revenue DESC LIMIT 10",
        "Gold: Daily Revenue Summary (gold_daily_revenue)": "SELECT date, ROUND(daily_revenue, 2) as revenue FROM gold_daily_revenue ORDER BY date DESC LIMIT 7",
        "Cross-Layer: Count Payments (bronze vs silver)": """
SELECT 'Bronze (Raw)' as layer, COUNT(*) as record_count FROM bronze_payment
UNION ALL
SELECT 'Silver (Cleaned)' as layer, COUNT(*) as record_count FROM silver_payment
"""
    }
    
    # --- UI Layout ---
    
    with st.expander("Explore Available Tables", expanded=False):
        st.markdown("**Click a table name to preview its content (LIMIT 10)**:")
        cols = st.columns(6)
        
        # Use session state to manage the custom query text area
        if 'custom_query_text' not in st.session_state:
            st.session_state.custom_query_text = predefined_queries["Bronze: Top 10 Raw Films (bronze_film)"]
        
        for i, table_name in enumerate(available_tables):
            if cols[i % 6].button(table_name, key=f"table_btn_{table_name}", help=f"Click to select SELECT * FROM {table_name} LIMIT 10"):
                st.session_state.custom_query_text = f"SELECT * FROM {table_name} LIMIT 10"

    st.subheader("Run Query")
    
    # Query Selection Dropdown
    selected_query_title = st.selectbox(
        "1. Select a Predefined Query:",
        options=list(predefined_queries.keys()),
        index=0,
    )
    
    # Update text area if a predefined query is selected
    if selected_query_title != "Select an option or write custom SQL...":
         st.session_state.custom_query_text = predefined_queries[selected_query_title]

    # Custom Query Input Area
    sql_query = st.text_area(
        "2. Custom SQL Query (Editable):",
        st.session_state.custom_query_text,
        height=150
    )

    # Use a run button to trigger execution
    run_button = st.button("🚀 Execute Query", type="primary")

    # --- Query Execution ---
    
    if run_button and sql_query.strip():
        with st.spinner(f"Executing SQL query..."):
            result, query_time = engine.query(sql_query)
            
            st.markdown("---")
            st.subheader("Query Results")
            st.code(sql_query, language='sql')
            
            # Display timing
            st.metric("Query Execution Time", f"{query_time:.3f} seconds")
            
            if isinstance(result, str):
                st.error(f"❌ Error executing query: {result}")
            else:
                if not result.empty:
                    st.dataframe(result, use_container_width=True)
                else:
                    st.warning("⚠️ Query executed successfully, but returned no results.")

if __name__ == "__main__":
    main_app()
