"""
Fast Query Interface - PostgreSQL
Query Gold layer with sub-second response times
"""
import pandas as pd
from sqlalchemy import create_engine, text
import sys

class FastQuery:
    def __init__(self):
        self.engine = create_engine('postgresql://analytics:analytics123@localhost:5432/analytics')
        print("Connected to PostgreSQL Analytics Database")
    
    def query(self, sql):
        """Execute SQL query and return results"""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(sql, conn)
            return df
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    def show_tables(self):
        """Show available tables"""
        sql = """
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        ORDER BY tablename
        """
        tables = self.query(sql)
        
        print("\n" + "=" * 60)
        print("AVAILABLE GOLD LAYER TABLES")
        print("=" * 60)
        if tables is not None:
            for table in tables['tablename']:
                print(f"  - {table}")
        print("=" * 60 + "\n")
    
    def describe_table(self, table_name):
        """Show table schema and row count"""
        # Get schema
        schema_sql = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        schema = self.query(schema_sql)
        
        # Get count
        count_sql = f"SELECT COUNT(*) as count FROM {table_name}"
        count = self.query(count_sql)
        
        print(f"\n{'=' * 60}")
        print(f"TABLE: {table_name}")
        print(f"{'=' * 60}")
        print(f"Row Count: {count['count'][0] if count is not None else 'N/A'}")
        print("\nSchema:")
        if schema is not None:
            print(schema.to_string(index=False))
        print("=" * 60 + "\n")
    
    def sample_data(self, table_name, limit=5):
        """Show sample rows"""
        sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        df = self.query(sql)
        
        if df is not None:
            print(f"\nSample data from {table_name}:")
            print(df.to_string(index=False))
            print()
    
    def sample_queries(self):
        """Run sample analytical queries"""
        print("\n" + "=" * 60)
        print("SAMPLE QUERIES")
        print("=" * 60)
        
        # Query 1
        print("\n1. Customer Value Distribution:")
        print("-" * 60)
        q1 = """
        SELECT 
            customer_value_tier,
            COUNT(*) as customers,
            ROUND(AVG(total_spent)::numeric, 2) as avg_spent,
            ROUND(SUM(total_spent)::numeric, 2) as total_revenue
        FROM customer_summary
        GROUP BY customer_value_tier
        ORDER BY total_revenue DESC
        """
        result = self.query(q1)
        if result is not None:
            print(result.to_string(index=False))
        
        # Query 2
        print("\n\n2. Top 10 Films by Revenue:")
        print("-" * 60)
        q2 = """
        SELECT 
            title,
            release_year,
            total_rentals,
            ROUND(total_revenue::numeric, 2) as revenue
        FROM film_performance
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        result = self.query(q2)
        if result is not None:
            print(result.to_string(index=False))
        
        # Query 3
        print("\n\n3. Recent Daily Revenue:")
        print("-" * 60)
        q3 = """
        SELECT 
            payment_date,
            total_transactions,
            ROUND(total_revenue::numeric, 2) as revenue,
            ROUND(avg_transaction_amount::numeric, 2) as avg_transaction
        FROM daily_revenue
        ORDER BY payment_date DESC
        LIMIT 7
        """
        result = self.query(q3)
        if result is not None:
            print(result.to_string(index=False))
        
        # Query 4
        print("\n\n4. High-Value Active Customers:")
        print("-" * 60)
        q4 = """
        SELECT 
            email,
            customer_value_tier,
            total_rentals,
            ROUND(total_spent::numeric, 2) as spent
        FROM customer_summary
        WHERE customer_value_tier IN ('Gold', 'Premium')
        ORDER BY total_spent DESC
        LIMIT 10
        """
        result = self.query(q4)
        if result is not None:
            print(result.to_string(index=False))
        
        print("\n" + "=" * 60)
    
    def interactive_mode(self):
        """Interactive SQL mode"""
        print("\n" + "=" * 60)
        print("FAST QUERY INTERFACE (PostgreSQL)")
        print("=" * 60)
        print("Commands:")
        print("  tables      - Show all tables")
        print("  describe X  - Show schema for table X")
        print("  sample X    - Show sample data from table X")
        print("  exit        - Quit")
        print("=" * 60 + "\n")
        
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
                elif query.lower().startswith('sample '):
                    table = query.split(maxsplit=1)[1]
                    self.sample_data(table)
                elif query:
                    import time
                    start = time.time()
                    result = self.query(query)
                    elapsed = time.time() - start
                    
                    if result is not None:
                        print(result.to_string(index=False))
                        print(f"\nQuery time: {elapsed:.3f} seconds")
                        print(f"Rows returned: {len(result)}\n")
                        
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except EOFError:
                break
            except Exception as e:
                print(f"Error: {e}")


def main():
    querier = FastQuery()
    
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
            import time
            start = time.time()
            result = querier.query(sql)
            elapsed = time.time() - start
            
            if result is not None:
                print(result.to_string(index=False))
                print(f"\nQuery time: {elapsed:.3f} seconds")
    else:
        querier.sample_queries()


if __name__ == "__main__":
    main()