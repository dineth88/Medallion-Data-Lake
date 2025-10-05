"""
MySQL to Kafka Producer
Reads data from MySQL Sakila DB and publishes to Kafka topics
"""
import json
import os
import time
from datetime import datetime
from kafka import KafkaProducer
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MySQLToKafka:
    def __init__(self, mysql_config, kafka_config):
        # MySQL connection
        self.engine = create_engine(
            f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@"
            f"{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        )
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        self.tables = [
            'actor', 'address', 'category', 'city', 'country', 
            'customer', 'film', 'film_actor', 'film_category',
            'inventory', 'language', 'payment', 'rental', 'staff', 'store'
        ]
    
    def fetch_table_data(self, table_name, batch_size=1000):
        """Fetch data from MySQL table in batches"""
        query = f"SELECT * FROM {table_name}"
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            
            batch = []
            for row in result:
                row_dict = dict(zip(columns, row))
                batch.append(row_dict)
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            
            if batch:
                yield batch
    
    def publish_to_kafka(self, table_name, full_load=True):
        """Publish table data to Kafka topic"""
        topic_name = f"sakila.{table_name}"
        record_count = 0
        
        try:
            logger.info(f"Starting data extraction from table: {table_name}")
            
            for batch in self.fetch_table_data(table_name):
                for record in batch:
                    # Add metadata
                    message = {
                        'table': table_name,
                        'operation': 'INSERT' if full_load else 'UPDATE',
                        'timestamp': datetime.now().isoformat(),
                        'data': record
                    }
                    
                    # Use primary key as message key if available
                    key = str(record.get(f'{table_name}_id', record_count))
                    
                    self.producer.send(topic_name, key=key, value=message)
                    record_count += 1
                
                logger.info(f"Published {record_count} records from {table_name}")
            
            self.producer.flush()
            logger.info(f"✓ Completed: {table_name} - {record_count} records")
            
        except Exception as e:
            logger.error(f"✗ Error processing {table_name}: {e}")
    
    def run_full_load(self):
        """Run full load for all tables"""
        logger.info("Starting full load from MySQL to Kafka")
        
        for table in self.tables:
            self.publish_to_kafka(table, full_load=True)
            time.sleep(1)
        
        logger.info("Full load completed!")
    
    def close(self):
        """Close connections"""
        self.producer.close()
        self.engine.dispose()

if __name__ == "__main__":
    # Configuration
    mysql_config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': 'localhost',
        'port': 3306,
        'database': 'sakila'
    }
    
    kafka_config = {
        'bootstrap_servers': ['localhost:29092']
    }
    
    # Run producer
    producer = MySQLToKafka(mysql_config, kafka_config)
    try:
        producer.run_full_load()
    finally:
        producer.close()