"""
Kafka to S3 Consumer (Bronze Layer)
Consumes messages from Kafka and stores raw data in S3/MinIO
"""
import json
from datetime import datetime
import os
from kafka import KafkaConsumer
import boto3
from botocore.client import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaToS3Bronze:
    def __init__(self, kafka_config, s3_config):
        # Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='bronze-layer-consumer'
        )
        
        # S3 client (MinIO)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_config['endpoint_url'],
            aws_access_key_id=s3_config['access_key'],
            aws_secret_access_key=s3_config['secret_key'],
            config=Config(signature_version='s3v4')
        )
        
        self.bucket = s3_config['bucket']
        self.buffer = {}
        self.buffer_size = 100
    
    def subscribe_topics(self, topics):
        """Subscribe to Kafka topics"""
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
    
    def write_to_s3(self, table_name, records):
        """Write records to S3 in JSON format"""
        if not records:
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"bronze/{table_name}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/{timestamp}.json"
        
        try:
            # Write as JSON Lines format
            content = '\n'.join([json.dumps(record) for record in records])
            
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"✓ Written {len(records)} records to s3://{self.bucket}/{key}")
            
        except Exception as e:
            logger.error(f"✗ Error writing to S3: {e}")
    
    def consume_messages(self):
        """Consume messages from Kafka and write to S3"""
        logger.info("Starting Kafka consumer...")
        
        try:
            for message in self.consumer:
                data = message.value
                table_name = data.get('table')
                
                # Buffer messages by table
                if table_name not in self.buffer:
                    self.buffer[table_name] = []
                
                self.buffer[table_name].append(data)
                
                # Flush buffer when it reaches size limit
                if len(self.buffer[table_name]) >= self.buffer_size:
                    self.write_to_s3(table_name, self.buffer[table_name])
                    self.buffer[table_name] = []
                    
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
            self.flush_all_buffers()
        finally:
            self.consumer.close()
    
    def flush_all_buffers(self):
        """Flush remaining buffered messages"""
        for table_name, records in self.buffer.items():
            if records:
                self.write_to_s3(table_name, records)

if __name__ == "__main__":
    # Configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:29092']
    }
    
    s3_config = {
        'endpoint_url': os.getenv('ENDPOINT_URL', 'http://localhost:9000'),
        'access_key': os.getenv('ACCESS_KEY'),
        'secret_key': os.getenv('SECRET_KEY'),
        'bucket': 'datalake-bronze'
    }
    
    # Subscribe to all sakila topics
    topics = [
        'sakila.actor', 'sakila.address', 'sakila.category', 
        'sakila.city', 'sakila.country', 'sakila.customer',
        'sakila.film', 'sakila.film_actor', 'sakila.film_category',
        'sakila.inventory', 'sakila.language', 'sakila.payment',
        'sakila.rental', 'sakila.staff', 'sakila.store'
    ]
    
    # Run consumer
    consumer = KafkaToS3Bronze(kafka_config, s3_config)
    consumer.subscribe_topics(topics)
    consumer.consume_messages()