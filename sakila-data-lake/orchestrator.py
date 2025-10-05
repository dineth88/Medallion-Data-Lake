"""
Data Lake Orchestrator
Manages the entire data pipeline execution
"""
import time
import yaml
import logging
from datetime import datetime
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataLakeOrchestrator:
    def __init__(self, config_file='config.yaml'):
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.pipeline_status = {
            'mysql_to_kafka': 'pending',
            'kafka_to_bronze': 'pending',
            'bronze_to_silver': 'pending',
            'silver_to_gold': 'pending'
        }
    
    def check_dependencies(self):
        """Check if all required services are running"""
        logger.info("Checking dependencies...")
        
        dependencies = {
            'Docker': 'docker ps',
            'Kafka': 'docker ps | findstr kafka',
            'MinIO': 'docker ps | findstr minio',
            'Spark': 'docker ps | findstr spark'
        }
        
        all_good = True
        for service, command in dependencies.items():
            try:
                result = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    logger.info(f"✓ {service} is running")
                else:
                    logger.error(f"✗ {service} is not running")
                    all_good = False
            except Exception as e:
                logger.error(f"✗ Error checking {service}: {e}")
                all_good = False
        
        return all_good
    
    def run_mysql_to_kafka(self):
        """Execute MySQL to Kafka data extraction"""
        logger.info("=" * 60)
        logger.info("STEP 1: MySQL to Kafka")
        logger.info("=" * 60)
        
        try:
            from mysql_to_kafka import MySQLToKafka
            
            mysql_config = self.config['mysql']
            kafka_config = {
                'bootstrap_servers': self.config['kafka']['bootstrap_servers']
            }
            
            producer = MySQLToKafka(mysql_config, kafka_config)
            producer.run_full_load()
            producer.close()
            
            self.pipeline_status['mysql_to_kafka'] = 'completed'
            logger.info("✓ MySQL to Kafka completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ MySQL to Kafka failed: {e}")
            self.pipeline_status['mysql_to_kafka'] = 'failed'
            return False
    
    def run_kafka_to_bronze(self, duration=60):
        """Execute Kafka to Bronze layer ingestion"""
        logger.info("=" * 60)
        logger.info("STEP 2: Kafka to Bronze Layer")
        logger.info("=" * 60)
        
        try:
            from kafka_to_bronze import KafkaToS3Bronze
            
            kafka_config = {
                'bootstrap_servers': self.config['kafka']['bootstrap_servers']
            }
            
            s3_config = {
                'endpoint_url': self.config['s3']['endpoint_url'],
                'access_key': self.config['s3']['access_key'],
                'secret_key': self.config['s3']['secret_key'],
                'bucket': self.config['s3']['buckets']['bronze']
            }
            
            consumer = KafkaToS3Bronze(kafka_config, s3_config)
            consumer.subscribe_topics(self.config['kafka']['topics'])
            
            logger.info(f"Consuming messages for {duration} seconds...")
            
            # Run consumer for specified duration
            import threading
            consumer_thread = threading.Thread(target=consumer.consume_messages)
            consumer_thread.daemon = True
            consumer_thread.start()
            
            time.sleep(duration)
            
            consumer.flush_all_buffers()
            
            self.pipeline_status['kafka_to_bronze'] = 'completed'
            logger.info("✓ Kafka to Bronze completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Kafka to Bronze failed: {e}")
            self.pipeline_status['kafka_to_bronze'] = 'failed'
            return False
    
    def run_bronze_to_silver(self):
        """Execute Bronze to Silver layer transformation"""
        logger.info("=" * 60)
        logger.info("STEP 3: Bronze to Silver Layer")
        logger.info("=" * 60)
        
        try:
            from bronze_to_silver import BronzeToSilver
            
            s3_config = {
                'endpoint_url': self.config['s3']['endpoint_url'],
                'access_key': self.config['s3']['access_key'],
                'secret_key': self.config['s3']['secret_key'],
                'bronze_bucket': self.config['s3']['buckets']['bronze'],
                'silver_bucket': self.config['s3']['buckets']['silver']
            }
            
            processor = BronzeToSilver(s3_config)
            processor.run_pipeline()
            processor.stop()
            
            self.pipeline_status['bronze_to_silver'] = 'completed'
            logger.info("✓ Bronze to Silver completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Bronze to Silver failed: {e}")
            self.pipeline_status['bronze_to_silver'] = 'failed'
            return False
    
    def run_silver_to_gold(self):
        """Execute Silver to Gold layer aggregation"""
        logger.info("=" * 60)
        logger.info("STEP 4: Silver to Gold Layer")
        logger.info("=" * 60)
        
        try:
            from silver_to_gold import SilverToGold
            
            s3_config = {
                'endpoint_url': self.config['s3']['endpoint_url'],
                'access_key': self.config['s3']['access_key'],
                'secret_key': self.config['s3']['secret_key'],
                'silver_bucket': self.config['s3']['buckets']['silver'],
                'gold_bucket': self.config['s3']['buckets']['gold']
            }
            
            processor = SilverToGold(s3_config)
            processor.run_pipeline()
            processor.stop()
            
            self.pipeline_status['silver_to_gold'] = 'completed'
            logger.info("✓ Silver to Gold completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Silver to Gold failed: {e}")
            self.pipeline_status['silver_to_gold'] = 'failed'
            return False
    
    def run_full_pipeline(self):
        """Execute the complete data lake pipeline"""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("DATA LAKE PIPELINE EXECUTION STARTED")
        logger.info(f"Start Time: {start_time}")
        logger.info("=" * 60)
        
        # Check dependencies
        if not self.check_dependencies():
            logger.error("✗ Dependency check failed. Please start all required services.")
            return False
        
        # Execute pipeline stages
        stages = [
            ('mysql_to_kafka', self.run_mysql_to_kafka),
            ('kafka_to_bronze', lambda: self.run_kafka_to_bronze(duration=60)),
            ('bronze_to_silver', self.run_bronze_to_silver),
            ('silver_to_gold', self.run_silver_to_gold)
        ]
        
        for stage_name, stage_func in stages:
            success = stage_func()
            if not success:
                logger.error(f"Pipeline failed at stage: {stage_name}")
                self.print_status()
                return False
            time.sleep(5)  # Brief pause between stages
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("DATA LAKE PIPELINE EXECUTION COMPLETED")
        logger.info(f"End Time: {end_time}")
        logger.info(f"Total Duration: {duration}")
        logger.info("=" * 60)
        
        self.print_status()
        return True
    
    def print_status(self):
        """Print pipeline execution status"""
        logger.info("\nPipeline Status:")
        logger.info("-" * 60)
        for stage, status in self.pipeline_status.items():
            status_icon = "✓" if status == "completed" else "✗" if status == "failed" else "⏳"
            logger.info(f"{status_icon} {stage}: {status}")
        logger.info("-" * 60)

if __name__ == "__main__":
    orchestrator = DataLakeOrchestrator()
    orchestrator.run_full_pipeline()