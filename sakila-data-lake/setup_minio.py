"""
MinIO Setup Script
Creates necessary buckets for the data lake
"""
from minio import Minio
from minio.error import S3Error
import os

def setup_minio():
    # Initialize MinIO client
    client = Minio(
        os.getenv('ENDPOINT_URL'),
        access_key=os.getenv('ACCESS_KEY'),
        secret_key=os.getenv('SECRET_KEY'),
        secure=False
    )
    
    # Create buckets
    buckets = ["datalake-bronze", "datalake-silver", "datalake-gold"]
    
    for bucket in buckets:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"✓ Created bucket: {bucket}")
            else:
                print(f"✓ Bucket already exists: {bucket}")
        except S3Error as e:
            print(f"✗ Error creating bucket {bucket}: {e}")

if __name__ == "__main__":
    setup_minio()