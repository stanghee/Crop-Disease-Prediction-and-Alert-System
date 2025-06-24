#!/bin/bash

echo "🚀 Starting Flink ML Service for Crop Disease Prediction..."

# Wait for required services to be ready
echo "⏳ Waiting for Kafka to be ready..."
python3 -c "
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

while True:
    try:
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092')
        consumer.close()
        print('✅ Kafka is ready!')
        break
    except NoBrokersAvailable:
        print('⏳ Waiting for Kafka...')
        time.sleep(5)
"

echo "⏳ Waiting for PostgreSQL to be ready..."
python3 -c "
import time
import psycopg2
import os

while True:
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'sensordb'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        conn.close()
        print('✅ PostgreSQL is ready!')
        break
    except Exception as e:
        print(f'⏳ Waiting for PostgreSQL... ({e})')
        time.sleep(5)
"

echo "✅ All dependencies ready. Starting Spark ML Service..."

# Start the Spark ML service
exec python3 spark_preprocessing_service.py 