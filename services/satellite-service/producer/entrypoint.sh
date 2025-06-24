#!/bin/sh

echo "⏳ Waiting for Kafka to be ready..."
python wait_for_kafka.py

echo "🚀 Starting satellite data producer..."
python producer.py 