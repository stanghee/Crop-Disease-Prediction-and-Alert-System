#!/bin/sh

echo "⏳ Waiting for Kafka to be ready..."
python wait_for_kafka.py

echo "🚀 Starting weather data producer..."
python Producer.py