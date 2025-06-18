#!/bin/sh

echo "⏳ Waiting for Kafka and Postgres to be ready..."
python wait_for_kafka.py
python wait_for_postgres.py

echo "🚀 Starting weather data consumer..."
python consumer.py