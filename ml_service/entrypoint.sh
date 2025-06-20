#!/bin/bash

echo "🚀 Starting ML Service for Anomaly Detection..."

# Attendi che PostgreSQL sia pronto
echo "⏳ Waiting for PostgreSQL..."
until nc -z ${POSTGRES_HOST:-postgres} 5432; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
done
echo "✅ PostgreSQL is ready!"

# Attendi che Kafka sia pronto
echo "⏳ Waiting for Kafka..."
until nc -z ${KAFKA_BOOTSTRAP_SERVERS%:*} ${KAFKA_BOOTSTRAP_SERVERS#*:}; do
    echo "Kafka is unavailable - sleeping"
    sleep 2
done
echo "✅ Kafka is ready!"

echo "🤖 Starting ML Service..."
python ml_service.py 