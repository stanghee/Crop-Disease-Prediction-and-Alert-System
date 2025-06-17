#!/bin/sh

# Wait for Kafka
for i in $(seq 1 60); do
  nc -z kafka 9092 && echo "✅ Kafka disponibile su kafka:9092" && break
  echo "⏳ Attesa Kafka... ($i/60)" && sleep 1
done

# Wait for MinIO
for i in $(seq 1 60); do
  nc -z minio 9000 && echo "✅ MinIO disponibile su minio:9000" && break
  echo "⏳ Attesa MinIO... ($i/60)" && sleep 1
done

echo "🚀 Starting satellite consumer..."
python consumer.py 