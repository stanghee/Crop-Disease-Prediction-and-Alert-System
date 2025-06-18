#!/bin/sh

# Funzione per verificare la disponibilità di un servizio
wait_for_service() {
    local host=$1
    local port=$2
    local service=$3
    local retries=60
    local wait=1

    echo "⏳ Attesa $service..."
    for i in $(seq 1 $retries); do
        if nc -z "$host" "$port" > /dev/null 2>&1; then
            echo "✅ $service disponibile su $host:$port"
            return 0
        fi
        echo "⏳ Attesa $service... ($i/$retries)"
        sleep $wait
    done
    echo "❌ $service non disponibile dopo $retries tentativi"
    return 1
}

# Verifica la disponibilità di Kafka
if ! wait_for_service kafka 9092 "Kafka"; then
    exit 1
fi

# Verifica la disponibilità di MinIO
if ! wait_for_service minio 9000 "MinIO"; then
    exit 1
fi

# Verifica che il bucket esista in MinIO
echo "🔄 Verifica bucket MinIO..."
for i in $(seq 1 30); do
    if curl -s "http://minio:9000/minio/health/live" > /dev/null; then
        echo "✅ MinIO è pronto"
        break
    fi
    echo "⏳ Attesa MinIO API... ($i/30)"
    sleep 2
done

echo "🚀 Starting satellite consumer..."
exec python consumer.py 