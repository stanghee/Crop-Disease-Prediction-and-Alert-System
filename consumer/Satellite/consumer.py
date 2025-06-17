import os
import json
import base64
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio_utils import upload_image, ensure_bucket_exists
from datetime import datetime

# === CONFIG ===
bucket_name = os.getenv("MINIO_BUCKET", "satellite-images")
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# === Inizializza MinIO bucket ===
ensure_bucket_exists(bucket_name)

# === Connessione con retry ===
MAX_RETRIES = 10
RETRY_DELAY = 5

consumer = None
for attempt in range(MAX_RETRIES):
    try:
        consumer = KafkaConsumer(
            'satellite-data',
            bootstrap_servers=[kafka_bootstrap],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='image-saver'
        )
        print("✅ Consumer connesso a Kafka.", flush=True)
        break
    except NoBrokersAvailable:
        print(f"❌ Kafka non disponibile (tentativo {attempt+1}/{MAX_RETRIES}) - ritento in {RETRY_DELAY}s", flush=True)
        time.sleep(RETRY_DELAY)

if not consumer:
    print("🚨 Impossibile connettersi a Kafka. Uscita.", flush=True)
    exit(1)

# === Polling continuo ===
print("🔄 Consumer attivo: in ascolto di nuove immagini...", flush=True)

while True:
    msg_pack = consumer.poll(timeout_ms=1000)
    for tp, messages in msg_pack.items():
        for message in messages:
            data = message.value
            timestamp = data['timestamp']
            bbox = data['location']['bbox']
            image_bytes = base64.b64decode(data['image_base64'])
            key = f"{timestamp.replace(':','_')}.png"

            upload_image(bucket_name, key, image_bytes)
            print(f"📤 Immagine salvata in MinIO: {key}", flush=True)
    time.sleep(1)