import socket
import time
import os

kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(":")[0]
kafka_port = int(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(":")[1])

while True:
    try:
        socket.create_connection((kafka_host, kafka_port))
        print("✅ Kafka is ready!")
        break
    except OSError:
        print("⏳ Waiting for Kafka...")
        time.sleep(1)