import socket
import time

host = 'kafka'
port = 9092

for i in range(60):
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f"✅ Kafka disponibile su {host}:{port}")
            break
    except OSError:
        print(f"⏳ Attesa Kafka... ({i+1}/60)")
        time.sleep(1)
else:
    raise Exception("❌ Kafka non disponibile")