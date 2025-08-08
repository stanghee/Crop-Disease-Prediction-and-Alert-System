import socket
import time

host = 'kafka'
port = 9092

while True:
    try:
        socket.create_connection((host, port))
        print("✅ Kafka is ready!")
        break
    except OSError:
        print(" Waiting for Kafka...")
        time.sleep(1)