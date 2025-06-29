import socket
import time
import os

pg_host = os.getenv("POSTGRES_HOST", "postgres")
pg_port = 5432

while True:
    try:
        socket.create_connection((pg_host, pg_port))
        print("✅ PostgreSQL is ready!")
        break
    except OSError:
        print("⏳ Waiting for PostgreSQL...")
        time.sleep(1)