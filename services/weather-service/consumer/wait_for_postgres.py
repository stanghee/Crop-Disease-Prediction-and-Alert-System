import socket
import time

host = 'postgres'
port = 5432

for i in range(60):
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f"✅ Postgres disponibile su {host}:{port}")
            break
    except OSError:
        print(f"⏳ Attesa Postgres... ({i+1}/60)")
        time.sleep(1)
else:
    raise Exception("❌ Postgres non disponibile")