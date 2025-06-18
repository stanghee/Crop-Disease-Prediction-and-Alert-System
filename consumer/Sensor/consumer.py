import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configurazione Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sensordb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default a Roma se non specificato

# Connessione al database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            options=f"-c timezone={TIMEZONE}"
        )
        logger.info("✅ Connesso al database PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"❌ Errore connessione al database: {e}")
        raise

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sensor_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                field_id TEXT,
                temperature FLOAT,
                humidity FLOAT,
                soil_pH FLOAT,
                anomaly BOOLEAN
            )
        ''')
    conn.commit()
    logger.info("✅ Tabella sensor_data creata/verificata")

def main():
    # Connessione al database e creazione tabella
    conn = connect_to_db()
    create_table(conn)

    # Configurazione consumer
    consumer = KafkaConsumer(
        "sensor_data",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="sensor-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info("🟢 In ascolto sul topic 'sensor_data'...")

    try:
        for message in consumer:
            try:
                data = message.value
                logger.info(f"📥 Ricevuto: {data}")

                # Validazione dati
                required_fields = ["timestamp", "field_id", "temperature", "humidity", "soil_pH", "anomaly"]
                if not all(field in data for field in required_fields):
                    logger.warning(f"⚠️ Dati incompleti: {data}")
                    continue

                # Conversione timestamp
                timestamp = datetime.fromisoformat(data["timestamp"].rstrip("Z"))

                # Inserimento nel database
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO sensor_data 
                        (timestamp, field_id, temperature, humidity, soil_pH, anomaly)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (
                        timestamp,
                        data["field_id"],
                        data["temperature"],
                        data["humidity"],
                        data["soil_pH"],
                        data["anomaly"]
                    ))
                conn.commit()
                logger.info("✅ Dati salvati nel database")

            except Exception as e:
                logger.error(f"❌ Errore processamento messaggio: {e}")
                continue

    except Exception as e:
        logger.error(f"❌ Errore fatale: {e}")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
