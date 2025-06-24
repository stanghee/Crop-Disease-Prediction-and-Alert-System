import os
import time
import json
import logging
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
import socket
from datetime import datetime
from zoneinfo import ZoneInfo

consumer_id = socket.gethostname()

# === Logging configuration ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# === Load environment variables ===
load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")
TOPIC = "processed_weather_data"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weatherdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")

if not POSTGRES_PASSWORD:
    logger.error("❌ POSTGRES_PASSWORD non definito!")
    exit(1)

def connect_to_db(retries=10, delay=5):
    for attempt in range(retries):
        try:
            logger.info(f"🔄 Connessione al database PostgreSQL ({attempt + 1}/{retries})...")
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            # Imposta il timezone per la sessione
            with conn.cursor() as cur:
                cur.execute(f"SET timezone = '{TIMEZONE}';")
            conn.commit()
            logger.info("✅ Connessione a PostgreSQL riuscita.")
            return conn
        except psycopg2.Error as e:
            if attempt == retries - 1:
                logger.error(f"❌ Impossibile connettersi al database dopo {retries} tentativi: {e}")
                raise
            logger.warning(f"⚠️ Tentativo {attempt + 1} fallito: {e}")
            time.sleep(delay)

conn = connect_to_db()
cursor = conn.cursor()

# === Create simplified table optimized for ML training ===
cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP WITH TIME ZONE,
        location TEXT,
        region TEXT,
        country TEXT,
        lat REAL,
        lon REAL,
        temp_c REAL,
        humidity INT,
        wind_kph REAL,
        condition TEXT,
        uv REAL,
        message_id TEXT,
        data_quality TEXT
    );
''')
conn.commit()

# === Connessione a Kafka per dati processati ===
consumer = None
for attempt in range(10):
    try:
        logger.info(f"🔄 Tentativo di connessione a Kafka ({attempt+1}/10)...")
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="processed-weather-group"
        )
        logger.info(f"✅ Connected to Kafka. Listening for ML-ready weather data...")
        break
    except NoBrokersAvailable as e:
        logger.warning("⚠️ Kafka non disponibile, nuovo tentativo tra 5 secondi...")
        time.sleep(5)
else:
    logger.error("❌ Errore: impossibile connettersi a Kafka dopo vari tentativi.")
    exit(1)

# === Consumo dei messaggi processati ===
for message in consumer:
    try:
        message_data = message.value
        
        # Extract data from Spark format
        if isinstance(message_data, dict) and 'value' in message_data:
            data_str = message_data['value'] 
            data = json.loads(data_str) if isinstance(data_str, str) else data_str
        else:
            data = message_data
            
        logger.info(f"📨 [{consumer_id}] ML-ready weather data: {data['location']} - Quality: {data['data_quality']}")

        # Data is already validated and processed by Spark
        timestamp = datetime.fromisoformat(data['timestamp'])

        # === Store only essential data for ML training ===
        cursor.execute('''
            INSERT INTO weather_data (
                timestamp, location, region, country, lat, lon,
                temp_c, humidity, wind_kph, condition, uv, message_id, data_quality
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        ''', (
            timestamp,
            data['location'], data['region'], data['country'],
            data['lat'], data['lon'], data['temp_c'], data['humidity'], 
            data['wind_kph'], data['condition'], data['uv'], data['message_id'],
            data['data_quality']
        ))
        conn.commit()
        
        status_emoji = "⚠️" if data["data_quality"] == "anomaly" else "✅"
        logger.info(f"{status_emoji} ML training data saved: {data['location']} - Quality: {data['data_quality']}")

    except Exception as e:
        logger.error(f"❌ Errore durante l'elaborazione del messaggio: {e}")
        continue