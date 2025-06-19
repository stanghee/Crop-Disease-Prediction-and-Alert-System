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
import uuid

consumer_id = socket.gethostname()

# === Configurazione logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# === Caricamento variabili d'ambiente ===
load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")
TOPIC = "weather-data"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weatherdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
SENTRY_DSN = os.getenv("SENTRY_DSN")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default a Roma se non specificato

# === Inizializzazione Sentry se configurato ===
if SENTRY_DSN:
    import sentry_sdk
    from sentry_sdk import capture_exception
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=1.0,
        environment="production"
    )
    logger.info("✅ Sentry inizializzato.")
else:
    def capture_exception(e):  # Dummy fallback se Sentry non è attivo
        pass

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

# === Creazione tabella se non esiste ===
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
        uv REAL
    );
''')
conn.commit()

# === Connessione a Kafka con retry ===
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
            group_id="weather-group"
        )
        logger.info(f"✅ Connesso a Kafka. In ascolto sul topic '{TOPIC}'...")
        break
    except NoBrokersAvailable as e:
        logger.warning("⚠️ Kafka non disponibile, nuovo tentativo tra 5 secondi...")
        capture_exception(e)
        time.sleep(5)
else:
    logger.error("❌ Errore: impossibile connettersi a Kafka dopo vari tentativi.")
    exit(1)

# === Consumo dei messaggi con validazione e back-off ===
invalid_counter = 0
max_invalid = 5
backoff_time = 30  # seconds

for message in consumer:
    try:
        data = message.value
        logger.info(f"📨 [{consumer_id}] Ricevuto: {data}")

        required_keys = {'timestamp', 'location', 'region', 'country', 'lat', 'lon',
                         'temp_c', 'humidity', 'wind_kph', 'condition', 'uv'}

        if not required_keys.issubset(data):
            logger.warning(f"⚠️ Dato incompleto o malformato: {data}")
            invalid_counter += 1
            continue

        # === Range validation ===
        if not (-50 <= data['temp_c'] <= 60):
            logger.warning("⚠️ Temperatura fuori intervallo logico.")
            invalid_counter += 1
            continue
        if not (0 <= data['humidity'] <= 100):
            logger.warning("⚠️ Umidità fuori intervallo logico.")
            invalid_counter += 1
            continue
        if not (0 <= data['uv'] <= 15):
            logger.warning("⚠️ Indice UV fuori intervallo logico.")
            invalid_counter += 1
            continue
        if data['wind_kph'] < 0:
            logger.warning("⚠️ Velocità del vento non può essere negativa.")
            invalid_counter += 1
            continue

        # Reset counter on valid message
        invalid_counter = 0

        # === Parse timestamp with timezone ===
        timestamp = datetime.fromisoformat(data['timestamp'])
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=ZoneInfo(TIMEZONE))

        # === Insert into DB ===
        cursor.execute('''
            INSERT INTO weather_data (
                timestamp, location, region, country, lat, lon,
                temp_c, humidity, wind_kph, condition, uv
            ) VALUES (
                %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        ''', (
            timestamp,
            data['location'], data['region'], data['country'],
            data['lat'], data['lon'], data['temp_c'], data['humidity'], data['wind_kph'],
            data['condition'], data['uv']
        ))
        conn.commit()
        logger.info("✅ Dato inserito nel database.")

    except Exception as e:
        logger.error(f"❌ Errore durante l'elaborazione del messaggio: {e}")
        capture_exception(e)
        invalid_counter += 1

    # === Back-off if too many invalid messages ===
    if invalid_counter >= max_invalid:
        logger.error("🚨 Troppi messaggi non validi consecutivi. Pausa automatica.")
        capture_exception(Exception("Max consecutive invalid messages reached."))
        time.sleep(backoff_time)
        invalid_counter = 0