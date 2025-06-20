import os
import json
import logging
import uuid
from datetime import datetime
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import sys
from zoneinfo import ZoneInfo

# === Caricamento variabili ambiente ===
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_URL = "https://api.weatherapi.com/v1/current.json"
DEFAULT_LOCATION = os.getenv("DEFAULT_LOCATION", "Verona")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default a Roma se non specificato

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

logger.info(f"🚀 Weather producer starting up with timezone {TIMEZONE}...")

# === Check API_KEY ===
if not API_KEY:
    logger.error("❌ API_KEY non trovato! Assicurati che sia presente nel file .env o nelle variabili d'ambiente.")
    sys.exit(1)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Backup ultimi dati validi
last_valid_data = {
    "location": None,
    "region": None,
    "country": None,
    "lat": None,
    "lon": None,
    "condition": "Unknown"
}

def fetch_weather_data(location):
    try:
        params = {
            "key": API_KEY,
            "q": location,
            "aqi": "no"
        }
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            logger.error(f"❌ Errore API: {response.status_code} - {response.text}")
            return None
        return response.json()
    except requests.RequestException as e:
        logger.error(f"❌ Errore nella richiesta all'API: {e}")
        return None

def validate_and_prepare_data(data):
    try:
        # Usiamo il timestamp corrente con il fuso orario locale
        current_timestamp = datetime.now(ZoneInfo(TIMEZONE))
        
        loc = data.get('location', {})
        curr = data.get('current', {})

        location = loc.get('name') or last_valid_data['location']
        if not location:
            logger.error("❌ Nessuna location disponibile.")
            return None
        last_valid_data['location'] = location

        region = loc.get('region') or last_valid_data['region']
        country = loc.get('country') or last_valid_data['country']
        lat = loc.get('lat') or last_valid_data['lat']
        lon = loc.get('lon') or last_valid_data['lon']

        temp_c = curr.get('temp_c')
        if temp_c is None or not (-50 <= temp_c <= 60):
            logger.warning("⚠️ Temperatura non valida.")
            return None

        humidity = curr.get('humidity') if 0 <= (curr.get('humidity') or -1) <= 100 else -1
        uv = curr.get('uv') if 0 <= (curr.get('uv') or -1) <= 15 else -1
        wind_kph = curr.get('wind_kph') or 0
        condition = curr.get('condition', {}).get('text') or last_valid_data['condition']

        return {
            "message_id": str(uuid.uuid4()),
            "timestamp": current_timestamp.isoformat(),
            "location": location,
            "region": region,
            "country": country,
            "lat": lat,
            "lon": lon,
            "temp_c": temp_c,
            "humidity": humidity,
            "wind_kph": wind_kph,
            "condition": condition,
            "uv": uv
        }

    except Exception as e:
        logger.error(f"❌ Errore durante la validazione dei dati: {e}")
        return None

# Main loop
while True:
    raw_data = fetch_weather_data(DEFAULT_LOCATION)
    if raw_data:
        prepared_data = validate_and_prepare_data(raw_data)
        if prepared_data:
            # Rimuoviamo il campo local_time se presente
            if 'local_time' in prepared_data:
                del prepared_data['local_time']
            logger.info(f"✅ Invio: {prepared_data}")
            producer.send('weather-data', value=prepared_data)
        else:
            logger.warning("⛔ Dati non validi.")
    else:
        logger.error("❌ Nessun dato ricevuto dall'API. Controlla la chiave API, la connessione di rete, o i limiti dell'API.")
    time.sleep(60)











