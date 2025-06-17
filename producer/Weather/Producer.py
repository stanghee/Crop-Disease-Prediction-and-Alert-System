import os
import json
import logging
import uuid
from datetime import datetime
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# === Caricamento variabili ambiente ===
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_URL = "https://api.weatherapi.com/v1/current.json"
DEFAULT_LOCATION = os.getenv("DEFAULT_LOCATION", "Verona")
SENTRY_DSN = os.getenv("SENTRY_DSN")

# === Inizializzazione Sentry (se configurato) ===
if SENTRY_DSN:
    import sentry_sdk
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=1.0,
        environment="production"
    )

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
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
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        sentry_sdk.capture_exception(e)
        logger.error(f"Errore nella richiesta all'API: {e}")
        return None

def validate_and_prepare_data(data):
    try:
        loc = data.get('location', {})
        curr = data.get('current', {})

        location = loc.get('name') or last_valid_data['location']
        if not location:
            sentry_sdk.capture_message("❌ Nessuna location disponibile.", level="error")
            return None
        last_valid_data['location'] = location

        region = loc.get('region') or last_valid_data['region']
        country = loc.get('country') or last_valid_data['country']
        lat = loc.get('lat') or last_valid_data['lat']
        lon = loc.get('lon') or last_valid_data['lon']
        local_time = loc.get('localtime') or datetime.utcnow().isoformat()

        temp_c = curr.get('temp_c')
        if temp_c is None or not (-50 <= temp_c <= 60):
            sentry_sdk.capture_message("⚠️ Temperatura non valida.", level="warning")
            return None

        humidity = curr.get('humidity') if 0 <= (curr.get('humidity') or -1) <= 100 else -1
        uv = curr.get('uv') if 0 <= (curr.get('uv') or -1) <= 15 else -1
        wind_kph = curr.get('wind_kph') or 0
        condition = curr.get('condition', {}).get('text') or last_valid_data['condition']

        return {
            "message_id": str(uuid.uuid4()),
            "location": location,
            "region": region,
            "country": country,
            "lat": lat,
            "lon": lon,
            "local_time": local_time,
            "temp_c": temp_c,
            "humidity": humidity,
            "wind_kph": wind_kph,
            "condition": condition,
            "uv": uv
        }

    except Exception as e:
        sentry_sdk.capture_exception(e)
        return None

# Main loop
while True:
    raw_data = fetch_weather_data(DEFAULT_LOCATION)
    if raw_data:
        prepared_data = validate_and_prepare_data(raw_data)
        if prepared_data:
            logger.info(f"✅ Invio: {prepared_data}")
            producer.send('weather-data', value=prepared_data)
        else:
            logger.warning("⛔ Dati non validi.")
    else:
        logger.error("❌ Nessun dato ricevuto dall'API.")
    time.sleep(10)











