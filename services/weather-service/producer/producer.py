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

# Load environment variables 
load_dotenv()

API_KEY = os.getenv("API_KEY") # from .env file
API_URL = "https://api.weatherapi.com/v1/current.json"
DEFAULT_LOCATION = os.getenv("DEFAULT_LOCATION", "Verona")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default to Rome if not specified

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

logger.info(f" Weather producer starting up with timezone {TIMEZONE}...")

# Check API_KEY
if not API_KEY:
    logger.error("❌ API_KEY not found! Make sure it's present in the .env file or environment variables.")
    sys.exit(1)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather_data(location):
    try:
        params = {
            "key": API_KEY,
            "q": location,
            "aqi": "no"
        }
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            logger.error(f"❌ API Error: {response.status_code} - {response.text}")
            return None
        return response.json()
    except requests.RequestException as e:
        logger.error(f"❌ API Request Error: {e}")
        return None

def prepare_raw_data(data):
    """Prepare raw data without validation - validation will be done in Spark"""
    try:
        # Use current timestamp with local timezone
        current_timestamp = datetime.now(ZoneInfo(TIMEZONE))
        
        loc = data.get('location', {})
        curr = data.get('current', {})

        # Extract raw data without validation
        return {
            "message_id": str(uuid.uuid4()),
            "timestamp": current_timestamp.isoformat(),
            "location": loc.get('name'),
            "region": loc.get('region'),
            "country": loc.get('country'),
            "lat": loc.get('lat'),
            "lon": loc.get('lon'),
            "temp_c": curr.get('temp_c'),
            "humidity": curr.get('humidity'),
            "wind_kph": curr.get('wind_kph'),
            "condition": curr.get('condition', {}).get('text'),
            "uv": curr.get('uv')
        }

    except Exception as e:
        logger.error(f"❌ Error preparing data: {e}")
        return None

# Main loop
while True:
    raw_data = fetch_weather_data(DEFAULT_LOCATION)
    if raw_data:
        prepared_data = prepare_raw_data(raw_data)
        if prepared_data:
            logger.info(f"✅ Sending raw data: {prepared_data}")
            producer.send('weather_data', value=prepared_data)
            producer.flush()  # Ensure message is sent immediately
        else:
            logger.warning(" Error preparing data.")
    else:
        logger.error("❌ No data received from API. Check API key, network connection, or API limits.")
    time.sleep(60)






