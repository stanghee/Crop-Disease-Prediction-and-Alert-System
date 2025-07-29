import random
import json
import time
from datetime import datetime
from copy import deepcopy
import os
from kafka import KafkaProducer
import logging
from zoneinfo import ZoneInfo

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default to Rome if not specified

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === CONFIGURATION PARAMETERS ===
random.seed(42)
# Configurable probability for anomalies (default 0.1% = 0.001)
ANOMALY_PROB = float(os.getenv("ANOMALY_PROBABILITY", "0.001")) 

# === SENSOR CONFIGURATION FOR EACH FIELD ===
FIELD_CONFIG = {
    "field_01": {
        "temperature": {"dist": "gauss", "mean": 24, "std": 2},
        "humidity": {"dist": "uniform", "low": 50, "high": 80},
        "soil_ph": {"dist": "gauss", "mean": 6.5, "std": 0.15}
    },
    "field_02": {
        "temperature": {"dist": "gauss", "mean": 28, "std": 1.5},
        "humidity": {"dist": "uniform", "low": 40, "high": 70},
        "soil_ph": {"dist": "gauss", "mean": 6.8, "std": 0.1}
    },
    "field_03": {
        "temperature": {"dist": "gauss", "mean": 22, "std": 2.5},
        "humidity": {"dist": "uniform", "low": 60, "high": 90},
        "soil_ph": {"dist": "gauss", "mean": 6.3, "std": 0.2}
    }
}


# === HELPER FUNCTIONS ===
def generate_value(config):
    if config["dist"] == "gauss":
        return round(random.gauss(config["mean"], config["std"]), 2)
    elif config["dist"] == "uniform":
        return round(random.uniform(config["low"], config["high"]), 2)
    else:
        raise ValueError("Unsupported distribution")

def inject_anomaly(sensor_type, value):
    """Inject anomalous values to simulate malfunctions or extreme conditions"""
    if sensor_type == "temperature":
        return round(value + random.choice([-10, 10]), 2)
    elif sensor_type == "humidity":
        return max(0, min(100, round(value + random.choice([-30, 30]), 2)))
    elif sensor_type == "soil_ph":
        return round(value + random.choice([-2, 2]), 2)
    else:
        return value

def generate_sensor_data(timestamp, field_id, config):
    data = {
        "timestamp": timestamp.isoformat(), 
        "field_id": field_id,
        "location": "Verona"  # Aggiunta della variabile location
    }
    
    for sensor in ["temperature", "humidity", "soil_ph"]:
        value = generate_value(config[sensor])
        
        # Generate anomaly with configurable probability
        if random.random() < ANOMALY_PROB:
            value = inject_anomaly(sensor, value)
            logger.debug(f"🔴 Anomaly generated for {sensor} in {field_id}: {value}")
        
        data[sensor] = value
    
    return data

# === REAL-TIME LOOP ===
def main():
    logger.info(f"Sensor simulation started with timezone {TIMEZONE}")
    logger.info(f"Anomaly probability: {ANOMALY_PROB:.3f} ({ANOMALY_PROB*100:.1f}%)")
    logger.info(f"Location: Verona")
    logger.info("CTRL+C to stop...\n")
    
    while True:
        try:
            now = datetime.now(ZoneInfo(TIMEZONE))
            for field_id, config in deepcopy(FIELD_CONFIG).items():
                data = generate_sensor_data(now, field_id, config)
                producer.send("sensor_data", data)  # send to Kafka topic
                logger.info(f"✅ Sent to Kafka: {data}")
            time.sleep(60)
        except Exception as e:
            logger.error(f"❌ Error sending data: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == "__main__":
    main()
