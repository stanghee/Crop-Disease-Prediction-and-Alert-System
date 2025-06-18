import random
import json
import time
from datetime import datetime
from copy import deepcopy
import os
from kafka import KafkaProducer
import logging
from zoneinfo import ZoneInfo

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default a Roma se non specificato

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === PARAMETRI DI CONFIGURAZIONE ===
random.seed(42)
ANOMALY_PROB = 0.001  # 0.1% di probabilità per evento anomalo

# === CONFIGURAZIONE SENSORI PER OGNI CAMPO ===
FIELD_CONFIG = {
    "field_01": {
        "temperature": {"dist": "gauss", "mean": 24, "std": 2},
        "humidity": {"dist": "uniform", "low": 50, "high": 80},
        "soil_pH": {"dist": "gauss", "mean": 6.5, "std": 0.15}
    },
    "field_02": {
        "temperature": {"dist": "gauss", "mean": 28, "std": 1.5},
        "humidity": {"dist": "uniform", "low": 40, "high": 70},
        "soil_pH": {"dist": "gauss", "mean": 6.8, "std": 0.1}
    },
    "field_03": {
        "temperature": {"dist": "gauss", "mean": 22, "std": 2.5},
        "humidity": {"dist": "uniform", "low": 60, "high": 90},
        "soil_pH": {"dist": "gauss", "mean": 6.3, "std": 0.2}
    }
}


# === FUNZIONI AUSILIARIE ===
def generate_value(config):
    if config["dist"] == "gauss":
        return round(random.gauss(config["mean"], config["std"]), 2)
    elif config["dist"] == "uniform":
        return round(random.uniform(config["low"], config["high"]), 2)
    else:
        raise ValueError("Distribuzione non supportata")

def inject_anomaly(sensor_type, value):
    if sensor_type == "temperature":
        return round(value + random.choice([-10, 10]), 2)
    elif sensor_type == "humidity":
        return max(0, min(100, round(value + random.choice([-30, 30]), 2)))
    elif sensor_type == "soil_pH":
        return round(value + random.choice([-2, 2]), 2)
    else:
        return value

def generate_sensor_data(timestamp, field_id, config):
    data = {"timestamp": timestamp.isoformat(), "field_id": field_id}
    anomaly_detected = False
    for sensor in ["temperature", "humidity", "soil_pH"]:
        value = generate_value(config[sensor])
        if random.random() < ANOMALY_PROB:
            value = inject_anomaly(sensor, value)
            anomaly_detected = True
        data[sensor] = value
    data["anomaly"] = anomaly_detected
    return data

# === LOOP IN TEMPO REALE ===
def main():
    logger.info(f"✅ Simulazione in tempo reale avviata con timezone {TIMEZONE} (CTRL+C per fermare)...\n")
    while True:
        try:
            now = datetime.now(ZoneInfo(TIMEZONE))
            for field_id, config in deepcopy(FIELD_CONFIG).items():
                data = generate_sensor_data(now, field_id, config)
                producer.send("sensor_data", data)  # invio al topic Kafka
                logger.info(f"✅ Inviato a Kafka: {data}")
            time.sleep(60)
        except Exception as e:
            logger.error(f"❌ Errore durante l'invio dei dati: {e}")
            time.sleep(5)  # Attendi prima di riprovare

if __name__ == "__main__":
    main()
