import os
import json
import logging
import pandas as pd
import numpy as np
import joblib
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import uuid
import time

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Caricamento variabili d'ambiente
load_dotenv()

# Configurazione Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SENSOR_TOPIC = "sensor_data"
ALERT_TOPIC = "sensor_alerts"

# Parametri del modello
MODEL_PATH = "/app/models/anomaly_model.joblib"
SCALER_PATH = "/app/models/scaler.joblib"

class RealtimeAnomalyDetector:
    def __init__(self):
        self.model = None
        self.scaler = None
        self.feature_columns = ['temperature', 'humidity', 'soil_ph']
        self.is_model_loaded = False
        
        # Configurazione Kafka
        self.consumer = None
        self.producer = None
        
    def load_model(self):
        """Carica il modello e il scaler"""
        try:
            if os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH):
                self.model = joblib.load(MODEL_PATH)
                self.scaler = joblib.load(SCALER_PATH)
                self.is_model_loaded = True
                logger.info("✅ Modello e scaler caricati con successo")
                return True
            else:
                logger.error("❌ File del modello non trovati. Eseguire prima il training!")
                return False
        except Exception as e:
            logger.error(f"❌ Errore caricamento modello: {e}")
            return False
    
    def setup_kafka(self):
        """Configura consumer e producer Kafka"""
        try:
            # Consumer per sensor_data
            self.consumer = KafkaConsumer(
                SENSOR_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="anomaly-detection-group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            # Producer per sensor_alerts
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            logger.info("✅ Kafka consumer e producer configurati")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore configurazione Kafka: {e}")
            return False
    
    def create_features(self, sensor_data):
        """Crea features dal dato del sensore"""
        try:
            # Features base
            features = {
                'temperature': sensor_data['temperature'],
                'humidity': sensor_data['humidity'],
                'soil_ph': sensor_data['soil_ph']
            }
            
            # Features derivate
            features['temp_humidity_ratio'] = features['temperature'] / (features['humidity'] + 1e-6)
            features['ph_deviation'] = abs(features['soil_ph'] - 7.0)
            
            # Features temporali
            timestamp = datetime.fromisoformat(sensor_data['timestamp'])
            features['hour'] = timestamp.hour
            features['day_of_week'] = timestamp.weekday()
            
            # Features per campo
            field_mapping = {'field_01': 0, 'field_02': 1, 'field_03': 2}
            features['field_encoded'] = field_mapping.get(sensor_data['field_id'], 0)
            
            return pd.DataFrame([features])
            
        except Exception as e:
            logger.error(f"❌ Errore creazione features: {e}")
            return None
    
    def detect_anomaly(self, sensor_data):
        """Detecta anomalie nei dati del sensore"""
        if not self.is_model_loaded:
            logger.warning("⚠️ Modello non caricato")
            return None
        
        try:
            # Crea features
            features_df = self.create_features(sensor_data)
            if features_df is None:
                return None
            
            # Preprocessing
            features_scaled = self.scaler.transform(features_df)
            
            # Prediction
            prediction = self.model.predict(features_scaled)[0]
            anomaly_score = self.model.decision_function(features_scaled)[0]
            
            # -1 = anomalia, 1 = normale
            is_anomaly = prediction == -1
            
            return {
                'is_anomaly': is_anomaly,
                'anomaly_score': float(anomaly_score),
                'confidence': abs(float(anomaly_score))
            }
            
        except Exception as e:
            logger.error(f"❌ Errore detection: {e}")
            return None
    
    def create_alert(self, sensor_data, detection_result):
        """Crea messaggio di alert per anomalia"""
        alert = {
            'alert_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'source_timestamp': sensor_data['timestamp'],
            'field_id': sensor_data['field_id'],
            'sensor_data': {
                'temperature': sensor_data['temperature'],
                'humidity': sensor_data['humidity'],
                'soil_ph': sensor_data['soil_ph']
            },
            'anomaly_info': {
                'severity': self.get_severity(detection_result['confidence'])
            },
            'alert_type': 'SENSOR_ANOMALY'
        }
        return alert
    
    def get_severity(self, confidence):
        """Determina severità dell'anomalia basata sulla confidence"""
        if confidence > 0.8:
            return 'HIGH'
        elif confidence > 0.5:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def send_alert(self, alert):
        """Invia alert al topic Kafka"""
        try:
            self.producer.send(ALERT_TOPIC, alert)
            self.producer.flush()
            logger.warning(f"🚨 ANOMALIA DETECTATA: {alert['field_id']} - Severity: {alert['anomaly_info']['severity']}")
            logger.info(f"📤 Alert inviato: {alert['alert_id']}")
            return True
        except Exception as e:
            logger.error(f"❌ Errore invio alert: {e}")
            return False
    
    def process_sensor_data(self, sensor_data):
        """Processa un singolo messaggio del sensore"""
        try:
            logger.info(f"📊 Processando dati: {sensor_data['field_id']} - T:{sensor_data['temperature']}°C H:{sensor_data['humidity']}% pH:{sensor_data['soil_ph']}")
            
            # Detection anomalia
            detection_result = self.detect_anomaly(sensor_data)
            
            if detection_result is None:
                logger.warning("⚠️ Detection fallita")
                return
            
            if detection_result['is_anomaly']:
                # Crea e invia alert
                alert = self.create_alert(sensor_data, detection_result)
                self.send_alert(alert)
            else:
                logger.info(f"✅ Dati normali - Score: {detection_result['anomaly_score']:.3f}")
                
        except Exception as e:
            logger.error(f"❌ Errore processing: {e}")
    
    def run(self):
        """Avvia il detector in tempo reale"""
        logger.info("🚀 Avvio Anomaly Detector in tempo reale...")
        
        # 1. Carica modello
        if not self.load_model():
            logger.error("💥 Impossibile caricare il modello. Terminating.")
            return False
        
        # 2. Setup Kafka
        if not self.setup_kafka():
            logger.error("💥 Impossibile configurare Kafka. Terminating.")
            return False
        
        logger.info(f"🎯 In ascolto su topic '{SENSOR_TOPIC}'...")
        logger.info(f"📢 Invio alert su topic '{ALERT_TOPIC}'...")
        
        try:
            # 3. Loop principale
            for message in self.consumer:
                sensor_data = message.value
                self.process_sensor_data(sensor_data)
                
        except KeyboardInterrupt:
            logger.info("🛑 Detector fermato dall'utente")
        except Exception as e:
            logger.error(f"❌ Errore fatale: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            logger.info("🔌 Connessioni Kafka chiuse")

def main():
    detector = RealtimeAnomalyDetector()
    detector.run()

if __name__ == "__main__":
    main() 