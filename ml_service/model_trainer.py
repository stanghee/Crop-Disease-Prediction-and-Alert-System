import os
import logging
import pandas as pd
import numpy as np
import psycopg2
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Caricamento variabili d'ambiente
load_dotenv()

# Configurazione database
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sensordb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")

# Parametri del modello
MODEL_PATH = "/app/models"
SCALER_PATH = "/app/models"

class SensorAnomalyTrainer:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = ['temperature', 'humidity', 'soil_ph']
        
    def connect_to_db(self):
        """Connessione al database PostgreSQL"""
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
    
    def load_historical_data(self, days_back=30):
        """Carica dati storici dal database"""
        conn = self.connect_to_db()
        
        # Query per ottenere dati degli ultimi N giorni
        query = """
        SELECT timestamp, field_id, temperature, humidity, soil_ph
        FROM sensor_data
        WHERE timestamp >= %s
        ORDER BY timestamp
        """
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        try:
            df = pd.read_sql_query(query, conn, params=[cutoff_date])
            logger.info(f"📊 Caricati {len(df)} record degli ultimi {days_back} giorni")
            return df
        except Exception as e:
            logger.error(f"❌ Errore caricamento dati: {e}")
            raise
        finally:
            conn.close()
    
    def create_features(self, df):
        """Crea features per il modello"""
        # Features base
        features = df[self.feature_columns].copy()
        
        # Aggiungi features derivate
        features['temp_humidity_ratio'] = df['temperature'] / (df['humidity'] + 1e-6)
        features['ph_deviation'] = abs(df['soil_ph'] - 7.0)  # Deviazione dal pH neutro
        
        # Features temporali (ora del giorno)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        features['hour'] = df['timestamp'].dt.hour
        features['day_of_week'] = df['timestamp'].dt.dayofweek
        
        # Features per campo (encoding)
        field_mapping = {'field_01': 0, 'field_02': 1, 'field_03': 2}
        features['field_encoded'] = df['field_id'].map(field_mapping)
        
        logger.info(f"🔧 Create {features.shape[1]} features")
        return features
    
    def train_model(self, features):
        """Training del modello Isolation Forest"""
        # Preprocessing
        features_scaled = self.scaler.fit_transform(features)
        
        # Divisione train/test
        X_train, X_test = train_test_split(features_scaled, test_size=0.2, random_state=42)
        
        # Training del modello
        self.model = IsolationForest(
            contamination=0.1,  # 10% di anomalie attese
            random_state=42,
            n_estimators=100
        )
        
        logger.info("🤖 Inizio training del modello...")
        self.model.fit(X_train)
        
        # Valutazione
        train_predictions = self.model.predict(X_train)
        test_predictions = self.model.predict(X_test)
        
        train_anomalies = np.sum(train_predictions == -1) / len(train_predictions) * 100
        test_anomalies = np.sum(test_predictions == -1) / len(test_predictions) * 100
        
        logger.info(f"📈 Training completato!")
        logger.info(f"   Anomalie Train Set: {train_anomalies:.2f}%")
        logger.info(f"   Anomalie Test Set: {test_anomalies:.2f}%")
        
        return self.model
    
    def save_model(self):
        """Salva il modello e il scaler"""
        os.makedirs(MODEL_PATH, exist_ok=True)
        os.makedirs(SCALER_PATH, exist_ok=True)
        
        model_file = os.path.join(MODEL_PATH, "anomaly_model.joblib")
        scaler_file = os.path.join(SCALER_PATH, "scaler.joblib")
        
        joblib.dump(self.model, model_file)
        joblib.dump(self.scaler, scaler_file)
        
        logger.info(f"💾 Modello salvato in: {model_file}")
        logger.info(f"💾 Scaler salvato in: {scaler_file}")
    
    def run_training(self):
        """Esegue il processo completo di training"""
        try:
            logger.info("🚀 Avvio training del modello di anomaly detection...")
            
            # 1. Carica dati storici
            df = self.load_historical_data(days_back=30)
            
            if len(df) == 0:
                logger.error("❌ Nessun dato disponibile per il training")
                return False
            
            # 2. Crea features
            features = self.create_features(df)
            
            # 3. Training
            self.train_model(features)
            
            # 4. Salva modello
            self.save_model()
            
            logger.info("✅ Training completato con successo!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore durante il training: {e}")
            return False

def main():
    trainer = SensorAnomalyTrainer()
    success = trainer.run_training()
    
    if success:
        logger.info("🎉 Modello pronto per la detection in tempo reale!")
    else:
        logger.error("💥 Training fallito!")
        exit(1)

if __name__ == "__main__":
    main() 