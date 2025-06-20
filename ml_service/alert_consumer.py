import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv

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
ALERT_TOPIC = "sensor_alerts"

# Configurazione Database
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sensordb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")

class AlertConsumer:
    def __init__(self):
        self.consumer = None
        self.db_conn = None
        
    def setup_kafka(self):
        """Configura il consumer Kafka per gli alert"""
        try:
            self.consumer = KafkaConsumer(
                ALERT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="alert-consumer-group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info("✅ Alert consumer configurato")
            return True
        except Exception as e:
            logger.error(f"❌ Errore configurazione Kafka: {e}")
            return False
    
    def connect_to_db(self):
        """Connessione al database PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                options=f"-c timezone={TIMEZONE}"
            )
            logger.info("✅ Connesso al database PostgreSQL per alert")
            return True
        except Exception as e:
            logger.error(f"❌ Errore connessione al database: {e}")
            return False
    
    def create_alerts_table(self):
        """Crea la tabella degli alert se non esiste e rimuove colonne obsolete"""
        try:
            with self.db_conn.cursor() as cur:
                # Crea la tabella se non esiste
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_alerts (
                        id SERIAL PRIMARY KEY,
                        alert_id VARCHAR(255) UNIQUE NOT NULL,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                        field_id VARCHAR(50) NOT NULL,
                        alert_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        temperature FLOAT NOT NULL,
                        humidity FLOAT NOT NULL,
                        soil_ph FLOAT NOT NULL
                    )
                ''')
                
                # Gestisce la migrazione della struttura tabella
                cur.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'sensor_alerts' 
                    AND column_name IN ('anomaly_score', 'confidence', 'processed_at')
                ''')
                
                obsolete_columns = [col[0] for col in cur.fetchall()]
                
                # Rimuove colonne obsolete
                for column_name in obsolete_columns:
                    logger.info(f"🔧 Rimozione colonna obsoleta: {column_name}")
                    cur.execute(f'ALTER TABLE sensor_alerts DROP COLUMN IF EXISTS {column_name}')
                
                # Gestisce la rinominazione di source_timestamp a timestamp
                cur.execute('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'sensor_alerts' 
                    AND column_name = 'source_timestamp'
                ''')
                
                if cur.fetchone():
                    logger.info("🔧 Rinominazione source_timestamp → timestamp")
                    # Prima rimuovi il vecchio timestamp se esiste
                    cur.execute('ALTER TABLE sensor_alerts DROP COLUMN IF EXISTS timestamp')
                    # Poi rinomina source_timestamp
                    cur.execute('ALTER TABLE sensor_alerts RENAME COLUMN source_timestamp TO timestamp')
                
                # Indici per performance
                cur.execute('''
                    CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON sensor_alerts(timestamp);
                    CREATE INDEX IF NOT EXISTS idx_alerts_field_id ON sensor_alerts(field_id);
                    CREATE INDEX IF NOT EXISTS idx_alerts_severity ON sensor_alerts(severity);
                ''')
                
            self.db_conn.commit()
            logger.info("✅ Tabella sensor_alerts creata/aggiornata")
            return True
        except Exception as e:
            logger.error(f"❌ Errore creazione/aggiornamento tabella alert: {e}")
            return False
    
    def save_alert_to_db(self, alert):
        """Salva l'alert nel database"""
        try:
            with self.db_conn.cursor() as cur:
                # Usa solo il source_timestamp come timestamp principale
                source_timestamp = datetime.fromisoformat(alert['source_timestamp'].replace('Z', '+00:00'))
                
                cur.execute('''
                    INSERT INTO sensor_alerts (
                        alert_id, timestamp, field_id, alert_type,
                        severity, temperature, humidity, soil_ph
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (alert_id) DO NOTHING
                ''', (
                    alert['alert_id'],
                    source_timestamp,
                    alert['field_id'],
                    alert['alert_type'],
                    alert['anomaly_info']['severity'],
                    alert['sensor_data']['temperature'],
                    alert['sensor_data']['humidity'],
                    alert['sensor_data']['soil_ph']
                ))
            
            self.db_conn.commit()
            logger.info(f"💾 Alert salvato nel database: {alert['alert_id']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore salvataggio alert nel database: {e}")
            return False
    
    def format_alert_message(self, alert):
        """Formatta il messaggio di alert per la visualizzazione"""
        severity_icons = {
            'HIGH': '🔴',
            'MEDIUM': '🟡', 
            'LOW': '🟢'
        }
        
        icon = severity_icons.get(alert['anomaly_info']['severity'], '⚪')
        
        message = f"""
 {icon} ANOMALIA DETECTATA {icon}
 ━━━━━━━━━━━━━━━━━━━━━━━━━
 📍 Campo: {alert['field_id']}
 🕐 Timestamp: {alert['source_timestamp']}
 ⚠️  Severità: {alert['anomaly_info']['severity']}

 📈 Dati Sensori:
    🌡️  Temperatura: {alert['sensor_data']['temperature']}°C
    💧 Umidità: {alert['sensor_data']['humidity']}%
    🧪 pH Suolo: {alert['sensor_data']['soil_ph']}

 🆔 Alert ID: {alert['alert_id']}
 ━━━━━━━━━━━━━━━━━━━━━━━━━
        """
        return message.strip()
    
    def process_alert(self, alert):
        """Processa un singolo alert"""
        try:
            # Formatta e logga l'alert
            alert_message = self.format_alert_message(alert)
            
            # Log con livello appropriato basato sulla severità
            severity = alert['anomaly_info']['severity']
            if severity == 'HIGH':
                logger.critical(alert_message)
            elif severity == 'MEDIUM':  
                logger.error(alert_message)
            else:
                logger.warning(alert_message)
            
            # Salva alert nel database
            self.save_alert_to_db(alert)
            
            # Invia notifiche esterne
            self.send_notification(alert)
            
        except Exception as e:
            logger.error(f"❌ Errore processing alert: {e}")
    
    def send_notification(self, alert):
        """Invia notifiche esterne (placeholder per future implementazioni)"""
        # Placeholder per integrazioni future:
        # - Email notifications
        # - Slack/Discord webhooks  
        # - SMS via Twilio
        # - Push notifications
        # - Database logging
        
        severity = alert['anomaly_info']['severity']
        if severity == 'HIGH':
            logger.info("🚨 Notification sent for HIGH severity alert")
        elif severity == 'MEDIUM':
            logger.info("⚠️ Notification sent for MEDIUM severity alert") 
        else:
            logger.info("ℹ️ Notification logged for LOW severity alert")
    
    def run(self):
        """Avvia il consumer degli alert"""
        logger.info("🚀 Avvio Alert Consumer...")
        
        # Setup Database
        if not self.connect_to_db():
            logger.error("💥 Impossibile connettersi al database. Terminating.")
            return False
        
        if not self.create_alerts_table():
            logger.error("💥 Impossibile creare tabella alert. Terminating.")
            return False
        
        # Setup Kafka
        if not self.setup_kafka():
            logger.error("💥 Impossibile configurare Kafka. Terminating.")
            return False
        
        logger.info(f"👂 In ascolto su topic '{ALERT_TOPIC}' per alert di anomalie...")
        logger.info("💾 Alert verranno salvati nel database PostgreSQL")
        
        try:
            for message in self.consumer:
                alert = message.value
                self.process_alert(alert)
                
        except KeyboardInterrupt:
            logger.info("🛑 Alert consumer fermato dall'utente")
        except Exception as e:
            logger.error(f"❌ Errore fatale: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.db_conn:
                self.db_conn.close()
            logger.info("🔌 Connessioni Kafka e Database chiuse")

def main():
    consumer = AlertConsumer()
    consumer.run()

if __name__ == "__main__":
    main() 