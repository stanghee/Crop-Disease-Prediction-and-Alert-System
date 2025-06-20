import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configurazione Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sensordb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default a Roma se non specificato

# Connessione al database
def connect_to_db():
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

def migrate_database_schema(conn):
    """Migra lo schema del database per rimuovere colonna anomaly e standardizzare soil_ph"""
    try:
        with conn.cursor() as cur:
            # Controlla se la tabella esiste già
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'sensor_data'
            """)
            existing_columns = [row[0] for row in cur.fetchall()]
            
            if existing_columns:
                logger.info("🔄 Tabella sensor_data esistente trovata, eseguo migrazione...")
                
                # Rimuovi colonna anomaly se esiste
                if 'anomaly' in existing_columns:
                    logger.info("🔧 Rimozione colonna anomaly...")
                    cur.execute('ALTER TABLE sensor_data DROP COLUMN IF EXISTS anomaly')
                
                # Rinomina soil_pH a soil_ph se necessario
                if 'soil_ph' in existing_columns:
                    # Colonna già corretta
                    pass
                elif 'soil_pH' in existing_columns:
                    logger.info("🔧 Rinominazione soil_pH → soil_ph...")
                    cur.execute('ALTER TABLE sensor_data RENAME COLUMN "soil_pH" TO soil_ph')
                
                logger.info("✅ Migrazione schema completata")
            
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"❌ Errore durante migrazione: {e}")
        conn.rollback()
        return False

def create_table(conn):
    """Crea la tabella sensor_data con il nuovo schema"""
    try:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE,
                    field_id TEXT,
                    temperature FLOAT,
                    humidity FLOAT,
                    soil_ph FLOAT
                )
            ''')
        conn.commit()
        logger.info("✅ Tabella sensor_data creata/verificata")
        return True
    except Exception as e:
        logger.error(f"❌ Errore creazione tabella: {e}")
        return False

def main():
    # Connessione al database e creazione tabella
    conn = connect_to_db()
    
    # Migrazione schema esistente
    migrate_database_schema(conn)
    
    # Creazione/verifica tabella
    create_table(conn)

    # Configurazione consumer
    consumer = KafkaConsumer(
        "sensor_data",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="sensor-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info("🟢 In ascolto sul topic 'sensor_data'...")

    try:
        for message in consumer:
            try:
                data = message.value
                logger.info(f"📥 Ricevuto: {data}")

                # Validazione dati
                required_fields = ["timestamp", "field_id", "temperature", "humidity", "soil_ph"]
                if not all(field in data for field in required_fields):
                    logger.warning(f"⚠️ Dati incompleti: {data}")
                    continue

                # Conversione timestamp
                timestamp = datetime.fromisoformat(data["timestamp"].rstrip("Z"))

                # Inserimento nel database
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO sensor_data 
                        (timestamp, field_id, temperature, humidity, soil_ph)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (
                        timestamp,
                        data["field_id"],
                        data["temperature"],
                        data["humidity"],
                        data["soil_ph"]
                    ))
                conn.commit()
                logger.info("✅ Dati salvati nel database")

            except Exception as e:
                logger.error(f"❌ Errore processamento messaggio: {e}")
                continue

    except Exception as e:
        logger.error(f"❌ Errore fatale: {e}")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
