import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sensordb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")

# Database connection
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            options=f"-c timezone={TIMEZONE}"
        )
        logger.info("✅ Connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection error: {e}")
        raise

def create_table(conn):
    """Create simplified sensor_data table optimized for ML training"""
    try:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE,
                    field_id TEXT,
                    temperature FLOAT,
                    humidity FLOAT,
                    soil_ph FLOAT,
                    data_quality TEXT
                )
            ''')
        conn.commit()
        logger.info("✅ Simplified sensor_data table created/verified for ML training")
        return True
    except Exception as e:
        logger.error(f"❌ Table creation error: {e}")
        return False

def main():
    # Database connection and table creation
    conn = connect_to_db()
    create_table(conn)

    # Consumer configuration - reading from processed topic
    consumer = KafkaConsumer(
        "processed_sensor_data",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="processed-sensor-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info("🟢 Listening for ML-ready sensor data from Spark preprocessing...")

    try:
        for message in consumer:
            try:
                message_data = message.value
                
                # Extract data from Spark format
                if isinstance(message_data, dict) and 'value' in message_data:
                    data_str = message_data['value']
                    data = json.loads(data_str) if isinstance(data_str, str) else data_str
                else:
                    data = message_data
                    
                logger.info(f"📥 Received ML-ready data: {data['field_id']} - Quality: {data['data_quality']}")

                # Data is already validated by Spark preprocessing
                timestamp = datetime.fromisoformat(data["timestamp"])

                # Store only essential data for ML training
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO sensor_data 
                        (timestamp, field_id, temperature, humidity, soil_ph, data_quality)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (
                        timestamp,
                        data["field_id"],
                        data["temperature"],
                        data["humidity"],
                        data["soil_ph"],
                        data["data_quality"]
                    ))
                conn.commit()
                
                status_emoji = "⚠️" if data["data_quality"] == "anomaly" else "✅"
                logger.info(f"{status_emoji} ML training data saved: {data['field_id']} - Quality: {data['data_quality']}")

            except Exception as e:
                logger.error(f"❌ Message processing error: {e}")
                continue

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
