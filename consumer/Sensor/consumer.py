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
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")  # Default to Rome if not specified

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

def migrate_database_schema(conn):
    """Migrate database schema to remove anomaly column and standardize soil_ph"""
    try:
        with conn.cursor() as cur:
            # Check if table already exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'sensor_data'
            """)
            existing_columns = [row[0] for row in cur.fetchall()]
            
            if existing_columns:
                logger.info("🔄 Existing sensor_data table found, executing migration...")
                
                # Remove anomaly column if exists
                if 'anomaly' in existing_columns:
                    logger.info("🔧 Removing anomaly column...")
                    cur.execute('ALTER TABLE sensor_data DROP COLUMN IF EXISTS anomaly')
                
                # Rename soil_pH to soil_ph if necessary
                if 'soil_ph' in existing_columns:
                    # Column already correct
                    pass
                elif 'soil_pH' in existing_columns:
                    logger.info("🔧 Renaming soil_pH → soil_ph...")
                    cur.execute('ALTER TABLE sensor_data RENAME COLUMN "soil_pH" TO soil_ph')
                
                logger.info("✅ Schema migration completed")
            
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"❌ Migration error: {e}")
        conn.rollback()
        return False

def create_table(conn):
    """Create sensor_data table with new schema"""
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
        logger.info("✅ sensor_data table created/verified")
        return True
    except Exception as e:
        logger.error(f"❌ Table creation error: {e}")
        return False

def main():
    # Database connection and table creation
    conn = connect_to_db()
    
    # Existing schema migration
    migrate_database_schema(conn)
    
    # Table creation/verification
    create_table(conn)

    # Consumer configuration
    consumer = KafkaConsumer(
        "sensor_data",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="sensor-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info("🟢 Listening on topic 'sensor_data'...")

    try:
        for message in consumer:
            try:
                data = message.value
                logger.info(f"📥 Received: {data}")

                # Data validation
                required_fields = ["timestamp", "field_id", "temperature", "humidity", "soil_ph"]
                if not all(field in data for field in required_fields):
                    logger.warning(f"⚠️ Incomplete data: {data}")
                    continue

                # Timestamp conversion
                timestamp = datetime.fromisoformat(data["timestamp"].rstrip("Z"))

                # Database insertion
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
                logger.info("✅ Data saved to database")

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
