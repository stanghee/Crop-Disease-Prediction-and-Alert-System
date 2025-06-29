import os
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from kafka import KafkaConsumer
from minio_utils import MinIOManager

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000") 
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "satellite-images")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")

def main():
    # Initialize MinIO manager
    minio_manager = MinIOManager(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=MINIO_BUCKET
    )
    
    # Consumer configuration - reading from processed topic
    consumer = KafkaConsumer(
        "processed_satellite_data",  # Changed to processed topic
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="processed-satellite-group",  # Changed group id  
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    logger.info("🟢 Listening on topic 'processed_satellite_data' for validated images...")
    
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
                    
                logger.info(f"📥 Received processed satellite data: Size={data.get('image_size_kb', 0)}KB, Quality={data.get('data_quality', 'unknown')}")
                
                # Data is already validated by Spark preprocessing
                timestamp_str = data["timestamp"]
                image_base64 = data["image_base64"]
                location = data["location"]
                
                # Parse and normalize timestamp to ensure proper timezone
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.rstrip("Z"))
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=ZoneInfo(TIMEZONE))
                    # Convert to Europe/Rome timezone for consistency
                    timestamp_rome = timestamp.astimezone(ZoneInfo(TIMEZONE))
                    timestamp_formatted = timestamp_rome.isoformat()
                except (ValueError, AttributeError):
                    # Fallback to original if parsing fails
                    timestamp_formatted = timestamp_str
                    logger.warning(f"⚠️ Could not parse timestamp, using original: {timestamp_str}")
                
                # Generate object name with quality info (use simplified timestamp for filename)
                quality_suffix = f"_{data.get('data_quality', 'unknown')}"
                timestamp_for_filename = timestamp_str.replace(':', '-').replace('T', '_').split('+')[0].split('Z')[0]
                object_name = f"satellite_{timestamp_for_filename}{quality_suffix}.png"
                
                # Store in MinIO with metadata
                metadata = {
                    "timestamp": timestamp_formatted,
                    "location": str(location),
                    "data_quality": data.get("data_quality", "unknown"),
                    "image_size_kb": str(data.get("image_size_kb", 0)),
                    "preprocessing_status": data.get("preprocessing_status", "unknown")
                }
                
                # Add quality flags as metadata
                quality_flags = data.get("quality_flags", {})
                for flag, value in quality_flags.items():
                    metadata[f"flag_{flag}"] = str(value)
                
                success = minio_manager.store_image_from_base64(
                    image_base64, 
                    object_name,
                    metadata=metadata
                )
                
                if success:
                    status_emoji = "⚠️" if data.get("data_quality") == "anomaly" else "✅"
                    logger.info(f"{status_emoji} Processed satellite image stored: {object_name}")
                else:
                    logger.error(f"❌ Failed to store satellite image: {object_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing satellite message: {e}")
                continue
                
    except Exception as e:
        logger.error(f"❌ Fatal error in satellite consumer: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()