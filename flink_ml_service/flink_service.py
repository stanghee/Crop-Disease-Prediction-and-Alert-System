import os
import json
import logging
from datetime import datetime
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.time import Time
from anomaly_detector import AnomalyDetector
from alert_generator import AlertGenerator
import psycopg2

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sensordb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
FLINK_PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "4"))
FLINK_CHECKPOINTS_INTERVAL = int(os.getenv("FLINK_CHECKPOINTS_INTERVAL", "60000"))


class SensorAnomalyProcessor(ProcessFunction):
    """
    Flink ProcessFunction for real-time anomaly detection on sensor data
    """
    
    def __init__(self):
        self.anomaly_detector = None
        self.alert_generator = None
        self.last_training_time = None
        
    def open(self, runtime_context):
        """Initialize the processor with models and state"""
        self.anomaly_detector = AnomalyDetector()
        self.alert_generator = AlertGenerator()
        
        # State for keeping track of field statistics
        self.field_state_descriptor = ValueStateDescriptor(
            "field_stats", Types.PICKLED_BYTE_ARRAY()
        )
        
        logger.info("🤖 SensorAnomalyProcessor initialized")
    
    def process_element(self, value, ctx):
        """Process each sensor data point for anomaly detection"""
        try:
            # Parse the JSON data
            data = json.loads(value)
            logger.info(f"📊 Processing sensor data: {data}")
            
            # Extract features for ML model
            features = {
                'temperature': data['temperature'],
                'humidity': data['humidity'],
                'soil_ph': data['soil_ph'],
                'field_id': data['field_id'],
                'timestamp': data['timestamp']
            }
            
            # Perform anomaly detection
            is_anomaly, anomaly_score, anomaly_type = self.anomaly_detector.detect_anomaly(features)
            
            if is_anomaly:
                logger.warning(f"🚨 ANOMALY DETECTED in {data['field_id']}: {anomaly_type} (score: {anomaly_score:.3f})")
                
                # Generate alert
                alert = self.alert_generator.generate_alert(
                    field_id=data['field_id'],
                    anomaly_type=anomaly_type,
                    anomaly_score=anomaly_score,
                    sensor_data=features,
                    timestamp=data['timestamp']
                )
                
                # Emit alert to downstream
                yield alert
            else:
                logger.info(f"✅ Normal reading for {data['field_id']} (score: {anomaly_score:.3f})")
                
        except Exception as e:
            logger.error(f"❌ Error processing sensor data: {e}")


def create_kafka_source():
    """Create Kafka source for sensor data stream"""
    return KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics("sensor_data") \
        .set_group_id("flink-ml-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


def create_kafka_sink():
    """Create Kafka sink for crop disease alerts"""
    return KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("crop_disease_alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()


def main():
    logger.info("🚀 Starting Flink ML Service for Crop Disease Prediction")
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FLINK_PARALLELISM)
    
    # Configure checkpointing for fault tolerance
    env.enable_checkpointing(FLINK_CHECKPOINTS_INTERVAL)
    
    # Set checkpoint storage location
    env.get_checkpoint_config().set_checkpoint_storage_dir("file:///app/flink-checkpoints")
    
    # Create Kafka source
    kafka_source = create_kafka_source()
    
    # Create data stream from Kafka
    sensor_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "sensor_data_source"
    )
    
    # Apply anomaly detection processing
    alerts_stream = sensor_stream.process(
        SensorAnomalyProcessor(),
        output_type=Types.STRING()
    )
    
    # Create Kafka sink for alerts
    kafka_sink = create_kafka_sink()
    
    # Send alerts to Kafka topic for dashboard consumption
    alerts_stream.sink_to(kafka_sink)
    
    # Also print alerts for debugging
    alerts_stream.print("ALERT")
    
    # Execute the Flink job
    logger.info("▶️ Executing Flink job...")
    env.execute("Crop Disease Anomaly Detection")


if __name__ == "__main__":
    main() 