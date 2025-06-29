import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Rome")
SPARK_PARALLELISM = int(os.getenv("SPARK_PARALLELISM", "4"))
CHECKPOINT_LOCATION = "/app/spark-checkpoints"


def preprocess_sensor_data(data_str):
    """Function for sensor data preprocessing"""
    try:
        import sys
        sys.path.append('/app/preprocessing')
        from data_preprocessor import DataPreprocessor
        
        raw_data = json.loads(data_str)
        preprocessor = DataPreprocessor(TIMEZONE)
        processed_data, status = preprocessor.preprocess_sensor_data(raw_data)
        
        if processed_data:
            processed_data["preprocessing_status"] = status
            return json.dumps({
                "status": status,
                "data": processed_data,
                "valid": True
            })
        else:
            return json.dumps({
                "status": "invalid",
                "data": raw_data,
                "valid": False,
                "error": "preprocessing_failed"
            })
    except Exception as e:
        return json.dumps({
            "status": "error",
            "data": {},
            "valid": False,
            "error": str(e)
        })


def preprocess_weather_data(data_str):
    """Function for weather data preprocessing"""
    try:
        import sys
        sys.path.append('/app/preprocessing')
        from data_preprocessor import DataPreprocessor
        
        raw_data = json.loads(data_str)
        preprocessor = DataPreprocessor(TIMEZONE)
        processed_data, status = preprocessor.preprocess_weather_data(raw_data)
        
        if processed_data:
            processed_data["preprocessing_status"] = status
            return json.dumps({
                "status": status,
                "data": processed_data,
                "valid": True
            })
        else:
            return json.dumps({
                "status": "invalid",
                "data": raw_data,
                "valid": False,
                "error": "preprocessing_failed"
            })
    except Exception as e:
        return json.dumps({
            "status": "error",  
            "data": {},
            "valid": False,
            "error": str(e)
        })


def preprocess_satellite_data(data_str):
    """Function for satellite data preprocessing"""
    try:
        import sys
        sys.path.append('/app/preprocessing')
        from data_preprocessor import DataPreprocessor
        
        raw_data = json.loads(data_str)
        preprocessor = DataPreprocessor(TIMEZONE)
        processed_data, status = preprocessor.preprocess_satellite_data(raw_data)
        
        if processed_data:
            processed_data["preprocessing_status"] = status
            return json.dumps({
                "status": status,
                "data": processed_data,
                "valid": True
            })
        else:
            return json.dumps({
                "status": "invalid",
                "data": {k: v for k, v in raw_data.items() if k != "image_base64"},
                "valid": False,
                "error": "preprocessing_failed"
            })
    except Exception as e:
        return json.dumps({
            "status": "error",
            "data": {},
            "valid": False,
            "error": str(e)
        })


def ml_analysis(processed_str):
    """Function for ML analysis on preprocessed sensor data"""
    try:
        import sys
        sys.path.append('/app/anomaly-detection')
        sys.path.append('/app/alert-generation')
        from anomaly_detector import AnomalyDetector
        from alert_generator import AlertGenerator
        
        processed_result = json.loads(processed_str)
        
        # Process both valid and anomaly data (both are structurally correct)
        if not processed_result.get("valid"):
            return None
        
        data = processed_result["data"]
        
        # Extract features for ML
        features = {
            'temperature': data['temperature'],
            'humidity': data['humidity'],
            'soil_ph': data['soil_ph'],
            'field_id': data['field_id'],
            'timestamp': data['timestamp']
        }
        
        # Initialize ML components
        anomaly_detector = AnomalyDetector()
        alert_generator = AlertGenerator()
        
        # Perform ML anomaly detection
        is_anomaly, anomaly_score, anomaly_type = anomaly_detector.detect_anomaly(features)
        
        if is_anomaly:
            alert = alert_generator.generate_alert(
                field_id=data['field_id'],
                anomaly_type=anomaly_type,
                anomaly_score=anomaly_score,
                sensor_data=features,
                timestamp=data['timestamp']
            )
            return alert
        
        return None
        
    except Exception as e:
        return None


class SparkPreprocessingService:
    """Apache Spark service for centralized data preprocessing and ML"""
    
    def __init__(self):
        self.spark = None
        self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session with optimized configuration"""
        self.spark = SparkSession.builder \
            .appName("CropDiseasePreprocessing") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
            .config("spark.default.parallelism", str(SPARK_PARALLELISM)) \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session initialized")
    
    def _create_kafka_source(self, topic: str):
        """Create Kafka streaming source"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def _create_kafka_sink(self, df, topic: str, checkpoint_path: str):
        """Create Kafka streaming sink"""
        return df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append")
    
    def start_processing(self):
        """Start all streaming processes"""
        logger.info("🚀 Starting Spark Streaming for Crop Disease Preprocessing")
        
        # Register UDFs
        preprocess_sensor_udf = udf(preprocess_sensor_data, StringType())
        preprocess_weather_udf = udf(preprocess_weather_data, StringType())
        preprocess_satellite_udf = udf(preprocess_satellite_data, StringType())
        ml_analysis_udf = udf(ml_analysis, StringType())
        
        # Process sensor data
        sensor_stream = self._create_kafka_source("sensor_data")
        sensor_processed = sensor_stream.select(
            col("value").cast("string").alias("raw_data")
        ).withColumn(
            "processed_result", 
            preprocess_sensor_udf(col("raw_data"))
        )
        
        # ML Analysis on all valid sensor data (including anomalies)
        sensor_all_valid = sensor_processed.filter(
            get_json_object(col("processed_result"), "$.valid") == "true"
        ).select(
            col("processed_result").alias("processed_data")
        )
        
        ml_alerts = sensor_all_valid.withColumn(
            "alert",
            ml_analysis_udf(col("processed_data"))
        ).filter(col("alert").isNotNull()).select(col("alert").alias("value"))
        
        # Split sensor results using get_json_object for better performance
        sensor_valid = sensor_processed.filter(
            get_json_object(col("processed_result"), "$.valid") == "true"
        ).filter(
            get_json_object(col("processed_result"), "$.status") == "valid"
        ).select(
            get_json_object(col("processed_result"), "$.data").alias("processed_data")
        )
        
        # Start sensor processed stream
        sensor_processed_query = self._create_kafka_sink(
            sensor_valid.select(col("processed_data").alias("value")),
            "processed_sensor_data",
            f"{CHECKPOINT_LOCATION}/sensor_processed"
        ).start()
        
        # ML Analysis on all valid sensor data
        ml_alerts_query = self._create_kafka_sink(
            ml_alerts,
            "crop_disease_alerts",
            f"{CHECKPOINT_LOCATION}/ml_alerts"
        ).start()
        
        # Process weather data
        weather_stream = self._create_kafka_source("weather_data")
        weather_processed = weather_stream.select(
            col("value").cast("string").alias("raw_data")
        ).withColumn(
            "processed_result",
            preprocess_weather_udf(col("raw_data"))
        )
        
        weather_valid = weather_processed.filter(
            get_json_object(col("processed_result"), "$.valid") == "true"
        ).select(
            get_json_object(col("processed_result"), "$.data").alias("processed_data")
        )
        
        weather_processed_query = self._create_kafka_sink(
            weather_valid.select(col("processed_data").alias("value")),
            "processed_weather_data",
            f"{CHECKPOINT_LOCATION}/weather_processed"
        ).start()
        
        # Process satellite data
        satellite_stream = self._create_kafka_source("satellite_data")
        satellite_processed = satellite_stream.select(
            col("value").cast("string").alias("raw_data")
        ).withColumn(
            "processed_result",
            preprocess_satellite_udf(col("raw_data"))
        )
        
        satellite_valid = satellite_processed.filter(
            get_json_object(col("processed_result"), "$.valid") == "true"
        ).select(
            get_json_object(col("processed_result"), "$.data").alias("processed_data")
        )
        
        satellite_processed_query = self._create_kafka_sink(
            satellite_valid.select(col("processed_data").alias("value")),
            "processed_satellite_data",
            f"{CHECKPOINT_LOCATION}/satellite_processed"
        ).start()
        
        logger.info("✅ All streaming queries started")
        
        # Wait for all streams
        try:
            sensor_processed_query.awaitTermination()
            ml_alerts_query.awaitTermination()
            weather_processed_query.awaitTermination()
            satellite_processed_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("🛑 Stopping streaming...")
            sensor_processed_query.stop()
            ml_alerts_query.stop()
            weather_processed_query.stop()
            satellite_processed_query.stop()


def main():
    """Main function"""
    service = SparkPreprocessingService()
    service.start_processing()


if __name__ == "__main__":
    main() 