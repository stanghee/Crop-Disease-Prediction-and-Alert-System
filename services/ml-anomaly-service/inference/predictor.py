#!/usr/bin/env python3
"""
Streaming Inference - Real-time anomaly detection
Reads from Kafka gold-ml-features topic and predicts anomalies
"""

import logging
import json
from datetime import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pickle

from utils.feature_config import CORE_FEATURES, ANOMALY_THRESHOLD, CRITICAL_THRESHOLD
from storage.model_manager import ModelManager
from storage.postgres_client import PostgresClient

logger = logging.getLogger(__name__)

class StreamingPredictor:
    """Real-time anomaly detection using Spark Structured Streaming"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model_manager = ModelManager()
        self.postgres_client = PostgresClient()
        
        # Load model on initialization
        self.model, self.scaler, self.metadata = self.model_manager.load_latest_model()
        if not self.model:
            raise ValueError("No model available for inference")
        
        # Broadcast model to all nodes
        self.broadcast_model = spark.sparkContext.broadcast(self.model)
        self.broadcast_scaler = spark.sparkContext.broadcast(self.scaler)
        
    def start_streaming(self):
        """Start the streaming inference pipeline"""
        logger.info("Starting streaming inference pipeline")
        
        # Define schema for gold-ml-features
        gold_schema = self._get_gold_schema()
        
        # Read from Kafka
        stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "gold-ml-features") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and extract features
        features_df = stream_df \
            .select(
                col("timestamp").alias("kafka_timestamp"),
                from_json(col("value").cast("string"), gold_schema).alias("data")
            ) \
            .select(
                col("kafka_timestamp"),
                col("data.*")
            ) \
            .filter(col("field_id").isNotNull())
        
        # Apply anomaly detection
        anomaly_df = self._detect_anomalies(features_df)
        
        # Write to multiple sinks
        query1 = self._write_to_kafka(anomaly_df)
        query2 = self._write_to_postgres(anomaly_df)
        
        return [query1, query2]
    
    def _detect_anomalies(self, df):
        """Apply anomaly detection to streaming data"""
        
        # UDF for anomaly detection
        @udf(returnType=StructType([
            StructField("anomaly_score", FloatType()),
            StructField("is_anomaly", BooleanType()),
            StructField("severity", StringType())
        ]))
        def predict_anomaly(features_array):
            try:
                # Get broadcasted model and scaler
                model = self.broadcast_model.value
                scaler = self.broadcast_scaler.value
                
                # Reshape and scale features
                X = np.array(features_array).reshape(1, -1)
                X_scaled = scaler.transform(X)
                
                # Get anomaly score (lower is more anomalous)
                score = model.score_samples(X_scaled)[0]
                # Convert to anomaly probability (lower score = higher anomaly probability)
                # Isolation Forest returns negative scores for anomalies
                normalized_score = -score  # Invert so positive = more anomalous
                
                # Normalize to 0-1 range
                # Typical scores range from -0.5 to 0.5, so we'll use a sigmoid-like transformation
                normalized_score = 1 / (1 + np.exp(-normalized_score * 5))
                
                # Determine if anomaly
                is_anomaly = normalized_score > ANOMALY_THRESHOLD
                
                # Determine severity
                if normalized_score > CRITICAL_THRESHOLD:
                    severity = "CRITICAL"
                elif normalized_score > ANOMALY_THRESHOLD:
                    severity = "HIGH"
                else:
                    severity = "NORMAL"
                
                return (float(normalized_score), bool(is_anomaly), severity)
                
            except Exception as e:
                logger.error(f"Prediction error: {e}")
                return (0.0, False, "ERROR")
        
        # Create feature array and apply UDF
        return df \
            .withColumn("feature_array", array([col(f) for f in CORE_FEATURES])) \
            .withColumn("prediction", predict_anomaly(col("feature_array"))) \
            .select(
                col("field_id"),
                col("location"),
                col("processing_timestamp"),
                col("prediction.anomaly_score"),
                col("prediction.is_anomaly"),
                col("prediction.severity"),
                col("sensor_avg_temperature"),
                col("sensor_avg_humidity"),
                col("sensor_avg_soil_ph"),
                col("temp_differential"),
                col("humidity_differential"),
                col("sensor_anomaly_rate")
            ) \
            .withColumn("model_version", lit(self.metadata['model_version'])) \
            .withColumn("prediction_timestamp", current_timestamp())
    
    def _write_to_kafka(self, df):
        """Write anomalies to Kafka ml-anomalies topic"""
        
        # Only send anomalies to alert system
        anomaly_stream = df \
            .filter(col("is_anomaly") == True) \
            .select(
                to_json(struct(
                    col("field_id"),
                    col("location"),
                    col("anomaly_score"),
                    col("is_anomaly"),
                    col("severity"),
                    col("model_version"),
                    col("prediction_timestamp"),
                    struct(
                        col("sensor_avg_temperature").alias("temperature"),
                        col("sensor_avg_humidity").alias("humidity"),
                        col("sensor_avg_soil_ph").alias("soil_ph"),
                        col("temp_differential"),
                        col("humidity_differential")
                    ).alias("features")
                )).alias("value")
            )
        
        query = anomaly_stream.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "ml-anomalies") \
            .option("checkpointLocation", "/tmp/ml-checkpoints/kafka") \
            .outputMode("append") \
            .start()
        
        return query
    
    def _write_to_postgres(self, df):
        """Write all predictions to PostgreSQL"""
        
        def write_batch(batch_df, batch_id):
            """Write each batch to PostgreSQL"""
            try:
                # Convert to list of dicts for batch insert
                records = batch_df.collect()
                self.postgres_client.save_predictions(records)
                logger.info(f"Wrote {len(records)} predictions to PostgreSQL")
            except Exception as e:
                logger.error(f"Failed to write batch {batch_id}: {e}")
        
        query = df.writeStream \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", "/tmp/ml-checkpoints/postgres") \
            .outputMode("append") \
            .start()
        
        return query
    
    def _get_gold_schema(self):
        """Get schema for gold-ml-features"""
        return StructType([
            StructField("field_id", StringType()),
            StructField("location", StringType()),
            StructField("sensor_avg_temperature", DoubleType()),
            StructField("sensor_std_temperature", DoubleType()),
            StructField("sensor_avg_humidity", DoubleType()),
            StructField("sensor_std_humidity", DoubleType()),
            StructField("sensor_avg_soil_ph", DoubleType()),
            StructField("sensor_std_soil_ph", DoubleType()),
            StructField("weather_avg_temperature", DoubleType()),
            StructField("weather_avg_humidity", DoubleType()),
            StructField("temp_differential", DoubleType()),
            StructField("humidity_differential", DoubleType()),
            StructField("sensor_anomaly_rate", DoubleType()),
            StructField("environmental_stress_score", StringType()),
            StructField("combined_risk_score", StringType()),
            StructField("processing_timestamp", TimestampType()),
            # Add other fields as needed
        ])
