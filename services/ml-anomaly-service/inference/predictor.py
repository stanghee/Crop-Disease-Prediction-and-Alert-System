#!/usr/bin/env python3
"""
Streaming Inference - Real-time anomaly detection
Reads from Kafka gold-ml-features topic and predicts anomalies
"""

import logging
import json
from datetime import datetime
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf
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
        
        # Load model metadata on initialization
        self.model, self.scaler, self.metadata = self.model_manager.load_latest_model()
        if not self.model:
            logger.warning("No model available for inference - will load when needed")
            self.metadata = {'model_version': 'no_model'}
        
    def start_streaming(self):
        """Start the streaming inference pipeline"""
        logger.info("Starting streaming inference pipeline")
        
        # Reload model if needed (in case it was updated)
        self._reload_model_if_needed()
        
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
        """Apply anomaly detection to streaming data using Pandas UDF"""
        
        # Pandas UDF for anomaly detection - uses global model variables
        @pandas_udf(returnType=StructType([
            StructField("anomaly_score", FloatType()),
            StructField("is_anomaly", BooleanType()),
            StructField("severity", StringType()),
            StructField("recommendations", StringType())
        ]))
        def predict_anomaly_batch(features_series):
            try:
                # Import required modules inside UDF
                import numpy as np
                import pandas as pd
                from utils.feature_config import ANOMALY_THRESHOLD, CRITICAL_THRESHOLD
                
                # Define inline recommendation function
                def generate_inline_recommendations(anomaly_score, temp, humidity, soil_ph, temp_diff, humidity_diff, anomaly_rate):
                    recommendations = []
                    
                    # Base recommendations based on anomaly score
                    if anomaly_score > 0.9:
                        recommendations.append("IMMEDIATE INTERVENTION REQUIRED")
                    elif anomaly_score > 0.7:
                        recommendations.append("WARNING: Critical conditions detected")
                    elif anomaly_score > 0.5:
                        recommendations.append("Enhanced monitoring recommended")
                    else:
                        recommendations.append("Normal conditions - continue monitoring")
                    
                    # Simple temperature-based recommendations
                    if temp > 30:
                        recommendations.append("High temperature: consider irrigation and shading")
                    elif temp < 10:
                        recommendations.append("Low temperature: protect crops from frost")
                    
                    # Simple humidity-based recommendations
                    if humidity > 85:
                        recommendations.append("High humidity: fungal risk - improve ventilation")
                    elif humidity < 40:
                        recommendations.append("Low humidity: increase irrigation")
                    
                    # Simple soil pH recommendations
                    if soil_ph < 6.0:
                        recommendations.append("Acidic pH: consider liming")
                    elif soil_ph > 7.5:
                        recommendations.append("Alkaline pH: consider organic acidifiers")
                    
                    # Join recommendations
                    if len(recommendations) > 1:
                        return " | ".join(recommendations)
                    else:
                        return recommendations[0] if recommendations else "No specific recommendations"
                
                # Load model and scaler inside UDF (this will be cached per worker)
                from storage.model_manager import ModelManager
                model_manager = ModelManager()
                model, scaler, metadata = model_manager.load_latest_model()
                
                if model is None or scaler is None:
                    # Return default values if no model available
                    return pd.DataFrame({
                        'anomaly_score': [0.0] * len(features_series),
                        'is_anomaly': [False] * len(features_series),
                        'severity': ['NO_MODEL'] * len(features_series),
                        'recommendations': ['Model not available - check training status'] * len(features_series)
                    })
                
                # Convert features to numpy array
                features_list = features_series.tolist()
                X = np.array(features_list)
                
                # Scale features
                X_scaled = scaler.transform(X)
                
                # Get anomaly scores
                scores = model.score_samples(X_scaled)
                
                # Convert scores to anomaly probabilities
                normalized_scores = -scores  # Invert so positive = more anomalous
                normalized_scores = 1 / (1 + np.exp(-normalized_scores * 5))
                
                # Determine anomalies and severity
                is_anomalies = normalized_scores > ANOMALY_THRESHOLD
                severities = []
                recommendations = []
                
                for i, score in enumerate(normalized_scores):
                    # Get features for this record
                    features = features_list[i]
                    temp, humidity, soil_ph, temp_diff, humidity_diff, anomaly_rate = features
                    
                    # Determine severity
                    if score > CRITICAL_THRESHOLD:
                        severities.append("CRITICAL")
                    elif score > ANOMALY_THRESHOLD:
                        severities.append("HIGH")
                    else:
                        severities.append("NORMAL")
                    
                    # Generate simple recommendations inline
                    rec = generate_inline_recommendations(score, temp, humidity, soil_ph, temp_diff, humidity_diff, anomaly_rate)
                    recommendations.append(rec)
                
                return pd.DataFrame({
                    'anomaly_score': normalized_scores.astype(float),
                    'is_anomaly': is_anomalies.astype(bool),
                    'severity': severities,
                    'recommendations': recommendations
                })
                
            except Exception as e:
                logger.error(f"Batch prediction error: {e}")
                # Return error values for the entire batch
                return pd.DataFrame({
                    'anomaly_score': [0.0] * len(features_series),
                    'is_anomaly': [False] * len(features_series),
                    'severity': ['ERROR'] * len(features_series),
                    'recommendations': ['Processing error - contact technical support'] * len(features_series)
                })
        
        # Create feature array and apply Pandas UDF
        return df \
            .withColumn("feature_array", array([col(f) for f in CORE_FEATURES])) \
            .withColumn("prediction", predict_anomaly_batch(col("feature_array"))) \
            .select(
                col("field_id"),
                col("location"),
                col("processing_timestamp"),
                col("prediction.anomaly_score"),
                col("prediction.is_anomaly"),
                col("prediction.severity"),
                col("prediction.recommendations"),
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
                    col("recommendations"),
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
    
    #TODO: remove this function if not needed
    def _generate_recommendations(self, anomaly_score, temp, humidity, soil_ph, temp_diff, humidity_diff, anomaly_rate):
        """Generate text recommendations based on anomaly score and features"""
        
        recommendations = []
        
        # Base recommendations based on anomaly score
        if anomaly_score > 0.9:
            recommendations.append("🚨 IMMEDIATE INTERVENTION REQUIRED")
        elif anomaly_score > 0.7:
            recommendations.append("⚠️ WARNING: Critical conditions detected")
        elif anomaly_score > 0.5:
            recommendations.append("📊 Enhanced monitoring recommended")
        else:
            recommendations.append("✅ Normal conditions - continue monitoring")
        
        # Temperature-based recommendations
        if temp > 30:
            recommendations.append("🌡️ High temperature: consider supplemental irrigation and shading")
        elif temp < 10:
            recommendations.append("❄️ Low temperature: protect crops from frost")
        
        # Humidity-based recommendations
        if humidity > 85:
            recommendations.append("💧 High humidity: fungal risk - improve ventilation and consider preventive fungicides")
        elif humidity < 40:
            recommendations.append("🏜️ Low humidity: increase irrigation and consider misting")
        
        # Soil pH recommendations
        if soil_ph < 6.0:
            recommendations.append("🌱 Acidic pH: consider liming to increase pH")
        elif soil_ph > 7.5:
            recommendations.append("🌱 Alkaline pH: consider organic acidifiers")
        
        # Differential-based recommendations
        if abs(temp_diff) > 5:
            recommendations.append("🌡️ High temperature differential: verify local microclimate and sensors")
        
        if abs(humidity_diff) > 15:
            recommendations.append("💧 High humidity differential: verify irrigation and drainage")
        
        # Anomaly rate recommendations
        if anomaly_rate > 0.3:
            recommendations.append("🔧 High anomaly rate: verify sensor operation and calibration")
        
        # Critical combinations
        if temp > 25 and humidity > 80:
            recommendations.append("🦠 Ideal conditions for pathogens: apply preventive fungicides")
        
        if soil_ph < 6.0 and humidity > 70:
            recommendations.append("🌱 Nutritional stress + humidity: consider fertilizers and drainage")
        
        # Join recommendations
        if len(recommendations) > 1:
            return " | ".join(recommendations)
        else:
            return recommendations[0] if recommendations else "No specific recommendations"
    
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
    
    def _reload_model_if_needed(self):
        """Reload model if a newer version is available"""
        try:
            # Check if there's a newer model available
            new_model, new_scaler, new_metadata = self.model_manager.load_latest_model()
            
            if new_model is not None and new_metadata.get('model_version') != self.metadata.get('model_version'):
                logger.info(f"Reloading model from {self.metadata.get('model_version')} to {new_metadata.get('model_version')}")
                self.model = new_model
                self.scaler = new_scaler
                self.metadata = new_metadata
                logger.info("Model reloaded successfully")
            else:
                logger.debug("No newer model available, using current model")
                
        except Exception as e:
            logger.warning(f"Failed to reload model: {e}")
            # Continue with current model
    

