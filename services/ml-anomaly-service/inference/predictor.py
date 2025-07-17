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
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, BooleanType, StringType
from utils.feature_config import CORE_FEATURES, KMEANS_CONFIG, ANOMALY_DISTANCE_THRESHOLD, CRITICAL_DISTANCE_THRESHOLD

from storage.model_manager import ModelManager
from storage.postgres_client import PostgresClient
import builtins

logger = logging.getLogger(__name__)

class StreamingPredictor:
    """Real-time anomaly detection using Spark Structured Streaming"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model_manager = ModelManager()
        self.postgres_client = PostgresClient()
        # Carica modello e scaler Spark ML
        self.model, self.scaler, self.metadata = self.model_manager.load_latest_model(spark)
        if not self.model:
            logger.warning("No model available for inference - will load when needed")
            self.metadata = {'model_version': 'no_model'}
        else:
            self.centers = [c for c in self.model.clusterCenters()]

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
        """Apply anomaly detection using Spark ML KMeans and distance from centroid"""
        if not self.model or not self.scaler:
            logger.warning("No model or scaler available for inference")
            return df.withColumn("anomaly_score", lit(0.0)) \
                     .withColumn("is_anomaly", lit(False)) \
                     .withColumn("severity", lit("NO_MODEL")) \
                     .withColumn("recommendations", lit("Model not available - check training status"))
        # Assembla e scala le feature
        assembler = VectorAssembler(inputCols=CORE_FEATURES, outputCol="features_vec")
        assembled_df = assembler.transform(df)
        scaled_df = self.scaler.transform(assembled_df)
        # Predici cluster
        pred_df = self.model.transform(scaled_df)
        # Calcola distanza dal centroide assegnato
        import numpy as np
        centers = self.centers
        def distance_from_centroid(features, cluster):
            v = np.array(features)
            c = np.array(centers[cluster])
            return float(np.linalg.norm(v - c))
        distance_udf = udf(distance_from_centroid, FloatType())
        pred_df = pred_df.withColumn("distance", distance_udf(col("scaled_features"), col("prediction")))
        # DEBUG: stampa le distanze e i field_id
        pred_df = pred_df.withColumn("_debug", \
            udf(lambda f, d: print(f"DEBUG: field_id={f}, distance={d}"), StringType())(col("field_id"), col("distance")))
        # Determina anomalia e severity
        pred_df = pred_df.withColumn(
            "is_anomaly", col("distance") > lit(ANOMALY_DISTANCE_THRESHOLD)
        ).withColumn(
            "severity", when(col("distance") > lit(CRITICAL_DISTANCE_THRESHOLD), "CRITICAL")
                        .when(col("distance") > lit(ANOMALY_DISTANCE_THRESHOLD), "HIGH")
                        .otherwise("NORMAL")
        )
        # Raccomandazioni
        def generate_recommendations(distance, temp, humidity, soil_ph, temp_diff, humidity_diff, anomaly_rate):
            recommendations = []
            # Base sulle distanze
            if distance > CRITICAL_DISTANCE_THRESHOLD:
                recommendations.append("IMMEDIATE INTERVENTION REQUIRED")
            elif distance > ANOMALY_DISTANCE_THRESHOLD:
                recommendations.append("WARNING: Outlier detected - enhanced monitoring recommended")
            else:
                recommendations.append("Normal conditions - continue monitoring")
            # Temperature
            if temp > 30:
                recommendations.append("High temperature: consider irrigation and shading")
            elif temp < 10:
                recommendations.append("Low temperature: protect crops from frost")
            # Humidity
            if humidity > 85:
                recommendations.append("High humidity: fungal risk - improve ventilation")
            elif humidity < 40:
                recommendations.append("Low humidity: increase irrigation")
            # Soil pH
            if soil_ph < 6.0:
                recommendations.append("Acidic pH: consider liming")
            elif soil_ph > 7.5:
                recommendations.append("Alkaline pH: consider organic acidifiers")
            # Differenziali
            if builtins.abs(temp_diff) > 5:
                recommendations.append("High temperature differential: verify local microclimate and sensors")
            if builtins.abs(humidity_diff) > 15:
                recommendations.append("High humidity differential: verify irrigation and drainage")
            if anomaly_rate > 0.3:
                recommendations.append("High anomaly rate: verify sensor operation and calibration")
            if temp > 25 and humidity > 80:
                recommendations.append("Ideal conditions for pathogens: apply preventive fungicides")
            if soil_ph < 6.0 and humidity > 70:
                recommendations.append("Nutritional stress + humidity: consider fertilizers and drainage")
            return " | ".join(recommendations) if recommendations else "No specific recommendations"
        rec_udf = udf(generate_recommendations, StringType())
        pred_df = pred_df.withColumn(
            "recommendations",
            rec_udf(
                col("distance"),
                col("sensor_avg_temperature"),
                col("sensor_avg_humidity"),
                col("sensor_avg_soil_ph"),
                col("temp_differential"),
                col("humidity_differential"),
                col("sensor_anomaly_rate")
            )
        )
        # Output finale
        return pred_df.select(
            col("field_id"),
            col("location"),
            col("processing_timestamp"),
            col("distance").alias("anomaly_score"),
            col("is_anomaly"),
            col("severity"),
            col("recommendations"),
            col("sensor_avg_temperature"),
            col("sensor_avg_humidity"),
            col("sensor_avg_soil_ph"),
            col("temp_differential"),
            col("humidity_differential"),
            col("sensor_anomaly_rate")
        ).withColumn("model_version", lit(self.metadata['model_version'])) \
         .withColumn("prediction_timestamp", current_timestamp())
    
    def _write_to_kafka(self, df):
        """Write anomalies to Kafka ml-anomalies topic"""
        # DEBUG: stampa field_id, distance, is_anomaly prima del filtro
        debug_df = df.withColumn(
            "_debug_kafka", udf(lambda f, d, a: print(f"KAFKA DEBUG: field_id={f}, distance={d}, is_anomaly={a}"), StringType())(
                col("field_id"), col("anomaly_score"), col("is_anomaly")
            )
        )
        # Only send anomalies to alert system
        anomaly_stream = debug_df \
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
    

