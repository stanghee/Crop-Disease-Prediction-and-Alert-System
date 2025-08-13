#!/usr/bin/env python3
"""
Streaming Inference - Real-time anomaly detection
Reads from Kafka gold-ml-features topic and predicts anomalies
"""


import logging
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import FloatType, BooleanType, StringType
from utils.feature_config import CORE_FEATURES, KMEANS_CONFIG, ANOMALY_DISTANCE_THRESHOLD, CRITICAL_DISTANCE_THRESHOLD

from storage.model_manager import ModelManager
from storage.postgres_client import PostgresClient

logger = logging.getLogger(__name__)

class StreamingPredictor:
    """Real-time anomaly detection using Spark Structured Streaming"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model_manager = ModelManager()
        self.postgres_client = PostgresClient()
        # Load model and scaler
        self.model, self.scaler, self.metadata = self.model_manager.load_latest_model(spark)
        if not self.model:
            logger.warning("No model available for inference - will load when needed")
            self.metadata = {'model_version': 'no_model'}

    def start_streaming(self):
        """Start the streaming inference pipeline"""
        logger.info("Starting streaming inference pipeline")
        
        # Reload model if needed 
        self._reload_model_if_needed()
        
        # Define schema for gold-ml-features
        gold_schema = self._get_gold_schema()
        
        # Read from Kafka
        stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "gold-ml-features") \
            .option("startingOffsets", "earliest") \
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
                     .withColumn("severity", lit("LOW")) \
                     .withColumn("recommendations", lit("Model not available - check training status"))
        
        # Assemble and scale features
        assembler = VectorAssembler(inputCols=CORE_FEATURES, outputCol="features_vec")
        assembled_df = assembler.transform(df)
        scaled_df = self.scaler.transform(assembled_df)
        
        # Predict cluster
        pred_df = self.model.transform(scaled_df)
        
        # Centroids
        centroids = self.model.clusterCenters()
        
        # Euclidean distance from centroid
        from pyspark.ml.linalg import Vectors
        from pyspark.sql.functions import array, sqrt, pow, when, lit
        from pyspark.ml.functions import vector_to_array
        
        # Convert features to array
        pred_df = pred_df.withColumn("features_array", vector_to_array(col("scaled_features")))
        
        # For each cluster, calculate the distance 
        distance_expr = lit(0.0)  # Default distance
        
        for cluster_id in range(len(centroids)):
            centroid = centroids[cluster_id]
            
            # Euclidean distance
            sum_squares = lit(0.0)
            for i, centroid_val in enumerate(centroid):
                sum_squares = sum_squares + pow(col("features_array")[i] - lit(float(centroid_val)), 2)
            
            cluster_distance = sqrt(sum_squares)
            
            # If the cluster prediction is this, use this distance
            distance_expr = when(col("prediction") == cluster_id, cluster_distance).otherwise(distance_expr)
        
        # Final distance from centroid
        pred_df = pred_df.withColumn("distance", distance_expr)
        
        # Anomaly and severity
        pred_df = pred_df.withColumn(
            "is_anomaly", col("distance") > lit(ANOMALY_DISTANCE_THRESHOLD)
        ).withColumn(
            "severity", when(col("distance") > lit(CRITICAL_DISTANCE_THRESHOLD), "CRITICAL")
                        .when(col("distance") > lit(ANOMALY_DISTANCE_THRESHOLD), "HIGH")
                        .when(col("distance") > lit(ANOMALY_DISTANCE_THRESHOLD * 0.5), "MEDIUM")
                        .otherwise("LOW")
        )
        # Recommendations
        pred_df = pred_df.withColumn(
            "recommendations",
            concat_ws(" | ",
                # Base sulla distanza dal centroide
                when(col("distance") > lit(CRITICAL_DISTANCE_THRESHOLD), "CRITICAL ANOMALY: Immediate intervention required - deviation from normal cluster")
                .when(col("distance") > lit(ANOMALY_DISTANCE_THRESHOLD), "WARNING: Significant deviation from normal patterns detected - enhanced monitoring recommended") 
                .when(col("distance") > lit(ANOMALY_DISTANCE_THRESHOLD * 0.7), "MEDIUM risk: Moderate deviation detected - increase monitoring frequency")
                .otherwise("LOW risk: Normal cluster behavior - continue routine monitoring"),
                
                # Temperature-based recommendations
                when(col("sensor_avg_temperature") > 30, "High temperature detected: consider irrigation and shading")
                .when(col("sensor_avg_temperature") < 10, "Low temperature detected: protect crops from frost"),
                
                # Humidity-based recommendations  
                when(col("sensor_avg_humidity") > 85, "High humidity detected: fungal risk - improve ventilation")
                .when(col("sensor_avg_humidity") < 40, "Low humidity detected: increase irrigation"),
                
                # Soil pH recommendations
                when(col("sensor_avg_soil_ph") < 6.0, "Acidic soil pH: consider liming")
                .when(col("sensor_avg_soil_ph") > 7.5, "Alkaline soil pH: consider organic acidifiers"),
                
                # Differential-based recommendations  
                when(abs(col("temp_differential")) > 10, "High temperature differential: verify sensor readings"),
                when(abs(col("humidity_differential")) > 20, "High humidity differential: check irrigation uniformity"),
                
                # High anomaly rate in sensor data
                when(col("sensor_anomaly_rate") > 0.1, "High sensor anomaly rate: verify sensor calibration and maintenance"),
                
                # Critical environmental combinations
                when((col("sensor_avg_temperature") > 25) & (col("sensor_avg_humidity") > 80), "High temp + humidity: Ideal pathogen conditions - apply preventive measures"),
                when((col("sensor_avg_soil_ph") < 6.0) & (col("sensor_avg_humidity") > 70), "Acidic pH + high humidity: Risk of nutritional stress - consider fertilizers and drainage"),
                
                # UV-based recommendations
                when(col("weather_avg_uv_index") > 8, "High UV index: Protect crops from sunburn - consider shading"),
                when(col("weather_avg_uv_index") < 3, "Low UV index: Reduced photosynthesis - monitor growth"),
                
                # Wind-based recommendations
                when(col("weather_avg_wind_speed") > 30, "High wind speed: Risk of physical damage - secure structures"),
                when(col("weather_avg_wind_speed") < 5, "Low wind speed: Poor air circulation - monitor for fungal diseases"),
                
                # Combined weather conditions
                when((col("weather_avg_uv_index") > 7) & (col("weather_avg_wind_speed") > 20), "High UV + wind: Stress conditions - increase irrigation"),
                when((col("sensor_avg_humidity") > 85) & (col("weather_avg_wind_speed") < 5), "High humidity + low wind: Fungal risk - improve ventilation")
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
            col("sensor_anomaly_rate"),
            col("weather_avg_uv_index"),
            col("weather_avg_wind_speed"),
            col("temporal_feature_ml_sin"),
            col("temporal_feature_ml_cos")
        ).withColumn("model_version", lit(self.metadata['model_version'])) \
         .withColumn("prediction_timestamp", current_timestamp())
    
    def _write_to_kafka(self, df):
        """Write anomalies to Kafka ml-anomalies topic"""
        debug_df = df
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
    
    
    def _get_gold_schema(self):
        """Get schema for gold-ml-features"""
        return StructType([
            StructField("field_id", StringType()),
            StructField("location", StringType()),
            StructField("sensor_avg_temperature", DoubleType()),
            StructField("sensor_avg_humidity", DoubleType()),
            StructField("sensor_avg_soil_ph", DoubleType()),
            StructField("temp_differential", DoubleType()),
            StructField("humidity_differential", DoubleType()),
            StructField("sensor_anomaly_rate", DoubleType()),
            StructField("weather_avg_uv_index", DoubleType()),
            StructField("weather_avg_wind_speed", DoubleType()),
            StructField("temporal_feature_ml_sin", DoubleType()),
            StructField("temporal_feature_ml_cos", DoubleType()),
            StructField("processing_timestamp", TimestampType()),
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
    

