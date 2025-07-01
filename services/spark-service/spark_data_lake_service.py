#!/usr/bin/env python3
"""
Crop Disease Prediction - Data Lake Service
3-Zone Architecture: Bronze -> Silver -> Gold
"""

import os
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import base64
from io import BytesIO

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta import configure_spark_with_delta_pip
from minio import Minio
from PIL import Image
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class DataLakeService:
    """
    Main service for managing the 3-zone data lake architecture
    """
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.minio_client = self._create_minio_client()
        self._setup_buckets()
        
        # Data lake paths
        self.bronze_path = "s3a://bronze/"
        self.silver_path = "s3a://silver/"
        self.gold_path = "s3a://gold/"
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake and S3 support"""
        builder = SparkSession.builder \
            .appName("CropDiseaseDataLake") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    def _create_minio_client(self) -> Minio:
        """Create MinIO client for bucket management"""
        return Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
    
    def _setup_buckets(self):
        """Create necessary MinIO buckets"""
        buckets = ["bronze", "silver", "gold", "satellite-images"]
        for bucket in buckets:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")

    # =================== BRONZE ZONE ===================
    
    def process_sensor_bronze(self):
        """
        BRONZE ZONE - IoT Sensor Data
        - Raw data ingestion from Kafka
        - Minimal transformation (just add ingestion timestamp)
        - Save as JSON (immutable, closest to source)
        """
        logger.info("Starting IoT Sensor Bronze processing...")
        
        sensor_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("field_id", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("soil_ph", DoubleType(), True)
        ])
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "sensor_data") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON and add ingestion metadata
        bronze_df = df.select(
            from_json(col("value").cast("string"), sensor_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
            lit("sensor").alias("source_type")
        ).select(
            "data.*",
            "kafka_timestamp",
            "ingestion_timestamp",
            "source_type"
        )
        
        # Write to Bronze zone as JSON (partitioned by date)
        query = bronze_df.writeStream \
            .format("json") \
            .option("path", f"{self.bronze_path}sensor_data/") \
            .option("checkpointLocation", "/tmp/checkpoints/bronze_sensor") \
            .partitionBy("source_type") \
            .trigger(processingTime="30 seconds") \
            .start()
            
        return query
    
    def process_weather_bronze(self):
        """
        BRONZE ZONE - Weather Data
        - Raw API data from Kafka
        - Save as JSON with ingestion metadata
        """
        logger.info("Starting Weather Bronze processing...")
        
        weather_schema = StructType([
            StructField("message_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("location", StringType(), True),
            StructField("region", StringType(), True),
            StructField("country", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("wind_kph", DoubleType(), True),
            StructField("condition", StringType(), True),
            StructField("uv", DoubleType(), True)
        ])
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "weather_data") \
            .option("startingOffsets", "latest") \
            .load()
        
        bronze_df = df.select(
            from_json(col("value").cast("string"), weather_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
            lit("weather").alias("source_type")
        ).select(
            "data.*",
            "kafka_timestamp", 
            "ingestion_timestamp",
            "source_type"
        )
        
        query = bronze_df.writeStream \
            .format("json") \
            .option("path", f"{self.bronze_path}weather_data/") \
            .option("checkpointLocation", "/tmp/checkpoints/bronze_weather") \
            .partitionBy("source_type") \
            .trigger(processingTime="30 seconds") \
            .start()
            
        return query
    
    def process_satellite_bronze(self):
        """
        BRONZE ZONE - Satellite Data
        - Raw satellite images from Kafka
        - Extract metadata and save image references
        - Images stored in MinIO, metadata in JSON
        """
        logger.info("Starting Satellite Bronze processing...")
        
        satellite_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("image_base64", StringType(), True),
            StructField("location", StructType([
                StructField("bbox", ArrayType(DoubleType()), True)
            ]), True)
        ])
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "satellite_data") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Process satellite data and extract metadata
        def process_satellite_row(row):
            data = json.loads(row.value)
            timestamp = data['timestamp']
            image_data = data['image_base64']
            bbox = data['location']['bbox']
            
            # Generate unique filename
            image_filename = f"satellite_{timestamp.replace(':', '-').replace('.', '_')}.png"
            
            # Decode and save image to MinIO
            try:
                image_bytes = base64.b64decode(image_data)
                self.minio_client.put_object(
                    "satellite-images",
                    image_filename,
                    BytesIO(image_bytes),
                    len(image_bytes),
                    content_type="image/png"
                )
                
                return {
                    "timestamp": timestamp,
                    "image_path": f"s3a://satellite-images/{image_filename}",
                    "bbox_min_lon": bbox[0],
                    "bbox_min_lat": bbox[1], 
                    "bbox_max_lon": bbox[2],
                    "bbox_max_lat": bbox[3],
                    "ingestion_timestamp": datetime.now().isoformat(),
                    "source_type": "satellite"
                }
            except Exception as e:
                logger.error(f"Error processing satellite image: {e}")
                return None
        
        # For now, we'll handle satellite data in batch mode due to image processing complexity
        # In production, consider using Spark Structured Streaming with foreachBatch
        return None  # Placeholder for satellite streaming implementation

    # =================== SILVER ZONE ===================
    
    def process_sensor_silver(self):
        """
        SILVER ZONE - IoT Sensor Data
        - Data validation and cleaning
        - Schema enforcement
        - Anomaly flagging
        - Save as Parquet with partitioning
        """
        logger.info("Processing Sensor Silver zone...")
        
        # Read from Bronze
        bronze_df = self.spark.read.json(f"{self.bronze_path}sensor_data/")
        
        # Data validation and cleaning
        silver_df = bronze_df \
            .filter(col("temperature").isNotNull() & col("humidity").isNotNull() & col("soil_ph").isNotNull()) \
            .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
            .withColumn("date", to_date(col("timestamp_parsed"))) \
            .withColumn("hour", hour(col("timestamp_parsed"))) \
            .withColumn("temperature_anomaly", 
                       when((col("temperature") < -10) | (col("temperature") > 50), True).otherwise(False)) \
            .withColumn("humidity_anomaly",
                       when((col("humidity") < 0) | (col("humidity") > 100), True).otherwise(False)) \
            .withColumn("ph_anomaly",
                       when((col("soil_ph") < 3) | (col("soil_ph") > 9), True).otherwise(False)) \
            .withColumn("has_anomaly",
                       col("temperature_anomaly") | col("humidity_anomaly") | col("ph_anomaly")) \
            .filter(col("timestamp_parsed").isNotNull())
        
        # Write to Silver as Parquet
        silver_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("date", "field_id") \
            .save(f"{self.silver_path}sensor_data/")
            
        logger.info("Sensor Silver processing completed")
    
    def process_weather_silver(self):
        """
        SILVER ZONE - Weather Data
        - Data validation and type conversion
        - Geospatial validation
        - Save as Parquet
        """
        logger.info("Processing Weather Silver zone...")
        
        bronze_df = self.spark.read.json(f"{self.bronze_path}weather_data/")
        
        silver_df = bronze_df \
            .filter(col("temp_c").isNotNull() & col("humidity").isNotNull()) \
            .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
            .withColumn("date", to_date(col("timestamp_parsed"))) \
            .withColumn("hour", hour(col("timestamp_parsed"))) \
            .withColumn("temp_valid", 
                       when((col("temp_c") >= -50) & (col("temp_c") <= 60), True).otherwise(False)) \
            .withColumn("humidity_valid",
                       when((col("humidity") >= 0) & (col("humidity") <= 100), True).otherwise(False)) \
            .withColumn("coordinates_valid",
                       when(col("lat").isNotNull() & col("lon").isNotNull(), True).otherwise(False)) \
            .filter(col("temp_valid") & col("humidity_valid") & col("coordinates_valid"))
        
        silver_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("date", "location") \
            .save(f"{self.silver_path}weather_data/")
            
        logger.info("Weather Silver processing completed")
    
    def process_satellite_silver(self):
        """
        SILVER ZONE - Satellite Data
        - Image metadata validation
        - Geospatial indexing
        - Image quality assessment
        - Save metadata as Parquet
        """
        logger.info("Processing Satellite Silver zone...")
        
        # This would read satellite metadata from Bronze
        # For now, placeholder implementation
        logger.info("Satellite Silver processing - placeholder")

    # =================== GOLD ZONE ===================
    
    def process_sensor_gold(self):
        """
        GOLD ZONE - IoT Sensor Data
        - Feature engineering for ML
        - Aggregations for dashboards
        - Risk scoring
        - Save as Delta tables for ACID transactions
        """
        logger.info("Processing Sensor Gold zone...")
        
        silver_df = self.spark.read.parquet(f"{self.silver_path}sensor_data/")
        
        # ML Features
        ml_features = silver_df \
            .groupBy("field_id", "date") \
            .agg(
                avg("temperature").alias("avg_temperature"),
                stddev("temperature").alias("std_temperature"),
                min("temperature").alias("min_temperature"),
                max("temperature").alias("max_temperature"),
                avg("humidity").alias("avg_humidity"),
                stddev("humidity").alias("std_humidity"),
                avg("soil_ph").alias("avg_soil_ph"),
                stddev("soil_ph").alias("std_soil_ph"),
                sum(when(col("has_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
                count("*").alias("total_readings")
            ) \
            .withColumn("anomaly_rate", col("anomaly_count") / col("total_readings")) \
            .withColumn("risk_score", 
                       when(col("anomaly_rate") > 0.1, "HIGH")
                       .when(col("anomaly_rate") > 0.05, "MEDIUM")
                       .otherwise("LOW"))
        
        # Save ML features as Delta table
        ml_features.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{self.gold_path}sensor_ml_features/")
        
        # Dashboard KPIs
        dashboard_kpis = silver_df \
            .filter(col("date") >= date_sub(current_date(), 7)) \
            .groupBy("field_id") \
            .agg(
                avg("temperature").alias("current_avg_temp"),
                avg("humidity").alias("current_avg_humidity"),
                avg("soil_ph").alias("current_avg_ph"),
                count(when(col("has_anomaly"), True)).alias("recent_anomalies")
            )
        
        dashboard_kpis.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{self.gold_path}sensor_dashboard_kpis/")
            
        logger.info("Sensor Gold processing completed")
    
    def process_weather_gold(self):
        """
        GOLD ZONE - Weather Data
        - Weather pattern analysis
        - Correlation features
        - Forecast preparation
        """
        logger.info("Processing Weather Gold zone...")
        
        silver_df = self.spark.read.parquet(f"{self.silver_path}weather_data/")
        
        # Weather aggregations for ML
        weather_features = silver_df \
            .groupBy("location", "date") \
            .agg(
                avg("temp_c").alias("avg_temperature"),
                max("temp_c").alias("max_temperature"),
                min("temp_c").alias("min_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("wind_kph").alias("avg_wind_speed"),
                avg("uv").alias("avg_uv_index"),
                first("condition").alias("dominant_condition")
            )
        
        weather_features.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{self.gold_path}weather_ml_features/")
            
        logger.info("Weather Gold processing completed")
    
    def process_satellite_gold(self):
        """
        GOLD ZONE - Satellite Data
        - Vegetation indices (NDVI, EVI)
        - Change detection
        - Crop health indicators
        """
        logger.info("Processing Satellite Gold zone...")
        # Placeholder for satellite image analysis
        logger.info("Satellite Gold processing - placeholder")
    
    def create_integrated_features(self):
        """
        Create integrated features combining all data sources
        """
        logger.info("Creating integrated features...")
        
        # Read Gold zone data
        sensor_features = self.spark.read.format("delta").load(f"{self.gold_path}sensor_ml_features/")
        weather_features = self.spark.read.format("delta").load(f"{self.gold_path}weather_ml_features/")
        
        # Join sensor and weather data by date
        integrated_df = sensor_features.alias("s") \
            .join(weather_features.alias("w"), 
                  col("s.date") == col("w.date"), "left") \
            .select(
                col("s.*"),
                col("w.avg_temperature").alias("weather_avg_temp"),
                col("w.avg_humidity").alias("weather_avg_humidity"),
                col("w.avg_wind_speed"),
                col("w.avg_uv_index"),
                col("w.dominant_condition")
            ) \
            .withColumn("temp_differential", 
                       abs(col("avg_temperature") - col("weather_avg_temp"))) \
            .withColumn("humidity_differential",
                       abs(col("avg_humidity") - col("weather_avg_humidity")))
        
        integrated_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{self.gold_path}integrated_ml_features/")
            
        logger.info("Integrated features created")
    
    def run_batch_processing(self):
        """Run batch processing for Silver and Gold zones"""
        logger.info("Starting batch processing...")
        
        try:
            # Silver zone processing
            self.process_sensor_silver()
            self.process_weather_silver()
            self.process_satellite_silver()
            
            # Gold zone processing
            self.process_sensor_gold()
            self.process_weather_gold()
            self.process_satellite_gold()
            
            # Integrated features
            self.create_integrated_features()
            
            logger.info("Batch processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            raise
    
    def run_streaming_processing(self):
        """Run streaming processing for Bronze zone"""
        logger.info("Starting streaming processing...")
        
        queries = []
        
        try:
            # Start Bronze zone streaming
            sensor_query = self.process_sensor_bronze()
            weather_query = self.process_weather_bronze()
            
            if sensor_query:
                queries.append(sensor_query)
            if weather_query:
                queries.append(weather_query)
            
            # Wait for termination
            for query in queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Error in streaming processing: {e}")
            # Stop all queries
            for query in queries:
                if query and query.isActive:
                    query.stop()
            raise

def main():
    """Main entry point"""
    logger.info("Starting Crop Disease Data Lake Service...")
    
    service = DataLakeService()
    
    # Run both streaming and batch processing
    import threading
    
    def run_streaming():
        service.run_streaming_processing()
    
    def run_batch():
        import time
        while True:
            try:
                service.run_batch_processing()
                time.sleep(300)  # Run batch every 5 minutes
            except Exception as e:
                logger.error(f"Batch processing error: {e}")
                time.sleep(60)  # Wait 1 minute before retry
    
    # Start streaming in background
    streaming_thread = threading.Thread(target=run_streaming)
    streaming_thread.daemon = True
    streaming_thread.start()
    
    # Run batch processing in main thread
    run_batch()

if __name__ == "__main__":
    main() 