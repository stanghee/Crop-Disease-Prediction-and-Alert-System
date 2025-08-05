#!/usr/bin/env python3
"""
Silver Zone Processor - Data Validation and Cleaning
Processes Bronze zone data into validated, cleaned datasets
"""

import os
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import to_json, struct

logger = logging.getLogger(__name__)

class SilverZoneProcessor:
    """
    Processes Bronze zone data into validated Silver zone datasets
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Allow overriding storage paths via environment variables for flexibility in local vs cloud runs
        self.bronze_path = os.getenv("BRONZE_PATH", "s3a://bronze/")
        # Silver path where cleaned data will be stored. Can be set e.g. SILVER_PATH="s3a://silver/"
        self.silver_path = os.getenv("SILVER_PATH", "s3a://silver/")
        # Kafka configuration for valid data topics
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        # Explicit schemas for streaming sources
        self.sensor_schema = StructType([
            StructField("humidity", DoubleType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_partition", LongType(), True),
            StructField("kafka_timestamp", StringType(), True),
            StructField("kafka_topic", StringType(), True),
            StructField("location", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("soil_ph", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("field_id", StringType(), True),
        ])
        self.weather_schema = StructType([
            StructField("condition", StringType(), True),
            StructField("country", StringType(), True),
            StructField("humidity", LongType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_partition", LongType(), True),
            StructField("kafka_timestamp", StringType(), True),
            StructField("kafka_topic", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("message_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("uv", DoubleType(), True),
            StructField("wind_kph", DoubleType(), True),
            StructField("location", StringType(), True),
            StructField("precip_mm", DoubleType(), True),
        ])
        self.satellite_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("image_path", StringType(), True),
            StructField("image_size_bytes", LongType(), True),
            StructField("bbox_min_lon", DoubleType(), True),
            StructField("bbox_min_lat", DoubleType(), True),
            StructField("bbox_max_lon", DoubleType(), True),
            StructField("bbox_max_lat", DoubleType(), True),
        ])
    
    def _wait_for_bronze_data(self, data_type: str, max_wait_minutes: int = 5):
        """
        Wait for Bronze data to become available before starting streaming
        """
        bronze_data_path = f"{self.bronze_path}{data_type}/"
        logger.info(f"Waiting for Bronze data at {bronze_data_path}")
        
        start_time = time.time()
        while time.time() - start_time < max_wait_minutes * 60:
            try:
                # Try to read a small sample to check if data exists
                test_df = self.spark.read.format("json").schema(self._get_schema(data_type)).load(bronze_data_path)
                count = test_df.limit(1).count()
                if count > 0:
                    logger.info(f"Bronze {data_type} data found, proceeding with Silver streaming")
                    return True
                else:
                    logger.info(f"Bronze {data_type} directory exists but no data yet, waiting...")
            except Exception as e:
                logger.info(f"Bronze {data_type} data not ready yet: {e}, waiting...")
            
            time.sleep(30)  # Wait 30 seconds before retrying
        
        logger.warning(f"Timeout waiting for Bronze {data_type} data after {max_wait_minutes} minutes")
        return False
    
    def _get_schema(self, data_type: str) -> StructType:
        """Get schema for specific data type"""
        if data_type == "iot":
            return self.sensor_schema
        elif data_type == "weather":
            return self.weather_schema
        elif data_type == "satellite":
            return self.satellite_schema
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def start_sensor_stream(self):
        """
        Structured Streaming: process IoT sensor data from Bronze to Silver in real-time.
        Reads new JSON files as they arrive, applies validation, and writes valid records to Silver as Parquet.
        Also sends validated data to Kafka topic 'iot_valid_data' for real-time alerting.
        Returns the streaming query handle.
        """
        logger.info("[STREAMING] Starting Silver streaming for sensors...")
        
        # Wait for Bronze data to be available
        if not self._wait_for_bronze_data("iot"):
            logger.error("Cannot start sensor Silver streaming - no Bronze data available")
            return None
        
        try:
            bronze_stream = self.spark.readStream.format("json").schema(self.sensor_schema).load(f"{self.bronze_path}iot/")
            silver_stream = bronze_stream \
                .filter(
                    col("temperature").isNotNull() &
                    col("humidity").isNotNull() &
                    col("soil_ph").isNotNull() &
                    col("field_id").isNotNull() &
                    col("location").isNotNull() &
                    col("timestamp").isNotNull()
                ) \
                .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")) \
                .withColumn("timestamp_parsed", when(col("timestamp_parsed").isNull(), to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")).otherwise(col("timestamp_parsed"))) \
                .filter(col("timestamp_parsed").isNotNull()) \
                .withColumn("date", to_date(col("timestamp_parsed"))) \
                .withColumn("hour", hour(col("timestamp_parsed"))) \
                .withColumn("day_of_week", dayofweek(col("timestamp_parsed"))) \
                .withColumn("month", month(col("timestamp_parsed"))) \
                .withColumn("year", year(col("timestamp_parsed"))) \
                .withColumn("temperature_valid", when((col("temperature") >= -20) & (col("temperature") <= 60), True).otherwise(False)) \
                .withColumn("humidity_valid", when((col("humidity") >= 0) & (col("humidity") <= 100), True).otherwise(False)) \
                .withColumn("ph_valid", when((col("soil_ph") >= 3.0) & (col("soil_ph") <= 9.0), True).otherwise(False)) \
                .withColumn("coordinates_valid", when(col("latitude").isNotNull() & col("longitude").isNotNull() & (col("latitude") >= -90) & (col("latitude") <= 90) & (col("longitude") >= -180) & (col("longitude") <= 180), True).otherwise(False)) \
                .filter(col("temperature_valid") & col("humidity_valid") & col("ph_valid") & col("coordinates_valid"))
            
            # Write to Silver zone (Parquet)
            silver_query = silver_stream.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", f"{self.silver_path}iot/") \
                .option("checkpointLocation", "/tmp/checkpoints/silver_iot") \
                .partitionBy("date", "field_id") \
                .trigger(processingTime="1 minute") \
                .start()
            
            # Send validated data to Kafka for real-time alerting
            kafka_stream = silver_stream.select(
                to_json(struct(
                    col("field_id"),
                    col("location"),
                    col("latitude"),
                    col("longitude"),
                    col("temperature"),
                    col("humidity"),
                    col("soil_ph"),
                    col("temperature_valid"),
                    col("humidity_valid"),
                    col("ph_valid"),
                    col("coordinates_valid"),
                    col("timestamp_parsed").alias("timestamp")
                )).alias("value")
            )
            
            kafka_query = kafka_stream.writeStream \
                .outputMode("append") \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("topic", "iot_valid_data") \
                .option("checkpointLocation", "/tmp/checkpoints/silver_iot_kafka") \
                .trigger(processingTime="1 minute") \
                .start()
            
            logger.info("[STREAMING] Sensor Silver streaming started successfully (Parquet + Kafka)")
            return silver_query, kafka_query
        except Exception as e:
            logger.error(f"[STREAMING] Error starting sensor Silver streaming: {e}")
            return None

    def start_weather_stream(self):
        """
        Structured Streaming: process weather data from Bronze to Silver in real-time.
        Reads new JSON files as they arrive, applies validation, and writes valid records to Silver as Parquet.
        Also sends validated data to Kafka topic 'weather_valid_data' for real-time alerting.
        Returns the streaming query handle.
        """
        logger.info("[STREAMING] Starting Silver streaming for weather...")
        
        # Wait for Bronze data to be available
        if not self._wait_for_bronze_data("weather"):
            logger.error("Cannot start weather Silver streaming - no Bronze data available")
            return None
        
        try:
            bronze_stream = self.spark.readStream.format("json").schema(self.weather_schema).load(f"{self.bronze_path}weather/")
            silver_stream = bronze_stream \
                .filter(
                    col("temp_c").isNotNull() &
                    col("humidity").isNotNull() &
                    col("wind_kph").isNotNull() &
                    col("uv").isNotNull() &
                    col("precip_mm").isNotNull() &
                    col("condition").isNotNull() &
                    col("location").isNotNull() &
                    col("timestamp").isNotNull()
                ) \
                .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")) \
                .withColumn("timestamp_parsed", when(col("timestamp_parsed").isNull(), to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")).otherwise(col("timestamp_parsed"))) \
                .filter(col("timestamp_parsed").isNotNull()) \
                .withColumn("date", to_date(col("timestamp_parsed"))) \
                .withColumn("hour", hour(col("timestamp_parsed"))) \
                .withColumn("day_of_week", dayofweek(col("timestamp_parsed"))) \
                .withColumn("month", month(col("timestamp_parsed"))) \
                .withColumn("year", year(col("timestamp_parsed"))) \
                .withColumn("temp_valid", when((col("temp_c") >= -50) & (col("temp_c") <= 60), True).otherwise(False)) \
                .withColumn("humidity_valid", when((col("humidity") >= 0) & (col("humidity") <= 100), True).otherwise(False)) \
                .withColumn("wind_valid", when((col("wind_kph") >= 0) & (col("wind_kph") <= 500), True).otherwise(False)) \
                .withColumn("uv_valid", when((col("uv") >= 0) & (col("uv") <= 20), True).otherwise(False)) \
                .withColumn("precip_valid", when((col("precip_mm") >= 0) & (col("precip_mm") <= 1000), True).otherwise(False)) \
                .withColumn("coordinates_valid", when(col("lat").isNotNull() & col("lon").isNotNull() & (col("lat") >= -90) & (col("lat") <= 90) & (col("lon") >= -180) & (col("lon") <= 180), True).otherwise(False)) \
                .filter(col("temp_valid") & col("humidity_valid") & col("wind_valid") & col("uv_valid") & col("precip_valid") & col("coordinates_valid"))
            
            # Write to Silver zone (Parquet)
            silver_query = silver_stream.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", f"{self.silver_path}weather/") \
                .option("checkpointLocation", "/tmp/checkpoints/silver_weather") \
                .partitionBy("date", "location") \
                .trigger(processingTime="1 minute") \
                .start()
            
            # Send validated data to Kafka for real-time alerting
            kafka_stream = silver_stream.select(
                to_json(struct(
                    col("location"),
                    col("temp_c"),
                    col("humidity"),
                    col("wind_kph"),
                    col("uv"),
                    col("condition"),
                    col("precip_mm"),
                    col("temp_valid"),
                    col("humidity_valid"),
                    col("wind_valid"),
                    col("uv_valid"),
                    col("precip_valid"),
                    col("coordinates_valid"),
                    col("timestamp_parsed").alias("timestamp")
                )).alias("value")
            )
            
            kafka_query = kafka_stream.writeStream \
                .outputMode("append") \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("topic", "weather_valid_data") \
                .option("checkpointLocation", "/tmp/checkpoints/silver_weather_kafka") \
                .trigger(processingTime="1 minute") \
                .start()
            
            logger.info("[STREAMING] Weather Silver streaming started successfully (Parquet + Kafka)")
            return silver_query, kafka_query
        except Exception as e:
            logger.error(f"[STREAMING] Error starting weather Silver streaming: {e}")
            return None

    def start_satellite_stream(self):
        """
        Structured Streaming: process satellite data from Bronze to Silver in real-time.
        Reads new JSON files as they arrive, applies validation, and writes valid records to Silver as Parquet.
        Returns the streaming query handle.
        """
        logger.info("[STREAMING] Starting Silver streaming for satellite...")
        
        # Wait for Bronze data to be available
        if not self._wait_for_bronze_data("satellite"):
            logger.error("Cannot start satellite Silver streaming - no Bronze data available")
            return None
        
        try:
            bronze_stream = self.spark.readStream.format("json").schema(self.satellite_schema).load(f"{self.bronze_path}satellite/")
            silver_stream = bronze_stream \
                .filter(
                    col("timestamp").isNotNull() &
                    col("image_path").isNotNull() &
                    col("image_size_bytes").isNotNull()
                ) \
                .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
                .filter(col("timestamp_parsed").isNotNull()) \
                .withColumn("date", to_date(col("timestamp_parsed"))) \
                .withColumn("hour", hour(col("timestamp_parsed"))) \
                .withColumn("month", month(col("timestamp_parsed"))) \
                .withColumn("year", year(col("timestamp_parsed"))) \
                .withColumn("bbox_valid", when(
                    col("bbox_min_lon").isNotNull() & col("bbox_min_lat").isNotNull() &
                    col("bbox_max_lon").isNotNull() & col("bbox_max_lat").isNotNull() &
                    (col("bbox_min_lon") >= -180) & (col("bbox_min_lon") <= 180) &
                    (col("bbox_min_lat") >= -90) & (col("bbox_min_lat") <= 90) &
                    (col("bbox_max_lon") >= -180) & (col("bbox_max_lon") <= 180) &
                    (col("bbox_max_lat") >= -90) & (col("bbox_max_lat") <= 90) &
                    (col("bbox_min_lon") < col("bbox_max_lon")) &
                    (col("bbox_min_lat") < col("bbox_max_lat")), True).otherwise(False)) \
                .withColumn("image_size_valid", when((col("image_size_bytes") > 1000) & (col("image_size_bytes") < 50000000), True).otherwise(False)) \
                .filter(col("bbox_valid") & col("image_size_valid"))
            query = silver_stream.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", f"{self.silver_path}satellite/") \
                .option("checkpointLocation", "/tmp/checkpoints/silver_satellite") \
                .partitionBy("date") \
                .trigger(processingTime="1 minute") \
                .start()
            logger.info("[STREAMING] Satellite Silver streaming started successfully")
            return query
        except Exception as e:
            logger.error(f"[STREAMING] Error starting satellite Silver streaming: {e}")
            return None 