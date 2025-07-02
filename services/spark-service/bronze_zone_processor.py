#!/usr/bin/env python3
"""
Bronze Zone Processor - Raw Data Ingestion
Handles IoT sensors, weather data, and satellite images
"""

import os
import logging
import json
import base64 as b64
from datetime import datetime
from io import BytesIO
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from minio import Minio
from PIL import Image

logger = logging.getLogger(__name__)

class BronzeZoneProcessor:
    """
    Processes raw data from Kafka topics and stores in Bronze zone
    """
    
    def __init__(self, spark: SparkSession, minio_client: Minio):
        self.spark = spark
        self.minio_client = minio_client
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.bronze_path = "s3a://bronze/"
    
    def process_sensor_data_stream(self) -> StreamingQuery:
        """
        Bronze processing for IoT sensor data
        Raw ingestion with minimal transformation
        """
        logger.info("Starting sensor data Bronze stream processing...")
        
        # Define sensor data schema
        sensor_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("field_id", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("soil_ph", DoubleType(), True)
        ])
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "sensor_data") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and add Bronze metadata with temporal partitioning
        bronze_df = kafka_df.select(
            # Parse the JSON value
            from_json(col("value").cast("string"), sensor_schema).alias("data"),
            # Kafka metadata
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            # Flatten the data structure
            "data.*",
            "kafka_topic",
            "kafka_partition", 
            "kafka_offset",
            "kafka_timestamp"
        ).withColumn(
            # Add temporal partitioning columns based on data timestamp (Europe/Rome timezone)
            "timestamp_parsed", to_timestamp(col("timestamp"))
        ).withColumn(
            # Extract timezone-aware partitioning columns for Europe/Rome
            "year", year(col("timestamp_parsed"))
        ).withColumn(
            "month", month(col("timestamp_parsed"))
        ).withColumn(
            "day", dayofmonth(col("timestamp_parsed"))
        ).withColumn(
            "hour", hour(col("timestamp_parsed"))
        ).drop("timestamp_parsed")
        
        # Write to Bronze zone as JSON with temporal partitioning
        query = bronze_df.writeStream \
            .format("json") \
            .option("path", f"{self.bronze_path}iot/") \
            .option("checkpointLocation", "/tmp/checkpoints/bronze_iot") \
            .partitionBy("year", "month", "day", "hour") \
            .trigger(processingTime="30 seconds") \
            .outputMode("append") \
            .start()
        
        logger.info("Sensor data Bronze stream started")
        return query
    
    def process_weather_data_stream(self) -> StreamingQuery:
        """
        Bronze processing for weather data
        Raw API data preservation
        """
        logger.info("Starting weather data Bronze stream processing...")
        
        # Define weather data schema
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
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "weather_data") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.group.id", f"weather-bronze-{datetime.now().strftime('%Y%m%d%H%M%S')}") \
            .load()
        
        # Parse JSON and add Bronze metadata with temporal partitioning
        bronze_df = kafka_df.select(
            from_json(col("value").cast("string"), weather_schema).alias("data"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            "data.*",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset", 
            "kafka_timestamp"
        ).withColumn(
            # Add temporal partitioning columns based on data timestamp (Europe/Rome timezone)
            "timestamp_parsed", to_timestamp(col("timestamp"))
        ).withColumn(
            # Extract timezone-aware partitioning columns for Europe/Rome
            "year", year(col("timestamp_parsed"))
        ).withColumn(
            "month", month(col("timestamp_parsed"))
        ).withColumn(
            "day", dayofmonth(col("timestamp_parsed"))
        ).withColumn(
            "hour", hour(col("timestamp_parsed"))
        ).drop("timestamp_parsed")
        
        # Write to Bronze zone as JSON with temporal partitioning
        query = bronze_df.writeStream \
            .format("json") \
            .option("path", f"{self.bronze_path}weather/") \
            .option("checkpointLocation", "/tmp/checkpoints/bronze_weather") \
            .partitionBy("year", "month", "day", "hour") \
            .trigger(processingTime="30 seconds") \
            .outputMode("append") \
            .start()
        
        logger.info("Weather data Bronze stream started")
        return query
    
    def process_satellite_data_stream(self) -> StreamingQuery:
        """
        Bronze processing for satellite data
        Extract images and store metadata
        """
        logger.info("Starting satellite data Bronze stream processing...")
        
        # Define satellite data schema
        satellite_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("image_base64", StringType(), True),
            StructField("location", StructType([
                StructField("bbox", ArrayType(DoubleType()), True)
            ]), True)
        ])
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "satellite_data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Process satellite data with foreachBatch for image handling
        def process_satellite_batch(batch_df, batch_id):
            """Process each micro-batch of satellite data"""
            if batch_df.count() == 0:
                return
            
            logger.info(f"Processing satellite batch {batch_id} with {batch_df.count()} records")
            
            # Parse JSON data
            parsed_df = batch_df.select(
                from_json(col("value").cast("string"), satellite_schema).alias("data"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Process each row to handle image extraction
            processed_rows = []
            for row in parsed_df.collect():
                try:
                    data = row.data
                    if data and data.image_base64:
                        # Generate unique filename
                        timestamp_str = data.timestamp.replace(':', '-').replace('.', '_')
                        image_filename = f"satellite_{timestamp_str}_{batch_id}.png"
                        
                        # Decode and save image to MinIO
                        image_bytes = b64.b64decode(data.image_base64)
                        self.minio_client.put_object(
                            "satellite-images",
                            image_filename,
                            BytesIO(image_bytes),
                            len(image_bytes),
                            content_type="image/png"
                        )
                        
                        # Create metadata record
                        bbox = data.location.bbox if data.location and data.location.bbox else [0.0, 0.0, 0.0, 0.0]
                        
                        # Parse timestamp for temporal partitioning (convert to Europe/Rome)
                        import pytz
                        
                        if data.timestamp.endswith('Z'):
                            # UTC timestamp
                            timestamp_parsed = datetime.fromisoformat(data.timestamp.replace('Z', '+00:00'))
                            # Convert to Europe/Rome
                            timestamp_parsed = timestamp_parsed.astimezone(pytz.timezone('Europe/Rome'))
                        elif '+' in data.timestamp or '-' in data.timestamp[-6:]:
                            # Timezone-aware timestamp
                            timestamp_parsed = datetime.fromisoformat(data.timestamp)
                            # Convert to Europe/Rome
                            timestamp_parsed = timestamp_parsed.astimezone(pytz.timezone('Europe/Rome'))
                        else:
                            # Assume local time (Europe/Rome)
                            timestamp_parsed = datetime.fromisoformat(data.timestamp)
                            rome_tz = pytz.timezone('Europe/Rome')
                            timestamp_parsed = rome_tz.localize(timestamp_parsed)
                        
                        metadata_record = {
                            "timestamp": data.timestamp,
                            "image_path": f"s3a://satellite-images/{image_filename}",
                            "image_size_bytes": len(image_bytes),
                            "bbox_min_lon": bbox[0] if len(bbox) > 0 else None,
                            "bbox_min_lat": bbox[1] if len(bbox) > 1 else None,
                            "bbox_max_lon": bbox[2] if len(bbox) > 2 else None,
                            "bbox_max_lat": bbox[3] if len(bbox) > 3 else None,
                            "kafka_topic": row.kafka_topic,
                            "kafka_partition": row.kafka_partition,
                            "kafka_offset": row.kafka_offset,
                            "kafka_timestamp": row.kafka_timestamp,
                            # Add temporal partitioning columns
                            "year": timestamp_parsed.year,
                            "month": timestamp_parsed.month,
                            "day": timestamp_parsed.day,
                            "hour": timestamp_parsed.hour
                        }
                        
                        processed_rows.append(metadata_record)
                        
                except Exception as e:
                    logger.error(f"Error processing satellite image in batch {batch_id}: {e}")
                    continue
            
            # Save metadata to Bronze zone with temporal partitioning
            if processed_rows:
                metadata_df = self.spark.createDataFrame(processed_rows)
                metadata_df.write \
                    .mode("append") \
                    .format("json") \
                    .partitionBy("year", "month", "day", "hour") \
                    .save(f"{self.bronze_path}satellite/")
                
                logger.info(f"Saved {len(processed_rows)} satellite metadata records to Bronze zone")
        
        # Start streaming query with foreachBatch
        query = kafka_df.writeStream \
            .foreachBatch(process_satellite_batch) \
            .option("checkpointLocation", "/tmp/checkpoints/bronze_satellite") \
            .trigger(processingTime="60 seconds") \
            .start()
        
        logger.info("Satellite data Bronze stream started")
        return query
    
    def start_all_streams(self):
        """
        Start all Bronze zone streaming processes
        """
        logger.info("Starting all Bronze zone streams...")
        
        queries = []
        
        try:
            # Start sensor data stream
            sensor_query = self.process_sensor_data_stream()
            queries.append(sensor_query)
            
            # Start weather data stream  
            weather_query = self.process_weather_data_stream()
            queries.append(weather_query)
            
            # Start satellite data stream
            satellite_query = self.process_satellite_data_stream()
            queries.append(satellite_query)
            
            logger.info(f"Started {len(queries)} Bronze zone streams")
            return queries
            
        except Exception as e:
            logger.error(f"Error starting Bronze zone streams: {e}")
            # Stop any started queries
            for query in queries:
                if query and query.isActive:
                    query.stop()
            raise
    
    def stop_all_streams(self, queries):
        """
        Stop all streaming queries gracefully
        """
        logger.info("Stopping all Bronze zone streams...")
        
        for i, query in enumerate(queries):
            try:
                if query and query.isActive:
                    query.stop()
                    logger.info(f"Stopped Bronze stream {i+1}")
            except Exception as e:
                logger.error(f"Error stopping Bronze stream {i+1}: {e}")
        
        logger.info("All Bronze zone streams stopped") 