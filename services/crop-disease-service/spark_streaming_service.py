#!/usr/bin/env python3
"""
Spark Streaming Alert Service - Simplified Version
Reads data from Kafka topics, applies alert thresholds, saves to the database
"""

import os
import logging
import json
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

logger = logging.getLogger(__name__)

class SparkStreamingAlertService:
    """
    Real-time alert service using Spark Structured Streaming
    Simplified approach: read from Kafka -> apply thresholds -> save alerts
    """
    
    def __init__(self):
        logger.info("🚀 Initializing Simple Spark Streaming Alert Service...")
        self.spark = self._create_spark_session()
        self.streaming_queries = []
        self.is_running = False
        logger.info("✅ Service initialized")

#TODO: fix the env variables, we should use the env variables from the docker-compose.yml file     
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Kafka support"""
        spark_master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        
        spark = SparkSession.builder \
            .appName("CropDiseaseAlerting_Simple") \
            .master(spark_master_url) \
            .config("spark.driver.host", "crop-disease-service") \
            .config("spark.driver.port", "4044") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.executor.cores", "1") \
            .config("spark.cores.max", "2") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session created")
        return spark
    
    def start_streaming(self):
        """Start streaming processing"""
        if self.is_running:
            logger.warning("Service is already running")
            return
            
        logger.info("🔄 Starting streaming processing...")
        self.is_running = True
        
        try:
            # Start IoT alert processing
            iot_query = self._process_iot_alerts()
            self.streaming_queries.append(iot_query)
            
            # Start weather alert processing
            weather_query = self._process_weather_alerts()
            self.streaming_queries.append(weather_query)
            
            logger.info(f"✅ Started {len(self.streaming_queries)} streaming queries")
            
        except Exception as e:
            logger.error(f"❌ Error starting streaming: {e}")
            self.stop_streaming()
            raise
    
    def _process_iot_alerts(self):
        """Process IoT data and generate alerts"""
        logger.info("Starting IoT alert processing...")
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "iot_valid_data") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        iot_schema = StructType([
            StructField("field_id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("soil_ph", DoubleType(), True),
            StructField("temperature_valid", BooleanType(), True),
            StructField("humidity_valid", BooleanType(), True),
            StructField("ph_valid", BooleanType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), iot_schema).alias("data")
        ).select("data.*")
        
        # Apply alert thresholds
        alerts_df = parsed_df.filter(
            # Temperature alerts
            ((col("temperature") < 5) & col("temperature_valid")) |
            ((col("temperature") > 40) & col("temperature_valid")) |
            # Humidity alerts  
            ((col("humidity") < 30) & col("humidity_valid")) |
            ((col("humidity") > 90) & col("humidity_valid")) |
            # pH alerts
            ((col("soil_ph") < 5.5) & col("ph_valid")) |
            ((col("soil_ph") > 8.0) & col("ph_valid"))
        ).withColumn(
            "alert_type", lit("SENSOR_ANOMALY")
        ).withColumn(
            "severity", 
            when((col("temperature") < 0) | (col("temperature") > 45) |
                 (col("humidity") < 20) | (col("humidity") > 95) |
                 (col("soil_ph") < 4.5) | (col("soil_ph") > 9.0), "HIGH")
            .otherwise("MEDIUM")
        ).withColumn(
            "message", 
            concat(
                lit("Alert: "),
                when(col("temperature") < 5, concat(lit("Low temperature "), col("temperature"), lit("°C")))
                .when(col("temperature") > 40, concat(lit("High temperature "), col("temperature"), lit("°C")))
                .when(col("humidity") < 30, concat(lit("Low humidity "), col("humidity"), lit("%")))
                .when(col("humidity") > 90, concat(lit("High humidity "), col("humidity"), lit("%")))
                .when(col("soil_ph") < 5.5, concat(lit("Low pH "), col("soil_ph")))
                .when(col("soil_ph") > 8.0, concat(lit("High pH "), col("soil_ph")))
                .otherwise(lit("Threshold exceeded"))
            )
        ).withColumn(
            "alert_timestamp", to_timestamp(col("timestamp"))
        ).withColumn(
            "status", lit("ACTIVE")
        ).select(
            col("field_id").alias("zone_id"),
            col("alert_timestamp"),
            col("alert_type"), 
            col("severity"),
            col("message"),
            col("status")
        )
        
        # Write alerts to PostgreSQL
        return alerts_df.writeStream \
            .foreachBatch(self._write_alerts_batch) \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .option("checkpointLocation", "/tmp/iot-alerts-checkpoint") \
            .start()
    
    def _process_weather_alerts(self):
        """Process weather data and generate alerts"""
        logger.info("Starting weather alert processing...")
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "weather_valid_data") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        weather_schema = StructType([
            StructField("location", StringType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("wind_kph", DoubleType(), True),
            StructField("temp_valid", BooleanType(), True),
            StructField("humidity_valid", BooleanType(), True),
            StructField("coordinates_valid", BooleanType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), weather_schema).alias("data")
        ).select("data.*")
        
        # Apply weather alert thresholds
        alerts_df = parsed_df.filter(
            # Temperature alerts
            ((col("temp_c") < 0) & col("temp_valid")) |
            ((col("temp_c") > 45) & col("temp_valid")) |
            # Wind alerts
            (col("wind_kph") > 80)
        ).withColumn(
            "alert_type", lit("WEATHER_ALERT")
        ).withColumn(
            "severity",
            when((col("temp_c") < -10) | (col("temp_c") > 50) | (col("wind_kph") > 100), "HIGH")
            .otherwise("MEDIUM")
        ).withColumn(
            "message",
            concat(
                lit("Weather Alert: "),
                when(col("temp_c") < 0, concat(lit("Low temperature "), col("temp_c"), lit("°C")))
                .when(col("temp_c") > 45, concat(lit("High temperature "), col("temp_c"), lit("°C")))
                .when(col("wind_kph") > 80, concat(lit("High wind speed "), col("wind_kph"), lit(" km/h")))
                .otherwise(lit("Weather threshold exceeded"))
            )
        ).withColumn(
            "alert_timestamp", to_timestamp(col("timestamp"))
        ).withColumn(
            "status", lit("ACTIVE")
        ).select(
            col("location").alias("zone_id"),
            col("alert_timestamp"),
            col("alert_type"),
            col("severity"),
            col("message"),
            col("status")
        )
        
        # Write alerts to PostgreSQL
        return alerts_df.writeStream \
            .foreachBatch(self._write_alerts_batch) \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .option("checkpointLocation", "/tmp/weather-alerts-checkpoint") \
            .start()
    
    def _write_alerts_batch(self, batch_df, batch_id):
        """Write alerts batch to PostgreSQL and Kafka"""
        try:
            logger.info(f"📊 Processing alerts batch {batch_id}")
            
            # Write to PostgreSQL
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/crop_disease_ml") \
                .option("dbtable", "alerts") \
                .option("user", "ml_user") \
                .option("password", "ml_password") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            logger.info(f"✅ Successfully saved alerts from batch {batch_id} to database")
            
            # Write to Kafka topic "alerts-anomalies"
            try:
                # Convert DataFrame to JSON format for Kafka
                kafka_df = batch_df.select(
                    to_json(struct("*")).alias("value")
                )
                
                kafka_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:9092") \
                    .option("topic", "alerts-anomalies") \
                    .option("checkpointLocation", f"/tmp/kafka-alerts-checkpoint-{batch_id}") \
                    .mode("append") \
                    .save()
                
                logger.info(f"✅ Successfully sent alerts from batch {batch_id} to Kafka topic 'alerts-anomalies'")
                
            except Exception as kafka_error:
                logger.error(f"❌ Error sending alerts to Kafka from batch {batch_id}: {kafka_error}")
            
        except Exception as e:
            logger.error(f"❌ Error writing alerts batch {batch_id}: {e}")
    
    def stop_streaming(self):
        """Stop all streaming queries"""
        logger.info("Stopping Spark Streaming Alert Service...")
        self.is_running = False
        
        for query in self.streaming_queries:
            try:
                if query and query.isActive:
                    query.stop()
            except Exception as e:
                logger.warning(f"Error stopping query: {e}")
        
        self.streaming_queries.clear()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("Spark Streaming Alert Service stopped")
    
    def get_streaming_status(self) -> Dict[str, Any]:
        """Get status of streaming queries"""
        status = {
            "is_running": self.is_running,
            "active_queries": len([q for q in self.streaming_queries if q and q.isActive]),
            "total_queries": len(self.streaming_queries)
        }
        
        for i, query in enumerate(self.streaming_queries):
            try:
                if query and query.lastProgress:
                    progress = query.lastProgress
                    status[f"query_{i}"] = {
                        "numInputRows": progress.get("numInputRows", 0),
                        "inputRowsPerSecond": progress.get("inputRowsPerSecond", 0.0),
                        "batchId": progress.get("batchId", 0)
                    }
            except Exception:
                pass
        
        return status 