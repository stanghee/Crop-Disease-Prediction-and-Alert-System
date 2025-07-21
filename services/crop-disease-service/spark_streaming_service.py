#!/usr/bin/env python3
"""
Spark Streaming Alert Service
Real-time alert generation using Spark Structured Streaming
Connects to the existing Spark cluster (master + 2 workers)
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import to_timestamp

from database.alert_repository import AlertRepository

# Fix import for directory with dash in name
import sys
sys.path.append('Threshold-alert')
from config.alert_thresholds import AlertConfiguration, RiskLevel

logger = logging.getLogger(__name__)

class SparkStreamingAlertService:
    """
    Real-time alert service using Spark Structured Streaming
    Processes IoT and weather data from Kafka topics in real-time
    """
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.alert_repository = AlertRepository(self.spark)
        self.config = AlertConfiguration()
        
        # Streaming queries
        self.streaming_queries: List[StreamingQuery] = []
        self.is_running = False
        
        # Define schemas for Kafka topics
        self.iot_schema = self._get_iot_schema()
        self.weather_schema = self._get_weather_schema()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session connected to existing cluster"""
        spark_master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        driver_host = os.getenv("SPARK_DRIVER_HOST", "crop-disease-service")
        driver_port = os.getenv("SPARK_DRIVER_PORT", "4042")
        driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "512m")
        executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "512m")
        executor_cores = os.getenv("SPARK_EXECUTOR_CORES", "1")
        cores_max = os.getenv("SPARK_CORES_MAX", "1")
        spark = SparkSession.builder \
            .appName("CropDiseaseAlerting") \
            .master(spark_master_url) \
            .config("spark.driver.host", driver_host) \
            .config("spark.driver.port", driver_port) \
            .config("spark.driver.memory", driver_memory) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.executor.cores", executor_cores) \
            .config("spark.cores.max", cores_max) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.session.timeZone", "Europe/Rome") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/alerts-checkpoints") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("DEBUG")
        return spark
    
    def _get_iot_schema(self) -> StructType:
        """Schema for IoT sensor data"""
        return StructType([
            StructField("field_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("soil_ph", DoubleType(), True),
            StructField("location", StringType(), True),
            StructField("temperature_valid", BooleanType(), True),
            StructField("humidity_valid", BooleanType(), True),
            StructField("ph_valid", BooleanType(), True)
        ])
    
    def _get_weather_schema(self) -> StructType:
        """Schema for weather data"""
        return StructType([
            StructField("location", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("wind_kph", DoubleType(), True),
            StructField("wind_dir", StringType(), True),
            StructField("pressure_mb", DoubleType(), True),
            StructField("precip_mm", DoubleType(), True),
            StructField("cloud", IntegerType(), True),
            StructField("uv", DoubleType(), True)
        ])
    
    def start_streaming(self) -> None:
        """Start real-time streaming processing"""
        if self.is_running:
            logger.warning("Spark Streaming Alert Service is already running")
            return
            
        logger.info("Starting Spark Streaming Alert Service...")
        self.is_running = True
        
        try:
            # Start IoT sensor alert processing
            iot_query = self._start_iot_processing()
            self.streaming_queries.append(iot_query)
            
            # Start weather alert processing  
            weather_query = self._start_weather_processing()
            self.streaming_queries.append(weather_query)
            
            logger.info("Spark Streaming Alert Service started successfully")
            
        except Exception as e:
            logger.error(f"Error starting Spark Streaming Alert Service: {e}")
            self.stop_streaming()
            raise
    
    def _start_iot_processing(self) -> StreamingQuery:
        """Start IoT sensor data processing stream"""
        logger.info("Starting IoT sensor alert processing...")
        
        # Read from Kafka
        iot_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "iot_valid_data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        parsed_iot = iot_stream.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), self.iot_schema).alias("data")
        ).select("kafka_timestamp", "data.*")
        
        # Filter out invalid records
        valid_iot = parsed_iot.filter(
            col("field_id").isNotNull() & 
            col("timestamp").isNotNull()
        )
        
        # Apply threshold checks and generate alerts
        iot_alerts = self._apply_iot_thresholds(valid_iot)
        
        # ✅ CORRECT BIG DATA SOLUTION: foreachBatch with direct JDBC (no object serialization)
        return iot_alerts.writeStream \
            .foreachBatch(self._write_alerts_via_jdbc) \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", "/tmp/alerts-checkpoints/iot") \
            .start()
    
    def _start_weather_processing(self) -> StreamingQuery:
        """Start weather data processing stream"""
        logger.info("Starting weather alert processing...")
        
        # Read from Kafka
        weather_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "weather_valid_data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        parsed_weather = weather_stream.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), self.weather_schema).alias("data")
        ).select("kafka_timestamp", "data.*")
        
        # Filter out invalid records
        valid_weather = parsed_weather.filter(
            col("location").isNotNull() & 
            col("timestamp").isNotNull()
        )
        
        # Apply threshold checks and generate alerts
        weather_alerts = self._apply_weather_thresholds(valid_weather)
        
        # ✅ CORRECT BIG DATA SOLUTION: foreachBatch with direct JDBC (no object serialization)
        return weather_alerts.writeStream \
            .foreachBatch(self._write_alerts_via_jdbc) \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", "/tmp/alerts-checkpoints/weather") \
            .start()
    
    def _apply_iot_thresholds(self, df: DataFrame) -> DataFrame:
        logger.debug("Applying simplified IoT threshold rules...")
        alerts_df = df.filter(
            ((col("humidity") < 20) & col("humidity_valid")) |
            ((col("humidity") > 95) & col("humidity_valid")) |
            ((col("humidity") < 30) & col("humidity_valid")) |
            ((col("humidity") > 90) & col("humidity_valid")) |
            ((col("temperature") < 0) & col("temperature_valid")) |
            ((col("temperature") > 45) & col("temperature_valid")) |
            ((col("temperature") < 5) & col("temperature_valid")) |
            ((col("temperature") > 40) & col("temperature_valid")) |
            ((col("soil_ph") < 4.5) & col("ph_valid")) |
            ((col("soil_ph") > 9.0) & col("ph_valid")) |
            ((col("soil_ph") < 5.5) & col("ph_valid")) |
            ((col("soil_ph") > 8.0) & col("ph_valid"))
        )
        message = (
            when((col("humidity") < 20) & col("humidity_valid"),
                 concat(lit("Humidity too low: "), col("humidity"), lit("% (threshold: 20%)")))
            .when((col("humidity") > 95) & col("humidity_valid"),
                 concat(lit("Humidity too high: "), col("humidity"), lit("% (threshold: 95%)")))
            .when((col("humidity") < 30) & col("humidity_valid"),
                 concat(lit("Low humidity: "), col("humidity"), lit("% (threshold: 30%)")))
            .when((col("humidity") > 90) & col("humidity_valid"),
                 concat(lit("High humidity: "), col("humidity"), lit("% (threshold: 90%)")))
            .when((col("temperature") < 0) & col("temperature_valid"),
                 concat(lit("Temperature too low: "), col("temperature"), lit("°C (threshold: 0°C)")))
            .when((col("temperature") > 45) & col("temperature_valid"),
                 concat(lit("Temperature too high: "), col("temperature"), lit("°C (threshold: 45°C)")))
            .when((col("temperature") < 5) & col("temperature_valid"),
                 concat(lit("Low temperature: "), col("temperature"), lit("°C (threshold: 5°C)")))
            .when((col("temperature") > 40) & col("temperature_valid"),
                 concat(lit("High temperature: "), col("temperature"), lit("°C (threshold: 40°C)")))
            .when((col("soil_ph") < 4.5) & col("ph_valid"),
                 concat(lit("Soil pH too low: "), col("soil_ph"), lit(" (threshold: 4.5)")))
            .when((col("soil_ph") > 9.0) & col("ph_valid"),
                 concat(lit("Soil pH too high: "), col("soil_ph"), lit(" (threshold: 9.0)")))
            .when((col("soil_ph") < 5.5) & col("ph_valid"),
                 concat(lit("Low soil pH: "), col("soil_ph"), lit(" (threshold: 5.5)")))
            .when((col("soil_ph") > 8.0) & col("ph_valid"),
                 concat(lit("High soil pH: "), col("soil_ph"), lit(" (threshold: 8.0)")))
            .otherwise(lit("Threshold exceeded"))
        )
        return alerts_df.select(
            col("field_id").alias("zone_id"),
            to_timestamp(col("timestamp")).alias("alert_timestamp"),
            lit("SENSOR_ANOMALY").alias("alert_type"),
            when(
                ((col("humidity") < 20) | (col("humidity") > 95)) |
                ((col("temperature") < 0) | (col("temperature") > 45)) |
                ((col("soil_ph") < 4.5) | (col("soil_ph") > 9.0)),
                "HIGH"
            ).when(
                ((col("humidity") < 30) | (col("humidity") > 90)) |
                ((col("temperature") < 5) | (col("temperature") > 40)) |
                ((col("soil_ph") < 5.5) | (col("soil_ph") > 8.0)),
                "MEDIUM"
            ).otherwise("LOW").alias("severity"),
            message.alias("message"),
            lit("ACTIVE").alias("status")
        )

    def _apply_weather_thresholds(self, df: DataFrame) -> DataFrame:
        logger.debug("Applying simplified weather threshold rules...")
        alerts_df = df.filter(
            (col("temp_c") < -10) | (col("temp_c") > 50) |
            (col("temp_c") < 0) | (col("temp_c") > 45) |
            (col("wind_kph") > 100) |
            (col("wind_kph") > 80) |
            (col("precip_mm") > 50)
        )
        message = (
            when((col("temp_c") < -10),
                 concat(lit("Temperature too low: "), col("temp_c"), lit("°C (threshold: -10°C)")))
            .when((col("temp_c") > 50),
                 concat(lit("Temperature too high: "), col("temp_c"), lit("°C (threshold: 50°C)")))
            .when((col("temp_c") < 0),
                 concat(lit("Low temperature: "), col("temp_c"), lit("°C (threshold: 0°C)")))
            .when((col("temp_c") > 45),
                 concat(lit("High temperature: "), col("temp_c"), lit("°C (threshold: 45°C)")))
            .when((col("wind_kph") > 100),
                 concat(lit("Wind speed too high: "), col("wind_kph"), lit(" km/h (threshold: 100 km/h)")))
            .when((col("wind_kph") > 80),
                 concat(lit("High wind speed: "), col("wind_kph"), lit(" km/h (threshold: 80 km/h)")))
            .when((col("precip_mm") > 50),
                 concat(lit("Excessive precipitation: "), col("precip_mm"), lit(" mm (threshold: 50 mm)")))
            .otherwise(lit("Weather threshold exceeded"))
        )
        return alerts_df.select(
            col("location").alias("zone_id"),
            to_timestamp(col("timestamp")).alias("alert_timestamp"),
            lit("WEATHER_ALERT").alias("alert_type"),
            when(
                (col("temp_c") < -10) | (col("temp_c") > 50) |
                (col("wind_kph") > 100) | (col("precip_mm") > 50),
                "HIGH"
            ).when(
                (col("temp_c") < 0) | (col("temp_c") > 45) |
                (col("wind_kph") > 80) | (col("precip_mm") > 30),
                "MEDIUM"  
            ).otherwise("LOW").alias("severity"),
            message.alias("message"),
            lit("ACTIVE").alias("status")
        )
    
    def _write_alerts_via_jdbc(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Write alerts via direct JDBC - BIG DATA SOLUTION
        Uses Spark JDBC without serializing Python objects
        """
        try:
            logger.info(f"Writing alerts via JDBC (batch {batch_id})")
            columns = ["zone_id", "alert_timestamp", "alert_type", "severity", "message", "status"]
            batch_df = batch_df.select(*columns)
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/crop_disease_ml") \
                .option("dbtable", "alerts") \
                .option("user", "ml_user") \
                .option("password", "ml_password") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info(f"Successfully saved alerts from batch {batch_id}")
        except Exception as e:
            logger.error(f"Error writing alerts batch {batch_id}: {e}")
    
    def stop_streaming(self) -> None:
        """Stop all streaming queries gracefully"""
        logger.info("Stopping Spark Streaming Alert Service...")
        self.is_running = False
        
        for query in self.streaming_queries:
            try:
                query.stop()
            except Exception as e:
                logger.warning(f"Error stopping streaming query: {e}")
        
        self.streaming_queries.clear()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("Spark Streaming Alert Service stopped")
    
    def get_streaming_status(self) -> Dict[str, Any]:
        """Get status of all streaming queries"""
        status = {
            "is_running": self.is_running,
            "active_queries": len(self.streaming_queries),
            "queries": []
        }
        
        for i, query in enumerate(self.streaming_queries):
            try:
                query_status = {
                    "id": query.id,
                    "name": query.name or f"query_{i}",
                    "is_active": query.isActive,
                    "exception": str(query.exception()) if query.exception() else None
                }
                
                # Add progress info if available
                try:
                    progress = query.lastProgress
                    if progress:
                        query_status["last_progress"] = {
                            "timestamp": progress.get("timestamp"),
                            "batchId": progress.get("batchId"),
                            "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                            "processedRowsPerSecond": progress.get("processedRowsPerSecond")
                        }
                except:
                    pass
                    
                status["queries"].append(query_status)
                
            except Exception as e:
                logger.warning(f"Error getting query status: {e}")
        
        return status 