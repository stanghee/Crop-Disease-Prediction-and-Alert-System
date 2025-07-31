#!/usr/bin/env python3
"""
Gold Zone Processor - ML Features with Sliding Window
Processes Silver zone data into ML-ready features:
- Sliding window aggregations (10 minutes)
- Location-based sensor-weather join
- 3 records per file (one per field) #TODO: check if we need to change this
- Optimized for machine learning
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import shutil
import glob

logger = logging.getLogger(__name__)

class GoldZoneProcessor:
    """
    Processes Silver zone data into ML-ready features with sliding window approach
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_path = os.getenv("SILVER_PATH", "s3a://silver/")
        self.gold_path = os.getenv("GOLD_PATH", "s3a://gold/")
        self.sliding_window_minutes = 10

  #TODO: check uv info and condition info from the weather schema, enviroment stress scoring and coombined risk score should be removed
  #TODO: we should add month sin cos 
    def create_ml_features_sliding_window(self) -> DataFrame:
        """
        Create ML features using sliding window approach (10 minutes)
        - Aggregates sensor and weather data for last 10 minutes
        - Joins by location to ensure data consistency
        - Creates exactly 3 records (one per field) #TODO: check if we need to change this
        - Outputs to ml_feature folder with timestamp - append mode
        """
        logger.info(f"Creating ML features with {self.sliding_window_minutes}-minute sliding window...")
        
        # Calculate timestamp for sliding window
        window_start = datetime.now() - timedelta(minutes=self.sliding_window_minutes)
        
        try:
            # Read sensor data from Silver zone (last 10 minutes)
            sensor_df = self.spark.read.parquet(f"{self.silver_path}iot/") \
                .filter(col("timestamp_parsed") >= window_start) \
                .filter(col("location").isNotNull()) \
                .filter(col("field_id").isNotNull())
            
            # Read weather data from Silver zone (last 10 minutes)
            weather_df = self.spark.read.parquet(f"{self.silver_path}weather/") \
                .filter(col("timestamp_parsed") >= window_start) \
                .filter(col("location").isNotNull())
            
            # Aggregate sensor data by field_id and location (10-minute window)
            sensor_aggregated = sensor_df \
                .groupBy("field_id", "location") \
                .agg(
                    # Temperature metrics
                    avg("temperature").alias("sensor_avg_temperature"),
                    stddev("temperature").alias("sensor_std_temperature"),
                    min("temperature").alias("sensor_min_temperature"),
                    max("temperature").alias("sensor_max_temperature"),
                    
                    # Humidity metrics
                    avg("humidity").alias("sensor_avg_humidity"),
                    stddev("humidity").alias("sensor_std_humidity"),
                    min("humidity").alias("sensor_min_humidity"),
                    max("humidity").alias("sensor_max_humidity"),
                    
                    # Soil pH metrics
                    avg("soil_ph").alias("sensor_avg_soil_ph"),
                    stddev("soil_ph").alias("sensor_std_soil_ph"),
                    min("soil_ph").alias("sensor_min_soil_ph"),
                    max("soil_ph").alias("sensor_max_soil_ph"),
                    
                    # Data quality metrics
                    avg(col("temperature_valid").cast("int")).alias("sensor_temp_valid_rate"),
                    avg(col("humidity_valid").cast("int")).alias("sensor_humidity_valid_rate"),
                    avg(col("ph_valid").cast("int")).alias("sensor_ph_valid_rate"),
                    
                    # Count metrics
                    count("*").alias("sensor_readings_count"),
                    count(when(~col("temperature_valid"), True)).alias("sensor_temp_invalid_count"),
                    count(when(~col("humidity_valid"), True)).alias("sensor_humidity_invalid_count"),
                    count(when(~col("ph_valid"), True)).alias("sensor_ph_invalid_count"),
                    
                    # Timestamp metrics
                    max("timestamp_parsed").alias("sensor_last_reading"),
                    min("timestamp_parsed").alias("sensor_first_reading")
                ) \
                .withColumn(
                    "sensor_temp_range", col("sensor_max_temperature") - col("sensor_min_temperature")
                ) \
                .withColumn(
                    "sensor_humidity_range", col("sensor_max_humidity") - col("sensor_min_humidity")
                ) \
                .withColumn(
                    "sensor_ph_range", col("sensor_max_soil_ph") - col("sensor_min_soil_ph")
                ) \
                .withColumn(
                    "sensor_anomaly_count", 
                    col("sensor_temp_invalid_count") + col("sensor_humidity_invalid_count") + col("sensor_ph_invalid_count")
                ) \
                .withColumn(
                    "sensor_anomaly_rate", 
                    when(col("sensor_readings_count") > 0, col("sensor_anomaly_count") / col("sensor_readings_count")).otherwise(0)
                ) \
                .withColumn(
                    "sensor_data_quality_score",
                    (col("sensor_temp_valid_rate") + col("sensor_humidity_valid_rate") + col("sensor_ph_valid_rate")) / 3
                )
            
            # Aggregate weather data by location (10-minute window)
            weather_aggregated = weather_df \
                .groupBy("location") \
                .agg(
                    # Temperature metrics
                    avg("temp_c").alias("weather_avg_temperature"),
                    stddev("temp_c").alias("weather_std_temperature"),
                    min("temp_c").alias("weather_min_temperature"),
                    max("temp_c").alias("weather_max_temperature"),
                    
                    # Humidity metrics
                    avg("humidity").alias("weather_avg_humidity"),
                    stddev("humidity").alias("weather_std_humidity"),
                    min("humidity").alias("weather_min_humidity"),
                    max("humidity").alias("weather_max_humidity"),
                    
                    # Wind metrics
                    avg("wind_kph").alias("weather_avg_wind_speed"),
                    stddev("wind_kph").alias("weather_std_wind_speed"),
                    min("wind_kph").alias("weather_min_wind_speed"),
                    max("wind_kph").alias("weather_max_wind_speed"),
                    
                    # UV metrics
                    avg("uv").alias("weather_avg_uv_index"),
                    stddev("uv").alias("weather_std_uv_index"),
                    min("uv").alias("weather_min_uv_index"),
                    max("uv").alias("weather_max_uv_index"),
                    
                    # Condition metrics
                    mode("condition").alias("weather_dominant_condition"),
                    count("*").alias("weather_readings_count"),
                    
                    # Timestamp metrics
                    max("timestamp_parsed").alias("weather_last_reading"),
                    min("timestamp_parsed").alias("weather_first_reading")
                ) \
                .withColumn(
                    "weather_temp_range", col("weather_max_temperature") - col("weather_min_temperature")
                ) \
                .withColumn(
                    "weather_humidity_range", col("weather_max_humidity") - col("weather_min_humidity")
                ) \
                .withColumn(
                    "weather_wind_range", col("weather_max_wind_speed") - col("weather_min_wind_speed")
                ) \
                .withColumn(
                    "weather_uv_range", col("weather_max_uv_index") - col("weather_min_uv_index")
                )
            
            # Join sensor and weather data by location
            ml_features = sensor_aggregated \
                .join(weather_aggregated, "location", "inner") \
                .withColumn(
                    # Differential features (sensor vs weather)
                    "temp_differential", abs(col("sensor_avg_temperature") - col("weather_avg_temperature"))
                ) \
                .withColumn(
                    "humidity_differential", abs(col("sensor_avg_humidity") - col("weather_avg_humidity"))
                ) \
                .withColumn(
                    # Data freshness (minutes since last reading)
                    "sensor_data_freshness_minutes",
                    (unix_timestamp(current_timestamp()) - unix_timestamp(col("sensor_last_reading"))) / 60
                ) \
                .withColumn(
                    "weather_data_freshness_minutes",
                    (unix_timestamp(current_timestamp()) - unix_timestamp(col("weather_last_reading"))) / 60
                ) \
                .withColumn(
                    # Processing metadata
                    "processing_timestamp", current_timestamp()
                ) \
                .withColumn(
                    "window_start_time", lit(window_start)
                ) \
                .withColumn(
                    "window_end_time", current_timestamp()
                ) \
                .withColumn(
                    "window_duration_minutes", lit(self.sliding_window_minutes)
                )
            
            # Ensure we have exactly 3 records (one per field)
            # Get the 3 fields we expect: field_01, field_02, field_03
            expected_fields = ["field_01", "field_02", "field_03"]
            
            # Create a DataFrame with expected fields
            expected_fields_df = self.spark.createDataFrame(
                [(field_id,) for field_id in expected_fields],
                ["expected_field_id"]
            )
            
            # Left join to ensure we get all 3 fields, even if some have no data
            final_ml_features = expected_fields_df \
                .join(ml_features, col("expected_field_id") == col("field_id"), "left") \
                .select(
                    # Use expected field_id to ensure we have all 3 fields
                    col("expected_field_id").alias("field_id"),
                    # All other columns from ml_features (will be null for missing fields)
                    col("location"),
                    col("sensor_avg_temperature"),
                    col("sensor_std_temperature"),
                    col("sensor_min_temperature"),
                    col("sensor_max_temperature"),
                    col("sensor_temp_range"),
                    col("sensor_avg_humidity"),
                    col("sensor_std_humidity"),
                    col("sensor_min_humidity"),
                    col("sensor_max_humidity"),
                    col("sensor_humidity_range"),
                    col("sensor_avg_soil_ph"),
                    col("sensor_std_soil_ph"),
                    col("sensor_min_soil_ph"),
                    col("sensor_max_soil_ph"),
                    col("sensor_ph_range"),
                    col("sensor_temp_valid_rate"),
                    col("sensor_humidity_valid_rate"),
                    col("sensor_ph_valid_rate"),
                    col("sensor_readings_count"),
                    col("sensor_anomaly_count"),
                    col("sensor_anomaly_rate"),
                    col("sensor_data_quality_score"),
                    col("weather_avg_temperature"),
                    col("weather_std_temperature"),
                    col("weather_min_temperature"),
                    col("weather_max_temperature"),
                    col("weather_temp_range"),
                    col("weather_avg_humidity"),
                    col("weather_std_humidity"),
                    col("weather_min_humidity"),
                    col("weather_max_humidity"),
                    col("weather_humidity_range"),
                    col("weather_avg_wind_speed"),
                    col("weather_std_wind_speed"),
                    col("weather_min_wind_speed"),
                    col("weather_max_wind_speed"),
                    col("weather_wind_range"),
                    col("weather_avg_uv_index"),
                    col("weather_std_uv_index"),
                    col("weather_min_uv_index"),
                    col("weather_max_uv_index"),
                    col("weather_uv_range"),
                    col("weather_dominant_condition"),
                    col("weather_readings_count"),
                    col("temp_differential"),
                    col("humidity_differential"),
                    col("sensor_data_freshness_minutes"),
                    col("weather_data_freshness_minutes"),
                    col("processing_timestamp"),
                    col("window_start_time"),
                    col("window_end_time"),
                    col("window_duration_minutes")
                )
            
            # Publish to Kafka for real-time ML processing 
            logger.info("Publishing ML features to Kafka topic 'gold-ml-features'")
            kafka_success = False
            try:
                # Convert to JSON and write to Kafka
                final_ml_features.selectExpr("to_json(struct(*)) AS value") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:9092") \
                    .option("topic", "gold-ml-features") \
                    .save()
                logger.info("Successfully published ML features to Kafka")
                kafka_success = True
            except Exception as kafka_error:
                logger.error(f"Failed to publish to Kafka: {kafka_error}")
                # Don't fail the entire process if Kafka write fails

            # Write to MinIO 
            minio_success = False
            try:
                # Generate timestamp for file naming
                current_timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
                output_dir = f"{self.gold_path}ml_feature/tmp_{current_timestamp_str}"  
                final_filename = f"ml_feature_{current_timestamp_str}.snappy.parquet"
                final_path = f"{self.gold_path}ml_feature/{final_filename}"

                # Scrivi in una sola partizione #TODO: check if we need to change this
                final_ml_features.coalesce(1).write \
                    .format("parquet") \
                    .option("compression", "snappy") \
                    .mode("overwrite") \
                    .save(output_dir)

                # Trova il file part-*.parquet e rinominalo #TODO: check if we need to change this
                import os
                from py4j.java_gateway import java_import
                java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")
                fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
                tmp_path = self.spark._jvm.org.apache.hadoop.fs.Path(output_dir)
                dest_path = self.spark._jvm.org.apache.hadoop.fs.Path(final_path)
                files = fs.listStatus(tmp_path)
                for f in files:
                    name = f.getPath().getName()
                    if name.startswith("part-") and name.endswith(".parquet"):
                        fs.rename(f.getPath(), dest_path)
                # Rimuovi la cartella temporanea
                fs.delete(tmp_path, True)
                
                logger.info(f"Successfully wrote ML features to MinIO: {final_path}")
                minio_success = True
                
            except Exception as minio_error:
                logger.error(f"Failed to write to MinIO: {minio_error}")
                # Don't fail the entire process if MinIO write fails
                # The important part (Kafka) was already done above

            # Log results
            record_count = final_ml_features.count()
            logger.info(f"Generated {record_count} ML feature records (expected: 3)")
            logger.info(f"Kafka publishing: {'SUCCESS' if kafka_success else 'FAILED'}")
            logger.info(f"MinIO storage: {'SUCCESS' if minio_success else 'FAILED'}")
            
            if minio_success:
                logger.info(f"Created ML feature file: {final_path}")
            
            # Log details for each field
            for field_id in expected_fields:
                field_data = final_ml_features.filter(col("field_id") == field_id)
                field_count = field_data.count()
                if field_count > 0:
                    location = field_data.select("location").first()
                    location_str = location.location if location and location.location else "NO_DATA"
                    logger.info(f"  - {field_id}: {location_str} (data available)")
                else:
                    logger.info(f"  - {field_id}: NO_DATA")
            
            return final_ml_features
            
        except Exception as e:
            logger.error(f"Error creating ML features: {e}")
            # Return empty DataFrame with expected schema
            schema = StructType([
                StructField("field_id", StringType(), True),
                StructField("location", StringType(), True),
                StructField("processing_timestamp", TimestampType(), True)
            ])
            return self.spark.createDataFrame([], schema)

#TODO: check if we need to change this
    def get_ml_features_summary(self) -> Dict[str, Any]:
        """
        Get summary of ML features processing
        """
        try:
            # List all ML feature files
            ml_feature_path = f"{self.gold_path}ml_feature/"
            
            # Try to read the most recent file
            try:
                # Read all files in ml_feature directory and get the most recent
                all_features = self.spark.read.parquet(f"{ml_feature_path}*.snappy.parquet")
                
                # Get the most recent data by processing_timestamp
                latest_features = all_features.orderBy(col("processing_timestamp").desc()).limit(3)
                
                summary = latest_features.agg(
                    count("*").alias("total_records"),
                    avg("sensor_data_quality_score").alias("avg_data_quality"),
                    avg("temp_differential").alias("avg_temp_differential"),
                    avg("humidity_differential").alias("avg_humidity_differential")
                ).collect()[0]
                
                return {
                    "total_records": int(summary.total_records or 0),
                    "avg_data_quality": float(summary.avg_data_quality or 0),
                    "avg_temp_differential": float(summary.avg_temp_differential or 0),
                    "avg_humidity_differential": float(summary.avg_humidity_differential or 0),
                    "processing_timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.warning(f"Could not read ML feature data for summary: {e}")
                return {
                    "total_records": 0,
                    "error": "No ML feature data available",
                    "processing_timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting ML feature summary: {e}")
            return {"error": str(e)}

#TODO: check if we need to change this    
    def run_ml_features_processing(self) -> Dict[str, Any]:
        """
        Run ML feature processing with sliding window
        """
        logger.info("Starting ML feature processing with sliding window...")
        
        try:
            # Create ML features
            ml_features_df = self.create_ml_features_sliding_window()
            
            # Get summary
            summary = self.get_ml_features_summary()
            
            results = {
                "ml_feature_created": True,
                "records_generated": ml_features_df.count(),
                "summary": summary,
                "processing_timestamp": datetime.now().isoformat()
            }
            
            logger.info("ML feature processing completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in ML feature processing: {e}")
            return {
                "ml_feature_created": False,
                "error": str(e),
                "processing_timestamp": datetime.now().isoformat()
            }

#TODO: check if we need to change this
    # Legacy methods for compatibility (simplified)
    def run_all_gold_processing(self, batch_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Legacy method - now just runs ML features processing
        """
        return self.run_ml_features_processing()
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Legacy method - returns ML features summary
        """
        return self.get_ml_features_summary() 