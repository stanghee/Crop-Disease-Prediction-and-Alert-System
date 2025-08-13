#!/usr/bin/env python3
"""
Gold Zone Processor - ML Features with Sliding Window
Processes Silver zone data into ML-ready features:
- Sliding window aggregations (10 minutes)
- Location-based sensor-weather join
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
        
        Big Data Optimization:
        - Uses repartition() instead of coalesce() for better scalability
        - For small datasets (3 records): repartition(1) is optimal
        - For larger datasets: Spark can auto-optimize partition count
        - Maintains single file output while being Big Data ready
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
                    min("timestamp_parsed").alias("sensor_first_reading"),
                    
                    # Temporal features from silver
                    mode("month").alias("sensor_dominant_month")
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
                    
                    # Precipitation metrics (added from silver)
                    avg("precip_mm").alias("weather_avg_precipitation"),
                    stddev("precip_mm").alias("weather_std_precipitation"),
                    min("precip_mm").alias("weather_min_precipitation"),
                    max("precip_mm").alias("weather_max_precipitation"),
                    sum("precip_mm").alias("weather_total_precipitation"),
                    
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
                ) \
                .withColumn(
                    "weather_precip_range", col("weather_max_precipitation") - col("weather_min_precipitation")
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
                ) \
                .withColumn(
                    # Temporal cyclic features for month (sin and cos) - using sensor dominant month
                    "temporal_feature_ml_sin", sin(2 * pi() * (col("sensor_dominant_month") - 1) / 12)
                ) \
                .withColumn(
                    "temporal_feature_ml_cos", cos(2 * pi() * (col("sensor_dominant_month") - 1) / 12)
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
                    col("sensor_dominant_month"),
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
                    col("weather_avg_precipitation"),
                    col("weather_std_precipitation"),
                    col("weather_min_precipitation"),
                    col("weather_max_precipitation"),
                    col("weather_precip_range"),
                    col("weather_total_precipitation"),
                    col("weather_dominant_condition"),
                    col("weather_readings_count"),
                    col("temp_differential"),
                    col("humidity_differential"),
                    col("sensor_data_freshness_minutes"),
                    col("weather_data_freshness_minutes"),
                    col("temporal_feature_ml_sin"),
                    col("temporal_feature_ml_cos"),
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

                # For small datasets (3 records), repartition(1)
                # For larger datasets, Spark can optimize the partition count automatically
                record_count = final_ml_features.count()
                
                # Determine optimal partition count based on data size
                if record_count <= 10:
                    # Small datasets: single partition for efficiency
                    optimal_partitions = 1
                elif record_count <= 1000:
                    # Medium datasets: 1 partition per 1000 records (max 10 partitions)
                    optimal_partitions = min(record_count // 1000, 10)
                else:
                    # Large datasets: let Spark decide (adaptive partitioning)
                    optimal_partitions = None
                
                if optimal_partitions is not None:
                    final_ml_features.repartition(optimal_partitions).write \
                        .format("parquet") \
                        .option("compression", "snappy") \
                        .mode("overwrite") \
                        .save(output_dir)
                else:
                    # Let Spark use adaptive partitioning for large datasets
                    final_ml_features.write \
                        .format("parquet") \
                        .option("compression", "snappy") \
                        .mode("overwrite") \
                        .save(output_dir)

                # Find the part-*.parquet file and rename it
                import os
                from py4j.java_gateway import java_import
                java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")
                fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
                tmp_path = self.spark._jvm.org.apache.hadoop.fs.Path(output_dir)
                dest_path = self.spark._jvm.org.apache.hadoop.fs.Path(final_path)
                files = fs.listStatus(tmp_path)
                
                # Handle file renaming based on partition count
                parquet_files = [f for f in files if f.getPath().getName().startswith("part-") and f.getPath().getName().endswith(".parquet")]
                
                if len(parquet_files) == 1:
                    # Single file: rename to final filename (small dataset case)
                    fs.rename(parquet_files[0].getPath(), dest_path)
                    logger.info(f"Single file created: {final_filename}")
                else:
                    # Multiple files: create directory structure for large datasets
                    # Remove timestamp suffix for directory name
                    base_dir_name = f"ml_feature_{current_timestamp_str}"
                    final_dir_path = f"{self.gold_path}ml_feature/{base_dir_name}/"
                    final_dir = self.spark._jvm.org.apache.hadoop.fs.Path(final_dir_path)
                    
                    # Create directory and move all files
                    if not fs.exists(final_dir):
                        fs.mkdirs(final_dir)
                    
                    for i, parquet_file in enumerate(parquet_files):
                        part_name = parquet_file.getPath().getName()
                        new_name = f"part_{i:05d}.parquet"
                        new_path = self.spark._jvm.org.apache.hadoop.fs.Path(f"{final_dir_path}{new_name}")
                        fs.rename(parquet_file.getPath(), new_path)
                    
                    logger.info(f"Multiple files created in directory: {base_dir_name} ({len(parquet_files)} partitions)")
                
                # Remove temporary directory
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
                if record_count <= 10:
                    logger.info(f"Big Data optimized: Single file created for small dataset ({record_count} records)")
                elif record_count <= 1000:
                    optimal_partitions = min(record_count // 1000, 10)
                    logger.info(f"Big Data optimized: {optimal_partitions} partitions for medium dataset ({record_count} records)")
                else:
                    logger.info(f"Big Data optimized: Adaptive partitioning for large dataset ({record_count} records)")
            
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