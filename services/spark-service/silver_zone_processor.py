#!/usr/bin/env python3
"""
Silver Zone Processor - Data Validation and Cleaning
Processes Bronze zone data into validated, cleaned datasets
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

class SilverZoneProcessor:
    """
    Processes Bronze zone data into validated Silver zone datasets
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_path = "s3a://bronze/"
        self.silver_path = "s3a://silver/"
    
    def process_sensor_data(self, batch_date: Optional[str] = None) -> DataFrame:
        """
        Silver processing for IoT sensor data
        - Data validation and cleaning
        - Anomaly detection
        - Schema enforcement
        """
        logger.info("Processing sensor data for Silver zone...")
        
        # Read Bronze sensor data from new temporal structure (specific path pattern)
        bronze_df = self.spark.read.json(f"{self.bronze_path}iot/year=*/month=*/day=*/hour=*/*.json")
        
        # Filter by date if specified
        if batch_date:
            bronze_df = bronze_df.filter(
                to_date(col("timestamp")) == batch_date
            )
        
        # Data validation and cleaning
        silver_df = bronze_df \
            .filter(
                # Remove records with critical null values
                col("temperature").isNotNull() & 
                col("humidity").isNotNull() & 
                col("soil_ph").isNotNull() &
                col("field_id").isNotNull() &
                col("timestamp").isNotNull()
            ) \
            .withColumn("timestamp_parsed", 
                       to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")) \
            .withColumn("timestamp_parsed", 
                       when(col("timestamp_parsed").isNull(),
                            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
                       .otherwise(col("timestamp_parsed"))) \
            .filter(col("timestamp_parsed").isNotNull()) \
            .withColumn("date", to_date(col("timestamp_parsed"))) \
            .withColumn("hour", hour(col("timestamp_parsed"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp_parsed"))) \
            .withColumn("month", month(col("timestamp_parsed"))) \
            .withColumn("year", year(col("timestamp_parsed"))) \
            .withColumn(
                # Temperature validation and anomaly detection
                "temperature_valid",
                when((col("temperature") >= -20) & (col("temperature") <= 60), True)
                .otherwise(False)
            ) \
            .withColumn(
                "temperature_anomaly",
                when((col("temperature") < -10) | (col("temperature") > 50), True)
                .otherwise(False)
            ) \
            .withColumn(
                # Humidity validation and anomaly detection
                "humidity_valid", 
                when((col("humidity") >= 0) & (col("humidity") <= 100), True)
                .otherwise(False)
            ) \
            .withColumn(
                "humidity_anomaly",
                when((col("humidity") < 10) | (col("humidity") > 95), True)
                .otherwise(False)
            ) \
            .withColumn(
                # pH validation and anomaly detection
                "ph_valid",
                when((col("soil_ph") >= 3.0) & (col("soil_ph") <= 9.0), True)
                .otherwise(False)
            ) \
            .withColumn(
                "ph_anomaly", 
                when((col("soil_ph") < 4.0) | (col("soil_ph") > 8.5), True)
                .otherwise(False)
            ) \
            .withColumn(
                # Overall data quality flags
                "has_anomaly",
                col("temperature_anomaly") | col("humidity_anomaly") | col("ph_anomaly")
            ) \
            .withColumn(
                "data_quality_score",
                (col("temperature_valid").cast("int") + 
                 col("humidity_valid").cast("int") + 
                 col("ph_valid").cast("int")) / 3.0
            ) \
            .filter(
                # Keep only records with valid basic ranges
                col("temperature_valid") & col("humidity_valid") & col("ph_valid")
            )
        
        # Add statistical features using window functions
        window_spec = Window.partitionBy("field_id").orderBy("timestamp_parsed") \
                           .rowsBetween(-5, 0)  # Rolling window of last 6 readings
                           # TODO: Review this approach - in the silver layer we should collect all cleaned data without windowing
        
        silver_df = silver_df \
            .withColumn("temp_rolling_avg", avg("temperature").over(window_spec)) \
            .withColumn("temp_rolling_std", stddev("temperature").over(window_spec)) \
            .withColumn("humidity_rolling_avg", avg("humidity").over(window_spec)) \
            .withColumn("ph_rolling_avg", avg("soil_ph").over(window_spec)) \
            .withColumn(
                "temp_deviation_from_avg",
                abs(col("temperature") - col("temp_rolling_avg"))
            ) \
            .withColumn(
                "humidity_deviation_from_avg", 
                abs(col("humidity") - col("humidity_rolling_avg"))
            )
        
        # Write to Silver zone as Parquet
        # TODO: Review this approach - in the silver layer we should collect all cleaned data and not overwrite with just the last 6 readings
        silver_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("date", "field_id") \
            .option("compression", "snappy") \
            .save(f"{self.silver_path}iot/")
        
        logger.info(f"Processed {silver_df.count()} sensor records to Silver zone")
        return silver_df
    
    def process_weather_data(self, batch_date: Optional[str] = None) -> DataFrame:
        """
        Silver processing for weather data
        - Data validation and cleaning
        - Geospatial validation
        - Save as Parquet
        """
        logger.info("Processing weather data for Silver zone...")
        
        # Read Bronze weather data from new temporal structure (specific path pattern)
        bronze_df = self.spark.read.json(f"{self.bronze_path}weather/year=*/month=*/day=*/hour=*/*.json")
        
        # Filter by date if specified
        if batch_date:
            bronze_df = bronze_df.filter(
                to_date(col("timestamp")) == batch_date
            )
        
        # Data validation and cleaning
        silver_df = bronze_df \
            .filter(
                # Remove records with critical null values
                col("temp_c").isNotNull() & 
                col("humidity").isNotNull() &
                col("location").isNotNull() &
                col("timestamp").isNotNull()
            ) \
            .withColumn("timestamp_parsed",
                       to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")) \
            .withColumn("timestamp_parsed",
                       when(col("timestamp_parsed").isNull(),
                            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
                       .otherwise(col("timestamp_parsed"))) \
            .filter(col("timestamp_parsed").isNotNull()) \
            .withColumn("date", to_date(col("timestamp_parsed"))) \
            .withColumn("hour", hour(col("timestamp_parsed"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp_parsed"))) \
            .withColumn("month", month(col("timestamp_parsed"))) \
            .withColumn("year", year(col("timestamp_parsed"))) \
            .withColumn(
                # Temperature validation
                "temp_valid",
                when((col("temp_c") >= -50) & (col("temp_c") <= 60), True)
                .otherwise(False)
            ) \
            .withColumn(
                "temp_extreme",
                when((col("temp_c") < 0) | (col("temp_c") > 40), True)
                .otherwise(False)
            ) \
            .withColumn(
                # Humidity validation
                "humidity_valid",
                when((col("humidity") >= 0) & (col("humidity") <= 100), True)
                .otherwise(False)
            ) \
            .withColumn(
                "humidity_extreme",
                when((col("humidity") < 20) | (col("humidity") > 90), True)
                .otherwise(False)
            ) \
            .withColumn(
                # Coordinates validation
                "coordinates_valid",
                when(
                    col("lat").isNotNull() & col("lon").isNotNull() &
                    (col("lat") >= -90) & (col("lat") <= 90) &
                    (col("lon") >= -180) & (col("lon") <= 180), True
                ).otherwise(False)
            ) \
            .withColumn(
                # Wind speed validation
                "wind_valid",
                when(
                    col("wind_kph").isNull() | 
                    ((col("wind_kph") >= 0) & (col("wind_kph") <= 300)), True
                ).otherwise(False)
            ) \
            .withColumn(
                "wind_extreme",
                when(col("wind_kph") > 50, True).otherwise(False)
            ) \
            .withColumn(
                # UV index validation
                "uv_valid", 
                when(
                    col("uv").isNull() |
                    ((col("uv") >= 0) & (col("uv") <= 15)), True
                ).otherwise(False)
            ) \
            .withColumn(
                "uv_extreme",
                when(col("uv") > 8, True).otherwise(False)
            ) \
            .withColumn(
                # Weather severity score
                "weather_severity_score",
                (col("temp_extreme").cast("int") + 
                 col("humidity_extreme").cast("int") + 
                 col("wind_extreme").cast("int") + 
                 col("uv_extreme").cast("int"))
            ) \
            .withColumn(
                # Data quality assessment
                "data_quality_score",
                (col("temp_valid").cast("int") + 
                 col("humidity_valid").cast("int") + 
                 col("coordinates_valid").cast("int") + 
                 col("wind_valid").cast("int") + 
                 col("uv_valid").cast("int")) / 5.0
            ) \
            .filter(
                # Keep only records with basic validation
                col("temp_valid") & col("humidity_valid") & col("coordinates_valid")
            )
        
        # Add weather pattern analysis
        # Window for rolling averages
        rolling_window_spec = Window.partitionBy("location").orderBy("timestamp_parsed") \
                                   .rowsBetween(-2, 0)  # 3-hour rolling window
        
        # Window for lag functions (no frame specification)
        lag_window_spec = Window.partitionBy("location").orderBy("timestamp_parsed")
        
        silver_df = silver_df \
            .withColumn("temp_trend", 
                       col("temp_c") - lag("temp_c", 1).over(lag_window_spec)) \
            .withColumn("humidity_trend",
                       col("humidity") - lag("humidity", 1).over(lag_window_spec)) \
            .withColumn("pressure_trend", 
                       when(col("wind_kph").isNotNull(),
                            col("wind_kph") - lag("wind_kph", 1).over(lag_window_spec))
                       .otherwise(0)) \
            .withColumn("temp_rolling_avg", avg("temp_c").over(rolling_window_spec)) \
            .withColumn("humidity_rolling_avg", avg("humidity").over(rolling_window_spec))
        
        # Categorize weather conditions
        silver_df = silver_df \
            .withColumn(
                "weather_category",
                when(col("condition").rlike("(?i)rain|shower|drizzle"), "rainy")
                .when(col("condition").rlike("(?i)cloud|overcast"), "cloudy")
                .when(col("condition").rlike("(?i)sun|clear|bright"), "sunny")
                .when(col("condition").rlike("(?i)snow|blizzard"), "snowy")
                .when(col("condition").rlike("(?i)fog|mist"), "foggy")
                .otherwise("other")
            )
        
        # Write to Silver zone as Parquet
        silver_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("date", "location") \
            .option("compression", "snappy") \
            .save(f"{self.silver_path}weather/")
        
        logger.info(f"Processed {silver_df.count()} weather records to Silver zone")
        return silver_df
    
    def process_satellite_data(self, batch_date: Optional[str] = None) -> DataFrame:
        """
        Silver processing for satellite data
        - Metadata validation
        - Image quality assessment
        - Geospatial indexing
        """
        logger.info("Processing satellite data for Silver zone...")
        
        # Read Bronze satellite data from new temporal structure (specific path pattern)
        bronze_df = self.spark.read.json(f"{self.bronze_path}satellite/year=*/month=*/day=*/hour=*/*.json")
        
        # Filter by date if specified
        if batch_date:
            bronze_df = bronze_df.filter(
                to_date(col("timestamp")) == batch_date
            )
        
        # Data validation and cleaning
        silver_df = bronze_df \
            .filter(
                # Remove records with critical null values
                col("timestamp").isNotNull() &
                col("image_path").isNotNull() &
                col("image_size_bytes").isNotNull()
            ) \
            .withColumn("timestamp_parsed",
                       to_timestamp(col("timestamp"))) \
            .filter(col("timestamp_parsed").isNotNull()) \
            .withColumn("date", to_date(col("timestamp_parsed"))) \
            .withColumn("hour", hour(col("timestamp_parsed"))) \
            .withColumn(
                # Validate bounding box coordinates
                "bbox_valid",
                when(
                    col("bbox_min_lon").isNotNull() & col("bbox_min_lat").isNotNull() &
                    col("bbox_max_lon").isNotNull() & col("bbox_max_lat").isNotNull() &
                    (col("bbox_min_lon") >= -180) & (col("bbox_min_lon") <= 180) &
                    (col("bbox_min_lat") >= -90) & (col("bbox_min_lat") <= 90) &
                    (col("bbox_max_lon") >= -180) & (col("bbox_max_lon") <= 180) &
                    (col("bbox_max_lat") >= -90) & (col("bbox_max_lat") <= 90) &
                    (col("bbox_min_lon") < col("bbox_max_lon")) &
                    (col("bbox_min_lat") < col("bbox_max_lat")), True
                ).otherwise(False)
            ) \
            .withColumn(
                # Calculate area covered (approximate)
                "area_km2",
                when(col("bbox_valid"),
                     (col("bbox_max_lon") - col("bbox_min_lon")) * 
                     (col("bbox_max_lat") - col("bbox_min_lat")) * 111.32 * 111.32)
                .otherwise(0.0)
            ) \
            .withColumn(
                # Image size validation
                "image_size_valid",
                when((col("image_size_bytes") > 1000) & (col("image_size_bytes") < 50000000), True)
                .otherwise(False)
            ) \
            .withColumn(
                # Image size category
                "image_size_category",
                when(col("image_size_bytes") < 100000, "small")
                .when(col("image_size_bytes") < 1000000, "medium")
                .when(col("image_size_bytes") < 5000000, "large")
                .otherwise("very_large")
            ) \
            .withColumn(
                # Calculate center coordinates
                "center_lon",
                when(col("bbox_valid"), 
                     (col("bbox_min_lon") + col("bbox_max_lon")) / 2)
                .otherwise(lit(None))
            ) \
            .withColumn(
                "center_lat",
                when(col("bbox_valid"),
                     (col("bbox_min_lat") + col("bbox_max_lat")) / 2)
                .otherwise(lit(None))
            ) \
            .withColumn(
                # Data quality score
                "data_quality_score",
                (col("bbox_valid").cast("int") + 
                 col("image_size_valid").cast("int")) / 2.0
            ) \
            .filter(
                # Keep only records with valid metadata
                col("bbox_valid") & col("image_size_valid")
            )
        
        # Add temporal analysis
        window_spec = Window.partitionBy("center_lon", "center_lat") \
                           .orderBy("timestamp_parsed")
        
        silver_df = silver_df \
            .withColumn("image_sequence_number", row_number().over(window_spec)) \
            .withColumn("time_since_last_image",
                       col("timestamp_parsed").cast("long") - 
                       lag("timestamp_parsed").over(window_spec).cast("long"))
        
        # Write to Silver zone as Parquet
        silver_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("date") \
            .save(f"{self.silver_path}satellite/")
        
        logger.info(f"Processed {silver_df.count()} satellite records to Silver zone")
        return silver_df
    
    def run_all_silver_processing(self, batch_date: Optional[str] = None):
        """
        Run all Silver zone processing for a specific date or latest data
        """
        logger.info(f"Starting Silver zone processing for date: {batch_date or 'latest'}")
        
        try:
            # Process sensor data
            sensor_df = self.process_sensor_data(batch_date)
            
            # Process weather data
            weather_df = self.process_weather_data(batch_date)
            
            # Process satellite data
            satellite_df = self.process_satellite_data(batch_date)
            
            logger.info("Silver zone processing completed successfully")
            
            return {
                "sensor_count": sensor_df.count(),
                "weather_count": weather_df.count(), 
                "satellite_count": satellite_df.count()
            }
            
        except Exception as e:
            logger.error(f"Error in Silver zone processing: {e}")
            raise
    
    def get_data_quality_report(self, batch_date: Optional[str] = None) -> dict:
        """
        Generate data quality report for Silver zone data
        """
        logger.info("Generating Silver zone data quality report...")
        
        report = {}
        
        try:
            # Sensor data quality
            sensor_df = self.spark.read.parquet(f"{self.silver_path}iot/")
            if batch_date:
                sensor_df = sensor_df.filter(col("date") == batch_date)
            
            sensor_quality = sensor_df.agg(
                avg("data_quality_score").alias("avg_quality"),
                sum(when(col("has_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
                count("*").alias("total_records")
            ).collect()[0]
            
            report["sensor"] = {
                "avg_quality_score": float(sensor_quality.avg_quality or 0),
                "anomaly_count": int(sensor_quality.anomaly_count or 0),
                "total_records": int(sensor_quality.total_records or 0),
                "anomaly_rate": float(sensor_quality.anomaly_count or 0) / max(1, sensor_quality.total_records or 1)
            }
            
            # Weather data quality
            weather_df = self.spark.read.parquet(f"{self.silver_path}weather/")
            if batch_date:
                weather_df = weather_df.filter(col("date") == batch_date)
            
            weather_quality = weather_df.agg(
                avg("data_quality_score").alias("avg_quality"),
                avg("weather_severity_score").alias("avg_severity"),
                count("*").alias("total_records")
            ).collect()[0]
            
            report["weather"] = {
                "avg_quality_score": float(weather_quality.avg_quality or 0),
                "avg_severity_score": float(weather_quality.avg_severity or 0),
                "total_records": int(weather_quality.total_records or 0)
            }
            
            # Satellite data quality
            satellite_df = self.spark.read.parquet(f"{self.silver_path}satellite/")
            if batch_date:
                satellite_df = satellite_df.filter(col("date") == batch_date)
            
            satellite_quality = satellite_df.agg(
                avg("data_quality_score").alias("avg_quality"),
                avg("area_km2").alias("avg_area"),
                count("*").alias("total_records")
            ).collect()[0]
            
            report["satellite"] = {
                "avg_quality_score": float(satellite_quality.avg_quality or 0),
                "avg_area_km2": float(satellite_quality.avg_area or 0),
                "total_records": int(satellite_quality.total_records or 0)
            }
            
            logger.info("Data quality report generated successfully")
            return report
            
        except Exception as e:
            logger.error(f"Error generating data quality report: {e}")
            return {"error": str(e)} 