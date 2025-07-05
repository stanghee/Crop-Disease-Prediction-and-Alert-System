#!/usr/bin/env python3
"""
Gold Zone Processor - Dashboard Metrics, Business KPIs and ML Features
Processes Silver zone data into organized Gold zone structure:
- sensor_metrics/ - Sensor dashboard metrics
- weather_metrics/ - Weather dashboard metrics  
- satellite_metrics/ - Satellite dashboard metrics
- ml_features/ - Machine Learning features
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

class GoldZoneProcessor:
    """
    Processes Silver zone data into organized Gold zone structure:
    - Dashboard metrics per data source
    - ML features for machine learning pipelines
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_path = "s3a://silver/"
        self.gold_path = "s3a://gold/"
    
    # =================== SENSOR METRICS ===================
    
    def create_sensor_recent_metrics(self) -> DataFrame:
        """
        Create recent sensor metrics for dashboard (last 30 minutes)
        - Current status per field
        - Recent readings and data validity
        - Performance indicators
        """
        logger.info("Creating sensor recent metrics for Gold zone...")
        
        # Calculate timestamp for 30 minutes ago
        thirty_minutes_ago = datetime.now() - timedelta(minutes=30)
        
        # Read from Silver zone - last 30 minutes
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/") \
            .filter(col("timestamp_parsed") >= thirty_minutes_ago)
        
        # Latest reading per field
        window_latest = Window.partitionBy("field_id").orderBy(desc("timestamp_parsed"))
        
        # 30-minute aggregations per field
        recent_metrics = silver_df \
            .groupBy("field_id") \
            .agg(
                # Current values (latest)
                first("temperature").alias("current_temperature"),
                first("humidity").alias("current_humidity"),
                first("soil_ph").alias("current_soil_ph"),
                first("timestamp_parsed").alias("last_reading_time"),
                
                # Statistical aggregations
                avg("temperature").alias("avg_temperature_30min"),
                stddev("temperature").alias("std_temperature_30min"),
                min("temperature").alias("min_temperature_30min"),
                max("temperature").alias("max_temperature_30min"),
                
                avg("humidity").alias("avg_humidity_30min"),
                stddev("humidity").alias("std_humidity_30min"),
                min("humidity").alias("min_humidity_30min"),
                max("humidity").alias("max_humidity_30min"),
                
                avg("soil_ph").alias("avg_soil_ph_30min"),
                stddev("soil_ph").alias("std_soil_ph_30min"),
                
                # Data validity
                avg(col("temperature_valid").cast("int")).alias("temperature_valid_rate_30min"),
                avg(col("humidity_valid").cast("int")).alias("humidity_valid_rate_30min"),
                avg(col("ph_valid").cast("int")).alias("ph_valid_rate_30min"),
                
                # Data quality tracking (using invalid data as proxy for anomalies)
                sum(when(~col("temperature_valid"), 1).otherwise(0)).alias("temp_invalid_count_30min"),
                sum(when(~col("humidity_valid"), 1).otherwise(0)).alias("humidity_invalid_count_30min"),
                sum(when(~col("ph_valid"), 1).otherwise(0)).alias("ph_invalid_count_30min"),
                count("*").alias("readings_count_30min")
            ) \
            .withColumn(
                "temp_range_30min", col("max_temperature_30min") - col("min_temperature_30min")
            ) \
            .withColumn(
                "humidity_range_30min", col("max_humidity_30min") - col("min_humidity_30min")
            ) \
            .withColumn(
                "anomaly_count_30min", col("temp_invalid_count_30min") + col("humidity_invalid_count_30min") + col("ph_invalid_count_30min")
            ) \
            .withColumn(
                "anomaly_rate_30min", col("anomaly_count_30min") / col("readings_count_30min")
            ) \
            .withColumn(
                # Status indicators (based on data quality)
                "status",
                when((col("temperature_valid_rate_30min") < 0.8) | (col("humidity_valid_rate_30min") < 0.8) | (col("ph_valid_rate_30min") < 0.8), "DATA_ISSUE")
                .when(col("anomaly_rate_30min") > 0.1, "HIGH_RISK")
                .when(col("anomaly_rate_30min") > 0.05, "MEDIUM_RISK")
                .otherwise("HEALTHY")
            ) \
            .withColumn(
                "data_freshness_minutes",
                (unix_timestamp(current_timestamp()) - unix_timestamp(col("last_reading_time"))) / 60
            ) \
            .withColumn(
                "is_online",
                when(col("data_freshness_minutes") <= 5, True).otherwise(False)
            ) \
            .withColumn("created_at", current_timestamp())
        
        # Write to organized Gold zone path
        recent_metrics.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}sensor_metrics/recent/")
        
        logger.info(f"Created {recent_metrics.count()} sensor recent metric records")
        return recent_metrics
    
    def create_sensor_hourly_aggregations(self) -> DataFrame:
        """
        Create hourly sensor aggregations for trend analysis
        - Hourly statistics per field
        - Trend indicators
        - Performance tracking
        """
        logger.info("Creating sensor hourly aggregations for Gold zone...")
        
        # Read from Silver zone - last 24 hours
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/") \
            .filter(col("timestamp_parsed") >= twenty_four_hours_ago)
        
        # Hourly aggregations per field
        hourly_metrics = silver_df \
            .groupBy("field_id", "date", "hour") \
            .agg(
                # Temperature metrics
                avg("temperature").alias("avg_temperature"),
                stddev("temperature").alias("std_temperature"),
                min("temperature").alias("min_temperature"),
                max("temperature").alias("max_temperature"),
                
                # Humidity metrics
                avg("humidity").alias("avg_humidity"),
                stddev("humidity").alias("std_humidity"),
                min("humidity").alias("min_humidity"),
                max("humidity").alias("max_humidity"),
                
                # Soil pH metrics
                avg("soil_ph").alias("avg_soil_ph"),
                stddev("soil_ph").alias("std_soil_ph"),
                
                # Data validity
                avg(col("temperature_valid").cast("int")).alias("temperature_valid_rate"),
                avg(col("humidity_valid").cast("int")).alias("humidity_valid_rate"),
                avg(col("ph_valid").cast("int")).alias("ph_valid_rate"),
                
                # Data quality tracking (using invalid data as proxy for anomalies)
                sum(when(~col("temperature_valid"), 1).otherwise(0)).alias("temp_invalid_count"),
                sum(when(~col("humidity_valid"), 1).otherwise(0)).alias("humidity_invalid_count"),
                sum(when(~col("ph_valid"), 1).otherwise(0)).alias("ph_invalid_count"),
                count("*").alias("readings_count")
            ) \
            .withColumn(
                "temp_range", col("max_temperature") - col("min_temperature")
            ) \
            .withColumn(
                "humidity_range", col("max_humidity") - col("min_humidity")
            ) \
            .withColumn(
                "anomaly_count", col("temp_invalid_count") + col("humidity_invalid_count") + col("ph_invalid_count")
            ) \
            .withColumn(
                "anomaly_rate", col("anomaly_count") / col("readings_count")
            ) \
            .withColumn(
                # Hourly data quality assessment
                "hourly_quality_status",
                when((col("temperature_valid_rate") < 0.8) | (col("humidity_valid_rate") < 0.8) | (col("ph_valid_rate") < 0.8), "DATA_ISSUE")
                .otherwise("VALID")
            ) \
            .withColumn("created_at", current_timestamp())
        
        # Write to organized Gold zone path
        hourly_metrics.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}sensor_metrics/hourly/")
        
        logger.info(f"Created {hourly_metrics.count()} sensor hourly aggregation records")
        return hourly_metrics
    
    def create_sensor_alert_summary(self) -> DataFrame:
        """
        Create sensor alert summary (data quality and anomaly alerts)
        """
        logger.info("Creating sensor alert summary for Gold zone...")
        
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/")
        
        alert_summary = silver_df \
            .groupBy("field_id") \
            .agg(
                count(when(~col("temperature_valid"), True)).alias("invalid_temperature_count"),
                count(when(~col("humidity_valid"), True)).alias("invalid_humidity_count"),
                count(when(~col("ph_valid"), True)).alias("invalid_ph_count"),
                count("*").alias("total_readings")
            ) \
            .withColumn(
                "total_anomalies", col("invalid_temperature_count") + col("invalid_humidity_count") + col("invalid_ph_count")
            ) \
            .withColumn(
                "invalid_rate",
                (col("invalid_temperature_count") + col("invalid_humidity_count") + col("invalid_ph_count")) / (col("total_readings") * 3)
            ) \
            .withColumn(
                "anomaly_rate",
                col("total_anomalies") / col("total_readings")
            ) \
            .withColumn(
                "alert_status",
                when((col("invalid_rate") > 0.2) | (col("anomaly_rate") > 0.1), "HIGH")
                .when((col("invalid_rate") > 0.05) | (col("anomaly_rate") > 0.05), "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn("created_at", current_timestamp())
        
        alert_summary.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}sensor_metrics/alerts/")
        
        logger.info(f"Created {alert_summary.count()} sensor alert summary records")
        return alert_summary
    
    def create_sensor_performance_ranking(self) -> DataFrame:
        """
        Create sensor field performance ranking (based on data validity and anomaly rates)
        """
        logger.info("Creating sensor field performance ranking for Gold zone...")
        
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/")
        
        performance_ranking = silver_df \
            .groupBy("field_id") \
            .agg(
                avg(col("temperature_valid").cast("int")).alias("temperature_valid_rate"),
                avg(col("humidity_valid").cast("int")).alias("humidity_valid_rate"),
                avg(col("ph_valid").cast("int")).alias("ph_valid_rate"),
                count(when(~col("temperature_valid"), True)).alias("invalid_temperature_count"),
                count(when(~col("humidity_valid"), True)).alias("invalid_humidity_count"),
                count(when(~col("ph_valid"), True)).alias("invalid_ph_count"),
                count("*").alias("total_readings")
            ) \
            .withColumn(
                "total_anomalies", col("invalid_temperature_count") + col("invalid_humidity_count") + col("invalid_ph_count")
            ) \
            .withColumn(
                "anomaly_rate",
                col("total_anomalies") / col("total_readings")
            ) \
            .withColumn(
                "data_validity_score",
                (col("temperature_valid_rate") + col("humidity_valid_rate") + col("ph_valid_rate")) / 3 * 100
            ) \
            .withColumn(
                "anomaly_score",
                (1 - col("anomaly_rate")) * 100  # Lower anomaly rate = higher score
            ) \
            .withColumn(
                "overall_performance_score",
                (col("data_validity_score") * 0.7 + col("anomaly_score") * 0.3)  # Weighted average
            ) \
            .withColumn(
                "performance_grade",
                when(col("overall_performance_score") >= 90, "A")
                .when(col("overall_performance_score") >= 80, "B")
                .when(col("overall_performance_score") >= 70, "C")
                .when(col("overall_performance_score") >= 60, "D")
                .otherwise("F")
            ) \
            .withColumn(
                "performance_rank",
                rank().over(Window.orderBy(desc("overall_performance_score")))
            ) \
            .withColumn("created_at", current_timestamp())
        
        performance_ranking.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}sensor_metrics/performance/")
        
        logger.info(f"Created {performance_ranking.count()} sensor field performance ranking records")
        return performance_ranking
    
    def create_sensor_overview(self) -> DataFrame:
        """
        Create sensor dashboard overview metrics
        - System-wide statistics
        - Overall health indicators
        - Summary KPIs
        """
        logger.info("Creating sensor dashboard overview for Gold zone...")
        
        # Read performance ranking
        ranking_df = self.spark.read.format("delta").load(f"{self.gold_path}sensor_metrics/performance/")
        
        # Calculate overview metrics
        overview_stats = ranking_df.agg(
            count("*").alias("total_fields"),
            sum(when(col("performance_grade") == "A", 1).otherwise(0)).alias("grade_a_fields"),
            sum(when(col("performance_grade") == "B", 1).otherwise(0)).alias("grade_b_fields"),
            sum(when(col("performance_grade") == "C", 1).otherwise(0)).alias("grade_c_fields"),
            sum(when(col("performance_grade") == "D", 1).otherwise(0)).alias("grade_d_fields"),
            sum(when(col("performance_grade") == "F", 1).otherwise(0)).alias("grade_f_fields"),
            avg("overall_performance_score").alias("avg_performance_score"),
            avg("data_validity_score").alias("avg_data_validity_score"),
            avg("anomaly_score").alias("avg_anomaly_score"),
            avg("temperature_valid_rate").alias("avg_temperature_valid_rate"),
            avg("humidity_valid_rate").alias("avg_humidity_valid_rate"),
            avg("ph_valid_rate").alias("avg_ph_valid_rate")
        ) \
        .withColumn("created_at", current_timestamp())
        
        # Write to organized Gold zone path
        overview_stats.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}sensor_metrics/overview/")
        
        logger.info(f"Created sensor dashboard overview record")
        return overview_stats
    
    # =================== WEATHER METRICS ===================
    
    def create_weather_monthly_stats(self) -> DataFrame:
        """
        Create weather monthly statistics for dashboard
        - Monthly weather statistics per location (rolling 30 days)
        - Weather pattern analysis
        """
        logger.info("Creating weather monthly statistics for Gold zone...")
        
        # Read from Silver zone - last 30 days
        thirty_days_ago = datetime.now() - timedelta(days=30)
        
        try:
            silver_df = self.spark.read.parquet(f"{self.silver_path}weather/") \
                .filter(col("timestamp_parsed") >= thirty_days_ago)
            
            # Monthly statistics per location (rolling 30 days)
            monthly_weather = silver_df \
                .groupBy("location", "date") \
                .agg(
                    avg("temp_c").alias("avg_temperature"),
                    max("temp_c").alias("max_temperature"),
                    min("temp_c").alias("min_temperature"),
                    avg("humidity").alias("avg_humidity"),
                    max("humidity").alias("max_humidity"),
                    min("humidity").alias("min_humidity"),
                    avg("wind_kph").alias("avg_wind_speed"),
                    max("wind_kph").alias("max_wind_speed"),
                    avg("uv").alias("avg_uv_index"),
                    max("uv").alias("max_uv_index"),
                    mode("condition").alias("dominant_condition"),
                    count("*").alias("readings_count")
                ) \
                .withColumn(
                    "temperature_range", col("max_temperature") - col("min_temperature")
                ) \
                .withColumn(
                    "humidity_range", col("max_humidity") - col("min_humidity")
                ) \
                .withColumn("created_at", current_timestamp())
            
            # Write to organized Gold zone path
            monthly_weather.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(f"{self.gold_path}weather_metrics/monthly_stats/")
            
            logger.info(f"Created {monthly_weather.count()} weather monthly statistics records")
            return monthly_weather
            
        except Exception as e:
            logger.warning(f"Weather data not available for processing: {e}")
            # Return empty DataFrame with expected schema
            schema = StructType([
                StructField("location", StringType(), True),
                StructField("date", DateType(), True),
                StructField("avg_temperature", DoubleType(), True),
                StructField("created_at", TimestampType(), True)
            ])
            return self.spark.createDataFrame([], schema)
    
    # overview for weather metrics of all locations (we have suppose that we near locations)
    def create_weather_overview(self) -> DataFrame:
        """
        Create weather overview metrics
        """
        logger.info("Creating weather overview for Gold zone...")
        
        try:
            monthly_df = self.spark.read.format("delta").load(f"{self.gold_path}weather_metrics/monthly_stats/")
            
            # Calculate overview metrics
            overview_stats = monthly_df.agg(
                countDistinct("location").alias("total_locations"),
                avg("avg_temperature").alias("avg_temperature_overall"),
                avg("avg_humidity").alias("avg_humidity_overall"),
                avg("avg_wind_speed").alias("avg_wind_speed_overall"),
                avg("avg_uv_index").alias("avg_uv_index_overall"),
                count("*").alias("total_monthly_records")
            ) \
            .withColumn("created_at", current_timestamp())
            
            # Write to organized Gold zone path
            overview_stats.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(f"{self.gold_path}weather_metrics/overview/")
            
            logger.info(f"Created weather overview record")
            return overview_stats
            
        except Exception as e:
            logger.warning(f"Weather overview creation failed: {e}")
            # Return empty DataFrame
            schema = StructType([
                StructField("total_locations", LongType(), True),
                StructField("created_at", TimestampType(), True)
            ])
            return self.spark.createDataFrame([], schema)
    
    # =================== ML FEATURES ===================
    
    def create_sensor_ml_features(self) -> DataFrame:
        """
        Create sensor ML features for machine learning models
        - Daily aggregations per field with statistical features
        - Risk scoring and anomaly indicators
        """
        logger.info("Creating sensor ML features for Gold zone...")
        
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/")
        
        # ML Features - Daily aggregations with comprehensive statistics
        ml_features = silver_df \
            .groupBy("field_id", "date") \
            .agg(
                # Temperature features
                avg("temperature").alias("avg_temperature"),
                stddev("temperature").alias("std_temperature"),
                min("temperature").alias("min_temperature"),
                max("temperature").alias("max_temperature"),
                
                # Humidity features
                avg("humidity").alias("avg_humidity"),
                stddev("humidity").alias("std_humidity"),
                min("humidity").alias("min_humidity"),
                max("humidity").alias("max_humidity"),
                
                # Soil pH features
                avg("soil_ph").alias("avg_soil_ph"),
                stddev("soil_ph").alias("std_soil_ph"),
                min("soil_ph").alias("min_soil_ph"),
                max("soil_ph").alias("max_soil_ph"),
                
                # Data quality features
                avg(col("temperature_valid").cast("int")).alias("temperature_valid_rate"),
                avg(col("humidity_valid").cast("int")).alias("humidity_valid_rate"),
                avg(col("ph_valid").cast("int")).alias("ph_valid_rate"),
                
                # Data quality features (proxy for anomalies)
                count(when(~col("temperature_valid"), True)).alias("temperature_invalid_count"),
                count(when(~col("humidity_valid"), True)).alias("humidity_invalid_count"),
                count(when(~col("ph_valid"), True)).alias("ph_invalid_count"),
                count("*").alias("total_readings")
            ) \
            .withColumn(
                # Derived features
                "temperature_range", col("max_temperature") - col("min_temperature")
            ) \
            .withColumn(
                "humidity_range", col("max_humidity") - col("min_humidity")
            ) \
            .withColumn(
                "ph_range", col("max_soil_ph") - col("min_soil_ph")
            ) \
            .withColumn(
                "anomaly_count", col("temperature_invalid_count") + col("humidity_invalid_count") + col("ph_invalid_count")
            ) \
            .withColumn(
                "anomaly_rate", col("anomaly_count") / col("total_readings")
            ) \
            .withColumn(
                "data_quality_score", 
                (col("temperature_valid_rate") + col("humidity_valid_rate") + col("ph_valid_rate")) / 3
            ) \
            .withColumn(
                "risk_score", 
                when(col("anomaly_rate") > 0.1, "HIGH")
                .when(col("anomaly_rate") > 0.05, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn("created_at", current_timestamp())
        
        # Write to organized Gold zone path
        ml_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}ml_features/sensor/")
        
        logger.info(f"Created {ml_features.count()} sensor ML feature records")
        return ml_features
    
    def create_weather_ml_features(self) -> DataFrame:
        """
        Create weather ML features for machine learning models
        - Daily weather aggregations with ML-ready features
        """
        logger.info("Creating weather ML features for Gold zone...")
        
        try:
            silver_df = self.spark.read.parquet(f"{self.silver_path}weather/")
            
            # Weather ML Features - Daily aggregations
            weather_features = silver_df \
                .groupBy("location", "date") \
                .agg(
                    # Temperature features
                    avg("temp_c").alias("avg_temperature"),
                    stddev("temp_c").alias("std_temperature"),
                    max("temp_c").alias("max_temperature"),
                    min("temp_c").alias("min_temperature"),
                    
                    # Humidity features
                    avg("humidity").alias("avg_humidity"),
                    stddev("humidity").alias("std_humidity"),
                    max("humidity").alias("max_humidity"),
                    min("humidity").alias("min_humidity"),
                    
                    # Wind features
                    avg("wind_kph").alias("avg_wind_speed"),
                    stddev("wind_kph").alias("std_wind_speed"),
                    max("wind_kph").alias("max_wind_speed"),
                    
                    # UV features
                    avg("uv").alias("avg_uv_index"),
                    max("uv").alias("max_uv_index"),
                    
                    # Condition features
                    mode("condition").alias("dominant_condition"),
                    count("*").alias("readings_count")
                ) \
                .withColumn(
                    # Derived features for ML
                    "temperature_range", col("max_temperature") - col("min_temperature")
                ) \
                .withColumn(
                    "humidity_range", col("max_humidity") - col("min_humidity")
                ) \
                .withColumn(
                    # Weather stability indicators
                    "temperature_stability",
                    when(col("std_temperature") < 2, "STABLE")
                    .when(col("std_temperature") < 5, "MODERATE")
                    .otherwise("UNSTABLE")
                ) \
                .withColumn(
                    "wind_category",
                    when(col("avg_wind_speed") < 10, "CALM")
                    .when(col("avg_wind_speed") < 20, "MODERATE")
                    .otherwise("STRONG")
                ) \
                .withColumn("created_at", current_timestamp())
            
            # Write to organized Gold zone path
            weather_features.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(f"{self.gold_path}ml_features/weather/")
            
            logger.info(f"Created {weather_features.count()} weather ML feature records")
            return weather_features
            
        except Exception as e:
            logger.warning(f"Weather ML features creation failed: {e}")
            # Return empty DataFrame with expected schema
            schema = StructType([
                StructField("location", StringType(), True),
                StructField("date", DateType(), True),
                StructField("avg_temperature", DoubleType(), True),
                StructField("created_at", TimestampType(), True)
            ])
            return self.spark.createDataFrame([], schema)
    
    def create_integrated_ml_features(self) -> DataFrame:
        """
        Create integrated ML features combining sensor and weather data
        - Join sensor and weather features by date
        - Create correlation features
        """
        logger.info("Creating integrated ML features for Gold zone...")
        
        try:
            # Read ML features
            sensor_features = self.spark.read.format("delta").load(f"{self.gold_path}ml_features/sensor/")
            weather_features = self.spark.read.format("delta").load(f"{self.gold_path}ml_features/weather/")
            
            # Join sensor and weather data by date (assuming single location for now)
            integrated_df = sensor_features.alias("s") \
                .join(weather_features.alias("w"), 
                      col("s.date") == col("w.date"), "left") \
                .select(
                    col("s.*"),
                    # Weather features
                    col("w.avg_temperature").alias("weather_avg_temp"),
                    col("w.avg_humidity").alias("weather_avg_humidity"),
                    col("w.avg_wind_speed"),
                    col("w.avg_uv_index"),
                    col("w.temperature_range").alias("weather_temp_range"),
                    col("w.humidity_range").alias("weather_humidity_range"),
                    col("w.dominant_condition"),
                    col("w.temperature_stability"),
                    col("w.wind_category")
                ) \
                .withColumn(
                    # Correlation features
                    "temp_differential", 
                    abs(col("avg_temperature") - col("weather_avg_temp"))
                ) \
                .withColumn(
                    "humidity_differential",
                    abs(col("avg_humidity") - col("weather_avg_humidity"))
                ) \
                .withColumn(
                    # Environmental stress indicators
                    "environmental_stress_score",
                    when((col("temp_differential") > 10) | (col("humidity_differential") > 20), "HIGH")
                    .when((col("temp_differential") > 5) | (col("humidity_differential") > 10), "MEDIUM")
                    .otherwise("LOW")
                ) \
                .withColumn(
                    # Combined risk assessment
                    "combined_risk_score",
                    when((col("risk_score") == "HIGH") | (col("environmental_stress_score") == "HIGH"), "HIGH")
                    .when((col("risk_score") == "MEDIUM") | (col("environmental_stress_score") == "MEDIUM"), "MEDIUM")
                    .otherwise("LOW")
                )
            
            # Write to organized Gold zone path
            integrated_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(f"{self.gold_path}ml_features/integrated/")
            
            logger.info(f"Created {integrated_df.count()} integrated ML feature records")
            return integrated_df
            
        except Exception as e:
            logger.warning(f"Integrated ML features creation failed: {e}")
            # Return empty DataFrame with sensor features only
            return self.spark.read.format("delta").load(f"{self.gold_path}ml_features/sensor/")
    
    # =================== ORCHESTRATION METHODS ===================
    
    def run_all_sensor_processing(self) -> Dict[str, Any]:
        """
        Run all sensor metrics processing
        """
        logger.info("Starting sensor metrics processing...")
        
        results = {}
        
        try:
            # Sensor dashboard metrics
            recent_df = self.create_sensor_recent_metrics()
            results["sensor_recent_count"] = recent_df.count()
            
            hourly_df = self.create_sensor_hourly_aggregations()
            results["sensor_hourly_count"] = hourly_df.count()
            
            alert_df = self.create_sensor_alert_summary()
            results["sensor_alerts_count"] = alert_df.count()
            
            ranking_df = self.create_sensor_performance_ranking()
            results["sensor_performance_count"] = ranking_df.count()
            
            overview_df = self.create_sensor_overview()
            results["sensor_overview_count"] = overview_df.count()
            
            logger.info("Sensor metrics processing completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in sensor metrics processing: {e}")
            raise
    
    def run_all_weather_processing(self) -> Dict[str, Any]:
        """
        Run all weather metrics processing
        """
        logger.info("Starting weather metrics processing...")
        
        results = {}
        
        try:
            # Weather dashboard metrics
            monthly_df = self.create_weather_monthly_stats()
            results["weather_monthly_count"] = monthly_df.count()
            
            overview_df = self.create_weather_overview()
            results["weather_overview_count"] = overview_df.count()
            
            logger.info("Weather metrics processing completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in weather metrics processing: {e}")
            raise
    
    def run_all_ml_processing(self) -> Dict[str, Any]:
        """
        Run all ML features processing
        """
        logger.info("Starting ML features processing...")
        
        results = {}
        
        try:
            # ML features
            sensor_ml_df = self.create_sensor_ml_features()
            results["sensor_ml_features_count"] = sensor_ml_df.count()
            
            weather_ml_df = self.create_weather_ml_features()
            results["weather_ml_features_count"] = weather_ml_df.count()
            
            integrated_ml_df = self.create_integrated_ml_features()
            results["integrated_ml_features_count"] = integrated_ml_df.count()
            
            logger.info("ML features processing completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in ML features processing: {e}")
            raise
    
    def run_all_gold_processing(self, batch_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Run all Gold zone processing - sensors, weather, and ML features
        """
        logger.info("Starting complete Gold zone processing...")
        
        all_results = {}
        
        try:
            # Sensor metrics
            sensor_results = self.run_all_sensor_processing()
            all_results.update(sensor_results)
            
            # Weather metrics
            weather_results = self.run_all_weather_processing()
            all_results.update(weather_results)
            
            # ML features
            ml_results = self.run_all_ml_processing()
            all_results.update(ml_results)
            
            all_results["processing_timestamp"] = datetime.now().isoformat()
            
            logger.info("Complete Gold zone processing completed successfully")
            return all_results
            
        except Exception as e:
            logger.error(f"Error in complete Gold zone processing: {e}")
            raise
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Generate comprehensive dashboard summary
        """
        logger.info("Generating dashboard summary...")
        
        summary = {}
        
        try:
            # Sensor metrics summary
            try:
                recent_df = self.spark.read.format("delta").load(f"{self.gold_path}sensor_metrics/recent/")
            
                recent_summary = recent_df.agg(
                count("*").alias("total_fields"),
                sum(when(col("status") == "HEALTHY", 1).otherwise(0)).alias("healthy_fields"),
                sum(when(col("status") == "MEDIUM_RISK", 1).otherwise(0)).alias("medium_risk_fields"),
                sum(when(col("status") == "HIGH_RISK", 1).otherwise(0)).alias("high_risk_fields"),
                    sum(when(col("status") == "DATA_ISSUE", 1).otherwise(0)).alias("data_issue_fields"),
                sum(when(col("is_online"), 1).otherwise(0)).alias("online_fields"),
                sum(col("anomaly_count_30min")).alias("total_anomalies"),
                avg("current_temperature").alias("avg_temp"),
                avg("current_humidity").alias("avg_humidity"),
                avg("current_soil_ph").alias("avg_ph")
            ).collect()[0]
            
                summary["sensors"] = {
                    "total_fields": int(recent_summary.total_fields or 0),
                    "healthy_fields": int(recent_summary.healthy_fields or 0),
                    "medium_risk_fields": int(recent_summary.medium_risk_fields or 0),
                    "high_risk_fields": int(recent_summary.high_risk_fields or 0),
                    "data_issue_fields": int(recent_summary.data_issue_fields or 0),
                    "online_fields": int(recent_summary.online_fields or 0),
                    "total_anomalies": int(recent_summary.total_anomalies or 0),
                    "avg_temperature": float(recent_summary.avg_temp or 0),
                    "avg_humidity": float(recent_summary.avg_humidity or 0),
                    "avg_soil_ph": float(recent_summary.avg_ph or 0)
                }
            except Exception as e:
                logger.warning(f"Could not load sensor summary: {e}")
                summary["sensors"] = {"error": "No sensor data available"}
            
            # Performance summary
            try:
                ranking_df = self.spark.read.format("delta").load(f"{self.gold_path}sensor_metrics/performance/")
                
                ranking_summary = ranking_df.agg(
                    avg("overall_performance_score").alias("avg_performance"),
                    sum(when(col("performance_grade") == "A", 1).otherwise(0)).alias("grade_a_count"),
                    sum(when(col("performance_grade") == "B", 1).otherwise(0)).alias("grade_b_count"),
                    sum(when(col("performance_grade") == "C", 1).otherwise(0)).alias("grade_c_count"),
                    sum(when(col("performance_grade") == "D", 1).otherwise(0)).alias("grade_d_count"),
                    sum(when(col("performance_grade") == "F", 1).otherwise(0)).alias("grade_f_count")
                ).collect()[0]
            
                summary["performance"] = {
                    "avg_performance_score": float(ranking_summary.avg_performance or 0),
                    "grade_a_count": int(ranking_summary.grade_a_count or 0),
                    "grade_b_count": int(ranking_summary.grade_b_count or 0),
                    "grade_c_count": int(ranking_summary.grade_c_count or 0),
                    "grade_d_count": int(ranking_summary.grade_d_count or 0),
                    "grade_f_count": int(ranking_summary.grade_f_count or 0)
                }
            except Exception as e:
                logger.warning(f"Could not load performance summary: {e}")
                summary["performance"] = {"error": "No performance data available"}
            
            # Weather summary
            try:
                weather_overview_df = self.spark.read.format("delta").load(f"{self.gold_path}weather_metrics/overview/")
                weather_summary = weather_overview_df.collect()[0]
                
                summary["weather"] = {
                    "total_locations": int(weather_summary.total_locations or 0),
                    "avg_temperature": float(weather_summary.avg_temperature_overall or 0),
                    "avg_humidity": float(weather_summary.avg_humidity_overall or 0),
                    "avg_wind_speed": float(weather_summary.avg_wind_speed_overall or 0),
                    "avg_uv_index": float(weather_summary.avg_uv_index_overall or 0)
                }
            except Exception as e:
                logger.warning(f"Could not load weather summary: {e}")
                summary["weather"] = {"error": "No weather data available"}
            
            # ML features summary
            try:
                ml_sensor_df = self.spark.read.format("delta").load(f"{self.gold_path}ml_features/sensor/")
                ml_integrated_df = self.spark.read.format("delta").load(f"{self.gold_path}ml_features/integrated/")
                
                ml_summary = ml_integrated_df.agg(
                    count("*").alias("total_ml_records"),
                    sum(when(col("combined_risk_score") == "HIGH", 1).otherwise(0)).alias("high_risk_ml"),
                    sum(when(col("combined_risk_score") == "MEDIUM", 1).otherwise(0)).alias("medium_risk_ml"),
                    sum(when(col("combined_risk_score") == "LOW", 1).otherwise(0)).alias("low_risk_ml"),
                    avg("data_quality_score").alias("avg_data_quality")
                ).collect()[0]
                
                summary["ml_features"] = {
                    "total_records": int(ml_summary.total_ml_records or 0),
                    "high_risk_records": int(ml_summary.high_risk_ml or 0),
                    "medium_risk_records": int(ml_summary.medium_risk_ml or 0),
                    "low_risk_records": int(ml_summary.low_risk_ml or 0),
                    "avg_data_quality_score": float(ml_summary.avg_data_quality or 0)
                }
            except Exception as e:
                logger.warning(f"Could not load ML features summary: {e}")
                summary["ml_features"] = {"error": "No ML features data available"}
            
            summary["generated_at"] = datetime.now().isoformat()
            
            logger.info("Dashboard summary generated successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating dashboard summary: {e}")
            return {"error": str(e)} 