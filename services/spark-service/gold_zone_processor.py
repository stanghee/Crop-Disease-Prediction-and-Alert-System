#!/usr/bin/env python3
"""
Gold Zone Processor - ML Features and Business KPIs
Processes Silver zone data into ML-ready features and dashboard metrics
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
    Processes Silver zone data into Gold zone ML features and KPIs
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_path = "s3a://silver/"
        self.gold_path = "s3a://gold/"
    
    def create_sensor_ml_features(self, batch_date: Optional[str] = None) -> DataFrame:
        """
        Create ML features from sensor data
        - Daily aggregations per field
        - Statistical features
        - Risk scoring
        """
        logger.info("Creating sensor ML features for Gold zone...")
        
        # Read from Silver zone
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/")
        
        # Filter by date if specified (default: last 30 days)
        if batch_date:
            silver_df = silver_df.filter(col("date") == batch_date)
        else:
            cutoff_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            silver_df = silver_df.filter(col("date") >= cutoff_date)
        
        # Create daily aggregations per field
        daily_features = silver_df \
            .groupBy("field_id", "date") \
            .agg(
                # Temperature features
                avg("temperature").alias("avg_temperature"),
                stddev("temperature").alias("std_temperature"),
                min("temperature").alias("min_temperature"),
                max("temperature").alias("max_temperature"),
                percentile_approx("temperature", 0.25).alias("q25_temperature"),
                percentile_approx("temperature", 0.75).alias("q75_temperature"),
                
                # Humidity features
                avg("humidity").alias("avg_humidity"),
                stddev("humidity").alias("std_humidity"),
                min("humidity").alias("min_humidity"),
                max("humidity").alias("max_humidity"),
                percentile_approx("humidity", 0.25).alias("q25_humidity"),
                percentile_approx("humidity", 0.75).alias("q75_humidity"),
                
                # Soil pH features
                avg("soil_ph").alias("avg_soil_ph"),
                stddev("soil_ph").alias("std_soil_ph"),
                min("soil_ph").alias("min_soil_ph"),
                max("soil_ph").alias("max_soil_ph"),
                
                # Data quality and anomaly features
                avg("data_quality_score").alias("avg_data_quality"),
                sum(when(col("has_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
                sum(when(col("temperature_anomaly"), 1).otherwise(0)).alias("temp_anomaly_count"),
                sum(when(col("humidity_anomaly"), 1).otherwise(0)).alias("humidity_anomaly_count"),
                sum(when(col("ph_anomaly"), 1).otherwise(0)).alias("ph_anomaly_count"),
                count("*").alias("total_readings"),
                
                # Rolling statistics features
                avg("temp_rolling_avg").alias("avg_temp_rolling_avg"),
                avg("temp_deviation_from_avg").alias("avg_temp_deviation"),
                avg("humidity_deviation_from_avg").alias("avg_humidity_deviation"),
                
                # Temporal features
                first("year").alias("year"),
                first("month").alias("month"),
                first("day_of_week").alias("day_of_week")
            ) \
            .withColumn(
                # Calculate anomaly rates
                "anomaly_rate", col("anomaly_count") / col("total_readings")
            ) \
            .withColumn(
                "temp_anomaly_rate", col("temp_anomaly_count") / col("total_readings")
            ) \
            .withColumn(
                "humidity_anomaly_rate", col("humidity_anomaly_count") / col("total_readings")
            ) \
            .withColumn(
                "ph_anomaly_rate", col("ph_anomaly_count") / col("total_readings")
            ) \
            .withColumn(
                # Temperature range and variability
                "temp_range", col("max_temperature") - col("min_temperature")
            ) \
            .withColumn(
                "temp_iqr", col("q75_temperature") - col("q25_temperature")
            ) \
            .withColumn(
                "humidity_range", col("max_humidity") - col("min_humidity")
            ) \
            .withColumn(
                "humidity_iqr", col("q75_humidity") - col("q25_humidity")
            ) \
            .withColumn(
                # Risk scoring based on anomaly rates and variability
                "risk_score",
                when(col("anomaly_rate") > 0.15, "HIGH")
                .when(col("anomaly_rate") > 0.08, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "variability_score",
                when((col("temp_range") > 20) | (col("humidity_range") > 50), "HIGH")
                .when((col("temp_range") > 10) | (col("humidity_range") > 30), "MEDIUM")
                .otherwise("LOW")
            )
        
        # Add historical comparison features
        window_spec = Window.partitionBy("field_id").orderBy("date").rowsBetween(-6, 0)
        
        ml_features = daily_features \
            .withColumn("temp_7day_avg", avg("avg_temperature").over(window_spec)) \
            .withColumn("humidity_7day_avg", avg("avg_humidity").over(window_spec)) \
            .withColumn("ph_7day_avg", avg("avg_soil_ph").over(window_spec)) \
            .withColumn("anomaly_7day_avg", avg("anomaly_rate").over(window_spec)) \
            .withColumn(
                "temp_trend_7day", 
                col("avg_temperature") - lag("avg_temperature", 7).over(
                    Window.partitionBy("field_id").orderBy("date")
                )
            ) \
            .withColumn(
                "humidity_trend_7day",
                col("avg_humidity") - lag("avg_humidity", 7).over(
                    Window.partitionBy("field_id").orderBy("date")
                )
            ) \
            .withColumn("created_at", current_timestamp())
        
        # Write to Gold zone as Delta table
        ml_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}sensor_ml_features/")
        
        logger.info(f"Created {ml_features.count()} sensor ML feature records")
        return ml_features
    
    def create_sensor_dashboard_kpis(self) -> DataFrame:
        """
        Create real-time KPIs for dashboard
        - Current status per field
        - Recent trends and alerts
        """
        logger.info("Creating sensor dashboard KPIs for Gold zone...")
        
        # Read from Silver zone
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/") \
            .filter(col("date") >= date_7d_ago)
        
        # Latest readings per field
        window_latest = Window.partitionBy("field_id").orderBy(desc("timestamp_parsed"))
        
        latest_readings = silver_df \
            .withColumn("row_num", row_number().over(window_latest)) \
            .filter(col("row_num") == 1) \
            .select(
                "field_id",
                "temperature",
                "humidity", 
                "soil_ph",
                "timestamp_parsed",
                "has_anomaly"
            )
        
        # 7-day aggregations
        weekly_stats = silver_df \
            .groupBy("field_id") \
            .agg(
                avg("temperature").alias("week_avg_temp"),
                avg("humidity").alias("week_avg_humidity"),
                avg("soil_ph").alias("week_avg_ph"),
                count(when(col("has_anomaly"), True)).alias("week_anomaly_count"),
                count("*").alias("week_total_readings"),
                max("timestamp_parsed").alias("last_reading_time")
            ) \
            .withColumn("week_anomaly_rate", 
                       col("week_anomaly_count") / col("week_total_readings"))
        
        # Join latest readings with weekly stats
        dashboard_kpis = latest_readings.alias("latest") \
            .join(weekly_stats.alias("weekly"), "field_id", "inner") \
            .select(
                "field_id",
                # Current readings
                col("latest.temperature").alias("current_temperature"),
                col("latest.humidity").alias("current_humidity"),
                col("latest.soil_ph").alias("current_soil_ph"),
                col("latest.timestamp_parsed").alias("last_update"),
                col("latest.has_anomaly").alias("current_anomaly"),
                
                # Weekly trends
                "week_avg_temp",
                "week_avg_humidity", 
                "week_avg_ph",
                "week_anomaly_count",
                "week_anomaly_rate",
                "week_total_readings"
            ) \
            .withColumn(
                # Status indicators
                "temp_status",
                when(col("current_temperature") < 10, "COLD")
                .when(col("current_temperature") > 35, "HOT")
                .otherwise("NORMAL")
            ) \
            .withColumn(
                "humidity_status",
                when(col("current_humidity") < 30, "DRY")
                .when(col("current_humidity") > 80, "WET")
                .otherwise("NORMAL")
            ) \
            .withColumn(
                "ph_status",
                when(col("current_soil_ph") < 6.0, "ACIDIC")
                .when(col("current_soil_ph") > 7.5, "ALKALINE")
                .otherwise("NORMAL")
            ) \
            .withColumn(
                "overall_status",
                when(col("current_anomaly") | (col("week_anomaly_rate") > 0.1), "ALERT")
                .when(col("week_anomaly_rate") > 0.05, "WARNING")
                .otherwise("HEALTHY")
            ) \
            .withColumn(
                # Data freshness
                "data_age_hours",
                (unix_timestamp(current_timestamp()) - unix_timestamp("last_update")) / 3600
            ) \
            .withColumn(
                "data_freshness",
                when(col("data_age_hours") < 2, "FRESH")
                .when(col("data_age_hours") < 6, "RECENT")
                .otherwise("STALE")
            ) \
            .withColumn("created_at", current_timestamp())
        
        # Write to Gold zone as Delta table
        dashboard_kpis.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{self.gold_path}sensor_dashboard_kpis/")
        
        logger.info(f"Created {dashboard_kpis.count()} dashboard KPI records")
        return dashboard_kpis
    
    def create_weather_ml_features(self, batch_date: Optional[str] = None) -> DataFrame:
        """
        Create ML features from weather data
        - Daily weather patterns
        - Extreme weather indicators
        """
        logger.info("Creating weather ML features for Gold zone...")
        
        # Read from Silver zone
        silver_df = self.spark.read.parquet(f"{self.silver_path}weather/")
        
        # Filter by date if specified
        if batch_date:
            silver_df = silver_df.filter(col("date") == batch_date)
        else:
            cutoff_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            silver_df = silver_df.filter(col("date") >= cutoff_date)
        
        # Daily weather aggregations
        weather_features = silver_df \
            .groupBy("location", "date") \
            .agg(
                # Temperature features
                avg("temp_c").alias("avg_temperature"),
                max("temp_c").alias("max_temperature"),
                min("temp_c").alias("min_temperature"),
                stddev("temp_c").alias("temp_variability"),
                
                # Humidity and comfort
                avg("humidity").alias("avg_humidity"),
                max("humidity").alias("max_humidity"),
                min("humidity").alias("min_humidity"),
                
                # Wind and weather dynamics
                avg("wind_kph").alias("avg_wind_speed"),
                max("wind_kph").alias("max_wind_speed"),
                avg("uv").alias("avg_uv_index"),
                max("uv").alias("max_uv_index"),
                
                # Weather conditions
                first("weather_category").alias("dominant_weather_category"),
                avg("weather_severity_score").alias("avg_weather_severity"),
                max("weather_severity_score").alias("max_weather_severity"),
                
                # Data quality
                avg("data_quality_score").alias("avg_data_quality"),
                count("*").alias("total_readings"),
                
                # Geographic info
                first("lat").alias("latitude"),
                first("lon").alias("longitude"),
                first("region").alias("region"),
                first("country").alias("country")
            ) \
            .withColumn("temp_range", col("max_temperature") - col("min_temperature")) \
            .withColumn("humidity_range", col("max_humidity") - col("min_humidity")) \
            .withColumn(
                "extreme_weather_score",
                when((col("max_temperature") > 40) | (col("min_temperature") < 0) | 
                     (col("max_wind_speed") > 50) | (col("max_uv_index") > 10), 3)
                .when((col("max_temperature") > 35) | (col("min_temperature") < 5) |
                      (col("max_wind_speed") > 30) | (col("max_uv_index") > 8), 2)
                .when((col("max_temperature") > 30) | (col("min_temperature") < 10) |
                      (col("max_wind_speed") > 20) | (col("max_uv_index") > 6), 1)
                .otherwise(0)
            ) \
            .withColumn("created_at", current_timestamp())
        
        # Write to Gold zone as Delta table
        weather_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}weather_ml_features/")
        
        logger.info(f"Created {weather_features.count()} weather ML feature records")
        return weather_features
    
    def create_integrated_features(self) -> DataFrame:
        """
        Create integrated features combining sensor and weather data
        - Cross-source correlations
        - Environmental stress indicators
        """
        logger.info("Creating integrated ML features for Gold zone...")
        
        # Read Gold zone features
        sensor_features = self.spark.read.format("delta") \
            .load(f"{self.gold_path}sensor_ml_features/")
        
        weather_features = self.spark.read.format("delta") \
            .load(f"{self.gold_path}weather_ml_features/")
        
        # Join sensor and weather data by date (assuming single location for now)
        integrated_df = sensor_features.alias("s") \
            .join(weather_features.alias("w"), 
                  col("s.date") == col("w.date"), "left") \
            .select(
                col("s.*"),
                # Weather features
                col("w.avg_temperature").alias("weather_avg_temp"),
                col("w.max_temperature").alias("weather_max_temp"),
                col("w.min_temperature").alias("weather_min_temp"),
                col("w.avg_humidity").alias("weather_avg_humidity"),
                col("w.avg_wind_speed"),
                col("w.avg_uv_index"),
                col("w.dominant_weather_category"),
                col("w.extreme_weather_score"),
                col("w.avg_weather_severity")
            ) \
            .withColumn(
                # Temperature differential analysis
                "temp_differential",
                when(col("weather_avg_temp").isNotNull(),
                     abs(col("avg_temperature") - col("weather_avg_temp")))
                .otherwise(null())
            ) \
            .withColumn(
                "humidity_differential", 
                when(col("weather_avg_humidity").isNotNull(),
                     abs(col("avg_humidity") - col("weather_avg_humidity")))
                .otherwise(null())
            ) \
            .withColumn(
                # Environmental stress indicators
                "heat_stress_indicator",
                when((col("weather_max_temp") > 35) & (col("avg_humidity") > 70), "HIGH")
                .when((col("weather_max_temp") > 30) & (col("avg_humidity") > 60), "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "drought_stress_indicator",
                when((col("weather_avg_humidity") < 40) & (col("avg_humidity") < 50), "HIGH")
                .when((col("weather_avg_humidity") < 50) & (col("avg_humidity") < 60), "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                # Multi-source risk assessment
                "environmental_risk_score",
                (col("extreme_weather_score") + 
                 when(col("risk_score") == "HIGH", 3)
                 .when(col("risk_score") == "MEDIUM", 2)
                 .when(col("risk_score") == "LOW", 1)
                 .otherwise(0) +
                 when(col("heat_stress_indicator") == "HIGH", 2)
                 .when(col("heat_stress_indicator") == "MEDIUM", 1)
                 .otherwise(0) +
                 when(col("drought_stress_indicator") == "HIGH", 2)
                 .when(col("drought_stress_indicator") == "MEDIUM", 1)
                 .otherwise(0)) / 4.0
            ) \
            .withColumn(
                "crop_health_prediction",
                when(col("environmental_risk_score") > 2.5, "HIGH_RISK")
                .when(col("environmental_risk_score") > 1.5, "MEDIUM_RISK")
                .when(col("environmental_risk_score") > 0.5, "LOW_RISK")
                .otherwise("HEALTHY")
            ) \
            .withColumn("integration_timestamp", current_timestamp())
        
        # Write to Gold zone as Delta table
        integrated_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}integrated_ml_features/")
        
        logger.info(f"Created {integrated_df.count()} integrated ML feature records")
        return integrated_df
    
    def run_all_gold_processing(self, batch_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Run all Gold zone processing
        """
        logger.info(f"Starting Gold zone processing for date: {batch_date or 'latest'}")
        
        results = {}
        
        try:
            # Create sensor ML features
            sensor_ml_df = self.create_sensor_ml_features(batch_date)
            results["sensor_ml_count"] = sensor_ml_df.count()
            
            # Create sensor dashboard KPIs
            sensor_kpi_df = self.create_sensor_dashboard_kpis()
            results["sensor_kpi_count"] = sensor_kpi_df.count()
            
            # Create weather ML features
            weather_ml_df = self.create_weather_ml_features(batch_date)
            results["weather_ml_count"] = weather_ml_df.count()
            
            # Create integrated features
            integrated_df = self.create_integrated_features()
            results["integrated_count"] = integrated_df.count()
            
            logger.info("Gold zone processing completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in Gold zone processing: {e}")
            raise
    
    def get_ml_feature_summary(self) -> Dict[str, Any]:
        """
        Generate summary statistics for ML features
        """
        logger.info("Generating ML feature summary...")
        
        summary = {}
        
        try:
            # Sensor ML features summary
            sensor_ml_df = self.spark.read.format("delta") \
                .load(f"{self.gold_path}sensor_ml_features/")
            
            sensor_summary = sensor_ml_df.agg(
                count("*").alias("total_records"),
                countDistinct("field_id").alias("unique_fields"),
                avg("anomaly_rate").alias("avg_anomaly_rate"),
                sum(when(col("risk_score") == "HIGH", 1).otherwise(0)).alias("high_risk_days"),
                sum(when(col("risk_score") == "MEDIUM", 1).otherwise(0)).alias("medium_risk_days"),
                sum(when(col("risk_score") == "LOW", 1).otherwise(0)).alias("low_risk_days")
            ).collect()[0]
            
            summary["sensor_ml"] = {
                "total_records": int(sensor_summary.total_records or 0),
                "unique_fields": int(sensor_summary.unique_fields or 0),
                "avg_anomaly_rate": float(sensor_summary.avg_anomaly_rate or 0),
                "high_risk_days": int(sensor_summary.high_risk_days or 0),
                "medium_risk_days": int(sensor_summary.medium_risk_days or 0),
                "low_risk_days": int(sensor_summary.low_risk_days or 0)
            }
            
            # Integrated features summary
            integrated_df = self.spark.read.format("delta") \
                .load(f"{self.gold_path}integrated_ml_features/")
            
            integrated_summary = integrated_df.agg(
                count("*").alias("total_records"),
                avg("environmental_risk_score").alias("avg_env_risk"),
                sum(when(col("crop_health_prediction") == "HIGH_RISK", 1).otherwise(0)).alias("high_risk_predictions"),
                sum(when(col("crop_health_prediction") == "MEDIUM_RISK", 1).otherwise(0)).alias("medium_risk_predictions"),
                sum(when(col("crop_health_prediction") == "HEALTHY", 1).otherwise(0)).alias("healthy_predictions")
            ).collect()[0]
            
            summary["integrated"] = {
                "total_records": int(integrated_summary.total_records or 0),
                "avg_environmental_risk": float(integrated_summary.avg_env_risk or 0),
                "high_risk_predictions": int(integrated_summary.high_risk_predictions or 0),
                "medium_risk_predictions": int(integrated_summary.medium_risk_predictions or 0),
                "healthy_predictions": int(integrated_summary.healthy_predictions or 0)
            }
            
            logger.info("ML feature summary generated successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating ML feature summary: {e}")
            return {"error": str(e)} 