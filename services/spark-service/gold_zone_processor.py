#!/usr/bin/env python3
"""
Gold Zone Processor - Dashboard Metrics and Business KPIs
Processes Silver zone data into dashboard metrics and real-time KPIs
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
    Processes Silver zone data into Gold zone dashboard metrics and KPIs
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_path = "s3a://silver/"
        self.gold_path = "s3a://gold/"
    
    def create_realtime_metrics(self) -> DataFrame:
        """
        Create real-time metrics for dashboard (last 30 minutes)
        - Current status per field
        - Recent readings and data validity
        - Performance indicators
        """
        logger.info("Creating real-time metrics for Gold zone...")
        
        # Calculate timestamp for 30 minutes ago
        thirty_minutes_ago = datetime.now() - timedelta(minutes=30)
        
        # Read from Silver zone - last 30 minutes
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/") \
            .filter(col("timestamp_parsed") >= thirty_minutes_ago)
        
        # Latest reading per field
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
                "temperature_valid",
                "humidity_valid",
                "ph_valid"
            )
        
        # 30-minute aggregations per field
        realtime_metrics = silver_df \
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
                
                count("*").alias("readings_count_30min")
            ) \
            .withColumn(
                "temp_range_30min", col("max_temperature_30min") - col("min_temperature_30min")
            ) \
            .withColumn(
                "humidity_range_30min", col("max_humidity_30min") - col("min_humidity_30min")
            ) \
            .withColumn(
                # Status indicators (basato sulla qualità dei dati)
                "status",
                when((col("temperature_valid_rate_30min") < 0.8) | (col("humidity_valid_rate_30min") < 0.8) | (col("ph_valid_rate_30min") < 0.8), "DATA_ISSUE")
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
        
        # Write to Gold zone as Delta table
        realtime_metrics.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}realtime_metrics/")
        
        logger.info(f"Created {realtime_metrics.count()} real-time metric records")
        return realtime_metrics
    
    def create_hourly_aggregations(self) -> DataFrame:
        """
        Create hourly aggregations for trend analysis
        - Hourly statistics per field
        - Trend indicators
        - Performance tracking
        """
        logger.info("Creating hourly aggregations for Gold zone...")
        
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
                
                count("*").alias("readings_count")
            ) \
            .withColumn(
                "temp_range", col("max_temperature") - col("min_temperature")
            ) \
            .withColumn(
                "humidity_range", col("max_humidity") - col("min_humidity")
            ) \
            .withColumn(
                # Hourly data quality assessment
                "hourly_quality_status",
                when((col("temperature_valid_rate") < 0.8) | (col("humidity_valid_rate") < 0.8) | (col("ph_valid_rate") < 0.8), "DATA_ISSUE")
                .otherwise("VALID")
            ) \
            .withColumn("created_at", current_timestamp())
        
        # Write to Gold zone as Delta table
        hourly_metrics.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}hourly_aggregations/")
        
        logger.info(f"Created {hourly_metrics.count()} hourly aggregation records")
        return hourly_metrics
    
    def create_alert_summary(self) -> DataFrame:
        """
        Create alert summary (in questa versione: summary di dati non validi)
        """
        logger.info("Creating alert summary for Gold zone...")
        
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
                "invalid_rate",
                (col("invalid_temperature_count") + col("invalid_humidity_count") + col("invalid_ph_count")) / (col("total_readings") * 3)
            ) \
            .withColumn(
                "alert_status",
                when(col("invalid_rate") > 0.2, "HIGH")
                .when(col("invalid_rate") > 0.05, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn("created_at", current_timestamp())
        
        alert_summary.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}alert_summary/")
        
        logger.info(f"Created {alert_summary.count()} alert summary records")
        return alert_summary
    
    def create_field_performance_ranking(self) -> DataFrame:
        """
        Create field performance ranking (basato su validità dati)
        """
        logger.info("Creating field performance ranking for Gold zone...")
        
        silver_df = self.spark.read.parquet(f"{self.silver_path}iot/")
        
        performance_ranking = silver_df \
            .groupBy("field_id") \
            .agg(
                avg(col("temperature_valid").cast("int")).alias("temperature_valid_rate"),
                avg(col("humidity_valid").cast("int")).alias("humidity_valid_rate"),
                avg(col("ph_valid").cast("int")).alias("ph_valid_rate"),
                count("*").alias("total_readings")
            ) \
            .withColumn(
                "overall_validity_score",
                (col("temperature_valid_rate") + col("humidity_valid_rate") + col("ph_valid_rate")) / 3 * 100
            ) \
            .withColumn(
                "performance_grade",
                when(col("overall_validity_score") >= 90, "A")
                .when(col("overall_validity_score") >= 80, "B")
                .when(col("overall_validity_score") >= 70, "C")
                .when(col("overall_validity_score") >= 60, "D")
                .otherwise("F")
            ) \
            .withColumn(
                "performance_rank",
                rank().over(Window.orderBy(desc("overall_validity_score")))
            ) \
            .withColumn("created_at", current_timestamp())
        
        performance_ranking.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}field_performance_ranking/")
        
        logger.info(f"Created {performance_ranking.count()} field performance ranking records")
        return performance_ranking
    
    def create_dashboard_overview(self) -> DataFrame:
        """
        Create dashboard overview metrics
        - System-wide statistics
        - Overall health indicators
        - Summary KPIs
        """
        logger.info("Creating dashboard overview for Gold zone...")
        
        # Read performance ranking
        ranking_df = self.spark.read.format("delta").load(f"{self.gold_path}field_performance_ranking/")
        
        # Calculate overview metrics
        overview_stats = ranking_df.agg(
            count("*").alias("total_fields"),
            sum(when(col("performance_grade") == "A", 1).otherwise(0)).alias("grade_a_fields"),
            sum(when(col("performance_grade") == "B", 1).otherwise(0)).alias("grade_b_fields"),
            sum(when(col("performance_grade") == "C", 1).otherwise(0)).alias("grade_c_fields"),
            sum(when(col("performance_grade") == "D", 1).otherwise(0)).alias("grade_d_fields"),
            sum(when(col("performance_grade") == "F", 1).otherwise(0)).alias("grade_f_fields"),
            avg("overall_validity_score").alias("avg_validity_score"),
            avg("temperature_valid_rate").alias("avg_temperature_valid_rate"),
            avg("humidity_valid_rate").alias("avg_humidity_valid_rate"),
            avg("ph_valid_rate").alias("avg_ph_valid_rate")
        ) \
        .withColumn("created_at", current_timestamp())
        
        # Write to Gold zone as Delta table
        overview_stats.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.gold_path}dashboard_overview/")
        
        logger.info(f"Created dashboard overview record")
        return overview_stats
    
    def run_all_gold_processing(self, batch_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Run all Gold zone processing for dashboard metrics
        """
        logger.info(f"Starting Gold zone processing for dashboard metrics")
        
        results = {}
        
        try:
            # Create real-time metrics
            realtime_df = self.create_realtime_metrics()
            results["realtime_metrics_count"] = realtime_df.count()
            
            # Create hourly aggregations
            hourly_df = self.create_hourly_aggregations()
            results["hourly_aggregations_count"] = hourly_df.count()
            
            # Create alert summary
            alert_df = self.create_alert_summary()
            results["alert_summary_count"] = alert_df.count()
            
            # Create field performance ranking
            ranking_df = self.create_field_performance_ranking()
            results["performance_ranking_count"] = ranking_df.count()
            
            # Create dashboard overview
            overview_df = self.create_dashboard_overview()
            results["overview_count"] = overview_df.count()
            
            logger.info("Gold zone processing completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in Gold zone processing: {e}")
            raise
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Generate summary statistics for dashboard
        """
        logger.info("Generating dashboard summary...")
        
        summary = {}
        
        try:
            # Real-time metrics summary
            realtime_df = self.spark.read.format("delta").load(f"{self.gold_path}realtime_metrics/")
            
            realtime_summary = realtime_df.agg(
                count("*").alias("total_fields"),
                sum(when(col("status") == "HEALTHY", 1).otherwise(0)).alias("healthy_fields"),
                sum(when(col("status") == "LOW_RISK", 1).otherwise(0)).alias("low_risk_fields"),
                sum(when(col("status") == "MEDIUM_RISK", 1).otherwise(0)).alias("medium_risk_fields"),
                sum(when(col("status") == "HIGH_RISK", 1).otherwise(0)).alias("high_risk_fields"),
                sum(when(col("is_online"), 1).otherwise(0)).alias("online_fields"),
                sum(col("anomaly_count_30min")).alias("total_anomalies"),
                avg("current_temperature").alias("avg_temp"),
                avg("current_humidity").alias("avg_humidity"),
                avg("current_soil_ph").alias("avg_ph")
            ).collect()[0]
            
            summary["realtime"] = {
                "total_fields": int(realtime_summary.total_fields or 0),
                "healthy_fields": int(realtime_summary.healthy_fields or 0),
                "low_risk_fields": int(realtime_summary.low_risk_fields or 0),
                "medium_risk_fields": int(realtime_summary.medium_risk_fields or 0),
                "high_risk_fields": int(realtime_summary.high_risk_fields or 0),
                "online_fields": int(realtime_summary.online_fields or 0),
                "total_anomalies": int(realtime_summary.total_anomalies or 0),
                "avg_temperature": float(realtime_summary.avg_temp or 0),
                "avg_humidity": float(realtime_summary.avg_humidity or 0),
                "avg_soil_ph": float(realtime_summary.avg_ph or 0)
            }
            
            # Performance ranking summary
            ranking_df = self.spark.read.format("delta").load(f"{self.gold_path}field_performance_ranking/")
            
            ranking_summary = ranking_df.agg(
                avg("overall_validity_score").alias("avg_performance"),
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
            
            logger.info("Dashboard summary generated successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating dashboard summary: {e}")
            return {"error": str(e)} 