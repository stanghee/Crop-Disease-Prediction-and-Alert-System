#!/usr/bin/env python3
"""
Test script for Gold zone processing

TEST SCRIPT FOR GOLD ZONE PROCESSING TODO: REMOVE IT AFTER COMPLEATING THE PROJECT
"""

import sys
import os
sys.path.append('/app')

from gold_zone_processor import GoldZoneProcessor
from pyspark.sql import SparkSession

def test_gold_processing():
    """Test Gold zone processing"""
    print("Starting Gold zone processing test...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("GoldProcessingTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    try:
        # Create Gold processor
        gold_processor = GoldZoneProcessor(spark)
        
        # Test real-time metrics creation
        print("Testing real-time metrics creation...")
        realtime_df = gold_processor.create_realtime_metrics()
        print(f"✅ Real-time metrics created: {realtime_df.count()} records")
        
        # Test hourly aggregations
        print("Testing hourly aggregations...")
        hourly_df = gold_processor.create_hourly_aggregations()
        print(f"✅ Hourly aggregations created: {hourly_df.count()} records")
        
        # Test alert summary
        print("Testing alert summary...")
        alert_df = gold_processor.create_alert_summary()
        print(f"✅ Alert summary created: {alert_df.count()} records")
        
        # Test performance ranking
        print("Testing performance ranking...")
        ranking_df = gold_processor.create_field_performance_ranking()
        print(f"✅ Performance ranking created: {ranking_df.count()} records")
        
        # Test dashboard overview
        print("Testing dashboard overview...")
        overview_df = gold_processor.create_dashboard_overview()
        print(f"✅ Dashboard overview created: {overview_df.count()} records")
        
        print("🎉 All Gold processing tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error in Gold processing test: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    test_gold_processing() 