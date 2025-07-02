#!/usr/bin/env python3
"""
Main Data Lake Service - Orchestrates Bronze, Silver, and Gold zones
"""

import os
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from minio import Minio

# Import zone processors
from bronze_zone_processor import BronzeZoneProcessor
from silver_zone_processor import SilverZoneProcessor
from gold_zone_processor import GoldZoneProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class MainDataLakeService:
    """
    Main orchestrator for the 3-zone data lake architecture
    """
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.minio_client = self._create_minio_client()
        self._setup_buckets()
        
        # Initialize zone processors
        self.bronze_processor = BronzeZoneProcessor(self.spark, self.minio_client)
        self.silver_processor = SilverZoneProcessor(self.spark)
        self.gold_processor = GoldZoneProcessor(self.spark)
        
        # Processing state
        self.streaming_queries = []
        self.is_running = True
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake and S3 support"""
        return SparkSession.builder \
            .appName("CropDiseaseDataLake") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.session.timeZone", "Europe/Rome") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.sql.warehouse.dir", "s3a://gold/warehouse/") \
            .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar,/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/commons-pool2-2.11.1.jar") \
            .getOrCreate()
    
    def _create_minio_client(self) -> Minio:
        """Create MinIO client for bucket management"""
        return Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
    
    def _setup_buckets(self):
        """Create necessary MinIO buckets"""
        buckets = ["bronze", "silver", "gold", "satellite-images"]
        for bucket in buckets:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
    
    def start_streaming_processing(self):
        """Start Bronze zone streaming processing"""
        logger.info("Starting Bronze zone streaming processing...")
        
        try:
            self.streaming_queries = self.bronze_processor.start_all_streams()
            logger.info(f"Started {len(self.streaming_queries)} streaming queries")
            
        except Exception as e:
            logger.error(f"Error starting streaming processing: {e}")
            self.stop_streaming_processing()
            raise
    
    def stop_streaming_processing(self):
        """Stop all streaming queries"""
        logger.info("Stopping streaming processing...")
        
        if self.streaming_queries:
            self.bronze_processor.stop_all_streams(self.streaming_queries)
            self.streaming_queries = []
    
    def run_batch_processing(self, batch_date: Optional[str] = None):
        """Run Silver and Gold zone batch processing"""
        logger.info(f"Starting batch processing for date: {batch_date or 'latest'}")
        
        try:
            # Silver zone processing
            logger.info("Processing Silver zone...")
            silver_results = self.silver_processor.run_all_silver_processing(batch_date)
            logger.info(f"Silver processing completed: {silver_results}")
            
            # Gold zone processing
            logger.info("Processing Gold zone...")
            gold_results = self.gold_processor.run_all_gold_processing(batch_date)
            logger.info(f"Gold processing completed: {gold_results}")
            
            return {
                "batch_date": batch_date or datetime.now().strftime("%Y-%m-%d"),
                "silver_results": silver_results,
                "gold_results": gold_results,
                "processing_time": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            raise
    
    def run_continuous_batch_processing(self, interval_minutes: int = 5):
        """Run batch processing continuously at specified intervals"""
        logger.info(f"Starting continuous batch processing every {interval_minutes} minutes")
        
        while self.is_running:
            try:
                # Run batch processing
                results = self.run_batch_processing()
                logger.info(f"Batch processing completed: {results}")
                
                # Wait for next interval
                time.sleep(interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Error in continuous batch processing: {e}")
                # Wait before retrying
                time.sleep(60)
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        # Get actual active streams from Spark
        actual_active_streams = len(self.spark.streams.active)
        
        status = {
            "timestamp": datetime.now().isoformat(),
            "streaming_queries": len(self.streaming_queries),
            "streaming_active": len([q for q in self.streaming_queries if q and q.isActive]),
            "actual_spark_streams": actual_active_streams,
            "spark_app_id": self.spark.sparkContext.applicationId,
            "spark_app_name": self.spark.sparkContext.appName
        }
        
        try:
            # Get ML feature summary
            ml_summary = self.gold_processor.get_ml_feature_summary()
            status["ml_features"] = ml_summary
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            status["error"] = str(e)
        
        return status
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Data Lake Service...")
        
        self.is_running = False
        self.stop_streaming_processing()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("Data Lake Service shutdown completed")

def run_streaming_worker(service: MainDataLakeService):
    """Worker function for streaming processing"""
    try:
        service.start_streaming_processing()
        
        # Keep streaming alive
        while service.is_running:
            # Check actual Spark streams, not just our list
            actual_active_streams = len(service.spark.streams.active)
            tracked_active_queries = len([q for q in service.streaming_queries if q and q.isActive])
            
            logger.info(f"Stream check - Tracked: {tracked_active_queries}, Actual Spark: {actual_active_streams}")
            
            if actual_active_streams == 0:
                logger.warning("No active Spark streaming queries detected, restarting...")
                try:
                    service.stop_streaming_processing()
                    time.sleep(5)  # Wait a bit before restarting
                    service.start_streaming_processing()
                except Exception as e:
                    logger.error(f"Error restarting streams: {e}")
            
            time.sleep(30)  # Check every 30 seconds
            
    except Exception as e:
        logger.error(f"Error in streaming worker: {e}")
        service.stop_streaming_processing()

def run_batch_worker(service: MainDataLakeService):
    """Worker function for batch processing"""
    try:
        service.run_continuous_batch_processing(interval_minutes=5)
    except Exception as e:
        logger.error(f"Error in batch worker: {e}")

def main():
    """Main entry point"""
    logger.info("Starting Crop Disease Data Lake Service...")
    
    service = MainDataLakeService()
    
    try:
        # Start streaming processing in background thread
        streaming_thread = threading.Thread(
            target=run_streaming_worker, 
            args=(service,),
            daemon=True
        )
        streaming_thread.start()
        
        # Start batch processing in background thread
        batch_thread = threading.Thread(
            target=run_batch_worker,
            args=(service,),
            daemon=True
        )
        batch_thread.start()
        
        # Main monitoring loop
        while True:
            try:
                status = service.get_system_status()
                logger.info(f"System status: {status}")
                
                time.sleep(300)  # Status check every 5 minutes
                
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(60)
    
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
    
    finally:
        service.shutdown()

if __name__ == "__main__":
    main() 