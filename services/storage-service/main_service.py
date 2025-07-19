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
        """Create Spark session with S3 and Kafka support - Connected to Spark Cluster"""
        spark_master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        driver_host = os.getenv("SPARK_DRIVER_HOST", "spark-data-lake-service")
        driver_port = os.getenv("SPARK_DRIVER_PORT", "4040")
        
        return SparkSession.builder \
            .appName("CropDiseaseDataLake") \
            .master(spark_master_url) \
            .config("spark.driver.host", driver_host) \
            .config("spark.driver.port", driver_port) \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.cores.max", os.getenv("SPARK_CORES_MAX", "2")) \
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
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.sql.warehouse.dir", "s3a://gold/warehouse/") \
            .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar,/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/commons-pool2-2.11.1.jar") \
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
    
    def start_silver_streaming(self):
        """
        Start all Silver zone streaming queries (sensors, weather, satellite).
        Returns a list of streaming query handles.
        """
        logger.info("Starting Silver zone streaming processing...")
        queries = []
        try:
            # Start sensor streaming (returns tuple: silver_query, kafka_query)
            sensor_queries = self.silver_processor.start_sensor_stream()
            if sensor_queries:
                silver_query, kafka_query = sensor_queries
                queries.extend([silver_query, kafka_query])
                logger.info("Sensor Silver streaming started (Parquet + Kafka)")
            else:
                logger.warning("Sensor Silver streaming failed to start")
            
            # Start weather streaming (returns tuple: silver_query, kafka_query)
            weather_queries = self.silver_processor.start_weather_stream()
            if weather_queries:
                silver_query, kafka_query = weather_queries
                queries.extend([silver_query, kafka_query])
                logger.info("Weather Silver streaming started (Parquet + Kafka)")
            else:
                logger.warning("Weather Silver streaming failed to start")
            
            # Start satellite streaming (returns single query)
            satellite_query = self.silver_processor.start_satellite_stream()
            if satellite_query:
                queries.append(satellite_query)
                logger.info("Satellite Silver streaming started")
            else:
                logger.warning("Satellite Silver streaming failed to start")
            
            logger.info(f"Started {len(queries)} Silver streaming queries out of 5 attempted")
        except Exception as e:
            logger.error(f"Error starting Silver streaming: {e}")
            # Stop any started queries
            for q in queries:
                if q and q.isActive:
                    q.stop()
            raise
        return queries
    
    def run_gold_processing(self) -> Dict[str, Any]:
        """
        Run Gold zone processing - ML features with sliding window
        """
        logger.info("Starting Gold zone ML features processing...")
        
        try:
            # Run ML features processing with sliding window
            results = self.gold_processor.run_ml_features_processing()
            
            logger.info(f"ML features processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error in Gold processing: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Get dashboard summary with all metrics
        """
        try:
            return self.gold_processor.get_dashboard_summary()
        except Exception as e:
            logger.error(f"Error getting dashboard summary: {e}")
            return {"error": str(e)}
    
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
        
        # Gold processor status
        try:
            status["gold_processor"] = "available"
            # Add last processing info if available
            dashboard_summary = self.get_dashboard_summary()
            if "error" not in dashboard_summary:
                status["gold_last_processed"] = dashboard_summary.get("generated_at", "unknown")
        except Exception as e:
            logger.error(f"Error getting Gold processor status: {e}")
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

# Separate Bronze and Silver streaming workers
def run_bronze_streaming_worker(service: MainDataLakeService):
    """
    Worker for Bronze streaming only. Keeps Bronze zone ingestion alive independently.
    """
    try:
        service.start_streaming_processing()
        while service.is_running:
            bronze_active = len([q for q in service.streaming_queries if q and q.isActive])
            logger.info(f"Bronze streaming check - Active queries: {bronze_active}")
            if bronze_active == 0:
                logger.warning("No active Bronze streaming queries! Exiting Bronze worker...")
                break
            time.sleep(30)
    except Exception as e:
        logger.error(f"Error in Bronze streaming worker: {e}")

def run_silver_streaming_worker(service: MainDataLakeService):
    """
    Worker for Silver streaming only. Keeps Silver zone processing alive independently.
    """
    max_retries = 3
    retry_count = 0
    
    while service.is_running and retry_count < max_retries:
        try:
            logger.info(f"Attempting to start Silver streaming (attempt {retry_count + 1}/{max_retries})")
            silver_queries = service.start_silver_streaming()
            
            if not silver_queries:
                logger.warning("No Silver streaming queries started, will retry")
                retry_count += 1
                time.sleep(60)  # Wait 1 minute before retry
                continue
            
            # Track only Silver queries
            while service.is_running:
                silver_active = len([q for q in silver_queries if q and q.isActive])
                logger.info(f"Silver streaming check - Active queries: {silver_active}")
                if silver_active == 0:
                    logger.warning("No active Silver streaming queries! Will retry...")
                    break
                time.sleep(30)
            
            retry_count += 1
            if retry_count < max_retries:
                logger.info(f"Silver streaming stopped, retrying in 60 seconds... (attempt {retry_count + 1}/{max_retries})")
                time.sleep(60)
            
        except Exception as e:
            logger.error(f"Error in Silver streaming worker: {e}")
            retry_count += 1
            if retry_count < max_retries:
                logger.info(f"Will retry Silver streaming in 60 seconds... (attempt {retry_count + 1}/{max_retries})")
                time.sleep(60)
    
    if retry_count >= max_retries:
        logger.error(f"Silver streaming failed after {max_retries} attempts, giving up")
    else:
        logger.info("Silver streaming worker stopped")

def run_gold_processing_worker(service: MainDataLakeService):
    """
    Worker for Gold zone processing. Runs periodically to process ML features with sliding window.
    """
    logger.info("Starting Gold zone processing worker...")
    
    # Wait for initial data to be available (2 minutes after startup)
    logger.info("Waiting for initial data to be available...")
    time.sleep(120)
    
    # Gold processing interval - every 10 minutes
    gold_processing_interval = 600
    last_processing_time = 0
    
    while service.is_running:
        try:
            current_time = time.time()
            
            # Check if it's time to run Gold processing
            if current_time - last_processing_time >= gold_processing_interval:
                logger.info("Starting Gold zone processing cycle...")
                
                # Run Gold processing
                results = service.run_gold_processing()
                
                if "error" not in results:
                    logger.info(f"Gold processing completed successfully: {results}")
                    last_processing_time = current_time
                else:
                    logger.error(f"Gold processing failed: {results}")
                    # Still update time to prevent rapid retries
                    last_processing_time = current_time
            
            # Sleep for 1 minute before next check
            time.sleep(60)
            
        except Exception as e:
            logger.error(f"Error in Gold processing worker: {e}")
            time.sleep(60)
    
    logger.info("Gold processing worker stopped")

def main():
    """Main entry point"""
    logger.info("Starting Crop Disease Data Lake Service...")
    service = MainDataLakeService()
    try:
        # Start Bronze streaming in its own thread
        bronze_thread = threading.Thread(
            target=run_bronze_streaming_worker,
            args=(service,),
            daemon=True
        )
        bronze_thread.start()

        # Start Silver streaming in its own thread
        silver_thread = threading.Thread(
            target=run_silver_streaming_worker,
            args=(service,),
            daemon=True
        )
        silver_thread.start()

        # Start Gold processing in its own thread
        gold_thread = threading.Thread(
            target=run_gold_processing_worker,
            args=(service,),
            daemon=True
        )
        gold_thread.start()

        logger.info("All workers started: Bronze streaming, Silver streaming, Gold processing")

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