#!/usr/bin/env python3
"""
ML Anomaly Service - Main orchestrator
Coordinates training and inference pipelines
"""

import os
import logging
import threading
import time
from datetime import datetime
from pyspark.sql import SparkSession
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import schedule

from training.trainer import AnomalyTrainer
from inference.predictor import StreamingPredictor
from storage.postgres_client import PostgresClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class MLAnomalyService:
    """Main ML service orchestrator"""
    
    def __init__(self):
        # Initialize Spark
        self.spark = self._create_spark_session()
        
        # Initialize components
        self.trainer = AnomalyTrainer(self.spark)
        self.predictor = None  # Will be initialized after first training
        self.postgres_client = PostgresClient()
        
        # FastAPI app
        self.app = FastAPI(
            title="ML Anomaly Detection Service",
            description="Real-time anomaly detection for crop disease",
            version="1.0.0"
        )
        
        # Add CORS
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Setup routes
        self._setup_routes()
        
        # Service state
        self.is_running = True
        self.streaming_queries = []
        self._start_time = time.time()
        
    def _create_spark_session(self):
        """Create Spark session - Connected to Spark Cluster"""
        # Fixed: Python compatibility resolved, using cluster mode
        spark_master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        driver_host = os.getenv("SPARK_DRIVER_HOST", "ml-anomaly-service")
        driver_port = os.getenv("SPARK_DRIVER_PORT", "4041")
        driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "1g")
        executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")
        executor_cores = os.getenv("SPARK_EXECUTOR_CORES", "1")
        cores_max = os.getenv("SPARK_CORES_MAX", "1")
        
        spark_builder = SparkSession.builder \
            .appName("MLAnomalyDetection") \
            .master(spark_master_url)
        
        # Add cluster-specific configurations only if using cluster
        if spark_master_url.startswith("spark://"):
            spark_builder = spark_builder \
                .config("spark.driver.host", driver_host) \
                .config("spark.driver.port", driver_port) \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .config("spark.cores.max", cores_max)
        
        # Configure Spark
        jars = [
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar",
            "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
            "/opt/spark/jars/kafka-clients-3.4.1.jar",
            "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
            "/opt/spark/jars/commons-pool2-2.11.1.jar",
            "/opt/spark/jars/lz4-java-1.8.0.jar",
            "/opt/spark/jars/zstd-jni-1.5.5-6.jar"
        ]
        
        # Set dynamic class loading to true for Kafka 
        return spark_builder \
            .config("spark.driver.memory", driver_memory) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.executor.cores", executor_cores) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.streaming.checkpointLocation.deleteOnStop", "true") \
            .config("spark.network.timeout", "300s") \
            .config("spark.executor.heartbeatInterval", "20s") \
            .config("spark.pyspark.python", "/usr/local/bin/python3") \
            .config("spark.pyspark.driver.python", "/usr/local/bin/python3") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.driver.extraClassPath", ":".join(jars)) \
            .config("spark.executor.extraClassPath", ":".join(jars)) \
            .config("spark.jars", ",".join(jars)) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/ml-checkpoints") \
            .getOrCreate()
    
    def _setup_routes(self):
        """Setup API endpoints"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "service": "ml-anomaly-service",
                "spark_running": self.spark is not None
            }
        
        @self.app.get("/status")
        async def get_status():
            """Get service status"""
            active_streams = len([q for q in self.streaming_queries if q and q.isActive])
            
            return {
                "service": "ml-anomaly-service",
                "training": {
                    "last_training": getattr(self.trainer, 'last_training', None),
                    "model_available": self.predictor is not None,
                    "initial_training_scheduled": hasattr(self, 'training_thread') and self.training_thread.is_alive()
                },
                "inference": {
                    "streaming_active": active_streams > 0,
                    "active_queries": active_streams
                },
                "timestamp": datetime.now().isoformat()
            }
        
        # TODO: check if this is needed
        @self.app.post("/train")
        async def trigger_training(background_tasks: BackgroundTasks, days: int = 30):
            """Manually trigger model training"""
            background_tasks.add_task(self._train_and_reload, days)
            return {
                "status": "training_started",
                "days_of_data": days,
                "timestamp": datetime.now().isoformat()
            }
        #TODO: It could be deleted now that we have the training after 4 minutes
        @self.app.post("/train/immediate")
        async def immediate_training():
            """Immediate training with available data"""
            result = self.trainer.manual_retrain()
            
            # Reload model if training successful
            if result['status'] == 'success':
                self._reload_model()
            
            return result
        
        @self.app.get("/predictions/recent")
        async def get_recent_predictions(field_id: str = None, limit: int = 50):
            """Get recent predictions"""
            predictions = self.postgres_client.get_recent_predictions(field_id, limit)
            return {
                "status": "success",
                "count": len(predictions),
                "predictions": predictions,
                "timestamp": datetime.now().isoformat()
            }
        
        @self.app.get("/model/info")
        async def get_model_info():
            """Get current model information"""
            if self.predictor and self.predictor.metadata:
                return {
                    "status": "success",
                    "model_info": self.predictor.metadata,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "status": "no_model",
                    "message": "No model loaded yet",
                    "timestamp": datetime.now().isoformat()
                }
        
        @self.app.get("/training/schedule")
        async def get_training_schedule():
            """Get training schedule information"""
            if hasattr(self, 'training_thread') and self.training_thread.is_alive():
                # Calculate remaining time (approximate)
                remaining_seconds = max(0, 240 - (time.time() - getattr(self, '_start_time', time.time())))
                return {
                    "status": "scheduled",
                    "initial_training": {
                        "scheduled": True,
                        "delay_minutes": 4,
                        "remaining_seconds": int(remaining_seconds),
                        "remaining_minutes": int(remaining_seconds // 60)
                    },
                    "weekly_training": {
                        "scheduled": True,
                        "day": "Sunday",
                        "time": "02:00 AM"
                    },
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "status": "no_schedule",
                    "message": "No training scheduled",
                    "timestamp": datetime.now().isoformat()
                }
        
        @self.app.post("/inference/start")
        async def start_inference():
            """Start streaming inference"""
            if not self.predictor:
                return {"status": "error", "message": "No model available"}
            
            if self.streaming_queries:
                return {"status": "already_running", "active_queries": len(self.streaming_queries)}
            
            self._start_streaming()
            return {
                "status": "started",
                "timestamp": datetime.now().isoformat()
            }
        
        @self.app.post("/inference/stop")
        async def stop_inference():
            """Stop streaming inference"""
            self._stop_streaming()
            return {
                "status": "stopped",
                "timestamp": datetime.now().isoformat()
            }
    
    def _train_and_reload(self, days: int):
        """Train model and reload for inference"""
        logger.info(f"Starting training with {days} days of data")
        result = self.trainer.train_model(days_back=days)
        
        if result['status'] == 'success':
            self._reload_model()
            logger.info("Model training completed and reloaded")
        else:
            logger.error(f"Training failed: {result.get('error')}")
    
    def _reload_model(self):
        """Reload model for inference"""
        try:
            # Stop existing streaming if running
            self._stop_streaming()
            
            # Create new predictor with latest model
            self.predictor = StreamingPredictor(self.spark)
            
            # Restart streaming if it was running
            if self.is_running:
                self._start_streaming()
            
        except Exception as e:
            logger.error(f"Failed to reload model: {e}")
    
    def _start_streaming(self):
        """Start streaming inference"""
        if self.predictor:
            try:
                logger.info("Starting streaming inference")
                self.streaming_queries = self.predictor.start_streaming()
                logger.info(f"Started {len(self.streaming_queries)} streaming queries")
            except Exception as e:
                logger.error(f"Failed to start streaming: {e}")
                logger.warning("Service will continue without real-time streaming")
                self.streaming_queries = []
    
    def _stop_streaming(self):
        """Stop streaming inference"""
        if self.streaming_queries:
            logger.info("Stopping streaming queries")
            for query in self.streaming_queries:
                if query and query.isActive:
                    query.stop()
            self.streaming_queries = []
    
    #TODO: It is necessary, it could be good to mantain but then we need to explain why
    def start_scheduled_training(self):
        """Start scheduled training job"""
        # Schedule weekly training on Sunday at 2 AM
        schedule.every().sunday.at("02:00").do(lambda: self._train_and_reload(30))
        
        def run_schedule():
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        schedule_thread = threading.Thread(target=run_schedule, daemon=True)
        schedule_thread.start()
        logger.info("Scheduled training started (weekly on Sunday at 2 AM)")
    
    # The train start after 4 minutes to allow data collection
    def initial_setup(self):
        """Initial setup - schedule training after 4 minutes and start inference"""
        logger.info("Performing initial setup...")
        
        # Schedule training after 4 minutes to allow data collection
        def delayed_training():
            logger.info("Starting delayed training after 4 minutes...")
            result = self.trainer.manual_retrain()
            
            if result['status'] == 'success':
                logger.info("Delayed training successful")
                # Initialize predictor
                self.predictor = StreamingPredictor(self.spark)
                # Start streaming
                self._start_streaming()
            else:
                logger.warning("Delayed training failed - service will run without ML")
        
        # Schedule training after 4 minutes
        self.training_thread = threading.Timer(240, delayed_training)  # 240 seconds = 4 minutes
        self.training_thread.daemon = True
        self.training_thread.start()
        
        logger.info("Training scheduled to start in 4 minutes...")
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down ML Anomaly Service...")
        self.is_running = False
        self._stop_streaming()
        if self.spark:
            self.spark.stop()

def main():
    """Main entry point"""
    logger.info("Starting ML Anomaly Detection Service...")
    
    # Create service
    service = MLAnomalyService()
    
    # Initial setup (train model)
    service.initial_setup()
    
    # Start scheduled training
    service.start_scheduled_training()
    
    # Get configuration
    host = os.getenv("ML_SERVICE_HOST", "0.0.0.0")
    port = int(os.getenv("ML_SERVICE_PORT", "8002"))
    
    logger.info(f"Starting FastAPI server on {host}:{port}")
    
    try:
        # Start FastAPI server
        uvicorn.run(
            service.app,
            host=host,
            port=port,
            log_level="info"
        )
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Error starting server: {e}")
    finally:
        service.shutdown()

if __name__ == "__main__":
    main()
