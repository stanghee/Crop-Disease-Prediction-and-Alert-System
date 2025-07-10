#!/usr/bin/env python3
"""
Crop Disease Service - Core Service
Implements Dual-Mode Architecture: Real-time Analysis + Batch Predictions
"""

import os
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, when

# Import ML components
from .models.disease_predictor import DiseasePredictor
from .models.alert_generator import AlertGenerator
from .data_loading.data_loader import DataLoader

logger = logging.getLogger(__name__)

class MLService:
    """
    Main Crop Disease Service implementing dual-mode architecture:
    - Real-time processing (every 5 minutes): Current analysis and guidance
    - Batch processing (every 6 hours): ML predictions and strategic alerts
    """
    
    def __init__(self):
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Initialize components
        self.data_loader = DataLoader(self.spark)
        self.disease_predictor = DiseasePredictor()
        self.alert_generator = AlertGenerator()
        
        # Service state
        self.last_batch_prediction = None
        self.is_initialized = False
        
        # Alert manager (set by orchestrator after initialization)
        self.alert_manager = None
        
        # Initialize models
        self._initialize_models()
    
    def set_alert_manager(self, alert_manager):
        """Set the alert manager for real-time alert system integration"""
        self.alert_manager = alert_manager
        logger.info("Alert manager connected to ML service")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session for ML processing"""
        return SparkSession.builder \
            .appName("CropDiseaseMLService") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.session.timeZone", "Europe/Rome") \
            .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
    
    def _initialize_models(self):
        """Initialize ML models"""
        try:
            logger.info("Initializing ML models...")
            
            # Load or train disease prediction model
            self.disease_predictor.initialize()
            
            # Initialize alert generator
            self.alert_generator.initialize()
            
            self.is_initialized = True
            logger.info("ML models initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing models: {e}")
            raise
    

    
    # Real-time alert generation functions have been moved to the Kafka alert consumer
    # See: kafka_alert_consumer.py, alert_factory.py
    
    # Economic impact calculation moved to config/alert_thresholds.py (EconomicImpactConfig)
    
    # Sensor recommendations moved to config/alert_thresholds.py (RecommendationConfig)
    

    
    # Real-time alert saving moved to database/alert_repository.py (AlertRepository)
    


    
    def run_batch_prediction(self) -> Dict[str, Any]:
        """
        Batch processing (Mode 2): Every 6 hours
        Purpose: ML predictions and strategic alerts
        """
        start_time = time.time()
        
        try:
            logger.info("Starting batch prediction processing...")
            
            # Load historical data for ML features
            ml_features = self.data_loader.load_ml_features()
            
            if ml_features.empty:
                logger.warning("No ML features available for batch prediction")
                return {"status": "no_data", "timestamp": datetime.now().isoformat()}
            
            # Run ML predictions
            predictions = []
            alerts = []
            
            for field_id in ml_features['field_id'].unique():
                field_features = ml_features[ml_features['field_id'] == field_id]
                
                # Generate ML prediction
                prediction = self.disease_predictor.predict(field_features, field_id)
                predictions.append(prediction)
                
                # Generate strategic alert for all risk levels
                alert = self.alert_generator.generate_alert(prediction, field_id)
                if alert:  # Only add if alert was generated successfully
                    alerts.append(alert)
            
            # Save predictions and alerts
            self._save_batch_results(predictions, alerts)
            
            # Update service state
            self.last_batch_prediction = {
                "timestamp": datetime.now().isoformat(),
                "predictions_count": len(predictions),
                "alerts_generated": len(alerts),
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "predictions": predictions,
                "alerts": alerts
            }
            
            logger.info(f"Batch prediction completed: {len(predictions)} predictions, {len(alerts)} alerts")
            return self.last_batch_prediction
            
        except Exception as e:
            logger.error(f"Error in batch prediction: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def _save_batch_results(self, predictions: List[Dict], alerts: List[Dict]):
        """Save batch prediction results to storage"""
        try:
            # Save to PostgreSQL via sync service
            from .data_loading.data_sync_service import DataSyncService
            sync_service = DataSyncService(self.spark)
            
            # Save predictions
            for prediction in predictions:
                sync_service.sync_ml_predictions_to_postgres(prediction)
            
            # Save alerts
            for alert in alerts:
                sync_service.sync_alerts_to_postgres(alert)
            
            logger.info(f"Saved {len(predictions)} predictions and {len(alerts)} alerts")
            
        except Exception as e:
            logger.error(f"Error saving batch results: {e}")
    
    def get_recent_predictions(self) -> Dict[str, Any]:
        """Get recent predictions for dashboard"""
        try:
            # Load recent predictions from PostgreSQL
            from .data_loading.data_sync_service import DataSyncService
            sync_service = DataSyncService(self.spark)
            
            # This would typically query the database
            # For now, return the last batch prediction
            if self.last_batch_prediction:
                return {
                    "timestamp": datetime.now().isoformat(),
                    "predictions": self.last_batch_prediction.get("predictions", []),
                    "source": "last_batch"
                }
            else:
                return {"predictions": [], "message": "No recent predictions"}
                
        except Exception as e:
            logger.error(f"Error getting recent predictions: {e}")
            return {"error": str(e)}
    
    def get_active_alerts(self) -> Dict[str, Any]:
        """Get active alerts from database (with refactored system integration)"""
        try:
            # Use the new centralized repository if available
            if self.alert_manager:
                alerts = self.alert_manager.alert_repository.get_active_alerts()
                return {
                    "timestamp": datetime.now().isoformat(),
                    "alerts": alerts,
                    "source": "refactored_alert_repository",
                    "count": len(alerts)
                }
            else:
                # Fallback to legacy query
                alerts = self._query_alerts_from_database()
                return {
                    "timestamp": datetime.now().isoformat(),
                    "alerts": alerts,
                    "source": "legacy_database_query",
                    "count": len(alerts)
                }
                
        except Exception as e:
            logger.error(f"Error getting active alerts: {e}")
            return {"error": str(e)}
    
    def _query_alerts_from_database(self) -> List[Dict[str, Any]]:
        """Legacy alert query method (kept as fallback)"""
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            # PostgreSQL connection
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB', 'crop_disease_ml'),
                user=os.getenv('POSTGRES_USER', 'ml_user'),
                password=os.getenv('POSTGRES_PASSWORD', 'ml_password')
            )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Query active alerts from the last 24 hours
            cursor.execute("""
                SELECT 
                    id,
                    field_id,
                    alert_timestamp,
                    alert_type,
                    severity,
                    message,
                    details,
                    prediction_id,
                    status,
                    created_at
                FROM alerts 
                WHERE status = 'ACTIVE' 
                AND alert_timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY alert_timestamp DESC
                LIMIT 100
            """)
            
            alerts = []
            for row in cursor.fetchall():
                alert = dict(row)
                # Convert datetime objects to strings
                alert['alert_timestamp'] = alert['alert_timestamp'].isoformat() if alert['alert_timestamp'] else None
                alert['created_at'] = alert['created_at'].isoformat() if alert['created_at'] else None
                alerts.append(alert)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Retrieved {len(alerts)} active alerts from legacy database query")
            return alerts
            
        except Exception as e:
            logger.error(f"Error querying alerts from database: {e}")
            return []
    
    def get_models_status(self) -> Dict[str, Any]:
        """Get ML models status"""
        return {
            "timestamp": datetime.now().isoformat(),
            "models": {
                "disease_predictor": {
                    "status": "active" if self.disease_predictor.is_initialized else "inactive",
                    "version": self.disease_predictor.model_version,
                    "last_training": self.disease_predictor.last_training_date
                },
                "alert_generator": {
                    "status": "active" if self.alert_generator.is_initialized else "inactive",
                    "version": self.alert_generator.version
                }
            },
            "service_status": {
                "initialized": self.is_initialized,
                "last_batch": self.last_batch_prediction.get("timestamp") if self.last_batch_prediction else None
            }
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get overall service status"""
        return {
            "service": "ml-service",
            "mode": "dual-mode-refactored",
            "timestamp": datetime.now().isoformat(),
            "status": "running",
            "initialized": self.is_initialized,
            "last_batch_prediction": self.last_batch_prediction.get("timestamp") if self.last_batch_prediction else None,
            "models_status": self.get_models_status(),
            "alert_system": {
                "real_time_alerts": "refactored_centralized_system",
                "batch_alerts": "ml_disease_predictions"
            }
        }
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down ML service...")
        if self.spark:
            self.spark.stop() 