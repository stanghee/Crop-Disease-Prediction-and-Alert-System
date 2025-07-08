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
from models.disease_predictor import DiseasePredictor
from models.alert_generator import AlertGenerator
from data.data_loader import DataLoader

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
        self.last_realtime_analysis = None
        self.is_initialized = False
        
        # Initialize models
        self._initialize_models()
    
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
    
    def process_realtime_data(self) -> Dict[str, Any]:
        """
        Real-time processing (Mode 1): Every 5 minutes
        Purpose: Immediate insights and current status
        """
        start_time = time.time()
        
        try:
            logger.info("Starting real-time data processing...")
            
            # Load current sensor data from Gold zone (for ML analysis)
            current_data = self.data_loader.load_realtime_data()
            
            # Check weather alerts separately (simple threshold-based)
            weather_data = self.data_loader.check_weather_alerts()
            
            # Convert weather data to alerts using unified alert generator
            weather_alerts = []
            for weather_condition in weather_data:
                weather_alert = self.alert_generator.generate_threshold_alert(
                    "WEATHER_ALERT", weather_condition
                )
                if weather_alert:
                    weather_alerts.append(weather_alert)
            
            if current_data.empty:
                logger.warning("No real-time sensor data available")
                return {
                    "status": "no_sensor_data", 
                    "timestamp": datetime.now().isoformat(),
                    "weather_alerts": weather_alerts
                }
            
            # Process each field for sensor analysis
            results = []
            sensor_alerts = []
            
            for field_id in current_data['field_id'].unique():
                field_data = current_data[current_data['field_id'] == field_id]
                
                # Real-time sensor analysis based on sensor data only
                analysis = self._analyze_realtime_field(field_data, field_id)
                results.append(analysis)
                
                # Generate sensor alert using unified alert generator
                sensor_data = analysis['current_data']
                sensor_alert = self.alert_generator.generate_threshold_alert(
                    "SENSOR_ANOMALY", sensor_data, field_id
                )
                if sensor_alert:
                    sensor_alerts.append(sensor_alert)
            
            # Save all threshold-based alerts to database
            all_threshold_alerts = sensor_alerts + weather_alerts
            if all_threshold_alerts:
                self._save_realtime_alerts(all_threshold_alerts)
            
            # Update service state
            self.last_realtime_analysis = {
                "timestamp": datetime.now().isoformat(),
                "fields_processed": len(results),
                "weather_alerts": weather_alerts,
                "sensor_alerts_generated": len(sensor_alerts),
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "results": results
            }
            
            logger.info(f"Real-time processing completed: {len(results)} fields processed, {len(weather_alerts)} weather alerts, {len(sensor_alerts)} sensor alerts")
            return self.last_realtime_analysis
            
        except Exception as e:
            logger.error(f"Error in real-time processing: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def _analyze_realtime_field(self, field_data: pd.DataFrame, field_id: str) -> Dict[str, Any]:
        """Analyze real-time data for a specific field"""
        
        # Calculate current metrics
        current_temp = field_data['avg_temperature'].iloc[-1] if not field_data.empty else None
        current_humidity = field_data['avg_humidity'].iloc[-1] if not field_data.empty else None
        current_soil_ph = field_data['avg_soil_ph'].iloc[-1] if not field_data.empty else None
        
        # Calculate trends (last 24 hours)
        if len(field_data) > 1:
            temp_trend = "↗️" if current_temp > field_data['avg_temperature'].iloc[-2] else "↘️"
            humidity_trend = "↗️" if current_humidity > field_data['avg_humidity'].iloc[-2] else "↘️"
        else:
            temp_trend = "→"
            humidity_trend = "→"
        
        # Determine current risk level
        risk_level = self._calculate_realtime_risk(current_temp, current_humidity, current_soil_ph)
        
        # Generate immediate guidance
        guidance = self._generate_realtime_guidance(risk_level, current_temp, current_humidity)
        
        return {
            "field_id": field_id,
            "timestamp": datetime.now().isoformat(),
            "current_data": {
                "temperature": current_temp,
                "humidity": current_humidity,
                "soil_ph": current_soil_ph
            },
            "trends": {
                "temperature_trend": temp_trend,
                "humidity_trend": humidity_trend
            },
            "analysis": {
                "current_risk": risk_level,
                "immediate_condition": self._get_condition_description(risk_level),
                "action_needed": guidance["action"],
                "current_guide": guidance["message"]
            }
        }
    
    def _calculate_realtime_risk(self, temp: float, humidity: float, soil_ph: float) -> str:
        """Calculate real-time risk level based on current conditions"""
        
        risk_score = 0
        
        # Temperature risk (20-25°C is optimal for many diseases)
        if 20 <= temp <= 25:
            risk_score += 2
        elif 18 <= temp <= 27:
            risk_score += 1
        
        # Humidity risk (high humidity favors disease)
        if humidity > 85:
            risk_score += 3
        elif humidity > 75:
            risk_score += 2
        elif humidity > 65:
            risk_score += 1
        
        # Soil pH risk (neutral pH is generally good)
        if 6.0 <= soil_ph <= 7.0:
            risk_score += 0
        else:
            risk_score += 1
        
        # Determine risk level
        if risk_score >= 5:
            return "HIGH"
        elif risk_score >= 3:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _generate_realtime_guidance(self, risk_level: str, temp: float, humidity: float) -> Dict[str, str]:
        """Generate real-time guidance based on current conditions"""
        
        if risk_level == "HIGH":
            return {
                "action": "Immediate monitoring required",
                "message": f"High risk conditions detected. Temperature: {temp}°C, Humidity: {humidity}%. Monitor closely for disease symptoms."
            }
        elif risk_level == "MEDIUM":
            return {
                "action": "Monitor conditions",
                "message": f"Moderate risk conditions. Temperature: {temp}°C, Humidity: {humidity}%. Check field in next 2 hours."
            }
        else:
            return {
                "action": "Continue normal operations",
                "message": f"Low risk conditions. Temperature: {temp}°C, Humidity: {humidity}%. No immediate action needed."
            }
    
    def _get_condition_description(self, risk_level: str) -> str:
        """Get description of current conditions"""
        descriptions = {
            "HIGH": "Favorable for disease development",
            "MEDIUM": "Moderate disease risk",
            "LOW": "Unfavorable for disease development"
        }
        return descriptions.get(risk_level, "Unknown")
    

    
    def _save_realtime_alerts(self, alerts: List[Dict[str, Any]]):
        """Save real-time alerts directly to PostgreSQL"""
        try:
            import psycopg2
            import json
            
            # PostgreSQL connection
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB', 'crop_disease_ml'),
                user=os.getenv('POSTGRES_USER', 'ml_user'),
                password=os.getenv('POSTGRES_PASSWORD', 'ml_password')
            )
            
            cursor = conn.cursor()
            
            # Insert alerts directly into PostgreSQL
            insert_query = """
                INSERT INTO alerts (
                    field_id, alert_timestamp, alert_type, severity, message,
                    details, prediction_id, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            for alert in alerts:
                cursor.execute(insert_query, (
                    alert.get('field_id'),
                    alert.get('alert_timestamp'),
                    alert.get('alert_type'),
                    alert.get('severity'),
                    alert.get('message'),
                    json.dumps(alert.get('details', {})),
                    alert.get('prediction_id'),
                    alert.get('status', 'ACTIVE')
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Saved {len(alerts)} real-time alerts directly to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error saving real-time alerts: {e}")
    

    
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
            from sync.data_sync_service import DataSyncService
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
            from sync.data_sync_service import DataSyncService
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
        """Get active alerts from database"""
        try:
            # Query active alerts from PostgreSQL
            alerts = self._query_alerts_from_database()
            
            return {
                "timestamp": datetime.now().isoformat(),
                "alerts": alerts,
                "source": "database",
                "count": len(alerts)
            }
                
        except Exception as e:
            logger.error(f"Error getting active alerts: {e}")
            return {"error": str(e)}
    
    def _query_alerts_from_database(self) -> List[Dict[str, Any]]:
        """Query alerts from PostgreSQL database"""
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
            
            logger.info(f"Retrieved {len(alerts)} active alerts from database")
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
                "last_realtime": self.last_realtime_analysis.get("timestamp") if self.last_realtime_analysis else None,
                "last_batch": self.last_batch_prediction.get("timestamp") if self.last_batch_prediction else None
            }
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get overall service status"""
        return {
            "service": "ml-service",
            "mode": "dual-mode",
            "timestamp": datetime.now().isoformat(),
            "status": "running",
            "initialized": self.is_initialized,
            "last_realtime_processing": self.last_realtime_analysis.get("timestamp") if self.last_realtime_analysis else None,
            "last_batch_prediction": self.last_batch_prediction.get("timestamp") if self.last_batch_prediction else None,
            "models_status": self.get_models_status()
        }
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down ML service...")
        if self.spark:
            self.spark.stop() 