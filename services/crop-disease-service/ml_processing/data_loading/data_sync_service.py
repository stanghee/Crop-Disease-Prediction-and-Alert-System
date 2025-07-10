#!/usr/bin/env python3
"""
Data Sync Service - Manages data synchronization between PostgreSQL and the crop disease service
Handles real-time and batch synchronization for data storage
"""

import os
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class DataSyncService:
    """
    Manages data synchronization between PostgreSQL and the crop disease service
    Implements hybrid storage strategy for optimal performance
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
        # PostgreSQL connection
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'crop_disease_ml'),
            'user': os.getenv('POSTGRES_USER', 'ml_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'ml_password')
        }
    
    def _get_postgres_connection(self):
        """Get PostgreSQL connection with error handling"""
        try:
            conn = psycopg2.connect(**self.postgres_config)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def sync_ml_predictions_to_postgres(self, prediction_data: Dict[str, Any]) -> bool:
        """
        Sync ML prediction data to PostgreSQL for real-time dashboard access
        """
        start_time = time.time()
        
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            
            # Insert prediction into PostgreSQL
            insert_query = """
                INSERT INTO ml_predictions (
                    field_id, prediction_timestamp, disease_probability, disease_type,
                    confidence_score, risk_level, avg_temperature, avg_humidity, avg_soil_ph,
                    temperature_range, humidity_range, anomaly_rate, data_quality_score,
                    weather_avg_temperature, weather_avg_humidity, weather_avg_wind_speed,
                    weather_avg_uv_index, model_version, model_type, features_used,
                    processing_time_ms
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            cursor.execute(insert_query, (
                prediction_data.get('field_id'),
                prediction_data.get('prediction_timestamp'),
                prediction_data.get('disease_probability'),
                prediction_data.get('disease_type'),
                prediction_data.get('confidence_score'),
                prediction_data.get('risk_level'),
                prediction_data.get('avg_temperature'),
                prediction_data.get('avg_humidity'),
                prediction_data.get('avg_soil_ph'),
                prediction_data.get('temperature_range'),
                prediction_data.get('humidity_range'),
                prediction_data.get('anomaly_rate'),
                prediction_data.get('data_quality_score'),
                prediction_data.get('weather_avg_temperature'),
                prediction_data.get('weather_avg_humidity'),
                prediction_data.get('weather_avg_wind_speed'),
                prediction_data.get('weather_avg_uv_index'),
                prediction_data.get('model_version'),
                prediction_data.get('model_type'),
                json.dumps(prediction_data.get('features_used', [])),
                prediction_data.get('processing_time_ms')
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Log sync activity
            sync_duration = int((time.time() - start_time) * 1000)
            self._log_sync_activity('REAL_TIME_SYNC', 'ml_predictions', 'postgres', 1, 1, 0, sync_duration, 'SUCCESS')
            
            logger.info(f"Successfully synced prediction for field {prediction_data.get('field_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync prediction to PostgreSQL: {e}")
            sync_duration = int((time.time() - start_time) * 1000)
            self._log_sync_activity('REAL_TIME_SYNC', 'ml_predictions', 'postgres', 1, 0, 1, sync_duration, 'FAILED', str(e))
            return False
    
    def sync_alerts_to_postgres(self, alert_data: Dict[str, Any]) -> bool:
        """
        Sync alert data to PostgreSQL for immediate access
        """
        start_time = time.time()
        
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            
            # Insert alert into PostgreSQL
            insert_query = """
                INSERT INTO alerts (
                    field_id, alert_timestamp, alert_type, severity, message,
                    details, prediction_id, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                alert_data.get('field_id'),
                alert_data.get('alert_timestamp'),
                alert_data.get('alert_type'),
                alert_data.get('severity'),
                alert_data.get('message'),
                json.dumps(alert_data.get('details', {})),
                alert_data.get('prediction_id'),
                alert_data.get('status', 'ACTIVE')
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Log sync activity
            sync_duration = int((time.time() - start_time) * 1000)
            self._log_sync_activity('REAL_TIME_SYNC', 'alerts', 'postgres', 1, 1, 0, sync_duration, 'SUCCESS')
            
            logger.info(f"Successfully synced alert for field {alert_data.get('field_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync alert to PostgreSQL: {e}")
            sync_duration = int((time.time() - start_time) * 1000)
            self._log_sync_activity('REAL_TIME_SYNC', 'alerts', 'postgres', 1, 0, 1, sync_duration, 'FAILED', str(e))
            return False
    

    
    def _log_sync_activity(self, sync_type: str, source_table: str, target_location: str,
                          records_processed: int, records_synced: int, records_failed: int,
                          sync_duration_ms: int, status: str, error_message: str = None):
        """Log sync activity to PostgreSQL"""
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO data_sync_log (
                    sync_type, source_table, target_location, records_processed,
                    records_synced, records_failed, sync_duration_ms, status, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                sync_type, source_table, target_location, records_processed,
                records_synced, records_failed, sync_duration_ms, status, error_message
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to log sync activity: {e}")
    
 