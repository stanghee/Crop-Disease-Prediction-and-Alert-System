#!/usr/bin/env python3
"""
Data Sync Service - Manages data synchronization between MinIO and PostgreSQL
Handles real-time and batch synchronization for ML data storage
"""

import os
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio
import pandas as pd
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class DataSyncService:
    """
    Manages data synchronization between MinIO (S3) and PostgreSQL
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
        
        # MinIO connection
        self.minio_client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False
        )
        
        # Storage paths
        self.gold_path = os.getenv('GOLD_PATH', 's3a://gold/')
        self.ml_path = os.getenv('ML_PATH', 's3a://ml/')
        
        # Initialize ML buckets
        self._setup_ml_buckets()
    
    def _setup_ml_buckets(self):
        """Create ML-specific MinIO buckets if they don't exist"""
        ml_buckets = ['ml', 'ml-models', 'ml-training-data', 'ml-results']
        
        for bucket in ml_buckets:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"Created ML bucket: {bucket}")
    
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
            
            # Also save to MinIO for backup and analytics
            self._save_prediction_to_minio(prediction_data)
            
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
            
            # Also save to MinIO for backup
            self._save_alert_to_minio(alert_data)
            
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
    
    def batch_sync_gold_to_postgres(self, data_type: str, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Batch sync Gold zone data to PostgreSQL for historical analysis
        """
        start_time = time.time()
        stats = {'processed': 0, 'synced': 0, 'failed': 0}
        
        try:
            # Read from Gold zone
            gold_df = self.spark.read.format("delta").load(f"{self.gold_path}{data_type}/")
            
            # Convert to pandas for easier PostgreSQL insertion
            pandas_df = gold_df.limit(batch_size).toPandas()
            
            if pandas_df.empty:
                logger.info(f"No data to sync for {data_type}")
                return stats
            
            # Insert into PostgreSQL based on data type
            if data_type == 'sensor_metrics/recent':
                stats = self._batch_insert_sensor_metrics(pandas_df)
            elif data_type == 'ml_features/sensor':
                stats = self._batch_insert_ml_features(pandas_df)
            else:
                logger.warning(f"Unknown data type for batch sync: {data_type}")
                return stats
            
            # Log sync activity
            sync_duration = int((time.time() - start_time) * 1000)
            self._log_sync_activity('BATCH_SYNC', data_type, 'postgres', 
                                  stats['processed'], stats['synced'], stats['failed'], 
                                  sync_duration, 'SUCCESS' if stats['failed'] == 0 else 'PARTIAL')
            
            logger.info(f"Batch sync completed for {data_type}: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to batch sync {data_type}: {e}")
            sync_duration = int((time.time() - start_time) * 1000)
            self._log_sync_activity('BATCH_SYNC', data_type, 'postgres', 
                                  stats['processed'], stats['synced'], stats['failed'], 
                                  sync_duration, 'FAILED', str(e))
            return stats
    
    def _batch_insert_sensor_metrics(self, df: pd.DataFrame) -> Dict[str, int]:
        """Batch insert sensor metrics to PostgreSQL"""
        stats = {'processed': len(df), 'synced': 0, 'failed': 0}
        
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            
            # Create temporary table for batch insert
            cursor.execute("""
                CREATE TEMP TABLE temp_sensor_metrics (
                    field_id VARCHAR(50),
                    current_temperature DECIMAL(5,2),
                    current_humidity DECIMAL(5,2),
                    current_soil_ph DECIMAL(4,2),
                    status VARCHAR(20),
                    anomaly_count_30min INTEGER,
                    created_at TIMESTAMP WITH TIME ZONE
                ) ON COMMIT DROP
            """)
            
            # Insert data into temporary table
            for _, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO temp_sensor_metrics VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row.get('field_id'),
                        row.get('current_temperature'),
                        row.get('current_humidity'),
                        row.get('current_soil_ph'),
                        row.get('status'),
                        row.get('anomaly_count_30min'),
                        row.get('created_at')
                    ))
                    stats['synced'] += 1
                except Exception as e:
                    logger.warning(f"Failed to insert sensor metric row: {e}")
                    stats['failed'] += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to batch insert sensor metrics: {e}")
            stats['failed'] = stats['processed'] - stats['synced']
        
        return stats
    
    def _batch_insert_ml_features(self, df: pd.DataFrame) -> Dict[str, int]:
        """Batch insert ML features to PostgreSQL"""
        stats = {'processed': len(df), 'synced': 0, 'failed': 0}
        
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            
            # Insert ML features (simplified for this example)
            for _, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO ml_predictions (
                            field_id, prediction_timestamp, disease_probability,
                            avg_temperature, avg_humidity, avg_soil_ph,
                            anomaly_rate, data_quality_score, model_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row.get('field_id'),
                        datetime.now(),
                        0.5,  # Default probability for batch data
                        row.get('avg_temperature'),
                        row.get('avg_humidity'),
                        row.get('avg_soil_ph'),
                        row.get('anomaly_rate'),
                        row.get('data_quality_score'),
                        'batch_sync_v1.0'
                    ))
                    stats['synced'] += 1
                except Exception as e:
                    logger.warning(f"Failed to insert ML feature row: {e}")
                    stats['failed'] += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to batch insert ML features: {e}")
            stats['failed'] = stats['processed'] - stats['synced']
        
        return stats
    
    def _save_prediction_to_minio(self, prediction_data: Dict[str, Any]):
        """Save prediction data to MinIO for backup and analytics"""
        try:
            # Save as JSON file with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            field_id = prediction_data.get('field_id', 'unknown')
            filename = f"predictions/{field_id}/{timestamp}_{field_id}_prediction.json"
            
            # Convert to JSON string
            json_data = json.dumps(prediction_data, default=str)
            
            # Upload to MinIO
            self.minio_client.put_object(
                'ml',
                filename,
                json_data.encode('utf-8'),
                length=len(json_data.encode('utf-8')),
                content_type='application/json'
            )
            
        except Exception as e:
            logger.warning(f"Failed to save prediction to MinIO: {e}")
    
    def _save_alert_to_minio(self, alert_data: Dict[str, Any]):
        """Save alert data to MinIO for backup"""
        try:
            # Save as JSON file with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            field_id = alert_data.get('field_id', 'unknown')
            filename = f"alerts/{field_id}/{timestamp}_{field_id}_alert.json"
            
            # Convert to JSON string
            json_data = json.dumps(alert_data, default=str)
            
            # Upload to MinIO
            self.minio_client.put_object(
                'ml',
                filename,
                json_data.encode('utf-8'),
                length=len(json_data.encode('utf-8')),
                content_type='application/json'
            )
            
        except Exception as e:
            logger.warning(f"Failed to save alert to MinIO: {e}")
    
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
    
    def get_sync_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get sync statistics for monitoring"""
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get sync statistics for the last N hours
            cursor.execute("""
                SELECT 
                    sync_type,
                    COUNT(*) as total_syncs,
                    SUM(records_processed) as total_processed,
                    SUM(records_synced) as total_synced,
                    SUM(records_failed) as total_failed,
                    AVG(sync_duration_ms) as avg_duration_ms,
                    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_syncs,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_syncs
                FROM data_sync_log
                WHERE sync_timestamp >= NOW() - INTERVAL '%s hours'
                GROUP BY sync_type
                ORDER BY total_syncs DESC
            """, (hours,))
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return {
                'sync_statistics': [dict(row) for row in results],
                'time_range_hours': hours,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get sync statistics: {e}")
            return {'error': str(e)}
    
    def cleanup_old_data(self, days: int = 30):
        """Clean up old sync logs and temporary data"""
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            
            # Delete old sync logs
            cursor.execute("""
                DELETE FROM data_sync_log 
                WHERE sync_timestamp < NOW() - INTERVAL '%s days'
            """, (days,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Cleaned up {deleted_count} old sync log entries")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}") 