#!/usr/bin/env python3
"""
PostgreSQL Client - Save ML predictions and metrics
"""

import os
import logging
import psycopg2
from psycopg2.extras import execute_batch
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class PostgresClient:
    """Handle PostgreSQL operations for ML results"""
    
    def __init__(self):
        self.connection_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'crop_disease_ml'),
            'user': os.getenv('POSTGRES_USER', 'ml_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'ml_password')
        }
        self.ensure_tables()
    
    def get_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(**self.connection_params)
    
    def ensure_tables(self):
        """Create tables if they don't exist"""
        create_predictions_table = """
        CREATE TABLE IF NOT EXISTS ml_predictions (
            prediction_id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            field_id VARCHAR(50) NOT NULL,
            location VARCHAR(100),
            anomaly_score FLOAT NOT NULL,
            is_anomaly BOOLEAN NOT NULL,
            severity VARCHAR(20),
            recommendations TEXT,
            model_version VARCHAR(50),
            features JSONB,
            processing_time_ms INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_field_timestamp ON ml_predictions(field_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_anomaly ON ml_predictions(is_anomaly, timestamp);
        """
        
        create_metrics_table = """
        CREATE TABLE IF NOT EXISTS ml_metrics (
            metric_id SERIAL PRIMARY KEY,
            model_version VARCHAR(50),
            metric_name VARCHAR(100),
            metric_value FLOAT,
            evaluated_at TIMESTAMP,
            data_period_start TIMESTAMP,
            data_period_end TIMESTAMP,
            metadata JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_predictions_table)
                    cursor.execute(create_metrics_table)
                    conn.commit()
            logger.info("PostgreSQL tables ready")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
    
    def save_predictions(self, predictions):
        """Save batch of predictions to PostgreSQL"""
        if not predictions:
            return
        
        insert_query = """
        INSERT INTO ml_predictions (
            timestamp, field_id, location, anomaly_score, 
            is_anomaly, severity, recommendations, model_version, features
        ) VALUES (
            %(timestamp)s, %(field_id)s, %(location)s, %(anomaly_score)s,
            %(is_anomaly)s, %(severity)s, %(recommendations)s, %(model_version)s, %(features)s
        )
        """
        
        try:
            # Convert Spark Row objects to dicts
            records = []
            for row in predictions:
                record = {
                    'timestamp': row.processing_timestamp,
                    'field_id': row.field_id,
                    'location': row.location,
                    'anomaly_score': float(row.anomaly_score),
                    'is_anomaly': bool(row.is_anomaly),
                    'severity': row.severity,
                    'recommendations': row.recommendations if hasattr(row, 'recommendations') else None,
                    'model_version': row.model_version,
                    'features': json.dumps({
                        'temperature': float(row.sensor_avg_temperature) if row.sensor_avg_temperature else None,
                        'humidity': float(row.sensor_avg_humidity) if row.sensor_avg_humidity else None,
                        'soil_ph': float(row.sensor_avg_soil_ph) if row.sensor_avg_soil_ph else None,
                        'temp_differential': float(row.temp_differential) if row.temp_differential else None,
                        'humidity_differential': float(row.humidity_differential) if row.humidity_differential else None,
                        'sensor_anomaly_rate': float(row.sensor_anomaly_rate) if row.sensor_anomaly_rate else None
                    })
                }
                records.append(record)
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    execute_batch(cursor, insert_query, records)
                    conn.commit()
            
            logger.info(f"Saved {len(records)} predictions to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Failed to save predictions: {e}")
    
    def save_metrics(self, model_version, metrics):
        """Save model metrics"""
        insert_query = """
        INSERT INTO ml_metrics (
            model_version, metric_name, metric_value, 
            evaluated_at, metadata
        ) VALUES (
            %s, %s, %s, %s, %s
        )
        """
        
        try:
            records = []
            for metric_name, metric_value in metrics.items():
                if isinstance(metric_value, (int, float)):
                    records.append((
                        model_version,
                        metric_name,
                        float(metric_value),
                        datetime.now(),
                        json.dumps({})
                    ))
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, records)
                    conn.commit()
            
            logger.info(f"Saved {len(records)} metrics for model {model_version}")
            
        except Exception as e:
            logger.error(f"Failed to save metrics: {e}")
    
    def get_recent_predictions(self, field_id=None, limit=100):
        """Get recent predictions"""
        query = """
        SELECT * FROM ml_predictions 
        WHERE (%s IS NULL OR field_id = %s)
        ORDER BY timestamp DESC
        LIMIT %s
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (field_id, field_id, limit))
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get predictions: {e}")
            return []
