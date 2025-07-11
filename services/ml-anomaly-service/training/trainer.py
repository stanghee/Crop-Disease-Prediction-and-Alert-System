#!/usr/bin/env python3
"""
ML Training Job - Isolation Forest for anomaly detection
Handles training with any amount of available data (not just 30 days)
"""

import logging
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import pickle
import json

from utils.feature_config import CORE_FEATURES, MODEL_CONFIG
from storage.model_manager import ModelManager

logger = logging.getLogger(__name__)

class AnomalyTrainer:
    """Train Isolation Forest model on Gold zone data"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model_manager = ModelManager()
        
    def train_model(self, days_back: int = 30, min_records: int = 100):
        """
        Train Isolation Forest model
        Args:
            days_back: Number of days of historical data to use
            min_records: Minimum records needed for training
        """
        logger.info(f"Starting training with {days_back} days of data")
        
        try:
            # Load training data from Gold zone
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Read Gold zone ML features
            gold_df = self.spark.read.parquet("s3a://gold/ml_feature/*.parquet") \
                .filter(col("processing_timestamp") >= start_date) \
                .filter(col("processing_timestamp") <= end_date)
            
            # Check if we have enough data
            record_count = gold_df.count()
            logger.info(f"Found {record_count} records for training")
            
            if record_count < min_records:
                # If not enough data, take whatever we have
                logger.warning(f"Only {record_count} records available (< {min_records})")
                gold_df = self.spark.read.parquet("s3a://gold/ml_feature/*.parquet")
                record_count = gold_df.count()
                
                if record_count < 10:  # Absolute minimum
                    raise ValueError(f"Not enough data for training: {record_count} records")
            
            # Select features and convert to Pandas
            feature_df = gold_df.select(CORE_FEATURES + ["field_id", "processing_timestamp"])
            pandas_df = feature_df.toPandas()
            
            # Handle missing values
            pandas_df[CORE_FEATURES] = pandas_df[CORE_FEATURES].fillna(pandas_df[CORE_FEATURES].median())
            
            # Prepare features
            X = pandas_df[CORE_FEATURES].values
            
            # Train scaler
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # Train Isolation Forest
            logger.info("Training Isolation Forest model...")
            model = IsolationForest(**MODEL_CONFIG)
            model.fit(X_scaled)
            
            # Calculate training metrics
            predictions = model.predict(X_scaled)
            anomaly_scores = model.score_samples(X_scaled)
            
            anomaly_count = (predictions == -1).sum()
            anomaly_rate = anomaly_count / len(predictions)
            
            # Model metadata
            metadata = {
                'model_version': f"v{datetime.now().strftime('%Y-%m-%d_%H-%M')}",
                'training_date': datetime.now().isoformat(),
                'features': CORE_FEATURES,
                'record_count': int(record_count),
                'days_of_data': days_back,
                'actual_date_range': {
                    'start': pandas_df['processing_timestamp'].min().isoformat(),
                    'end': pandas_df['processing_timestamp'].max().isoformat()
                },
                'model_config': MODEL_CONFIG,
                'training_metrics': {
                    'anomaly_count': int(anomaly_count),
                    'anomaly_rate': float(anomaly_rate),
                    'score_distribution': {
                        'min': float(anomaly_scores.min()),
                        'max': float(anomaly_scores.max()),
                        'mean': float(anomaly_scores.mean()),
                        'std': float(anomaly_scores.std())
                    }
                }
            }
            
            # Save model and scaler
            model_path = self.model_manager.save_model(model, scaler, metadata)
            logger.info(f"Model saved to: {model_path}")
            
            return {
                'status': 'success',
                'model_version': metadata['model_version'],
                'training_metrics': metadata['training_metrics'],
                'model_path': model_path
            }
            
        except Exception as e:
            logger.error(f"Training failed: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def manual_retrain(self):
        """Manual retrain endpoint - uses all available data"""
        logger.info("Manual retraining triggered")
        
        # First try with 30 days
        result = self.train_model(days_back=30)
        
        # If not enough data, try with 7 days
        if result['status'] == 'failed' and 'Not enough data' in result.get('error', ''):
            logger.info("Retrying with 7 days of data")
            result = self.train_model(days_back=7)
        
        # If still not enough, try with 1 day
        if result['status'] == 'failed' and 'Not enough data' in result.get('error', ''):
            logger.info("Retrying with 1 day of data")
            result = self.train_model(days_back=1)
            
        return result
