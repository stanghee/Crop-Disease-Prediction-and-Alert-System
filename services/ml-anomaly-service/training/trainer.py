#!/usr/bin/env python3
"""
ML Training Job - Kmeans for anomaly detection
Handles training with any amount of available data (not just 30 days)
"""

import logging
import math
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import json

from pyspark.ml.feature import VectorAssembler, StandardScaler as SparkStandardScaler
from pyspark.ml.clustering import KMeans
from utils.feature_config import CORE_FEATURES, KMEANS_CONFIG
from storage.model_manager import ModelManager
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)

class AnomalyTrainer:
    """Train Kmeans model on Gold zone data"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model_manager = ModelManager()
        
    def train_model(self, days_back: int = 30, min_records: int = 100, k: int = None):
        """
        Train KMeans model (Spark ML)
        Args:
            days_back: Number of days of historical data to use
            min_records: Minimum records needed for training
            k: Number of clusters (check config)
        """
        logger.info(f"Starting training with {days_back} days of data")
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            gold_df = self.spark.read.parquet("s3a://gold/ml_feature/**/*.parquet") \
                .filter(col("processing_timestamp") >= start_date) \
                .filter(col("processing_timestamp") <= end_date)
            record_count = gold_df.count()
            logger.info(f"Found {record_count} records for training")
            if record_count < min_records:
                logger.warning(f"Only {record_count} records available (< {min_records})")
                gold_df = self.spark.read.parquet("s3a://gold/ml_feature/**/*.parquet")
                record_count = gold_df.count()
                if record_count < 1:
                    raise ValueError(f"Not enough data for training: {record_count} records")
            # Handle missing values (fill with median)
            for f in CORE_FEATURES:
                median = gold_df.approxQuantile(f, [0.5], 0.01)[0]
                gold_df = gold_df.na.fill({f: median})
            # Filter out rows with null values
            for f in CORE_FEATURES:
                gold_df = gold_df.filter(col(f).isNotNull())
            # Double type
            for f in CORE_FEATURES:
                gold_df = gold_df.withColumn(f, col(f).cast(DoubleType()))
            # Assemble features
            assembler = VectorAssembler(inputCols=CORE_FEATURES, outputCol="features_vec")
            assembled_df = assembler.transform(gold_df)
            # Scale features 
            scaler = SparkStandardScaler(inputCol="features_vec", outputCol="scaled_features", withMean=True, withStd=True)
            scaler_model = scaler.fit(assembled_df)
            scaled_df = scaler_model.transform(assembled_df)
            # KMeans config
            k_val = k if k is not None else KMEANS_CONFIG['k']
            kmeans = KMeans(featuresCol="scaled_features", k=k_val, maxIter=KMEANS_CONFIG['maxIter'], seed=KMEANS_CONFIG['seed'])
            model = kmeans.fit(scaled_df)

            cost = model.summary.trainingCost
            logger.info(f"Training completed with K-Means cost: {cost}")
            
            avg_distance = cost / scaled_df.count() if scaled_df.count() > 0 else 0.0 #TODO: Can be used for anomaly detection as a threshold (not in the demo pahse because we have to force anomaly detection)
            max_distance = cost  # TODO: fix it to the correct max distance 

            # Model metadata
            metadata = {
                'model_version': f"v{datetime.now().strftime('%Y-%m-%d_%H-%M')}",
                'training_date': datetime.now().isoformat(),
                'features': CORE_FEATURES,
                'record_count': int(record_count),
                'days_of_data': days_back,
                'actual_date_range': {
                    'start': str(gold_df.agg({"processing_timestamp": "min"}).collect()[0][0]),
                    'end': str(gold_df.agg({"processing_timestamp": "max"}).collect()[0][0])
                },
                'model_config': {'k': k_val, 'maxIter': KMEANS_CONFIG['maxIter'], 'seed': KMEANS_CONFIG['seed']},
                'training_metrics': {
                    'avg_distance': float(avg_distance),
                    'max_distance': float(max_distance)
                }
            }
            # Save model
            model_path = self.model_manager.save_model(model, scaler_model, metadata)
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
    
    # Automatic training scheduled after 4 minutes from service startup
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
