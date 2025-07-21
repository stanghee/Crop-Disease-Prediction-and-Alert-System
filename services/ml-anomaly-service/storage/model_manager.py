#!/usr/bin/env python3
"""
Model Manager - Handles model storage and versioning in MinIO
"""

import os
import logging
import pickle
import json
from datetime import datetime
from minio import Minio
from io import BytesIO
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import StandardScalerModel

logger = logging.getLogger(__name__)

class ModelManager:
    """Manage ML model storage and versioning"""
    
    def __init__(self):
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )
        self.bucket = "ml-models" 
        self.ensure_bucket()
        
    def ensure_bucket(self):
        """Ensure ML bucket exists"""
        if not self.minio_client.bucket_exists(self.bucket):
            self.minio_client.make_bucket(self.bucket)
            logger.info(f"Created bucket: {self.bucket}")
    
    def save_model(self, model, scaler_model, metadata):
        """Save Spark ML model and scaler to MinIO (via s3a path) and save metadata"""
        version = metadata['model_version']
        # Path su MinIO (s3a)
        base_path = f"s3a://{self.bucket}/models/kmeans/{version}"
        model_path = f"{base_path}/model"
        scaler_path = f"{base_path}/scaler"
        metadata_path = f"models/kmeans/{version}_metadata.json"
        # Salva modello e scaler Spark ML
        model.write().overwrite().save(model_path)
        scaler_model.write().overwrite().save(scaler_path)
        # Salva metadati
        metadata_data = json.dumps(metadata, indent=2).encode()
        self.minio_client.put_object(
            self.bucket,
            metadata_path,
            BytesIO(metadata_data),
            len(metadata_data)
        )
        # Aggiorna latest
        self.update_latest_model(version)
        logger.info(f"Model {version} saved successfully (Spark ML)")
        return model_path

    def load_latest_model(self, spark=None):
        """Load the latest Spark ML model and scaler"""
        try:
            # Get latest model version
            latest_data = self.minio_client.get_object(
                self.bucket, 
                "models/kmeans/latest.json"
            )
            latest_info = json.loads(latest_data.read())
            version = latest_info['version']
            base_path = f"s3a://{self.bucket}/models/kmeans/{version}"
            model_path = f"{base_path}/model"
            scaler_path = f"{base_path}/scaler"
            metadata_path = f"models/kmeans/{version}_metadata.json"
            if spark is None:
                from pyspark.sql import SparkSession
                import os
                # Use cluster configuration if available
                spark_master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
                if spark_master_url.startswith("spark://"):
                    # Connect to Spark cluster
                    driver_host = os.getenv("SPARK_DRIVER_HOST", "ml-anomaly-service")
                    driver_port = "4041"  # Different port to avoid conflicts
                    spark = SparkSession.builder \
                        .appName("ModelManager") \
                        .master(spark_master_url) \
                        .config("spark.driver.host", driver_host) \
                        .config("spark.driver.port", driver_port) \
                        .config("spark.driver.bindAddress", "0.0.0.0") \
                        .getOrCreate()
                else:
                    # Fallback to local mode
                    spark = SparkSession.builder.getOrCreate()
            model = KMeansModel.load(model_path)
            scaler_model = StandardScalerModel.load(scaler_path)
            # Load metadata
            metadata_data = self.minio_client.get_object(
                self.bucket,
                metadata_path
            )
            metadata = json.loads(metadata_data.read())
            logger.info(f"Loaded model version: {version}")
            return model, scaler_model, metadata
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return None, None, None

    def update_latest_model(self, version):
        """Update pointer to latest model"""
        latest_info = {
            'version': version,
            'updated_at': datetime.now().isoformat()
        }
        latest_data = json.dumps(latest_info).encode()
        self.minio_client.put_object(
            self.bucket,
            "models/kmeans/latest.json",
            BytesIO(latest_data),
            len(latest_data)
        )
