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
    
    def save_model(self, model, scaler, metadata):
        """Save model, scaler and metadata to MinIO"""
        version = metadata['model_version']
        
        # Save model
        model_data = pickle.dumps(model)
        model_path = f"models/isolation_forest/{version}_model.pkl"
        self.minio_client.put_object(
            self.bucket, 
            model_path, 
            BytesIO(model_data), 
            len(model_data)
        )
        
        # Save scaler
        scaler_data = pickle.dumps(scaler)
        scaler_path = f"models/isolation_forest/{version}_scaler.pkl"
        self.minio_client.put_object(
            self.bucket,
            scaler_path,
            BytesIO(scaler_data),
            len(scaler_data)
        )
        
        # Save metadata
        metadata_data = json.dumps(metadata, indent=2).encode()
        metadata_path = f"models/isolation_forest/{version}_metadata.json"
        self.minio_client.put_object(
            self.bucket,
            metadata_path,
            BytesIO(metadata_data),
            len(metadata_data)
        )
        
        # Update latest model pointer
        self.update_latest_model(version)
        
        logger.info(f"Model {version} saved successfully")
        return f"s3a://{self.bucket}/{model_path}"
    
    def load_latest_model(self):
        """Load the latest model and scaler"""
        try:
            # Get latest model version
            latest_data = self.minio_client.get_object(
                self.bucket, 
                "models/isolation_forest/latest.json"
            )
            latest_info = json.loads(latest_data.read())
            version = latest_info['version']
            
            # Load model
            model_data = self.minio_client.get_object(
                self.bucket,
                f"models/isolation_forest/{version}_model.pkl"
            )
            model = pickle.loads(model_data.read())
            
            # Load scaler
            scaler_data = self.minio_client.get_object(
                self.bucket,
                f"models/isolation_forest/{version}_scaler.pkl"
            )
            scaler = pickle.loads(scaler_data.read())
            
            # Load metadata
            metadata_data = self.minio_client.get_object(
                self.bucket,
                f"models/isolation_forest/{version}_metadata.json"
            )
            metadata = json.loads(metadata_data.read())
            
            logger.info(f"Loaded model version: {version}")
            return model, scaler, metadata
            
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
            "models/isolation_forest/latest.json",
            BytesIO(latest_data),
            len(latest_data)
        )
