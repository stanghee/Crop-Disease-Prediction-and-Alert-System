#!/usr/bin/env python3
"""
Feature configuration for ML anomaly detection
Simple core features as requested
"""

# Core features for anomaly detection
CORE_FEATURES = [
    'sensor_avg_temperature',
    'sensor_avg_humidity', 
    'sensor_avg_soil_ph',
    'temp_differential',      # sensor vs weather temperature difference
    'humidity_differential',  # sensor vs weather humidity difference
    'sensor_anomaly_rate'     # existing anomaly rate from data quality
]

# Model configuration
MODEL_CONFIG = {
    'contamination': 0.05,    # Expect 5% anomalies
    'n_estimators': 100,
    'max_samples': 'auto',
    'random_state': 42,
    'n_jobs': -1              # Use all cores
}

# Anomaly thresholds
ANOMALY_THRESHOLD = 0.7       # Score > 0.7 = anomaly
CRITICAL_THRESHOLD = 0.9      # Score > 0.9 = critical

# Processing configuration  
BATCH_INTERVAL_SECONDS = 30   # Process every 30 seconds
WATERMARK_DURATION = "5 minutes"
