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

# Model configuration for KMeans
KMEANS_CONFIG = {
    'k': 3,                # Numero di cluster, configurabile (ridotto per test)
    'maxIter': 20,         # Iterazioni massime
    'seed': 42             # Random seed
}

# Soglie per anomaly score (0.0-1.0+ range)
ANOMALY_DISTANCE_THRESHOLD = 0.3   # Score > 0.3 = anomalia
CRITICAL_DISTANCE_THRESHOLD = 0.7  # Score > 0.7 = criticità

# Processing configuration  
BATCH_INTERVAL_SECONDS = 30   # Process every 30 seconds
WATERMARK_DURATION = "5 minutes"
