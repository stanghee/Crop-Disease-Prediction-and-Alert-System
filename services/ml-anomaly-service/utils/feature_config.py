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

# Soglie per distanza dal centroide (anomaly detection)
ANOMALY_DISTANCE_THRESHOLD = 0.01   # Distanza > soglia = anomalia (ulteriormente abbassata per test)
CRITICAL_DISTANCE_THRESHOLD = 0.05  # Distanza > soglia critica = criticità (ulteriormente abbassata per test)

# Processing configuration  
BATCH_INTERVAL_SECONDS = 30   # Process every 30 seconds
WATERMARK_DURATION = "5 minutes"
