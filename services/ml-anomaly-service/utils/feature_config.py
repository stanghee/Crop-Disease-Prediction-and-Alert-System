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
    'humidity_differential'   # sensor vs weather humidity difference
] 

# Model configuration for KMeans
KMEANS_CONFIG = {
    'k': 3,                # Numero di cluster, configurabile (ridotto per test)
    'maxIter': 20,         # Iterazioni massime
    'seed': 42             # Random seed
}

# Soglie per anomaly score basate su distanza euclidea dal centroide
# Le distanze tipiche con 5 features scalate sono nell'ordine di 1-3 per punti normali
ANOMALY_DISTANCE_THRESHOLD = 2.5   # Distanza > 2.5 = anomalia
CRITICAL_DISTANCE_THRESHOLD = 4.0  # Distanza > 4.0 = criticità

# Processing configuration  
BATCH_INTERVAL_SECONDS = 30   # Process every 30 seconds
WATERMARK_DURATION = "5 minutes"
