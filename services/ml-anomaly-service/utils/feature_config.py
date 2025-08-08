#!/usr/bin/env python3
"""
Feature configuration for ML anomaly detection
Simple core features as requested
"""
#TODO: check if sensor anomaly rate is usefull 
# Core features for anomaly detection
CORE_FEATURES = [
    'sensor_avg_temperature',
    'sensor_avg_humidity', 
    'sensor_avg_soil_ph',
    'temp_differential',      # sensor vs weather temperature difference
    'humidity_differential',  # sensor vs weather humidity difference
    'sensor_anomaly_rate',    # sensor anomaly rate
    'weather_avg_uv_index',   # UV index from weather data
    'weather_avg_wind_speed', # wind speed from weather data
    'temporal_feature_ml_sin', # cyclic month feature (sin)
    'temporal_feature_ml_cos'  # cyclic month feature (cos)
] 

# Model configuration for KMeans
KMEANS_CONFIG = {
    'k': 3,                # Numero di cluster, configurabile (ridotto per test)
    'maxIter': 20,         # Iterazioni massime
    'seed': 42             # Random seed
}

# Soglie per anomaly score basate su distanza euclidea dal centroide
# Le distanze tipiche con 5 features scalate sono nell'ordine di 1-3 per punti normali
ANOMALY_DISTANCE_THRESHOLD = 1   # Distanza > 1 = anomalia, for the DEMO
CRITICAL_DISTANCE_THRESHOLD = 4.0  # Distanza > 4.0 = criticità