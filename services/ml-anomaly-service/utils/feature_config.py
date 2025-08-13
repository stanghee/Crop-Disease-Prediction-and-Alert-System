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
    'sensor_anomaly_rate',    # sensor anomaly rate
    'weather_avg_uv_index',   # UV index from weather data
    'weather_avg_wind_speed', # wind speed from weather data
    'temporal_feature_ml_sin', # cyclic month feature (sin)
    'temporal_feature_ml_cos'  # cyclic month feature (cos)
] 

# Model configuration for KMeans
KMEANS_CONFIG = {
    'k': 3,                # Number of clusters, configurable (reduced for testing)
    'maxIter': 20,         # Maximum iterations
    'seed': 42             # Random seed
}

# Thresholds for anomaly score based on euclidean distance from centroid
# Typical distances with 5 scaled features are in the range of 1-3 for normal points
ANOMALY_DISTANCE_THRESHOLD = 1   # Distance > 1 = anomaly, for the DEMO
CRITICAL_DISTANCE_THRESHOLD = 4.0  # Distance > 4.0 = critical severity