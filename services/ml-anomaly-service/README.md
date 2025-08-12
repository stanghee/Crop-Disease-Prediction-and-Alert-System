# ML Anomaly Detection Service

## Overview

The ML Anomaly Detection Service is a real-time anomaly detection system that uses unsupervised machine learning to identify unusual patterns in agricultural sensor data. Built on Apache Spark and PySpark ML, it implements K-means clustering to detect complex environmental anomalies that could indicate crop disease risk, complementing the threshold-based alerting system.

## Key Features

- **Real-time Anomaly Detection**: Processes streaming data using Apache Spark
- **K-means Clustering**: Identifies complex environmental patterns
- **Intelligent Recommendations**: Provides context-aware alerts based on environmental conditions
- **Automatic Model Training**: Scheduled retraining to adapt to seasonal changes
- **Severity Classification**: Four-level risk assessment (LOW, MEDIUM, HIGH, CRITICAL)

## Architecture

```
ml-anomaly-service/
├── main.py                  # Service orchestrator 
├── training/
│   └── trainer.py          # K-means model training logic
├── inference/
│   └── predictor.py        # Real-time streaming predictions
├── storage/
│   ├── model_manager.py    # Model persistence and versioning
│   └── postgres_client.py  # Database operations
└── utils/
    └── feature_config.py   # Feature definitions and thresholds
```

## How It Works

### Feature Engineering
The service processes 10 engineered features from the Gold zone:
- **Environmental Metrics**: Temperature, humidity, soil pH averages
- **Differential Features**: Sensor vs weather differences
- **Quality Indicators**: Anomaly rates and data freshness
- **Temporal Features**: Cyclical month encoding (sin/cos)

### Anomaly Detection Process
1. Reads aggregated features from `gold-ml-features` Kafka topic
2. Standardizes features using trained StandardScaler
3. Assigns data points to nearest cluster
4. Calculates Euclidean distance from cluster centroid
5. Classifies severity based on distance thresholds

### Severity Classification

| Severity | Distance Range | Description |
|----------|----------------|-------------|
| LOW | < 0.5 | Normal behavior within cluster |
| MEDIUM | 0.5 - 1.0 | Mild deviation, increased monitoring |
| HIGH | 1.0 - 4.0 | Significant anomaly, intervention needed |
| CRITICAL | ≥ 4.0 | Severe anomaly, immediate action required |

### Model Configuration

The K-means model is configured in `utils/feature_config.py`:

```python
KMEANS_CONFIG = {
    'k': 3,           # Number of clusters
    'maxIter': 20,    # Maximum iterations
    'seed': 42        # Random seed for reproducibility
}
```

## Training Strategy

- **Initial Training**: 4 minutes after service startup
- **Scheduled Retraining**: Weekly on sunday at 2:00 AM
- **Model Storage**: Versioned models stored in MinIO with metadata

## Data Flow

1. **Input**: `gold-ml-features` Kafka topic
2. **Processing**: Real-time anomaly detection using Spark Structured Streaming
3. **Outputs**:
   - Anomalies → `ml-anomalies` Kafka topic
   - All predictions → PostgreSQL `ml_predictions` table
   - Cached results → Redis for dashboard
