# Crop Disease Service - Dual-Mode Architecture

## Overview

The Crop Disease Service implements the **Dual-Mode Architecture** for crop disease prediction, providing both **real-time analysis** and **batch predictions** to support agricultural decision-making.

## Dual-Mode Architecture

### Mode 1: Real-time Processing (Every 5 minutes)
- **Purpose**: Immediate insights and current status
- **Frequency**: Every 5 minutes
- **Output**: Current analysis, guidance, live dashboard updates
- **Storage**: PostgreSQL (fast queries)
- **Data Source**: Gold zone sensor data only (no weather features for ML)

### Mode 2: Batch Processing (Every 6 hours)
- **Purpose**: ML predictions and strategic alerts
- **Frequency**: Every 6 hours
- **Output**: Disease predictions, economic impact analysis, strategic recommendations
- **Storage**: PostgreSQL (long-term, queryable)
- **Data Source**: Gold zone ML features (includes weather features)

## Data Flow Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SENSOR        │    │     BATCH       │    │    WEATHER      │
│   ALERTS        │    │   PROCESSING    │    │    ALERTS       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   GOLD ZONE     │    │   GOLD ZONE     │    │   SILVER ZONE   │
│ Sensor Data     │    │ ML Features     │    │ Weather Data    │
│ (Recent)        │    │ (Historical)    │    │ (Current)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Simple Rules    │    │ Random Forest   │    │ Threshold       │
│ Risk Calc       │    │ ML Model        │    │ Rules           │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ SENSOR_ANOMALY  │    │ DISEASE_DETECTED│    │ WEATHER_ALERT   │
│ Alert           │    │ Alert           │    │ Alert           │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │     POSTGRESQL          │
                    │ • All alerts stored     │
                    │ • Dashboard access      │
                    │ • Fast queries          │
                    └─────────────────────────┘
```

## Service Structure

```
services/crop-disease-service/
├── main.py                 # Main entry point with FastAPI orchestrator
├── ml_service.py           # Core crop disease service with dual-mode processing
├── api_service.py          # REST API endpoints
├── scheduler_service.py    # Batch scheduling (6-hour intervals)
├── requirements.txt        # Python dependencies
├── Dockerfile             # Container configuration
├── ml_service_documentation.md              # This file
├── models/                # ML models directory
│   ├── disease_predictor.py    # Disease prediction model (Random Forest + fallback)
│   └── alert_generator.py      # Alert generation with economic impact
├── data/                  # Data handling
│   └── data_loader.py         # Data loading from Gold/Silver zones
├── sync/                  # Data synchronization
│   └── data_sync_service.py   # PostgreSQL sync for real-time access
└── database/              # Database schema
    └── init.sql               # PostgreSQL tables and views
```

## Quick Start

### 1. Start the Service

```bash
# Start all services including crop disease service
docker-compose up -d

# Check crop disease service status
curl http://localhost:8000/health
```

### 2. Access the API

The crop disease service provides a REST API at `http://localhost:8000`:

```bash
# Health check
curl http://localhost:8000/health

# Get system status
curl http://localhost:8000/status

# Get recent predictions
curl http://localhost:8000/predictions/recent

# Get active alerts
curl http://localhost:8000/alerts/active
```

## 📊 API Endpoints

### Real-time Endpoints
- `GET /api/v1/realtime/status` - Get real-time processing status
- `POST /api/v1/realtime/process` - Trigger real-time processing

### Batch Endpoints
- `GET /api/v1/batch/status` - Get batch processing status
- `POST /api/v1/batch/predict` - Trigger batch prediction
- `GET /api/v1/batch/schedule` - Get batch schedule
- `PUT /api/v1/batch/schedule` - Update batch schedule

### Predictions Endpoints
- `GET /api/v1/predictions` - Get recent predictions
- `POST /api/v1/predictions/predict` - Make prediction for field

### Alerts Endpoints
- `GET /api/v1/alerts` - Get active alerts
- `GET /api/v1/alerts/weather` - Get weather alerts
- `PUT /api/v1/alerts/{alert_id}` - Update alert status

### Models Endpoints
- `GET /api/v1/models` - Get ML models status

### System Endpoints
- `GET /api/v1/system/status` - Get system status
- `GET /api/v1/system/health` - Health check

## ML Models

### Disease Predictor
- **Algorithm**: Random Forest (with fallback to rule-based)
- **Features**: 
  - Real-time: Temperature, humidity, soil pH (sensor data only)
  - Batch: Temperature, humidity, soil pH, weather data, anomalies
- **Output**: Disease probability, risk level, confidence score
- **Training**: Synthetic data initially, retrained weekly with real data

### Alert Generator
- **Input**: ML predictions
- **Output**: Strategic alerts with economic impact analysis
- **Features**: Risk assessment, treatment recommendations, ROI calculation

## Data Flow

### Real-time Processing (Mode 1)
1. **Data Source**: Gold zone sensor metrics (recent data)
2. **Processing**: Simple risk calculation based on current conditions
3. **Output**: Real-time analysis and immediate guidance
4. **Storage**: PostgreSQL for dashboard access

### Batch Processing (Mode 2)
1. **Data Source**: Gold zone ML features (includes weather data)
2. **Processing**: Full ML model prediction with confidence scoring
3. **Output**: Disease predictions and strategic alerts
4. **Storage**: PostgreSQL for long-term analysis

## Real-time Processing Example

```python
# Real-time analysis result
{
    "field_id": "field_02",
    "timestamp": "2024-01-15 14:30:00",
    "current_data": {
        "temperature": 24.5,
        "humidity": 88,
        "soil_ph": 6.2
    },
    "trends": {
        "temperature_trend": "↗️",
        "humidity_trend": "↗️"
    },
    "analysis": {
        "current_risk": "MEDIUM",
        "immediate_condition": "Favorable for Late Blight",
        "action_needed": "Monitor humidity for next 2 hours",
        "current_guide": "High humidity detected. Check for leaf wetness."
    }
}
```

## Batch Processing Example

```python
# Batch prediction result
{
    "field_id": "field_02",
    "prediction_timestamp": "2024-01-15 12:00:00",
    "ml_analysis": {
        "disease_risk_7_days": "HIGH",
        "probability": 0.78,
        "confidence": 0.85,
        "expected_onset": "3-5 days",
        "triggering_factors": [
            "Extended humidity > 85%",
            "Temperature 20-25°C",
            "Weather forecast: 5 days rain"
        ]
    },
    "strategic_alert": {
        "priority": "HIGH",
        "message": "Preventive treatment recommended within 48 hours",
        "economic_impact": {
            "potential_loss": "€1,200",
            "treatment_cost": "€200",
            "roi": "€1,000 savings"
        }
    }
}
```

## Configuration

### Environment Variables

```bash
# Crop Disease Service Configuration
CROP_DISEASE_SERVICE_HOST=0.0.0.0
CROP_DISEASE_SERVICE_PORT=8000

# PostgreSQL Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=crop_disease_ml
POSTGRES_USER=ml_user
POSTGRES_PASSWORD=ml_password

# MinIO Configuration (Data Lake Only)
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Data Lake Paths
GOLD_PATH=s3a://gold/
SILVER_PATH=s3a://silver/

# ML Model Configuration
BATCH_INTERVAL_HOURS=6
BATCH_START_HOUR=0
```

## Scheduling

### Batch Processing Schedule
- **Frequency**: Every 6 hours (00:00, 06:00, 12:00, 18:00)
- **Duration**: ~30 minutes per batch
- **Output**: ML predictions and strategic alerts

### Model Retraining Schedule
- **Frequency**: Weekly (Sunday at 2:00 AM)
- **Duration**: ~1 hour
- **Input**: 30 days of historical data

### Data Quality Check Schedule
- **Frequency**: Daily (6:00 AM)
- **Purpose**: Monitor data quality and completeness

## Database Schema

### Key Tables
- **ml_predictions**: Stores all ML predictions with features and metadata
- **alerts**: Stores all alerts (real-time and batch) with status tracking
- **model_metadata**: Tracks ML model versions and performance
- **data_sync_log**: Logs data synchronization activities
- **user_preferences**: User alert and dashboard preferences

### Views
- **recent_predictions**: Latest predictions for dashboard
- **active_alerts**: Currently active alerts
- **model_performance_summary**: Model performance metrics

## Monitoring

### Health Checks
```bash
# Service health
curl http://localhost:8000/health

# System status
curl http://localhost:8000/api/v1/system/status

# Data quality
curl http://localhost:8000/api/v1/data/quality
```

### Performance Metrics
- **Real-time latency**: < 30 seconds
- **Batch processing time**: < 30 minutes
- **Prediction accuracy**: > 85%
- **API response time**: < 100ms

## Development

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run service locally
python main.py

# Run tests
python -m pytest tests/
```

### Adding New Models
1. Create model class in `models/` directory
2. Implement required methods (initialize, predict, retrain)
3. Update `ml_service.py` to include new model
4. Add API endpoints in `api_service.py`

### Adding New Features
1. Create feature engineering logic in `data/data_loader.py`
2. Update model feature columns in `models/disease_predictor.py`
3. Retrain models with new features
4. Update API documentation

## Key Implementation Details

### Data Sources
- **Real-time**: Uses Gold zone sensor data only (no weather features)
- **Batch**: Uses Gold zone ML features (includes weather data from Silver zone)
- **Weather Alerts**: Separate threshold-based alerts from Silver zone weather data

### Storage Strategy
- **All data stored in PostgreSQL**: Predictions, alerts, user preferences
- **MinIO used only for data lake**: Raw data processing and feature engineering
- **No data duplication**: Single source of truth for ML results

### Error Handling
- **Fallback models**: Rule-based prediction when ML models fail
- **Graceful degradation**: Service continues with reduced functionality
- **Comprehensive logging**: All operations logged for debugging

### Scalability
- **Background processing**: Real-time and batch run in separate threads
- **Database indexing**: Optimized queries for dashboard performance
- **Modular architecture**: Easy to add new models and features

