# Crop Disease Service - Dual-Mode Architecture

## Overview

The Crop Disease Service implements the **Dual-Mode Architecture** for crop disease prediction, providing both **real-time analysis** and **batch predictions** to support agricultural decision-making.

## Dual-Mode Architecture

The system implements a **dual-mode architecture** that combines **real-time monitoring** and **ML batch analysis** to provide comprehensive crop disease risk coverage.

### Mode 1: Real-time Processing (Every minute)

**Purpose**: Immediate monitoring and rapid response
- **Frequency**: Every minute (1,440 times per day)
- **Speed**: < 30 seconds processing time
- **Data**: Sensor and weather data from **Silver layer**
- **Logic**: Centralized threshold-based rules
- **Output**: 
  - `SENSOR_ANOMALY` alerts (field-specific)
  - `WEATHER_ALERT` alerts (regional)
  - Immediate guidance for operators
- **Storage**: PostgreSQL for fast access
- **Use**: Daily operational monitoring

### Mode 2: Batch Processing (Every 6 hours)

**Purpose**: Strategic ML predictions and economic analysis
- **Frequency**: Every 6 hours (00:00, 06:00, 12:00, 18:00)
- **Speed**: ~30 minutes processing time
- **Data**: Complete ML features from **Gold layer**
- **Logic**: Random Forest model with economic analysis
- **Output**:
  - `DISEASE_DETECTED` alerts
  - 7-day disease predictions
  - Economic impact analysis
  - Strategic recommendations
- **Storage**: PostgreSQL for historical analysis
- **Use**: Strategic planning and business decisions

## Service Structure

```
services/crop-disease-service/
├── ml_processing/                    # Centralized ML System
│   ├── __init__.py                   # Main package
│   ├── ml_service.py                 # Core ML service (batch processing)
│   ├── models/                       # ML models for Gold layer
│   │   ├── __init__.py
│   │   ├── disease_predictor.py      # Random Forest ML model
│   │   └── alert_generator.py        # Strategic alert generator
│   ├── data_loading/                 # Data loading for Gold layer
│   │   ├── __init__.py
│   │   ├── data_loader.py            # Gold layer data loading
│   │   └── data_sync_service.py      # PostgreSQL sync
│   └── scheduling/                   # Batch scheduling
│       ├── __init__.py
│       └── scheduler_service.py      # 6-hour batch scheduler
├── realtime_alert_manager.py         # Real-time alert orchestrator
├── alert_factory.py                  # Unified alert creation
├── alert_handlers/                   # Specialized handlers
│   ├── sensor_alert_handler.py       # Sensor alerts (Silver layer)
│   └── weather_alert_handler.py      # Weather alerts (Silver layer)
├── config/                           # Centralized configuration
│   └── alert_thresholds.py           # Threshold rules & economic impact
├── database/                         # Database abstraction
│   └── alert_repository.py           # Alert database operations
├── continuous_monitor_refactored.py  # Clean real-time monitor
├── api_service.py                    # REST API endpoints
├── main.py                           # Main orchestrator
├── crop_disease_service_documentation.md  # This file
├── logs/                             # Log files
└── requirements.txt & Dockerfile     # Deployment files
```

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    REAL-TIME PROCESSING                        │
│                    (Every minute)                              │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌─────────────────┐    
│   SILVER LAYER  │    │   SILVER LAYER  │    
│ Sensor Data     │    │ Weather Data    │    
│                 │    │                 │    
└─────────────────┘    └─────────────────┘    
         │                       │                       
         ▼                       ▼                       
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│SensorAlertHandler│   │WeatherAlertHandler│  │RealtimeAlertManager│
│ • Thresholds    │    │ • Thresholds    │    │ • Orchestration │
│ • Validation    │    │ • Validation    │    │ • Statistics    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │    AlertFactory         │
                    │ • Unified formatting    │
                    │ • Economic impact       │
                    │ • Recommendations       │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  AlertRepository        │
                    │ • PostgreSQL storage    │
                    │ • Batch operations      │
                    │ • Statistics            │
                    └─────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    BATCH PROCESSING                             │
│                    (Every 6 hours)                              │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                               │
│ • ML Features (historical)                                      │
│ • Integrated sensor + weather data                              │
│ • Aggregated metrics                                            │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ML PROCESSING SYSTEM                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  DataLoader     │  │DiseasePredictor │  │AlertGenerator   │  │
│  │ • Gold layer    │  │ • Random Forest │  │ • Strategic     │  │
│  │ • Features      │  │ • Probability   │  │ • Economic      │  │
│  │ • Historical    │  │ • Confidence    │  │ • ROI analysis  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │  AlertRepository        │
                    │ • DISEASE_DETECTED      │
                    │ • Strategic alerts      │
                    │ • Historical analysis   │
                    └─────────────────────────┘
```

## Core Components

### ML Processing System (`ml_processing/`)
**Purpose**: Centralized batch ML processing with Gold layer data

#### Core Components:
- **`ml_service.py`**: Main orchestrator for batch processing
- **`models/`**: ML models for disease prediction
  - `disease_predictor.py`: Random Forest model with fallback
  - `alert_generator.py`: Strategic alert generation
- **`data_loading/`**: Gold layer data management
  - `data_loader.py`: ML features loading
  - `data_sync_service.py`: PostgreSQL synchronization
- **`scheduling/`**: Batch processing scheduling
  - `scheduler_service.py`: 6-hour batch scheduler

#### Key Features:
- **Gold Layer Integration**: Uses aggregated ML features
- **Batch Processing**: Every 6 hours for strategic predictions
- **Economic Analysis**: ROI calculations and impact assessment
- **Model Management**: Training, retraining, versioning

### Real-time Alert System
**Purpose**: Immediate monitoring with Silver layer data

#### Core Components:
- **`realtime_alert_manager.py`**: Central orchestrator
- **`alert_factory.py`**: Unified alert creation
- **`alert_handlers/`**: Specialized processors
  - `sensor_alert_handler.py`: Sensor data processing
  - `weather_alert_handler.py`: Weather data processing
- **`config/alert_thresholds.py`**: Centralized configuration
- **`database/alert_repository.py`**: Database abstraction

#### Key Features:
- **Silver Layer Integration**: Uses real-time sensor and weather data
- **Centralized Configuration**: No more hardcoded thresholds
- **Unified Format**: Consistent alert structure
- **Real-time Processing**: Every minute monitoring

## Alert Types and Data Sources

### Real-time Alerts (Silver Layer)
| Alert Type | Data Source | Frequency | Purpose |
|------------|-------------|-----------|---------|
| `SENSOR_ANOMALY` | Silver layer sensors | Every minute | Field-specific monitoring |
| `WEATHER_ALERT` | Silver layer weather | Every minute | Regional weather protection |

### Batch Alerts (Gold Layer)
| Alert Type | Data Source | Frequency | Purpose |
|------------|-------------|-----------|---------|
| `DISEASE_DETECTED` | Gold layer ML features | Every 6 hours | Strategic disease prediction |

## Configuration Management

### Centralized Thresholds (`config/alert_thresholds.py`)
```python
# Sensor Thresholds
TEMPERATURE_THRESHOLDS = [
    ThresholdRule("temperature", ">", 35.0, RiskLevel.HIGH, "Temperature too high: {value}°C"),
    ThresholdRule("temperature", "<", 5.0, RiskLevel.HIGH, "Temperature too low: {value}°C"),
]

# Weather Thresholds  
WEATHER_THRESHOLDS = [
    ThresholdRule("temp_c", ">", 40.0, RiskLevel.CRITICAL, "Temperature extremely high: {value}°C"),
    ThresholdRule("wind_kph", ">", 40.0, RiskLevel.CRITICAL, "Wind speed extremely high: {value} km/h"),
]

# Economic Impact
ECONOMIC_IMPACT = {
    'HIGH_TEMPERATURE': {
        RiskLevel.HIGH: {'potential_loss': 1200, 'treatment_cost': 200},
        RiskLevel.CRITICAL: {'potential_loss': 2000, 'treatment_cost': 300}
    }
}
```

## Quick Start

### 1. Start the Service
```bash
# Start all services
docker-compose up -d

# Check service status
curl http://localhost:8000/health
```

### 2. Access the API
```bash
# Real-time alerts
curl http://localhost:8000/alerts/sensors
curl http://localhost:8000/alerts/weather

# Batch predictions
curl http://localhost:8000/predictions/recent

# System status
curl http://localhost:8000/system/status
```

## API Endpoints

### Real-time Alert Endpoints
- `GET /alerts/sensors` - Get sensor alerts (Silver layer)
- `GET /alerts/weather` - Get weather alerts (Silver layer)
- `GET /alerts/statistics` - Get alert statistics
- `GET /alerts/configuration` - Get threshold configuration
- `POST /alerts/cleanup` - Clean up old alerts

### Batch Processing Endpoints
- `GET /batch/status` - Get batch processing status
- `POST /batch/predict` - Trigger batch prediction
- `GET /batch/schedule` - Get batch schedule
- `PUT /batch/schedule` - Update batch schedule

### ML Predictions Endpoints
- `GET /predictions` - Get recent predictions (Gold layer)
- `POST /predictions/predict` - Make prediction for field

### Monitor Endpoints
- `GET /monitor/status` - Get real-time monitor status
- `POST /monitor/check` - Trigger manual alert check
- `GET /monitor/health` - Get monitor health check

### System Endpoints
- `GET /system/status` - Get overall system status
- `GET /system/health` - Health check

## Migration Guide

### From Legacy to Current Architecture

#### Real-time Alerts:
```python
# OLD (Legacy)
alerts = data_loader.check_sensor_alerts_from_silver()

# NEW (Current)
alerts = alert_manager.sensor_handler.get_threshold_violations()
```

#### Weather Alerts:
```python
# OLD (Legacy)
alerts = data_loader.check_weather_alerts()

# NEW (Current)
alerts = alert_manager.weather_handler.get_threshold_violations()
```

#### Batch Processing:
```python
# OLD (Legacy)
from ml_service import MLService

# NEW (Current)
from ml_processing.ml_service import MLService
```

## Development Guidelines

### Adding New Alert Types
1. Create handler in `alert_handlers/`
2. Add thresholds in `config/alert_thresholds.py`
3. Update `alert_factory.py` for new alert type
4. Add API endpoints in `api_service.py`

### Adding New ML Models
1. Create model in `ml_processing/models/`
2. Update `ml_processing/ml_service.py`
3. Add to `ml_processing/models/__init__.py`
4. Update API endpoints

### Adding New Data Sources
1. Update `ml_processing/data_loading/data_loader.py` for Gold layer
2. Update `alert_handlers/` for Silver layer
3. Add configuration in `config/`

## Performance Metrics

### Real-time Processing
- **Latency**: < 30 seconds
- **Frequency**: Every minute
- **Data Source**: Silver layer
- **Alert Types**: SENSOR_ANOMALY, WEATHER_ALERT

### Batch Processing
- **Latency**: < 30 minutes
- **Frequency**: Every 6 hours
- **Data Source**: Gold layer
- **Alert Types**: DISEASE_DETECTED

## Monitoring and Health Checks

### Service Health
```bash
# Overall health
curl http://localhost:8000/health

# Real-time monitor
curl http://localhost:8000/monitor/health

# ML processing
curl http://localhost:8000/system/status
```

### Alert Statistics
```bash
# Get alert statistics
curl http://localhost:8000/alerts/statistics

# Get configuration info
curl http://localhost:8000/alerts/configuration
```

## Key Benefits

1. **Maintainability**: Centralized configuration and clean architecture
2. **Testability**: Modular components with clear interfaces
3. **Consistency**: Unified alert format across all sources
4. **Observability**: Better monitoring and statistics
5. **Extensibility**: Easy to add new alert types and models
6. **Organization**: Clear separation between real-time and batch processing
7. **Performance**: Optimized data flow and processing
8. **Reliability**: Better error handling and fallback mechanisms

---

**The Crop Disease Service provides a clean, maintainable, and scalable architecture for comprehensive crop disease monitoring and prediction!**


