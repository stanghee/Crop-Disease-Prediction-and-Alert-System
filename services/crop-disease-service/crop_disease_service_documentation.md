# Threshold-Based Alert Service

## Overview

The Threshold-Based Alert Service implements a **real-time monitoring system** for agricultural monitoring, providing **immediate alert generation** based on configurable thresholds for weather and sensor data.

## Threshold-Based Architecture

The system implements a **threshold-based architecture** that provides **real-time monitoring** and **immediate alert generation** for comprehensive agricultural risk coverage.

### Real-time Processing (Every 10 seconds)

**Purpose**: Immediate monitoring and rapid response
- **Frequency**: Every 10 seconds (8,640 times per day)
- **Speed**: < 1 second processing time
- **Data**: Sensor and weather data from Kafka topics
- **Logic**: Centralized threshold-based rules
- **Output**: 
  - `SENSOR_ANOMALY` alerts (field-specific)
  - `WEATHER_ALERT` alerts (regional)
  - Immediate guidance for operators
- **Storage**: PostgreSQL for fast access
- **Use**: Real-time operational monitoring

## Service Structure

```
services/crop-disease-service/
├── Threshold-alert/                  # Threshold-based alert system
│   ├── alert_factory.py              # Unified alert creation
│   ├── continuous_monitor_refactored.py  # Real-time monitor
│   ├── kafka_alert_consumer.py       # Kafka consumer for alerts
│   └── config/                       # Centralized configuration
│       └── alert_thresholds.py       # Threshold rules configuration
├── database/                         # Database abstraction
│   ├── alert_repository.py           # Alert database operations
│   └── init.sql                      # Database schema
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
│                    (Every 10 seconds)                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌─────────────────┐    
│   KAFKA TOPICS  │    │   KAFKA TOPICS  │    
│ iot_valid_data  │    │weather_valid_data│    
│                 │    │                 │    
└─────────────────┘    └─────────────────┘    
         │                       │                       
         ▼                       ▼                       
┌─────────────────────────────────────────────────────────────────┐
│                    KAFKA ALERT CONSUMER                        │
│ • Consumes sensor and weather data                             │
│ • Applies threshold rules                                      │
│ • Generates alerts in real-time                                │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │    ALERT FACTORY        │
                    │ • Unified formatting    │
                    │ • Standardized alerts   │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  ALERT REPOSITORY       │
                    │ • PostgreSQL storage    │
                    │ • Real-time operations  │
                    │ • Statistics            │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  CONTINUOUS MONITOR     │
                    │ • Background processing │
                    │ • Health monitoring     │
                    │ • Performance tracking  │
                    └─────────────────────────┘
```

## Core Components

### Threshold-Based Alert System (`Threshold-alert/`)
**Purpose**: Real-time monitoring and alert generation based on configurable thresholds

#### Core Components:
- **`kafka_alert_consumer.py`**: Kafka consumer for real-time data processing
- **`alert_factory.py`**: Unified alert creation and formatting
- **`continuous_monitor_refactored.py`**: Background monitoring service
- **`config/alert_thresholds.py`**: Centralized threshold configuration

#### Key Features:
- **Kafka Integration**: Real-time data consumption from validated topics
- **Threshold-Based Logic**: Configurable rules for different conditions
- **Unified Alert Format**: Consistent alert structure across all types
- **Real-time Processing**: Every 10 seconds monitoring cycle

### Database System (`database/`)
**Purpose**: Centralized data storage and retrieval

#### Core Components:
- **`alert_repository.py`**: Database operations for alerts
- **`init.sql`**: Database schema definition

#### Key Features:
- **PostgreSQL Storage**: Reliable and fast data storage
- **Batch Operations**: Efficient bulk insert/update operations
- **Statistics**: Real-time alert statistics and metrics
- **Cleanup**: Automatic cleanup of old alerts

## Alert Types and Data Sources

### Real-time Alerts (Threshold-Based)
| Alert Type | Data Source | Frequency | Purpose |
|------------|-------------|-----------|---------|
| `SENSOR_ANOMALY` | Kafka topic: `iot_valid_data` | Every 10 seconds | Field-specific monitoring |
| `WEATHER_ALERT` | Kafka topic: `weather_valid_data` | Every 10 seconds | Regional weather protection |

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
curl http://localhost:8000/api/v1/alerts/sensors
curl http://localhost:8000/api/v1/alerts/weather

# System status
curl http://localhost:8000/api/v1/system/status
curl http://localhost:8000/api/v1/system/health

# Monitor status
curl http://localhost:8000/monitor/status
curl http://localhost:8000/monitor/health
```

## API Endpoints

### Real-time Alert Endpoints
- `GET /api/v1/alerts` - Get all alerts
- `GET /api/v1/alerts/sensors` - Get sensor alerts
- `GET /api/v1/alerts/weather` - Get weather alerts
- `GET /api/v1/alerts/statistics` - Get alert statistics
- `GET /api/v1/alerts/configuration` - Get threshold configuration
- `POST /api/v1/alerts/cleanup` - Clean up old alerts

### Monitor Endpoints
- `GET /monitor/status` - Get real-time monitor status
- `POST /monitor/check` - Trigger manual alert check
- `GET /monitor/health` - Get monitor health check

### System Endpoints
- `GET /api/v1/system/status` - Get overall system status
- `GET /api/v1/system/health` - Health check

## Migration Guide

### From Legacy to Current Architecture

#### Real-time Alerts:
```python
# OLD (Legacy)
alerts = data_loader.check_sensor_alerts_from_silver()

# NEW (Current)
alerts = continuous_monitor.alert_consumer.get_sensor_alerts()
```

#### Weather Alerts:
```python
# OLD (Legacy)
alerts = data_loader.check_weather_alerts()

# NEW (Current)
alerts = continuous_monitor.alert_consumer.get_weather_alerts()
```

#### System Status:
```python
# OLD (Legacy)
status = ml_service.get_status()

# NEW (Current)
status = threshold_service.get_status()
```

## Development Guidelines

### Adding New Alert Types
1. Create handler in `Threshold-alert/alert_handlers/`
2. Add thresholds in `Threshold-alert/config/alert_thresholds.py`
3. Update `Threshold-alert/alert_factory.py` for new alert type
4. Add API endpoints in `api_service.py`

### Adding New Data Sources
1. Update `Threshold-alert/kafka_alert_consumer.py` for new Kafka topics
2. Add configuration in `Threshold-alert/config/`
3. Update alert handlers for new data format

## Performance Metrics

### Real-time Processing
- **Latency**: < 1 second
- **Frequency**: Every 10 seconds
- **Data Source**: Kafka topics (iot_valid_data, weather_valid_data)
- **Alert Types**: SENSOR_ANOMALY, WEATHER_ALERT

## Monitoring and Health Checks

### Service Health
```bash
# Overall health
curl http://localhost:8000/api/v1/system/health

# Real-time monitor
curl http://localhost:8000/monitor/health

# System status
curl http://localhost:8000/api/v1/system/status
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
5. **Extensibility**: Easy to add new alert types and thresholds
6. **Organization**: Clear separation between real-time monitoring components
7. **Performance**: Optimized real-time data flow and processing
8. **Reliability**: Better error handling and fallback mechanisms

---

**The Threshold-Based Alert Service provides a clean, maintainable, and scalable architecture for comprehensive agricultural monitoring!**


