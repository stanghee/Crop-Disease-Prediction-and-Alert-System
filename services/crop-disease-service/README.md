# Crop Disease Alert Service

## Overview

The **Crop Disease Alert Service** is a real-time monitoring system built on Apache Spark Structured Streaming that processes agricultural sensor and weather data to detect potential crop threats. The service reads validated data from Kafka topics, applies threshold-based rules in a distributed manner, and generates immediate alerts when conditions could harm crops.

Key capabilities:
- Real-time processing of IoT sensor data (temperature, humidity, pH)
- Weather condition monitoring and alerting
- Distributed threshold evaluation using Spark cluster
- Fault-tolerant alert generation with checkpointing
- REST API for alert management and monitoring

## How It Works

The service operates as a distributed stream processing engine, leveraging Apache Spark's cluster computing capabilities to ensure high availability and scalability. It maintains two parallel processing pipelines - one for IoT sensor data and another for weather data - each optimized for its specific data characteristics and alert requirements.

### Main Components

1. **Spark Streaming Engine**: The core processing engine that reads from Kafka and generates alerts
2. **Alert Repository**: Manages database operations for storing and retrieving alerts
3. **REST API Service**: Provides real-time access to alerts for the dashboard and external systems
4. **Service Orchestrator**: Coordinates all components and manages the service lifecycle

### Processing Pipelines

The service runs two independent streaming queries:

- **IoT Alert Pipeline**: Monitors sensor data from agricultural fields
  - Reads from `iot_valid_data` Kafka topic
  - Applies thresholds for temperature, humidity, and soil pH
  - Generates `SENSOR_ANOMALY` alerts for field-specific issues
  - Processing window: 30 seconds
  - **Note**: Only processes measurements with valid data flags (temperature_valid, humidity_valid, ph_valid)

- **Weather Alert Pipeline**: Monitors meteorological conditions
  - Reads from `weather_valid_data` Kafka topic
  - Evaluates weather parameters against risk thresholds
  - Generates `WEATHER_ALERT` alerts for location-specific risks
  - Processing window: 30 seconds
  - **Note**: Only processes measurements with valid data flags (temp_valid, wind_valid, uv_valid, precip_valid)

## Data Processing

### IoT Sensor Data
The service processes the following sensor data from agricultural fields:
- **Temperature**: Field temperature in Celsius
- **Humidity**: Humidity percentage
- **Soil pH**: Soil acidity/alkalinity level
- **Data Quality**: Validity flags for each measurement
- **Metadata**: Field ID, location, and timestamp

Schema:
```json
{
    "field_id": "string",
    "location": "string",
    "temperature": "double",
    "humidity": "double",
    "soil_ph": "double",
    "temperature_valid": "boolean",  // Used to filter out invalid temperature readings
    "humidity_valid": "boolean",     // Used to filter out invalid humidity readings
    "ph_valid": "boolean",          // Used to filter out invalid pH readings
    "timestamp": "string"
}
```

### Weather Data
The service processes the following weather parameters:
- **Temperature**: Air temperature in Celsius
- **Humidity**: Air humidity percentage
- **Wind Speed**: Wind velocity in km/h
- **UV Index**: Ultraviolet radiation level
- **Precipitation**: Rainfall in millimeters
- **Data Quality**: Validity flags for each measurement
- **Metadata**: Location, coordinates validity, and timestamp

Schema:
```json
{
    "location": "string",
    "temp_c": "double",
    "humidity": "double",
    "wind_kph": "double",
    "uv": "double",
    "precip_mm": "double",
    "temp_valid": "boolean",        // Used to filter out invalid temperature readings
    "humidity_valid": "boolean",    // Used to filter out invalid humidity readings
    "wind_valid": "boolean",        // Used to filter out invalid wind readings
    "uv_valid": "boolean",         // Used to filter out invalid UV readings
    "precip_valid": "boolean",     // Used to filter out invalid precipitation readings
    "coordinates_valid": "boolean", // Used to validate location data
    "timestamp": "string"
}
```

## Alert Generation Logic

### Sensor Alert Thresholds

The service monitors critical agricultural parameters with the following thresholds:

| Parameter | Low Alert | High Alert | Critical Range |
|-----------|-----------|------------|----------------|
| Temperature | < 5°C | > 40°C | < 0°C or > 45°C |
| Humidity | < 30% | > 90% | < 20% or > 95% |
| Soil pH | < 5.5 | > 8.0 | < 4.5 or > 9.0 |

### Weather Alert Conditions

Weather alerts are triggered based on extreme conditions that could impact crops:

| Parameter | Low Alert | High Alert | Critical Range |
|-----------|-----------|------------|----------------|
| Temperature | < 0°C | > 45°C | < -10°C or > 50°C |
| Wind Speed | - | > 80 km/h | > 100 km/h |
| UV Index | - | ≥ 8 | ≥ 11 |
| Precipitation | - | ≥ 50 mm | ≥ 100 mm |

### Alert Severity Levels

Each alert is classified into severity levels to help prioritize responses:

- **HIGH**: Urgent attention needed - significant risk
- **MEDIUM**: Monitor closely - potential risk developing

## Output Strategy

The service implements a dual-output strategy to ensure both data persistence and real-time availability:

### PostgreSQL Storage
- **Purpose**: Persistent storage for alert history and API access
- **Table**: `alerts` table in `crop_disease_ml` database
- **Operations**: Batch inserts every 30 seconds
- **Benefits**: 
  - Historical data retention
  - Complex querying capabilities
  - REST API data source

### Kafka Topic Output
- **Topic**: `alerts-anomalies`
- **Purpose**: Real-time streaming for other services
- **Format**: JSON messages with complete alert data
- **Benefits**:
  - Real-time notification to downstream services
  - Decoupled architecture
  - Scalable message distribution

### Dashboard Integration
- **Framework**: Streamlit dashboard
- **Data Source**: REST API endpoints 
- **Real-time Updates**: Dashboard polls API every few seconds
- **Features**:
  - Real-time alert visualization
  - Historical alert analysis
  - Alert statistics and trends
  - Interactive alert management

This dual-output approach ensures that alerts are both stored persistently for historical analysis and distributed in real-time for immediate action and integration with other system components.

## Code Structure

```
services/crop-disease-service/
├── main.py                           # Service orchestrator and FastAPI application
├── spark_streaming_service.py        # Spark Structured Streaming processing logic
├── api_service.py                    # REST API endpoints and request handling
├── database/                         # Database layer components
│   ├── __init__.py                   # Python package initialization
│   ├── alert_repository.py           # PostgreSQL operations and data access
│   └── init.sql                      # Database schema and table definitions
├── requirements.txt                  # Python dependencies and versions
└── Dockerfile                        # Container configuration and build instructions
```

## Database Schema

The service uses and defines PostgreSQL tables:

### Alerts Table
Stores all generated alerts with full context:
- **zone_id**: Field ID for sensors, location for weather
- **alert_timestamp**: When the alert was triggered
- **alert_type**: SENSOR_ANOMALY or WEATHER_ALERT
- **severity**: MEDIUM, HIGH
- **message**: Detailed alert description
- **status**: ACTIVE, ACKNOWLEDGED, RESOLVED, or IGNORED

### ML Predictions Table
Stores machine learning predictions (managed by ML service):
- **field_id**: Agricultural field identifier
- **anomaly_score**: ML-calculated anomaly score
- **severity**: Predicted severity level
- **recommendations**: Recommendations based on anomaly score and feature used for the prediction