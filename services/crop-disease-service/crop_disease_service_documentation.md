# Real-Time Alert Service

## Overview

The **Real-Time Alert Service** implements a **Spark Streaming-based monitoring system** for agricultural monitoring, providing **immediate alert generation** based on configurable thresholds for weather and sensor data.

## Spark Streaming Architecture

The system implements a **Spark Structured Streaming architecture** connected to the existing **Spark cluster (master + 2 workers)** that provides **real-time monitoring** and **immediate alert generation** for comprehensive agricultural risk coverage.

### Real-time Processing (Every 5 seconds)

**Purpose**: Immediate monitoring and rapid response
- **Frequency**: Every 5 seconds (17,280 processing cycles per day)
- **Speed**: < 1 second processing time per batch
- **Data Sources**: 
  - `iot_valid_data` Kafka topic (sensor data)
  - `weather_valid_data` Kafka topic (weather data)
- **Processing Engine**: Apache Spark Structured Streaming
- **Logic**: Distributed threshold-based rules processing
- **Output**: 
  - `SENSOR_ANOMALY` alerts (field-specific)
  - `WEATHER_ALERT` alerts (regional)
  - Immediate guidance for operators
- **Storage**: PostgreSQL for fast access and API serving
- **Cluster**: Connects to `spark://spark-master:7077`

## Service Structure

```
services/crop-disease-service/
├── spark_streaming_service.py        # Main Spark Streaming logic
├── Threshold-alert/                  # Threshold configuration system
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

## Spark Streaming Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPARK CLUSTER                               │
│          Master + 2 Workers (4 cores, 4GB RAM total)         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                REAL-TIME ALERT PROCESSING                      │
│                    (Every 5 seconds)                           │
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
│                  SPARK STRUCTURED STREAMING                    │
│ • Real-time data consumption from Kafka                        │
│ • Parallel threshold rule processing                           │
│ • Distributed alert generation                                 │
│ • Fault-tolerant with checkpointing                           │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │    THRESHOLD RULES      │
                    │ • Temperature limits    │
                    │ • Humidity boundaries   │
                    │ • pH range checks       │
                    │ • Wind speed alerts     │
                    └─────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │  BATCH PROCESSING       │
                    │ • Efficient batch writes│
                    │ • Error resilience      │
                    │ • PostgreSQL storage    │
                    └─────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │  REST API SERVICE       │
                    │ • Real-time alert access│
                    │ • Dashboard integration │
                    │ • Health monitoring     │
                    └─────────────────────────┘
```



