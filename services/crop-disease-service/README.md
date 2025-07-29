# Real-Time Alert Service

## Overview

The **Real-Time Alert Service** is a Spark Streaming-based monitoring system designed for agricultural environments. It provides immediate alert generation based on configurable thresholds for both weather and sensor data, enabling rapid response to potential risks in the field.

## Architecture

This service leverages **Apache Spark Structured Streaming** connected to a dedicated Spark cluster (master + 2 workers) to deliver real-time monitoring and alerting.
For this service we allocated 1 Core and 512 mb. 

### Real-Time Data Processing
- **Data Sources:**
  - `iot_valid_data` Kafka topic (sensor data)
  - `weather_valid_data` Kafka topic (weather data)
- **Processing Engine:** Apache Spark Structured Streaming
- **Business Logic:** Distributed, threshold-based rule evaluation
- **Outputs:**
  - `SENSOR_ANOMALY` alerts (field-specific)
  - `WEATHER_ALERT` alerts (location-specific)
  - Immediate operator guidance
- **Storage:** PostgreSQL (for fast access and API serving)
- **Cluster Connection:** `spark://spark-master:7077`

## Service Structure

```
services/crop-disease-service/
├── spark_streaming_service.py        # Main Spark Streaming logic
├── database/                         # Database 
│   ├── alert_repository.py           # Alert database operations
│   └── init.sql                      # Database schema (it also contain ml_predictions databse)
├── api_service.py                    # REST API endpoints
├── main.py                           # Service orchestrator
├── README.md                         # This file                            
├── requirements.txt & Dockerfile     # Deployment files
```

## Data Flow and Processing Pipeline

```
+-----------------------------+
|        SPARK CLUSTER        |
|  Master + 2 Workers (4 CPU, |
|  4GB RAM total)             |
+-----------------------------+


+-------------------+   +-------------------+
|  KAFKA TOPIC      |   |  KAFKA TOPIC      |
|  iot_valid_data   |   | weather_valid_data|
+-------------------+   +-------------------+
            |                   |
            v                   v
+-------------------------------------------+
|      SPARK STRUCTURED STREAMING           |
|  - Real-time data consumption from Kafka  |
|  - Parallel threshold rule processing     |
|  - Distributed alert generation           |
|  - Fault-tolerant with checkpointing      |
+-------------------------------------------+
            |
            v
+---------------------------+
|    THRESHOLD RULES        |
|  - Temperature            |
|  - Humidity               |
|  - pH range               |
|  - Wind                   |
+---------------------------+
            |
            v
+---------------------------+
|   BATCH PROCESSING        |
|  - Batch writes           |
|  - PostgreSQL storage     |
+---------------------------+
            |
            v
+---------------------------+
|   REST API SERVICE        |
|  - Real-time alert access |
|  - Dashboard integration  |
|  - Health monitoring      |
+---------------------------+
```

## Technologies Used
- **Apache Spark Structured Streaming**: Distributed, real-time data processing
- **Kafka**: High-throughput data ingestion
- **PostgreSQL**: Persistent storage for alerts
- **FastAPI**: REST API for alert access and system monitoring






