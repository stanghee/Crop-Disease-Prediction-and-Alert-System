# Redis Cache Service

This service connects Kafka and Redis to provide real-time, up-to-date data to the crop disease monitoring dashboard and other system components.

## What it does
- Reads data from multiple Kafka topics (sensors, weather, ML anomalies, alerts)
- Validates and processes streaming data in real-time
- Saves the latest data in Redis using dedicated keys with automatic expiration (TTL)
- Makes data available very quickly to the dashboard and other services

## How it works

1. **Startup**: The service connects to Redis and Kafka, then starts 5 separate processing threads
2. **Concurrent Processing**: Each thread reads messages from its dedicated Kafka topic:
   - **SensorProcessor**: Processes IoT sensor data (temperature, humidity, pH)
   - **WeatherProcessor**: Processes weather data (temperature, humidity, wind)
   - **MLAnomalyProcessor**: Processes ML anomaly predictions
   - **AlertsProcessor**: Processes threshold alerts from Spark Streaming
3. **Data Validation**: Each message is validated against predefined schemas before caching
4. **Batch Processing**: Sensor and weather data are processed in batches for efficiency
5. **Redis Caching**: Data is saved with specific keys and configurable TTL values
6. **High Performance**: Uses connection pooling, pipelines, and batch operations for optimal performance

## Data Types & Redis Keys

### Sensor Data
- **Source**: `iot_valid_data` Kafka topic
- **Redis Key**: `sensors:latest:{field_id}`
- **TTL**: 5 minutes
- **Example**: `sensors:latest:field_01`

### Weather Data
- **Source**: `weather_valid_data` Kafka topic
- **Redis Key**: `weather:latest:{location}`
- **TTL**: 10 minutes
- **Example**: `weather:latest:Verona`

### ML Predictions
- **Source**: `ml-anomalies` Kafka topic
- **Redis Key**: `predictions:latest:{field_id}`
- **TTL**: 1 hour
- **Example**: `predictions:latest:field_01`

### Threshold Alerts
- **Source**: `alerts-anomalies` Kafka topic
- **Redis Key**: `alerts:latest:{zone_id}`
- **TTL**: 30 minutes
- **Example**: `alerts:latest:field_01` (sensors) or `alerts:latest:Verona` (weather)

## Configuration

Main environment variables:

### Redis Configuration
- `REDIS_HOST`, `REDIS_PORT`: Redis server location
- `REDIS_DB`: Redis database number (default: 0)
- `REDIS_PASSWORD`: Redis password (optional)
- `REDIS_MAX_CONNECTIONS`: Connection pool size (default: 20)

### Kafka Configuration
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_CONSUMER_GROUP_ID`: Consumer group ID (default: redis-cache-group)
- `KAFKA_AUTO_OFFSET_RESET`: Offset reset strategy (default: latest)

### Cache TTL Configuration
- `SENSOR_DATA_TTL`: Sensor data cache duration (default: 300s)
- `WEATHER_DATA_TTL`: Weather data cache duration (default: 600s)
- `ALERT_DATA_TTL`: Alert data cache duration (default: 1800s)
- `PREDICTION_DATA_TTL`: ML prediction cache duration (default: 3600s)

### Processing Configuration
- `PROCESSING_BATCH_SIZE`: Batch size for sensor/weather processing (default: 100)
- `PROCESSING_INTERVAL`: Processing interval in seconds (default: 1.0)
- `HEALTH_CHECK_INTERVAL`: Health monitoring interval (default: 30s)

## Performance Features

- **Connection Pooling**: Efficient Redis connection management
- **Batch Operations**: Groups multiple operations for better throughput
- **Pipeline Support**: Uses Redis pipelines for bulk operations
- **Concurrent Processing**: 4 independent threads for parallel data processing
- **Smart Caching**: Automatic TTL management and metadata tracking
- **Error Handling**: Robust error handling with retry logic and graceful degradation

## Monitoring & Health

The service provides comprehensive monitoring:

- **Processing Statistics**: Messages processed, cached, and error counts
- **Performance Metrics**: Messages per second, cache success rates
- **Redis Health**: Connection status, memory usage, hit ratios
- **Thread Status**: Active thread monitoring via main monitoring loop




