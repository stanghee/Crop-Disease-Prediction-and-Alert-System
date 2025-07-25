# Redis Cache Service

This service connects Kafka and Redis to provide real-time, up-to-date data to the crop disease monitoring dashboard.

## What it does
- Reads data from Kafka (sensors, weather, ML anomalies, alerts)
- Validates and saves the latest data in Redis using dedicated keys
- Sets an automatic expiration (TTL) for each data entry, so the cache is always fresh
- Makes data available very quickly to the dashboard and other services

## How it works
1. **Startup**: The service connects to Redis and Kafka, then starts separate threads for each data type.
2. **Processing**: Each thread reads messages from its Kafka topic, validates them, and saves them in Redis.
3. **Cache**: Data is saved with a specific key (e.g., `sensors:latest:field_01`) and a configurable TTL.
4. **Read**: The dashboard reads data directly from Redis, getting responses in less than a second.
5. **Monitoring**: The service tracks statistics and cache health status.

## Example Redis keys
- `sensors:latest:{field_id}` — latest sensor data for each field
- `weather:latest:{location}` — latest weather data for each location
- `predictions:latest:{field_id}` — latest ML anomaly for each field
- `alerts:active` — list of active alerts

## Configuration
Main environment variables:
- `REDIS_HOST`, `REDIS_PORT`: where Redis is located
- `KAFKA_BOOTSTRAP_SERVERS`: where Kafka is located
- `SENSOR_DATA_TTL`, `WEATHER_DATA_TTL`, etc.: how long data stays in cache (in seconds)


## Why use it
- The dashboard becomes much faster (responses in less than 1 second)
- Data is always fresh and up-to-date
- The system is more stable and scalable
