#!/usr/bin/env python3
"""
Redis Cache Service Configuration
Centralized configuration management for the Redis cache service
"""

import os
from dataclasses import dataclass
from typing import Dict, List
from decouple import config


@dataclass
class RedisConfig:
    """Redis connection configuration"""
    host: str = config("REDIS_HOST", default="redis")
    port: int = config("REDIS_PORT", default=6379, cast=int)
    db: int = config("REDIS_DB", default=0, cast=int)
    password: str = config("REDIS_PASSWORD", default="")
    socket_timeout: float = config("REDIS_SOCKET_TIMEOUT", default=5.0, cast=float)
    connection_pool_max_connections: int = config("REDIS_MAX_CONNECTIONS", default=20, cast=int)


@dataclass
class KafkaConfig:
    """Kafka connection configuration"""
    bootstrap_servers: List[str] = None
    consumer_group_id: str = config("KAFKA_CONSUMER_GROUP_ID", default="redis-cache-group")
    auto_offset_reset: str = config("KAFKA_AUTO_OFFSET_RESET", default="latest")
    enable_auto_commit: bool = config("KAFKA_ENABLE_AUTO_COMMIT", default=True, cast=bool)
    max_poll_records: int = config("KAFKA_MAX_POLL_RECORDS", default=500, cast=int)
    session_timeout_ms: int = config("KAFKA_SESSION_TIMEOUT_MS", default=30000, cast=int)
    heartbeat_interval_ms: int = config("KAFKA_HEARTBEAT_INTERVAL_MS", default=3000, cast=int)
    
    def __post_init__(self):
        if self.bootstrap_servers is None:
            servers_str = config("KAFKA_BOOTSTRAP_SERVERS", default="kafka:9092")
            self.bootstrap_servers = [s.strip() for s in servers_str.split(",")]


@dataclass
class CacheConfig:
    """Cache TTL and key configuration"""
    # TTL values in seconds
    sensor_data_ttl: int = config("SENSOR_DATA_TTL", default=300, cast=int)  # 5 minutes
    weather_data_ttl: int = config("WEATHER_DATA_TTL", default=600, cast=int)  # 10 minutes
    alert_data_ttl: int = config("ALERT_DATA_TTL", default=1800, cast=int)  # 30 minutes
    prediction_data_ttl: int = config("PREDICTION_DATA_TTL", default=3600, cast=int)  # 1 hour
    
    # Key patterns
    sensor_latest_pattern: str = "sensors:latest:{field_id}"
    weather_latest_pattern: str = "weather:latest:{location}"
    alerts_active_key: str = "alerts:active" 
    alerts_latest_pattern: str = "alerts:latest:{zone_id}"
    predictions_latest_key: str = "predictions:latest" #TODO: # NOTA: Questo pattern NON viene utilizzato nel codice attuale!È definito ma non implementato
    prediction_latest_pattern: str = "predictions:latest:{field_id}"
    
    #TODO: # Template per dati aggregati per ora
# {field_id} = identificatore campo
# {hour} = timestamp ora (es: "2024-01-15-10"
# Chiavi che verrebbero generate:
# "sensors:hourly:field_01:2024-01-15-10"  # Dati field_01 alle 10:00
# "sensors:hourly:field_01:2024-01-15-11"  # Dati field_01 alle 11:00
# "sensors:hourly:field_02:2024-01-15-10"  # Dati field_02 alle 10:00
#Al momento non utilizzato, ma è definito per poterlo utilizzare in futuro
    # Aggregation patterns
    sensor_hourly_pattern: str = "sensors:hourly:{field_id}:{hour}"
    weather_hourly_pattern: str = "weather:hourly:{location}:{hour}"
    

@dataclass
class ServiceConfig:
    """General service configuration"""
    timezone: str = config("TIMEZONE", default="Europe/Rome")
    log_level: str = config("LOG_LEVEL", default="INFO")
    service_name: str = "redis-cache-service"
    
    # Processing configuration
    batch_size: int = config("PROCESSING_BATCH_SIZE", default=100, cast=int)
    processing_interval: float = config("PROCESSING_INTERVAL", default=1.0, cast=float)
    
    # Health monitoring
    health_check_interval: int = config("HEALTH_CHECK_INTERVAL", default=30, cast=int)
    
    # Graceful shutdown
    shutdown_timeout: int = config("SHUTDOWN_TIMEOUT", default=30, cast=int)


@dataclass
class AppConfig:
    """Main application configuration"""
    redis: RedisConfig = None
    kafka: KafkaConfig = None
    cache: CacheConfig = None
    service: ServiceConfig = None
    
    def __post_init__(self):
        self.redis = RedisConfig()
        self.kafka = KafkaConfig()
        self.cache = CacheConfig()
        self.service = ServiceConfig()


# Kafka topics configuration
KAFKA_TOPICS = {
    "sensor_data": "iot_valid_data",
    "weather_data": "weather_valid_data",  
    "ml_anomalies": "ml-anomalies",
    "alerts_anomalies": "alerts-anomalies" 
}

# Redis key prefixes for different data types
REDIS_PREFIXES = {
    "sensors": "sensors",
    "weather": "weather", 
    "alerts": "alerts",
    "predictions": "predictions",
    "system": "system"
}

# Data validation schemas (basic validation)
SENSOR_REQUIRED_FIELDS = ["field_id", "location", "latitude", "longitude", "temperature", "humidity", "soil_ph"]
WEATHER_REQUIRED_FIELDS = ["location", "temp_c", "humidity", "wind_kph", "uv", "condition", "precip_mm"]

# ML anomaly required fields
ML_ANOMALY_REQUIRED_FIELDS = [
    "field_id",
    "location",
    "anomaly_score",
    "is_anomaly",
    "severity",
    "recommendations",
    "model_version",
    "prediction_timestamp",
    "features"
]

# Alert required fields
ALERT_REQUIRED_FIELDS = [
    "zone_id",
    "alert_timestamp", 
    "alert_type",
    "severity",
    "message",
    "status"
]

# Global configuration instance
config_instance = AppConfig()


def get_redis_config() -> RedisConfig:
    """Get Redis configuration"""
    return config_instance.redis


def get_kafka_config() -> KafkaConfig:
    """Get Kafka configuration"""
    return config_instance.kafka


def get_cache_config() -> CacheConfig:
    """Get cache configuration"""
    return config_instance.cache


def get_service_config() -> ServiceConfig:
    """Get service configuration"""
    return config_instance.service 