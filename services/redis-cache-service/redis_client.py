#!/usr/bin/env python3
"""
Redis Client - High-performance Redis operations manager
Handles all Redis interactions with connection pooling, error handling, and data serialization
"""

import redis
import ujson
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from loguru import logger
from contextlib import contextmanager
from dataclasses import asdict

from config import get_redis_config, get_cache_config, get_service_config


class RedisClient:
    """
    High-performance Redis client with connection pooling and smart caching strategies
    """
    
    def __init__(self):
        self.redis_config = get_redis_config()
        self.cache_config = get_cache_config()
        self.service_config = get_service_config()
        
        # Initialize connection pool
        self.pool = redis.ConnectionPool(
            host=self.redis_config.host,
            port=self.redis_config.port,
            db=self.redis_config.db,
            password=self.redis_config.password if self.redis_config.password else None,
            socket_timeout=self.redis_config.socket_timeout,
            max_connections=self.redis_config.connection_pool_max_connections,
            retry_on_timeout=True,
            health_check_interval=30
        )
        
        self.redis = redis.Redis(connection_pool=self.pool)
        self.connected = False
        
        # Pipeline for batch operations
        self.pipeline_batch_size = 100
        
        logger.info(f"🔧 Redis client initialized for {self.redis_config.host}:{self.redis_config.port}")
    
    def connect(self) -> bool:
        """Establish connection to Redis with retry logic"""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Test connection
                self.redis.ping()
                self.connected = True
                logger.success(f"✅ Connected to Redis at {self.redis_config.host}:{self.redis_config.port}")
                return True
            except Exception as e:
                logger.warning(f"❌ Redis connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        
        logger.error("❌ Failed to connect to Redis after all retry attempts")
        return False
    
    def disconnect(self):
        """Close Redis connection"""
        try:
            self.pool.disconnect()
            self.connected = False
            logger.info("🔌 Redis connection closed")
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")
    
    @contextmanager
    def get_pipeline(self):
        """Context manager for Redis pipeline operations"""
        pipeline = self.redis.pipeline()
        try:
            yield pipeline
        finally:
            pass  # Pipeline will be executed explicitly
    
    # ==================== SENSOR DATA OPERATIONS ====================
    
    def cache_sensor_data(self, field_id: str, sensor_data: Dict[str, Any]) -> bool:
        """
        Cache latest sensor data for a specific field
        
        Args:
            field_id: Unique field identifier
            sensor_data: Sensor measurement data
        
        Returns:
            bool: True if cached successfully
        """
        try:
            key = self.cache_config.sensor_latest_pattern.format(field_id=field_id)
            
            # Add caching metadata
            cache_data = {
                **sensor_data,
                "cached_at": datetime.now().isoformat(),
                "cache_ttl": self.cache_config.sensor_data_ttl
            }
            
            # Serialize and cache with TTL
            serialized_data = ujson.dumps(cache_data)
            result = self.redis.setex(
                key, 
                self.cache_config.sensor_data_ttl, 
                serialized_data
            )
            
            if result:
                logger.debug(f"📡 Cached sensor data for field {field_id}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error caching sensor data for {field_id}: {e}")
            return False
    
    def get_sensor_data(self, field_id: str) -> Optional[Dict[str, Any]]:
        """Get cached sensor data for a specific field"""
        try:
            key = self.cache_config.sensor_latest_pattern.format(field_id=field_id)
            cached_data = self.redis.get(key)
            
            if cached_data:
                data = ujson.loads(cached_data)
                logger.debug(f"📖 Retrieved sensor data for field {field_id}")
                return data
            
            logger.debug(f"❌ No cached sensor data for field {field_id}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error retrieving sensor data for {field_id}: {e}")
            return None
    
    def get_all_sensor_data(self) -> Dict[str, Dict[str, Any]]:
        """Get all cached sensor data"""
        try:
            pattern = self.cache_config.sensor_latest_pattern.format(field_id="*")
            keys = self.redis.keys(pattern)
            
            if not keys:
                return {}
            
            # Use pipeline for batch retrieval
            with self.get_pipeline() as pipe:
                for key in keys:
                    pipe.get(key)
                results = pipe.execute()
            
            sensor_data = {}
            for key, data in zip(keys, results):
                if data:
                    # Extract field_id from key
                    field_id = key.decode('utf-8').split(':')[-1]
                    sensor_data[field_id] = ujson.loads(data)
            
            logger.debug(f"📖 Retrieved sensor data for {len(sensor_data)} fields")
            return sensor_data
            
        except Exception as e:
            logger.error(f"❌ Error retrieving all sensor data: {e}")
            return {}
    
    # ==================== WEATHER DATA OPERATIONS ====================
    
    def cache_weather_data(self, location: str, weather_data: Dict[str, Any]) -> bool:
        """Cache latest weather data for a specific location"""
        try:
            key = self.cache_config.weather_latest_pattern.format(location=location)
            
            # Add caching metadata
            cache_data = {
                **weather_data,
                "cached_at": datetime.now().isoformat(),
                "cache_ttl": self.cache_config.weather_data_ttl
            }
            
            # Serialize and cache with TTL
            serialized_data = ujson.dumps(cache_data)
            result = self.redis.setex(
                key,
                self.cache_config.weather_data_ttl,
                serialized_data
            )
            
            if result:
                logger.debug(f"🌦️ Cached weather data for location {location}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error caching weather data for {location}: {e}")
            return False
    
    def get_weather_data(self, location: str) -> Optional[Dict[str, Any]]:
        """Get cached weather data for a specific location"""
        try:
            key = self.cache_config.weather_latest_pattern.format(location=location)
            cached_data = self.redis.get(key)
            
            if cached_data:
                data = ujson.loads(cached_data)
                logger.debug(f"📖 Retrieved weather data for location {location}")
                return data
            
            logger.debug(f"❌ No cached weather data for location {location}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error retrieving weather data for {location}: {e}")
            return None
    
    def get_all_weather_data(self) -> Dict[str, Dict[str, Any]]:
        """Get all cached weather data"""
        try:
            pattern = self.cache_config.weather_latest_pattern.format(location="*")
            keys = self.redis.keys(pattern)
            
            if not keys:
                return {}
            
            # Use pipeline for batch retrieval
            with self.get_pipeline() as pipe:
                for key in keys:
                    pipe.get(key)
                results = pipe.execute()
            
            weather_data = {}
            for key, data in zip(keys, results):
                if data:
                    # Extract location from key
                    location = key.decode('utf-8').split(':')[-1]
                    weather_data[location] = ujson.loads(data)
            
            logger.debug(f"📖 Retrieved weather data for {len(weather_data)} locations")
            return weather_data
            
        except Exception as e:
            logger.error(f"❌ Error retrieving all weather data: {e}")
            return {}
    
    # ==================== ALERTS OPERATIONS ====================
    
    def cache_active_alerts(self, alerts: List[Dict[str, Any]]) -> bool:
        """Cache list of active alerts"""
        try:
            # Add metadata
            cache_data = {
                "alerts": alerts,
                "cached_at": datetime.now().isoformat(),
                "count": len(alerts),
                "cache_ttl": self.cache_config.alert_data_ttl
            }
            
            serialized_data = ujson.dumps(cache_data)
            result = self.redis.setex(
                self.cache_config.alerts_active_key,
                self.cache_config.alert_data_ttl,
                serialized_data
            )
            
            if result:
                logger.debug(f"🚨 Cached {len(alerts)} active alerts")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error caching active alerts: {e}")
            return False
    
    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get cached active alerts"""
        try:
            cached_data = self.redis.get(self.cache_config.alerts_active_key)
            
            if cached_data:
                data = ujson.loads(cached_data)
                alerts = data.get("alerts", [])
                logger.debug(f"📖 Retrieved {len(alerts)} active alerts")
                return alerts
            
            logger.debug("❌ No cached active alerts")
            return []
            
        except Exception as e:
            logger.error(f"❌ Error retrieving active alerts: {e}")
            return []
    
    # ==================== UTILITY OPERATIONS ====================
    
    def batch_cache_sensor_data(self, sensor_batch: List[Dict[str, Any]]) -> int:
        """
        Batch cache multiple sensor data records efficiently
        
        Args:
            sensor_batch: List of sensor data records with field_id
        
        Returns:
            int: Number of successfully cached records
        """
        if not sensor_batch:
            return 0
        
        cached_count = 0
        
        try:
            with self.get_pipeline() as pipe:
                for sensor_data in sensor_batch:
                    field_id = sensor_data.get("field_id")
                    if field_id:
                        key = self.cache_config.sensor_latest_pattern.format(field_id=field_id)
                        
                        cache_data = {
                            **sensor_data,
                            "cached_at": datetime.now().isoformat(),
                            "cache_ttl": self.cache_config.sensor_data_ttl
                        }
                        
                        serialized_data = ujson.dumps(cache_data)
                        pipe.setex(key, self.cache_config.sensor_data_ttl, serialized_data)
                
                results = pipe.execute()
                cached_count = sum(1 for result in results if result)
                
            logger.debug(f"📡 Batch cached {cached_count}/{len(sensor_batch)} sensor records")
            return cached_count
            
        except Exception as e:
            logger.error(f"❌ Error batch caching sensor data: {e}")
            return 0
    
    def batch_cache_weather_data(self, weather_batch: List[Dict[str, Any]]) -> int:
        """Batch cache multiple weather data records efficiently"""
        if not weather_batch:
            return 0
        
        cached_count = 0
        
        try:
            with self.get_pipeline() as pipe:
                for weather_data in weather_batch:
                    location = weather_data.get("location")
                    if location:
                        key = self.cache_config.weather_latest_pattern.format(location=location)
                        
                        cache_data = {
                            **weather_data,
                            "cached_at": datetime.now().isoformat(),
                            "cache_ttl": self.cache_config.weather_data_ttl
                        }
                        
                        serialized_data = ujson.dumps(cache_data)
                        pipe.setex(key, self.cache_config.weather_data_ttl, serialized_data)
                
                results = pipe.execute()
                cached_count = sum(1 for result in results if result)
                
            logger.debug(f"🌦️ Batch cached {cached_count}/{len(weather_batch)} weather records")
            return cached_count
            
        except Exception as e:
            logger.error(f"❌ Error batch caching weather data: {e}")
            return 0
    
    def health_check(self) -> Dict[str, Any]:
        """Perform Redis health check"""
        try:
            start_time = time.time()
            
            # Test basic operations
            test_key = "health:check"
            self.redis.set(test_key, "ok", ex=10)
            value = self.redis.get(test_key)
            self.redis.delete(test_key)
            
            response_time = time.time() - start_time
            
            # Get Redis info
            info = self.redis.info()
            
            return {
                "status": "healthy",
                "connected": value == b"ok",
                "response_time_ms": round(response_time * 1000, 2),
                "memory_used": info.get("used_memory_human"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "connected_clients": info.get("connected_clients", 0)
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "connected": False
            }
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache statistics and key counts"""
        try:
            stats = {
                "sensor_keys": len(self.redis.keys("sensors:latest:*")),
                "weather_keys": len(self.redis.keys("weather:latest:*")),
                "alert_keys": len(self.redis.keys("alerts:*")),
                "total_keys": self.redis.dbsize()
            }
            
            # Redis info
            info = self.redis.info()
            stats.update({
                "memory_used": info.get("used_memory_human"),
                "memory_peak": info.get("used_memory_peak_human"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_ratio": 0 if info.get("keyspace_hits", 0) == 0 else 
                           info.get("keyspace_hits", 0) / (info.get("keyspace_hits", 0) + info.get("keyspace_misses", 1)) * 100
            })
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting cache statistics: {e}")
            return {}
    
    def clear_expired_keys(self) -> int:
        """Clear expired keys manually (Redis handles this automatically, but this is for monitoring)"""
        try:
            # This is mainly for monitoring - Redis handles TTL automatically
            # We can get expired key count from Redis info
            info = self.redis.info()
            expired_keys = info.get("expired_keys", 0)
            return expired_keys
        except Exception as e:
            logger.error(f"❌ Error getting expired keys count: {e}")
            return 0 