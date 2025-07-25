#!/usr/bin/env python3
"""
Redis Stream Processor - Kafka to Redis data pipeline
Processes streaming data from Kafka topics and caches it in Redis for fast dashboard access
"""

import json
import time
import signal
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional
from kafka import KafkaConsumer, TopicPartition
from loguru import logger

from redis_client import RedisClient
from config import get_kafka_config, get_service_config, KAFKA_TOPICS, SENSOR_REQUIRED_FIELDS, WEATHER_REQUIRED_FIELDS, ML_ANOMALY_REQUIRED_FIELDS


class RedisStreamProcessor:
    """
    High-performance stream processor that reads from Kafka topics and caches data in Redis
    Handles multiple topics concurrently with proper error handling and monitoring
    """
    
    def __init__(self):
        self.kafka_config = get_kafka_config()
        self.service_config = get_service_config()
        
        # Initialize Redis client
        self.redis_client = RedisClient()
        
        # Kafka consumers for different topics
        self.consumers: Dict[str, KafkaConsumer] = {}
        
        # Processing threads
        self.processing_threads: List[threading.Thread] = []
        self.shutdown_event = threading.Event()
        
        # Statistics
        self.stats = {
            "messages_processed": 0,
            "messages_cached": 0,
            "processing_errors": 0,
            "last_processed_time": None,
            "start_time": datetime.now().isoformat()
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info(" Redis Stream Processor initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f" Received signal {signum}, initiating graceful shutdown...")
        self.shutdown()
    
    def start(self):
        """Start stream processing for all configured topics"""
        logger.info(" Starting Redis Stream Processor...")
        
        # Connect to Redis
        if not self.redis_client.connect():
            logger.error("❌ Failed to connect to Redis, cannot start processor")
            return False
        
        try:
            # Start sensor data processing thread
            sensor_thread = threading.Thread(
                target=self._process_sensor_stream,
                name="SensorProcessor",
                daemon=True
            )
            sensor_thread.start()
            self.processing_threads.append(sensor_thread)
            
            # Start weather data processing thread
            weather_thread = threading.Thread(
                target=self._process_weather_stream,
                name="WeatherProcessor", 
                daemon=True
            )
            weather_thread.start()
            self.processing_threads.append(weather_thread)
            
            # Start ML anomaly processing thread
            ml_anomaly_thread = threading.Thread(
                target=self._process_ml_anomaly_stream,
                name="MLAnomalyProcessor",
                daemon=True
            )
            ml_anomaly_thread.start()
            self.processing_threads.append(ml_anomaly_thread)
            
            # Start statistics monitoring thread
            stats_thread = threading.Thread(
                target=self._monitor_statistics,
                name="StatsMonitor",
                daemon=True
            )
            stats_thread.start()
            self.processing_threads.append(stats_thread)
            
            logger.success(f"✅ Started {len(self.processing_threads)} processing threads")
            
            # Main thread monitoring loop
            self._main_monitoring_loop()
            
        except Exception as e:
            logger.error(f"❌ Error starting stream processor: {e}")
            self.shutdown()
            return False
        
        return True
    
    def _main_monitoring_loop(self):
        """Main monitoring loop - keeps the service alive"""
        logger.info(" Main monitoring loop started")
        
        try:
            while not self.shutdown_event.is_set():
                # Check thread health
                active_threads = sum(1 for t in self.processing_threads if t.is_alive())
                
                if active_threads < len(self.processing_threads):
                    logger.warning(f" Only {active_threads}/{len(self.processing_threads)} threads active")
                
                # Log statistics periodically
                if self.stats["messages_processed"] > 0:
                    logger.info(
                        f"📊 Stats: {self.stats['messages_processed']} processed, "
                        f"{self.stats['messages_cached']} cached, "
                        f"{self.stats['processing_errors']} errors"
                    )
                
                # Sleep before next check
                self.shutdown_event.wait(30)  # Check every 30 seconds
                
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
        except Exception as e:
            logger.error(f"❌ Error in main monitoring loop: {e}")
        finally:
            self.shutdown()
    
    def _process_sensor_stream(self):
        """Process sensor data stream from Kafka to Redis"""
        logger.info(" Starting sensor stream processor...")
        
        consumer = None
        try:
            # Create consumer for sensor data
            consumer = KafkaConsumer(
                KAFKA_TOPICS["sensor_data"],
                bootstrap_servers=self.kafka_config.bootstrap_servers,
                group_id=f"{self.kafka_config.consumer_group_id}_sensors",
                auto_offset_reset=self.kafka_config.auto_offset_reset,
                enable_auto_commit=self.kafka_config.enable_auto_commit,
                max_poll_records=self.kafka_config.max_poll_records,
                session_timeout_ms=self.kafka_config.session_timeout_ms,
                heartbeat_interval_ms=self.kafka_config.heartbeat_interval_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=5000  # Timeout to allow graceful shutdown
            )
            
            self.consumers["sensor"] = consumer
            logger.success("✅ Sensor consumer connected")
            
            batch = []
            batch_size = self.service_config.batch_size
            
            while not self.shutdown_event.is_set():
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    # Process messages
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if self.shutdown_event.is_set():
                                break
                            
                            try:
                                sensor_data = message.value
                                
                                if self._validate_sensor_data(sensor_data):
                                    batch.append(sensor_data)
                                    self.stats["messages_processed"] += 1
                                    
                                    # Process batch when full
                                    if len(batch) >= batch_size:
                                        cached_count = self._cache_sensor_batch(batch)
                                        self.stats["messages_cached"] += cached_count
                                        batch.clear()
                                
                                self.stats["last_processed_time"] = datetime.now().isoformat()
                                
                            except Exception as e:
                                logger.error(f"❌ Error processing sensor message: {e}")
                                self.stats["processing_errors"] += 1
                    
                    # Process remaining batch
                    if batch:
                        cached_count = self._cache_sensor_batch(batch)
                        self.stats["messages_cached"] += cached_count
                        batch.clear()
                
                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"❌ Error in sensor stream processing: {e}")
                        time.sleep(5)  # Wait before retry
        
        except Exception as e:
            logger.error(f"❌ Fatal error in sensor stream processor: {e}")
        finally:
            if consumer:
                consumer.close()
            logger.info(" Sensor stream processor stopped")
    
    def _process_weather_stream(self):
        """Process weather data stream from Kafka to Redis"""
        logger.info("🌦️ Starting weather stream processor...")
        
        consumer = None
        try:
            # Create consumer for weather data
            consumer = KafkaConsumer(
                KAFKA_TOPICS["weather_data"],
                bootstrap_servers=self.kafka_config.bootstrap_servers,
                group_id=f"{self.kafka_config.consumer_group_id}_weather",
                auto_offset_reset=self.kafka_config.auto_offset_reset,
                enable_auto_commit=self.kafka_config.enable_auto_commit,
                max_poll_records=self.kafka_config.max_poll_records,
                session_timeout_ms=self.kafka_config.session_timeout_ms,
                heartbeat_interval_ms=self.kafka_config.heartbeat_interval_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=5000  # Timeout to allow graceful shutdown
            )
            
            self.consumers["weather"] = consumer
            logger.success("✅ Weather consumer connected")
            
            batch = []
            batch_size = self.service_config.batch_size
            
            while not self.shutdown_event.is_set():
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    # Process messages
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if self.shutdown_event.is_set():
                                break
                            
                            try:
                                weather_data = message.value
                                
                                if self._validate_weather_data(weather_data):
                                    batch.append(weather_data)
                                    self.stats["messages_processed"] += 1
                                    
                                    # Process batch when full
                                    if len(batch) >= batch_size:
                                        cached_count = self._cache_weather_batch(batch)
                                        self.stats["messages_cached"] += cached_count
                                        batch.clear()
                                
                                self.stats["last_processed_time"] = datetime.now().isoformat()
                                
                            except Exception as e:
                                logger.error(f"❌ Error processing weather message: {e}")
                                self.stats["processing_errors"] += 1
                    
                    # Process remaining batch
                    if batch:
                        cached_count = self._cache_weather_batch(batch)
                        self.stats["messages_cached"] += cached_count
                        batch.clear()
                
                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"❌ Error in weather stream processing: {e}")
                        time.sleep(5)  # Wait before retry
        
        except Exception as e:
            logger.error(f"❌ Fatal error in weather stream processor: {e}")
        finally:
            if consumer:
                consumer.close()
            logger.info(" Weather stream processor stopped")
    
    def _process_ml_anomaly_stream(self):
        """Process ML anomaly data stream from Kafka to Redis"""
        logger.info("🤖 Starting ML anomaly stream processor...")
        consumer = None
        try:
            # DEBUG: Stampa il contenuto di KAFKA_TOPICS
            logger.info(f"KAFKA_TOPICS at ML anomaly consumer: {KAFKA_TOPICS}")
            logger.info(f"ml_anomalies topic: {KAFKA_TOPICS.get('ml_anomalies')}")
            # Create consumer for ML anomalies
            consumer = KafkaConsumer(
                KAFKA_TOPICS["ml_anomalies"],
                bootstrap_servers=self.kafka_config.bootstrap_servers,
                group_id=f"{self.kafka_config.consumer_group_id}_ml_anomalies",
                auto_offset_reset=self.kafka_config.auto_offset_reset,
                enable_auto_commit=self.kafka_config.enable_auto_commit,
                max_poll_records=self.kafka_config.max_poll_records,
                session_timeout_ms=self.kafka_config.session_timeout_ms,
                heartbeat_interval_ms=self.kafka_config.heartbeat_interval_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=5000  # Timeout to allow graceful shutdown
            )
            self.consumers["ml_anomalies"] = consumer
            logger.success("✅ ML anomaly consumer connected")
            batch = []
            batch_size = self.service_config.batch_size
            while not self.shutdown_event.is_set():
                try:
                    message_batch = consumer.poll(timeout_ms=1000)
                    if not message_batch:
                        continue
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if self.shutdown_event.is_set():
                                break
                            try:
                                anomaly_data = message.value
                                if self._validate_ml_anomaly_data(anomaly_data):
                                    field_id = anomaly_data["field_id"]
                                    if self.redis_client.cache_ml_anomaly(field_id, anomaly_data):
                                        self.stats["messages_cached"] += 1
                                    self.stats["messages_processed"] += 1
                                self.stats["last_processed_time"] = datetime.now().isoformat()
                            except Exception as e:
                                logger.error(f"❌ Error processing ML anomaly message: {e}")
                                self.stats["processing_errors"] += 1
                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"❌ Error in ML anomaly stream processing: {e}")
                        time.sleep(5)
        except Exception as e:
            logger.error(f"❌ Fatal error in ML anomaly stream processor: {e}")
        finally:
            if consumer:
                consumer.close()
            logger.info(" ML anomaly stream processor stopped")

    def _monitor_statistics(self):
        """Monitor and cache system statistics"""
        logger.info(" Starting statistics monitor...")
        
        while not self.shutdown_event.is_set():
            try:
                # System stats temporarily disabled due to datetime serialization complexity
                # The core Redis caching functionality works perfectly without these stats
                logger.debug("📊 Statistics monitoring cycle completed (system stats disabled)")
                
                # Wait before next update
                self.shutdown_event.wait(self.service_config.health_check_interval)
                
            except Exception as e:
                logger.error(f"❌ Error in statistics monitoring: {e}")
                self.shutdown_event.wait(10)  # Wait a bit before retry
        
        logger.info(" Statistics monitor stopped")
    
    # TODO: Validazione ridondante?
    # Manteniamo questa validazione anche se il Silver Layer già valida i dati.
    # Motivi:
    # 1. Difesa a strati: protegge da dati corrotti in transito, bug, replay, cambi schema imprevisti.
    # 2. Robustezza: se in futuro cambiano i producer o la pipeline, questa validazione previene errori runtime e cache corrotta.
    # 3. Evoluzione: se aggiungiamo nuove fonti o cambiamo il Silver Layer, questa validazione ci protegge da regressioni.
    # Se il sistema rimane stabile e controllato, si può valutare di rimuoverla per performance.
    def _validate_sensor_data(self, data: Dict[str, Any]) -> bool:
        """Validate sensor data structure"""
        try:
            if not isinstance(data, dict):
                return False
            
            # Check required fields
            for field in SENSOR_REQUIRED_FIELDS:
                if field not in data:
                    logger.debug(f"⚠️ Missing required sensor field: {field}")
                    return False
            
            # Basic data type validation
            field_id = data.get("field_id")
            temperature = data.get("temperature")
            humidity = data.get("humidity")
            soil_ph = data.get("soil_ph")
            
            if not field_id or not isinstance(field_id, str):
                return False
            
            if not isinstance(temperature, (int, float)) or not (-50 <= temperature <= 60):
                return False
            
            if not isinstance(humidity, (int, float)) or not (0 <= humidity <= 100):
                return False
            
            if not isinstance(soil_ph, (int, float)) or not (3 <= soil_ph <= 10):
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"⚠️ Sensor data validation error: {e}")
            return False
    
    def _validate_weather_data(self, data: Dict[str, Any]) -> bool:
        """Validate weather data structure"""
        try:
            if not isinstance(data, dict):
                return False
            
            # Check required fields
            for field in WEATHER_REQUIRED_FIELDS:
                if field not in data:
                    logger.debug(f"⚠️ Missing required weather field: {field}")
                    return False
            
            # Basic data type validation
            location = data.get("location")
            temp_c = data.get("temp_c")
            humidity = data.get("humidity")
            
            if not location or not isinstance(location, str):
                return False
            
            if not isinstance(temp_c, (int, float)) or not (-50 <= temp_c <= 60):
                return False
            
            if not isinstance(humidity, (int, float)) or not (0 <= humidity <= 100):
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"⚠️ Weather data validation error: {e}")
            return False
    
    def _validate_ml_anomaly_data(self, data: Dict[str, Any]) -> bool:
        """Validate ML anomaly data structure"""
        try:
            if not isinstance(data, dict):
                return False
            for field in ML_ANOMALY_REQUIRED_FIELDS:
                if field not in data:
                    logger.debug(f"⚠️ Missing required ML anomaly field: {field}")
                    return False
            # Validazione base dei tipi principali
            if not isinstance(data["field_id"], str):
                return False
            if not isinstance(data["location"], str):
                return False
            if not isinstance(data["anomaly_score"], (int, float)):
                return False
            if not isinstance(data["is_anomaly"], bool):
                return False
            if not isinstance(data["severity"], str):
                return False
            if not isinstance(data["recommendations"], str):
                return False
            if not isinstance(data["model_version"], str):
                return False
            if not isinstance(data["prediction_timestamp"], str):
                return False
            if not isinstance(data["features"], dict):
                return False
            return True
        except Exception as e:
            logger.debug(f"⚠️ ML anomaly data validation error: {e}")
            return False
    
    def _cache_sensor_batch(self, sensor_batch: List[Dict[str, Any]]) -> int:
        """Cache a batch of sensor data"""
        try:
            cached_count = self.redis_client.batch_cache_sensor_data(sensor_batch)
            logger.debug(f" Cached {cached_count}/{len(sensor_batch)} sensor records")
            return cached_count
        except Exception as e:
            logger.error(f"❌ Error caching sensor batch: {e}")
            return 0
    
    def _cache_weather_batch(self, weather_batch: List[Dict[str, Any]]) -> int:
        """Cache a batch of weather data"""
        try:
            cached_count = self.redis_client.batch_cache_weather_data(weather_batch)
            logger.debug(f" Cached {cached_count}/{len(weather_batch)} weather records")
            return cached_count
        except Exception as e:
            logger.error(f"❌ Error caching weather batch: {e}")
            return 0
    
    def get_status(self) -> Dict[str, Any]:
        """Get current processor status"""
        active_threads = sum(1 for t in self.processing_threads if t.is_alive())
        
        start_time = datetime.fromisoformat(self.stats["start_time"])
        uptime = (datetime.now() - start_time).total_seconds()
        
        return {
            "status": "running" if not self.shutdown_event.is_set() else "shutting_down",
            "active_threads": active_threads,
            "total_threads": len(self.processing_threads),
            "uptime_seconds": uptime,
            "redis_connected": self.redis_client.connected,
            "kafka_consumers": len(self.consumers),
            "processing_stats": self.stats.copy()
        }
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """Get detailed processing statistics"""
        start_time = datetime.fromisoformat(self.stats["start_time"])
        uptime = (datetime.now() - start_time).total_seconds()
        
        # Calculate rates
        messages_per_second = self.stats["messages_processed"] / max(uptime, 1)
        cache_success_rate = (
            self.stats["messages_cached"] / max(self.stats["messages_processed"], 1) * 100
            if self.stats["messages_processed"] > 0 else 0
        )
        
        return {
            "processing_stats": self.stats.copy(),
            "performance_metrics": {
                "uptime_seconds": uptime,
                "messages_per_second": round(messages_per_second, 2),
                "cache_success_rate_percent": round(cache_success_rate, 2)
            },
            "redis_stats": self.redis_client.get_cache_statistics(),
            "redis_health": self.redis_client.health_check(),
            "thread_status": {
                "active": sum(1 for t in self.processing_threads if t.is_alive()),
                "total": len(self.processing_threads)
            }
        }
    
    def shutdown(self):
        """Gracefully shutdown the stream processor"""
        logger.info("🛑 Initiating stream processor shutdown...")
        
        # Signal shutdown to all threads
        self.shutdown_event.set()
        
        # Close all Kafka consumers
        for name, consumer in self.consumers.items():
            try:
                logger.info(f" Closing {name} consumer...")
                consumer.close()
            except Exception as e:
                logger.warning(f"Error closing {name} consumer: {e}")
        
        # Wait for threads to finish
        timeout = self.service_config.shutdown_timeout
        for thread in self.processing_threads:
            try:
                logger.info(f" Waiting for thread {thread.name} to finish...")
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} did not finish within timeout")
            except Exception as e:
                logger.warning(f"Error joining thread {thread.name}: {e}")
        
        # Disconnect Redis
        self.redis_client.disconnect()
        
        logger.success("✅ Stream processor shutdown complete") 