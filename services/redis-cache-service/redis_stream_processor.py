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
from config import get_kafka_config, get_service_config, KAFKA_TOPICS, SENSOR_REQUIRED_FIELDS, WEATHER_REQUIRED_FIELDS, ML_ANOMALY_REQUIRED_FIELDS, ALERT_REQUIRED_FIELDS


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
            
            # Start alerts processing thread
            alerts_thread = threading.Thread(
                target=self._process_alerts_stream,
                name="AlertsProcessor",
                daemon=True
            )
            alerts_thread.start()
            self.processing_threads.append(alerts_thread)
            
            
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
        logger.info("Main monitoring loop started")
        
        try:
            while not self.shutdown_event.is_set():
                # Check thread health
                active_threads = sum(1 for t in self.processing_threads if t.is_alive())
                
                if active_threads < len(self.processing_threads):
                    logger.warning(f"Only {active_threads}/{len(self.processing_threads)} threads active")
                
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
        logger.info("Starting sensor stream processor...")
        
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
            logger.info("Sensor stream processor stopped")
    
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
            logger.info("Weather stream processor stopped")
    
    def _process_ml_anomaly_stream(self):
        """Process ML anomaly data stream from Kafka to Redis"""
        logger.info("🤖 Starting ML anomaly stream processor...")
        consumer = None
        try:
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
            logger.info("ML anomaly stream processor stopped")

    def _process_alerts_stream(self):
        """Process alerts data stream from Kafka to Redis"""
        logger.info("🚨 Starting alerts stream processor...")
        consumer = None
        try:
            # Create consumer for alerts
            consumer = KafkaConsumer(
                KAFKA_TOPICS["alerts_anomalies"],
                bootstrap_servers=self.kafka_config.bootstrap_servers,
                group_id=f"{self.kafka_config.consumer_group_id}_alerts",
                auto_offset_reset=self.kafka_config.auto_offset_reset,
                enable_auto_commit=self.kafka_config.enable_auto_commit,
                max_poll_records=self.kafka_config.max_poll_records,
                session_timeout_ms=self.kafka_config.session_timeout_ms,
                heartbeat_interval_ms=self.kafka_config.heartbeat_interval_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=5000  # Timeout to allow graceful shutdown
            )
            
            self.consumers["alerts"] = consumer
            logger.success("✅ Alerts consumer connected")
            
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
                                alert_data = message.value
                                
                                if self._validate_alert_data(alert_data):
                                    zone_id = alert_data["zone_id"]
                                    if self.redis_client.cache_latest_alert(zone_id, alert_data):
                                        self.stats["messages_cached"] += 1
                                    self.stats["messages_processed"] += 1
                                
                                self.stats["last_processed_time"] = datetime.now().isoformat()
                                
                            except Exception as e:
                                logger.error(f"❌ Error processing alert message: {e}")
                                self.stats["processing_errors"] += 1
                
                except Exception as e:
                    if not self.shutdown_event.is_set():
                        logger.error(f"❌ Error in alerts stream processing: {e}")
                        time.sleep(5)
        
        except Exception as e:
            logger.error(f"❌ Fatal error in alerts stream processor: {e}")
        finally:
            if consumer:
                consumer.close()
            logger.info("Alerts stream processor stopped")

   
    # ==================== DATA VALIDATION FUNCTIONS ====================
    # 
    # Ottimizzazioni implementate:
    # 1. Allineamento range con Silver Layer per coerenza
    # 2. Aggiunta validazione timestamp per robustezza
    # 3. Aggiunta validazione coordinate per completezza
    # 4. Riduzione logging da debug a warning per performance
    # 5. Aggiunta tutti i campi inviati dal Silver Layer
    #
    # Motivi per mantenere questa validazione:
    # - Difesa a strati: protegge da dati corrotti in transito
    # - Robustezza: previene errori runtime e cache corrotta
    # - Evoluzione: protegge da cambiamenti futuri nella pipeline
    
    def _validate_sensor_data(self, data: Dict[str, Any]) -> bool:
        """Validate sensor data structure"""
        try:
            if not isinstance(data, dict):
                return False
            
            # Check required fields
            for field in SENSOR_REQUIRED_FIELDS:
                if field not in data:
                    logger.warning(f"Missing required sensor field: {field}")
                    return False
            
            # Basic data type validation
            field_id = data.get("field_id")
            location = data.get("location")
            latitude = data.get("latitude")
            longitude = data.get("longitude")
            temperature = data.get("temperature")
            humidity = data.get("humidity")
            soil_ph = data.get("soil_ph")
            
            # Validate field_id and location
            if not field_id or not isinstance(field_id, str):
                return False
            
            if not location or not isinstance(location, str):
                return False
            
            # Validate coordinates (allineato con Silver Layer)
            if not isinstance(latitude, (int, float)) or not (-90 <= latitude <= 90):
                return False
            
            if not isinstance(longitude, (int, float)) or not (-180 <= longitude <= 180):
                return False
            
            # Validate temperature (allineato con Silver Layer: -20°C to 60°C)
            if not isinstance(temperature, (int, float)) or not (-20 <= temperature <= 60):
                return False
            
            # Validate humidity (allineato con Silver Layer: 0% to 100%)
            if not isinstance(humidity, (int, float)) or not (0 <= humidity <= 100):
                return False
            
            # Validate soil_ph (allineato con Silver Layer: 3.0 to 9.0)
            if not isinstance(soil_ph, (int, float)) or not (3.0 <= soil_ph <= 9.0):
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Sensor data validation error: {e}")
            return False
    
    def _validate_weather_data(self, data: Dict[str, Any]) -> bool:
        """Validate weather data structure"""
        try:
            if not isinstance(data, dict):
                return False
            
            # Check required fields
            for field in WEATHER_REQUIRED_FIELDS:
                if field not in data:
                    logger.warning(f"Missing required weather field: {field}")
                    return False
            
            # Basic data type validation
            location = data.get("location")
            temp_c = data.get("temp_c")
            humidity = data.get("humidity")
            wind_kph = data.get("wind_kph")
            uv = data.get("uv")
            condition = data.get("condition")
            precip_mm = data.get("precip_mm")
            
            # Validate location
            if not location or not isinstance(location, str):
                return False
            
            # Validate temp_c (allineato con Silver Layer: -50°C to 60°C)
            if not isinstance(temp_c, (int, float)) or not (-50 <= temp_c <= 60):
                return False
            
            # Validate humidity (allineato con Silver Layer: 0% to 100%)
            if not isinstance(humidity, (int, float)) or not (0 <= humidity <= 100):
                return False
            
            # Validate wind_kph (allineato con Silver Layer: 0 to 500)
            if not isinstance(wind_kph, (int, float)) or not (0 <= wind_kph <= 500):
                return False
            
            # Validate uv (allineato con Silver Layer: 0 to 20)
            if not isinstance(uv, (int, float)) or not (0 <= uv <= 20):
                return False
            
            # Validate condition
            if not condition or not isinstance(condition, str):
                return False
            
            # Validate precip_mm (allineato con Silver Layer: 0 to 1000)
            if not isinstance(precip_mm, (int, float)) or not (0 <= precip_mm <= 1000):
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Weather data validation error: {e}")
            return False
            

    def _validate_alert_data(self, data: Dict[str, Any]) -> bool:
        """Validate alert data structure"""
        try:
            if not isinstance(data, dict):
                return False
            
            # Check required fields
            for field in ALERT_REQUIRED_FIELDS:
                if field not in data:
                    logger.warning(f"Missing required alert field: {field}")
                    return False
            
            # Basic data type validation
            zone_id = data.get("zone_id")
            alert_type = data.get("alert_type")
            severity = data.get("severity")
            status = data.get("status")
            
            if not zone_id or not isinstance(zone_id, str):
                return False
            
            if not alert_type or not isinstance(alert_type, str):
                return False
            
            if not severity or not isinstance(severity, str):
                return False
            
            if not status or not isinstance(status, str):
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Alert data validation error: {e}")
            return False

    def _validate_ml_anomaly_data(self, data: Dict[str, Any]) -> bool:
        """Validate ML anomaly data structure"""
        try:
            if not isinstance(data, dict):
                return False
            for field in ML_ANOMALY_REQUIRED_FIELDS:
                if field not in data:
                    logger.warning(f"Missing required ML anomaly field: {field}")
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
            logger.warning(f"ML anomaly data validation error: {e}")
            return False
    
    def _cache_sensor_batch(self, sensor_batch: List[Dict[str, Any]]) -> int:
        """Cache a batch of sensor data"""
        try:
            cached_count = self.redis_client.batch_cache_sensor_data(sensor_batch)
            logger.debug(f" Cached {cached_count}/{len(sensor_batch)} sensor records")
            return cached_count
        except Exception as e:
            logger.error(f"Error caching sensor batch: {e}")
            return 0
    
    def _cache_weather_batch(self, weather_batch: List[Dict[str, Any]]) -> int:
        """Cache a batch of weather data"""
        try:
            cached_count = self.redis_client.batch_cache_weather_data(weather_batch)
            logger.debug(f" Cached {cached_count}/{len(weather_batch)} weather records")
            return cached_count
        except Exception as e:
            logger.error(f"Error caching weather batch: {e}")
            return 0
    
 
    def shutdown(self):
        """Gracefully shutdown the stream processor"""
        logger.info("🛑 Initiating stream processor shutdown...")
        
        # Signal shutdown to all threads
        self.shutdown_event.set()
        
        # Close all Kafka consumers
        for name, consumer in self.consumers.items():
            try:
                logger.info(f"Closing {name} consumer...")
                consumer.close()
            except Exception as e:
                logger.warning(f"Error closing {name} consumer: {e}")
        
        # Wait for threads to finish
        timeout = self.service_config.shutdown_timeout
        for thread in self.processing_threads:
            try:
                logger.info(f"Waiting for thread {thread.name} to finish...")
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} did not finish within timeout")
            except Exception as e:
                logger.warning(f"Error joining thread {thread.name}: {e}")
        
        # Disconnect Redis
        self.redis_client.disconnect()
        
        logger.success("✅ Stream processor shutdown complete") 