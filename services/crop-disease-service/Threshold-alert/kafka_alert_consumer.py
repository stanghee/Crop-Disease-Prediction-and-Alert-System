#!/usr/bin/env python3
"""
Kafka Alert Consumer
Consumes validated data from Kafka topics and generates alerts
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from pytz import timezone as ZoneInfo
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from kafka import KafkaConsumer
from config.alert_thresholds import AlertConfiguration, RiskLevel
from alert_factory import AlertCondition, AlertFactory
from database.alert_repository import AlertRepository

logger = logging.getLogger(__name__)

@dataclass
class AlertProcessingResult:
    """Result of alert processing operation"""
    success: bool
    alerts_generated: int
    errors: List[str]
    processing_time_ms: int

class KafkaAlertConsumer:
    """
    Consumes validated data from Kafka topics and generates alerts
    """
    
    def __init__(self, spark_session):
        self.spark_session = spark_session
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        
        # Initialize components
        self.config = AlertConfiguration()
        self.alert_factory = AlertFactory()
        self.alert_repository = AlertRepository(spark_session)
        
        # Track processed records to avoid duplicates
        self.processed_records = set()
        
        # Rate limiting: track last alert time per field and alert type
        self.last_alert_times = {}  # key: (field_id, alert_type), value: timestamp
        
        # Statistics
        self.stats = {
            'iot_messages_processed': 0,
            'weather_messages_processed': 0,
            'iot_alerts_generated': 0,
            'weather_alerts_generated': 0,
            'total_alerts_generated': 0,
            'last_processing_time': None
        }
        
        # Initialize consumers
        self.iot_consumer = None
        self.weather_consumer = None
        self.is_running = False
    
    def start(self):
        """Start the Kafka consumers"""
        if self.is_running:
            logger.warning("Kafka Alert Consumer is already running")
            return
        
        logger.info("Starting Kafka Alert Consumer...")
        
        try:
            # Initialize IoT consumer
            self.iot_consumer = KafkaConsumer(
                'iot_valid_data',
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='iot_alert_consumer_group'
            )
            
            # Initialize weather consumer
            self.weather_consumer = KafkaConsumer(
                'weather_valid_data',
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='weather_alert_consumer_group'
            )
            
            self.is_running = True
            logger.info("Kafka Alert Consumer started successfully")
            
        except Exception as e:
            logger.error(f"Error starting Kafka Alert Consumer: {e}")
            self.stop()
            raise
    
    def stop(self):
        """Stop the Kafka consumers"""
        logger.info("Stopping Kafka Alert Consumer...")
        self.is_running = False
        
        if self.iot_consumer:
            self.iot_consumer.close()
            self.iot_consumer = None
        
        if self.weather_consumer:
            self.weather_consumer.close()
            self.weather_consumer = None
        
        logger.info("Kafka Alert Consumer stopped")
    
    def process_iot_alerts(self) -> AlertProcessingResult:
        """Process IoT sensor data and generate alerts"""
        start_time = datetime.now()
        errors = []
        alerts_generated = 0
        
        try:
            if not self.iot_consumer:
                return AlertProcessingResult(False, 0, ["IoT consumer not initialized"], 0)
            
            # Process messages with shorter timeout for real-time processing
            messages = self.iot_consumer.poll(timeout_ms=100)
            
            for topic_partition, partition_messages in messages.items():
                for message in partition_messages:
                    try:
                        self.stats['iot_messages_processed'] += 1
                        
                        # Parse message
                        data = message.value
                        if not data:
                            continue
                        
                        # Generate alerts for this data point
                        alerts = self._generate_iot_alerts(data)
                        
                        if alerts:
                            # Calculate latency
                            data_timestamp = data.get('timestamp', datetime.now(timezone.utc).isoformat())
                            current_time = datetime.now(timezone.utc)
                            try:
                                if isinstance(data_timestamp, str):
                                    if data_timestamp.endswith('Z'):
                                        data_time = datetime.fromisoformat(data_timestamp.replace('Z', '+00:00'))
                                    else:
                                        data_time = datetime.fromisoformat(data_timestamp)
                                else:
                                    data_time = data_timestamp
                                
                                latency_seconds = (current_time - data_time).total_seconds()
                                logger.info(f"Alert latency: {latency_seconds:.2f}s for field {data.get('field_id', 'unknown')}")
                            except Exception as e:
                                logger.warning(f"Could not calculate latency: {e}")
                            
                            # Save alerts to database
                            self.alert_repository.save_alerts(alerts)
                            alerts_generated += len(alerts)
                            self.stats['iot_alerts_generated'] += len(alerts)
                            self.stats['total_alerts_generated'] += len(alerts)
                            
                            logger.info(f"Generated {len(alerts)} IoT alerts for field {data.get('field_id', 'unknown')}")
                        
                    except Exception as e:
                        error_msg = f"Error processing IoT message: {e}"
                        errors.append(error_msg)
                        logger.error(error_msg)
            
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            self.stats['last_processing_time'] = datetime.now().isoformat()
            
            return AlertProcessingResult(
                success=True,
                alerts_generated=alerts_generated,
                errors=errors,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"Error in IoT alert processing: {e}")
            return AlertProcessingResult(
                success=False,
                alerts_generated=0,
                errors=[str(e)],
                processing_time_ms=processing_time
            )
    
    def process_weather_alerts(self) -> AlertProcessingResult:
        """Process weather data and generate alerts"""
        start_time = datetime.now()
        errors = []
        alerts_generated = 0
        
        try:
            if not self.weather_consumer:
                return AlertProcessingResult(False, 0, ["Weather consumer not initialized"], 0)
            
            # Process messages with shorter timeout for real-time processing
            messages = self.weather_consumer.poll(timeout_ms=100)
            
            for topic_partition, partition_messages in messages.items():
                for message in partition_messages:
                    try:
                        self.stats['weather_messages_processed'] += 1
                        
                        # Parse message
                        data = message.value
                        if not data:
                            continue
                        
                        # Generate alerts for this data point
                        alerts = self._generate_weather_alerts(data)
                        
                        if alerts:
                            # Save alerts to database
                            self.alert_repository.save_alerts(alerts)
                            alerts_generated += len(alerts)
                            self.stats['weather_alerts_generated'] += len(alerts)
                            self.stats['total_alerts_generated'] += len(alerts)
                            
                            logger.info(f"Generated {len(alerts)} weather alerts for location {data.get('location', 'unknown')}")
                        
                    except Exception as e:
                        error_msg = f"Error processing weather message: {e}"
                        errors.append(error_msg)
                        logger.error(error_msg)
            
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            self.stats['last_processing_time'] = datetime.now().isoformat()
            
            return AlertProcessingResult(
                success=True,
                alerts_generated=alerts_generated,
                errors=errors,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"Error in weather alert processing: {e}")
            return AlertProcessingResult(
                success=False,
                alerts_generated=0,
                errors=[str(e)],
                processing_time_ms=processing_time
            )
    
    def _generate_iot_alerts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts from IoT sensor data"""
        alerts = []
        
        # Get all sensor threshold rules
        threshold_rules = self.config.get_sensor_thresholds()
        
        for rule in threshold_rules:
            field_value = data.get(rule.condition)
            
            if field_value is not None:
                # Evaluate threshold
                if self.config.evaluate_threshold(rule.condition, field_value, rule):
                    # Create AlertCondition
                    condition = AlertCondition(
                        zone_id=data.get('field_id', 'UNKNOWN_FIELD'),
                        alert_type=self._map_condition_to_alert_type(rule.condition, rule.operator, field_value),
                        risk_level=rule.risk_level,
                        value=field_value,
                        threshold=rule.value,
                        message=rule.message_template.format(value=field_value),
                        timestamp=data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        source='sensor',
                        metadata={
                            'data_quality': {
                                'temperature_valid': data.get('temperature_valid', True),
                                'humidity_valid': data.get('humidity_valid', True),
                                'ph_valid': data.get('ph_valid', True)
                            },
                            'rule_applied': {
                                'condition': rule.condition,
                                'operator': rule.operator,
                                'threshold': rule.value
                            }
                        }
                    )
                    
                    # Check if this alert should be generated (rate limiting and duplicates)
                    if self._should_generate_alert(condition):
                        alert = self.alert_factory.create_sensor_alert(condition)
                        if alert:
                            alerts.append(alert)
        
        return alerts
    
    def _generate_weather_alerts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts from weather data"""
        alerts = []
        
        # Get all weather threshold rules
        threshold_rules = self.config.get_weather_thresholds()
        
        for rule in threshold_rules:
            field_value = data.get(rule.condition)
            
            if field_value is not None:
                # Evaluate threshold
                if self.config.evaluate_threshold(rule.condition, field_value, rule):
                    # Create AlertCondition
                    condition = AlertCondition(
                        zone_id=data.get('location', 'UNKNOWN_LOCATION'),
                        alert_type=self._map_condition_to_alert_type(rule.condition, rule.operator, field_value),
                        risk_level=rule.risk_level,
                        value=field_value,
                        threshold=rule.value,
                        message=rule.message_template.format(value=field_value),
                        timestamp=data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        source='weather',
                        metadata={
                            'data_quality': {
                                'temp_valid': data.get('temp_valid', True),
                                'humidity_valid': data.get('humidity_valid', True),
                                'coordinates_valid': data.get('coordinates_valid', True)
                            },
                            'rule_applied': {
                                'condition': rule.condition,
                                'operator': rule.operator,
                                'threshold': rule.value
                            }
                        }
                    )
                    
                    # Check if this alert should be generated (rate limiting and duplicates)
                    if self._should_generate_alert(condition):
                        alert = self.alert_factory.create_weather_alert(condition)
                        if alert:
                            alerts.append(alert)
        
        return alerts
    
    def _map_condition_to_alert_type(self, condition: str, operator: str, value: Any) -> str:
        """Map condition to alert type for economic impact calculation"""
        if condition == 'temperature' or condition == 'temp_c':
            return 'HIGH_TEMPERATURE' if operator == '>' else 'LOW_TEMPERATURE'
        elif condition == 'humidity':
            return 'HIGH_HUMIDITY' if operator == '>' else 'LOW_HUMIDITY'
        elif condition == 'wind_kph':
            return 'HIGH_WIND'
        elif condition == 'soil_ph':
            return 'SOIL_PH_ISSUE'
        elif '_valid' in condition:
            return 'SENSOR_VALIDATION_ERROR'
        else:
            return 'WEATHER_ANOMALY'
    
    def _should_generate_alert(self, condition: AlertCondition) -> bool:
        """Check if alert should be generated (rate limiting and duplicates)"""
        current_time = datetime.now(ZoneInfo('Europe/Rome'))
        
        # Create unique identifier for this violation
        violation_id = f"{condition.zone_id}_{condition.alert_type}_{condition.value}_{condition.timestamp}"
        
        # Skip if already processed
        if violation_id in self.processed_records:
            return False
        
        # Rate limiting: check if we've alerted for this zone+type recently
        rate_limit_key = (condition.zone_id, condition.alert_type)
        if rate_limit_key in self.last_alert_times:
            last_alert_time = self.last_alert_times[rate_limit_key]
            time_since_last_alert = (current_time - last_alert_time).total_seconds()
            
            # Rate limit: minimum 30 seconds between alerts for same zone+type
            if time_since_last_alert < 30:  # 30 seconds
                logger.info(f"Rate limiting alert for {condition.zone_id} - {condition.alert_type} (last alert {time_since_last_alert:.1f}s ago)")
                return False
        
        # Skip if too old (avoid processing stale data)
        try:
            if isinstance(condition.timestamp, str):
                # Parse timestamp e rendilo timezone-aware
                if condition.timestamp.endswith('Z'):
                    violation_time = datetime.fromisoformat(condition.timestamp.replace('Z', '+00:00'))
                elif '+' in condition.timestamp:
                    violation_time = datetime.fromisoformat(condition.timestamp)
                else:
                    # Assume Europe/Rome se no timezone info
                    try:
                        violation_time = datetime.fromisoformat(condition.timestamp).replace(tzinfo=ZoneInfo('Europe/Rome'))
                    except Exception:
                        violation_time = datetime.fromisoformat(condition.timestamp).replace(tzinfo=timezone.utc)
            else:
                violation_time = condition.timestamp
                if violation_time.tzinfo is None:
                    violation_time = violation_time.replace(tzinfo=timezone.utc)
            
            # current_time is already timezone-aware (Europe/Rome)
            
            time_diff = (current_time - violation_time).total_seconds()
            if time_diff > 300:  # 5 minutes
                logger.info(f"Skipping old violation: {condition.alert_type} from {condition.timestamp}")
                return False
                
        except Exception as e:
            logger.warning(f"Could not parse violation timestamp {condition.timestamp}: {e}")
        
        # Add to processed records and update last alert time
        self.processed_records.add(violation_id)
        self.last_alert_times[rate_limit_key] = current_time
        
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        return {
            'consumer_type': 'kafka_alert_consumer',
            'statistics': self.stats,
            'configuration': {
                'threshold_rules_count': len(self.config.get_sensor_thresholds()) + len(self.config.get_weather_thresholds()),
                'processed_records_count': len(self.processed_records)
            },
            'timestamp': datetime.now().isoformat()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Health check for the consumer"""
        return {
            'status': 'healthy' if self.is_running else 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'components': {
                'iot_consumer': self.iot_consumer is not None,
                'weather_consumer': self.weather_consumer is not None,
                'alert_factory': self.alert_factory.is_healthy(),
                'alert_repository': self.alert_repository.is_healthy()
            }
        } 