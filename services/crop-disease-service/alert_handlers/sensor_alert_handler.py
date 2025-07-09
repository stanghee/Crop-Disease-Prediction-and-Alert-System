#!/usr/bin/env python3
"""
Sensor Alert Handler
Specialized handler for sensor-based alerts using centralized configuration
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from config.alert_thresholds import AlertConfiguration, RiskLevel
from alert_factory import AlertCondition

logger = logging.getLogger(__name__)

class SensorAlertHandler:
    """
    Handles sensor alert detection using centralized threshold configuration
    Replaces hardcoded logic previously in data_loader.py
    """
    
    def __init__(self, data_loader):
        self.data_loader = data_loader
        self.config = AlertConfiguration()
        self.processed_records = set()
        
        # Track last processed timestamp to avoid reprocessing old data
        self.last_processed_timestamp = None
        
        # Rate limiting: track last alert time per field and alert type
        self.last_alert_times = {}  # key: (field_id, alert_type), value: timestamp
        
        # Statistics
        self.stats = {
            'total_checks': 0,
            'violations_found': 0,
            'alerts_generated': 0,
            'last_check_time': None
        }
    
    def get_threshold_violations(self) -> List[AlertCondition]:
        """
        Get sensor threshold violations from Silver layer using centralized configuration
        Replaces the hardcoded logic from data_loader.check_sensor_alerts_from_silver()
        """
        try:
            self.stats['total_checks'] += 1
            self.stats['last_check_time'] = datetime.now().isoformat()
            
            # Get raw sensor data from Silver layer using existing data_loader
            raw_sensor_data = self._get_silver_sensor_data()
            
            if not raw_sensor_data:
                logger.debug("No sensor data found in Silver layer")
                return []
            
            # Apply centralized threshold rules
            violations = []
            for data_point in raw_sensor_data:
                violations.extend(self._evaluate_sensor_thresholds(data_point))
            
            # Filter out already processed violations
            new_violations = self._filter_new_violations(violations)
            
            self.stats['violations_found'] += len(new_violations)
            logger.info(f"Found {len(new_violations)} new sensor threshold violations")
            
            return new_violations
            
        except Exception as e:
            logger.error(f"Error getting sensor threshold violations: {e}")
            return []
    
    def _get_silver_sensor_data(self) -> List[Dict[str, Any]]:
        """Get sensor data from Silver layer (MinIO Parquet files)"""
        try:
            # Build query to read from MinIO Parquet files
            if self.last_processed_timestamp:
                # Get data newer than last processed timestamp
                query = f"""
                    SELECT 
                        field_id,
                        temperature,
                        humidity,
                        soil_ph,
                        temperature_valid,
                        humidity_valid,
                        ph_valid,
                        timestamp_parsed as timestamp
                    FROM parquet.`s3a://silver/iot/`
                    WHERE timestamp_parsed > '{self.last_processed_timestamp}'
                    ORDER BY timestamp_parsed DESC
                """
            else:
                # First run: get data from last 2 minutes only
                query = """
                    SELECT 
                        field_id,
                        temperature,
                        humidity,
                        soil_ph,
                        temperature_valid,
                        humidity_valid,
                        ph_valid,
                        timestamp_parsed as timestamp
                    FROM parquet.`s3a://silver/iot/`
                    WHERE timestamp_parsed >= current_timestamp() - INTERVAL 2 MINUTES
                    ORDER BY timestamp_parsed DESC
                """
            
            spark_df = self.data_loader.spark.sql(query)
            
            # Convert to list of dictionaries
            sensor_data = []
            latest_timestamp = None
            
            for row in spark_df.collect():
                data_dict = row.asDict()
                sensor_data.append(data_dict)
                
                # Track the latest timestamp
                if latest_timestamp is None or data_dict['timestamp'] > latest_timestamp:
                    latest_timestamp = data_dict['timestamp']
            
            # Update last processed timestamp
            if latest_timestamp:
                self.last_processed_timestamp = latest_timestamp
            
            logger.debug(f"Retrieved {len(sensor_data)} new sensor records since last check")
            return sensor_data
            
        except Exception as e:
            logger.error(f"Error in sensor data retrieval: {e}")
            return []
    
    def _evaluate_sensor_thresholds(self, data_point: Dict[str, Any]) -> List[AlertCondition]:
        """Evaluate sensor data against centralized thresholds"""
        violations = []
        
        # Get all sensor threshold rules from centralized configuration
        threshold_rules = self.config.get_sensor_thresholds()
        
        for rule in threshold_rules:
            field_value = data_point.get(rule.condition)
            
            if field_value is not None:
                # Use centralized evaluation logic
                if self.config.evaluate_threshold(rule.condition, field_value, rule):
                    # Create standardized AlertCondition
                    condition = AlertCondition(
                        zone_id=data_point.get('field_id', 'UNKNOWN_FIELD'),  # Use field_id as zone_id for sensor alerts
                        alert_type=self._map_condition_to_alert_type(rule.condition, rule.operator, field_value),
                        risk_level=rule.risk_level,
                        value=field_value,
                        threshold=rule.value,
                        message=rule.message_template.format(value=field_value),
                        timestamp=data_point.get('timestamp', datetime.now().isoformat()),
                        source='sensor',
                        metadata={
                            'data_quality': {
                                'temperature_valid': data_point.get('temperature_valid', True),
                                'humidity_valid': data_point.get('humidity_valid', True),
                                'ph_valid': data_point.get('ph_valid', True)
                            },
                            'rule_applied': {
                                'condition': rule.condition,
                                'operator': rule.operator,
                                'threshold': rule.value
                            }
                        }
                    )
                    violations.append(condition)
        
        return violations
    
    def _map_condition_to_alert_type(self, condition: str, operator: str, value: Any) -> str:
        """Map sensor condition to alert type for economic impact calculation"""
        if condition == 'temperature':
            return 'HIGH_TEMPERATURE' if operator == '>' else 'LOW_TEMPERATURE'
        elif condition == 'humidity':
            return 'HIGH_HUMIDITY' if operator == '>' else 'LOW_HUMIDITY'
        elif condition == 'soil_ph':
            return 'SOIL_PH_ISSUE'
        elif '_valid' in condition:
            return 'SENSOR_VALIDATION_ERROR'
        else:
            return 'SENSOR_ANOMALY'
    
    def _filter_new_violations(self, violations: List[AlertCondition]) -> List[AlertCondition]:
        """Filter out already processed violations to avoid duplicates and apply rate limiting"""
        new_violations = []
        current_time = datetime.now()
        
        for violation in violations:
            # Create unique identifier for this violation
            violation_id = f"{violation.zone_id}_{violation.alert_type}_{violation.value}_{violation.timestamp}"
            
            # Skip if already processed
            if violation_id in self.processed_records:
                continue
            
            # Rate limiting: check if we've alerted for this zone+type recently
            rate_limit_key = (violation.zone_id, violation.alert_type)
            if rate_limit_key in self.last_alert_times:
                last_alert_time = self.last_alert_times[rate_limit_key]
                time_since_last_alert = (current_time - last_alert_time).total_seconds()
                
                # Rate limit: minimum 2 minutes between alerts for same zone+type
                if time_since_last_alert < 120:  # 2 minutes
                    logger.debug(f"Rate limiting alert for {violation.zone_id} - {violation.alert_type} (last alert {time_since_last_alert:.1f}s ago)")
                    continue
            
            # Skip if too old (avoid processing stale data)
            try:
                if isinstance(violation.timestamp, str):
                    violation_time = datetime.fromisoformat(violation.timestamp.replace('Z', '+00:00'))
                else:
                    violation_time = violation.timestamp
                
                time_diff = (current_time - violation_time).total_seconds()
                if time_diff > 300:  # 5 minutes
                    logger.debug(f"Skipping old violation: {violation.alert_type} from {violation.timestamp}")
                    continue
                    
            except Exception as e:
                logger.warning(f"Could not parse violation timestamp {violation.timestamp}: {e}")
            
            # Add to processed records and include in new violations
            self.processed_records.add(violation_id)
            self.last_alert_times[rate_limit_key] = current_time
            new_violations.append(violation)
        
        return new_violations
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics"""
        return {
            'handler_type': 'sensor',
            'statistics': self.stats,
            'configuration': {
                'threshold_rules_count': len(self.config.get_sensor_thresholds()),
                'processed_records_count': len(self.processed_records)
            },
            'timestamp': datetime.now().isoformat()
        }
    
    def is_healthy(self) -> bool:
        """Check if handler is healthy"""
        try:
            # Basic health checks
            config_loaded = len(self.config.get_sensor_thresholds()) > 0
            data_loader_available = self.data_loader is not None
            
            return config_loaded and data_loader_available
        except Exception:
            return False
    
    def clear_processed_records(self):
        """Clear processed records cache (for testing/maintenance)"""
        self.processed_records.clear()
        logger.info("Cleared sensor alert handler processed records cache") 