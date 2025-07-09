#!/usr/bin/env python3
"""
Weather Alert Handler
Specialized handler for weather-based alerts using centralized configuration
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from config.alert_thresholds import AlertConfiguration, RiskLevel
from alert_factory import AlertCondition

logger = logging.getLogger(__name__)

class WeatherAlertHandler:
    """
    Handles weather alert detection using centralized threshold configuration
    Replaces hardcoded logic previously in data_loader.py
    """
    
    def __init__(self, data_loader):
        self.data_loader = data_loader
        self.config = AlertConfiguration()
        self.processed_records = set()
        
        # Track last processed timestamp to avoid reprocessing old data
        self.last_processed_timestamp = None
        
        # Rate limiting: track last alert time per location and alert type
        self.last_alert_times = {}  # key: (location, alert_type), value: timestamp
        
        # Statistics
        self.stats = {
            'total_checks': 0,
            'violations_found': 0,
            'alerts_generated': 0,
            'last_check_time': None
        }
    
    def get_threshold_violations(self) -> List[AlertCondition]:
        """
        Get weather threshold violations from Silver layer using centralized configuration
        Replaces the hardcoded logic from data_loader.check_weather_alerts()
        """
        try:
            self.stats['total_checks'] += 1
            self.stats['last_check_time'] = datetime.now().isoformat()
            
            # Get raw weather data from Silver layer using existing data_loader
            raw_weather_data = self._get_silver_weather_data()
            
            if not raw_weather_data:
                logger.debug("No weather data found in Silver layer")
                return []
            
            # Apply centralized threshold rules
            violations = []
            for data_point in raw_weather_data:
                violations.extend(self._evaluate_weather_thresholds(data_point))
            
            # Filter out already processed violations
            new_violations = self._filter_new_violations(violations)
            
            self.stats['violations_found'] += len(new_violations)
            logger.info(f"Found {len(new_violations)} new weather threshold violations")
            
            return new_violations
            
        except Exception as e:
            logger.error(f"Error getting weather threshold violations: {e}")
            return []
    
    def _get_silver_weather_data(self) -> List[Dict[str, Any]]:
        """Get weather data from Silver layer (MinIO Parquet files)"""
        try:
            # Build query to read from MinIO Parquet files
            if self.last_processed_timestamp:
                # Get data newer than last processed timestamp
                query = f"""
                    SELECT 
                        location,
                        temp_c,
                        humidity,
                        wind_kph,
                        uv as uv_index,
                        condition as condition_text,
                        timestamp_parsed as timestamp
                    FROM parquet.`s3a://silver/weather/`
                    WHERE timestamp_parsed > '{self.last_processed_timestamp}'
                    ORDER BY timestamp_parsed DESC
                """
            else:
                # First run: get data from last 5 minutes only
                query = """
                    SELECT 
                        location,
                        temp_c,
                        humidity,
                        wind_kph,
                        uv as uv_index,
                        condition as condition_text,
                        timestamp_parsed as timestamp
                    FROM parquet.`s3a://silver/weather/`
                    WHERE timestamp_parsed >= current_timestamp() - INTERVAL 5 MINUTES
                    ORDER BY timestamp_parsed DESC
                """
            
            spark_df = self.data_loader.spark.sql(query)
            
            # Convert to list of dictionaries
            weather_data = []
            latest_timestamp = None
            
            for row in spark_df.collect():
                data_dict = row.asDict()
                weather_data.append(data_dict)
                
                # Track the latest timestamp
                if latest_timestamp is None or data_dict['timestamp'] > latest_timestamp:
                    latest_timestamp = data_dict['timestamp']
            
            # Update last processed timestamp
            if latest_timestamp:
                self.last_processed_timestamp = latest_timestamp
            
            logger.debug(f"Retrieved {len(weather_data)} new weather records since last check")
            return weather_data
            
        except Exception as e:
            logger.error(f"Error in weather data retrieval: {e}")
            return []
    
    def _evaluate_weather_thresholds(self, data_point: Dict[str, Any]) -> List[AlertCondition]:
        """Evaluate weather data against centralized thresholds"""
        violations = []
        
        # Get all weather threshold rules from centralized configuration
        threshold_rules = self.config.get_weather_thresholds()
        
        for rule in threshold_rules:
            field_value = data_point.get(rule.condition)
            
            if field_value is not None:
                # Use centralized evaluation logic
                if self.config.evaluate_threshold(rule.condition, field_value, rule):
                    # Create standardized AlertCondition
                    condition = AlertCondition(
                        zone_id=data_point.get('location', 'UNKNOWN_LOCATION'),  # Use location as zone_id for weather alerts
                        alert_type=self._map_condition_to_alert_type(rule.condition, rule.operator, field_value),
                        risk_level=rule.risk_level,
                        value=field_value,
                        threshold=rule.value,
                        message=rule.message_template.format(value=field_value),
                        timestamp=data_point.get('timestamp', datetime.now().isoformat()),
                        source='weather',
                        location=data_point.get('location', 'Unknown'),
                        metadata={
                            'weather_condition': data_point.get('condition_text', 'Unknown'),
                            'uv_index': data_point.get('uv_index'),
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
        """Map weather condition to alert type for economic impact calculation"""
        if condition == 'temp_c':
            return 'HIGH_TEMPERATURE' if operator == '>' else 'LOW_TEMPERATURE'
        elif condition == 'humidity':
            return 'HIGH_HUMIDITY' if operator == '>' else 'LOW_HUMIDITY'
        elif condition == 'wind_kph':
            return 'HIGH_WIND'
        elif condition == 'uv_index':
            return 'HIGH_UV' if operator == '>' else 'LOW_UV'
        else:
            return 'WEATHER_ANOMALY'
    
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
                
                # Rate limit: minimum 5 minutes between weather alerts for same zone+type
                if time_since_last_alert < 300:  # 5 minutes
                    logger.debug(f"Rate limiting weather alert for {violation.zone_id} - {violation.alert_type} (last alert {time_since_last_alert:.1f}s ago)")
                    continue
            
            # Skip if too old (avoid processing stale data)
            try:
                if isinstance(violation.timestamp, str):
                    violation_time = datetime.fromisoformat(violation.timestamp.replace('Z', '+00:00'))
                else:
                    violation_time = violation.timestamp
                
                time_diff = (current_time - violation_time).total_seconds()
                if time_diff > 600:  # 10 minutes (weather data updates less frequently)
                    logger.debug(f"Skipping old weather violation: {violation.alert_type} from {violation.timestamp}")
                    continue
                    
            except Exception as e:
                logger.warning(f"Could not parse weather violation timestamp {violation.timestamp}: {e}")
            
            # Add to processed records and include in new violations
            self.processed_records.add(violation_id)
            self.last_alert_times[rate_limit_key] = current_time
            new_violations.append(violation)
        
        return new_violations
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics"""
        return {
            'handler_type': 'weather',
            'statistics': self.stats,
            'configuration': {
                'threshold_rules_count': len(self.config.get_weather_thresholds()),
                'processed_records_count': len(self.processed_records)
            },
            'timestamp': datetime.now().isoformat()
        }
    
    def is_healthy(self) -> bool:
        """Check if handler is healthy"""
        try:
            # Basic health checks
            config_loaded = len(self.config.get_weather_thresholds()) > 0
            data_loader_available = self.data_loader is not None
            
            return config_loaded and data_loader_available
        except Exception:
            return False
    
    def clear_processed_records(self):
        """Clear processed records cache (for testing/maintenance)"""
        self.processed_records.clear()
        logger.info("Cleared weather alert handler processed records cache") 