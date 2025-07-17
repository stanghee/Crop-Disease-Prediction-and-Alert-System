#!/usr/bin/env python3
"""
Alert Factory
Unified alert generation for sensors and weather
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass

from config.alert_thresholds import AlertConfiguration, RiskLevel

logger = logging.getLogger(__name__)

@dataclass
class AlertCondition:
    """Standardized alert condition"""
    zone_id: Optional[str]  # Contains field_id for sensors, location for weather
    alert_type: str
    risk_level: RiskLevel
    value: Any
    threshold: Any
    message: str
    timestamp: str
    source: str  # 'sensor' or 'weather'
    location: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class AlertFactory:
    """
    Factory for creating standardized alerts
    Handles both sensor and weather alerts consistently
    """
    
    def __init__(self):
        self.config = AlertConfiguration()
        self.is_healthy_flag = True
    
    def create_sensor_alert(self, condition: AlertCondition) -> Dict[str, Any]:
        """Create a sensor alert with essential details """
        try:
            # Create standardized alert 
            alert = {
                'zone_id': condition.zone_id,
                'alert_timestamp': condition.timestamp,
                'alert_type': 'SENSOR_ANOMALY',
                'severity': condition.risk_level.value,
                'prediction_id': None,
                'status': 'ACTIVE',
                'message': condition.message
            }
            logger.debug(f"Created sensor alert: {condition.alert_type} for field {condition.zone_id}")
            return alert
        except Exception as e:
            logger.error(f"Error creating sensor alert: {e}")
            return self._create_fallback_alert(condition)

    def create_weather_alert(self, condition: AlertCondition) -> Dict[str, Any]:
        """Create a weather alert with essential details (no economic info or recommendations)"""
        try:
            # Create standardized alert 
            alert = {
                'zone_id': condition.zone_id,  # Use location as zone_id for weather alerts
                'alert_timestamp': condition.timestamp,
                'alert_type': 'WEATHER_ALERT',
                'severity': condition.risk_level.value,
                'prediction_id': None,
                'status': 'ACTIVE',
                'message': condition.message
            }
            logger.debug(f"Created weather alert: {condition.alert_type} for location {condition.location}")
            return alert
        except Exception as e:
            logger.error(f"Error creating weather alert: {e}")
            return self._create_fallback_alert(condition)
    
    def _create_fallback_alert(self, condition: AlertCondition) -> Dict[str, Any]:
        """Create a fallback alert when normal creation fails"""
        alert_type = 'SENSOR_ANOMALY' if condition.source == 'sensor' else 'WEATHER_ALERT'
        
        return {
            'zone_id': condition.zone_id,
            'alert_timestamp': condition.timestamp,
            'alert_type': alert_type,
            'severity': 'MEDIUM',
            'prediction_id': None,
            'status': 'ACTIVE',
            'message': condition.message or f'{condition.source.title()} condition detected'
        }
    
    def is_healthy(self) -> bool:
        """Check if the alert factory is healthy"""
        return self.is_healthy_flag
    
    def get_configuration_info(self) -> Dict[str, Any]:
        """Get information about current configuration"""
        return {
            'factory_version': '1.0.0',
            'configuration_loaded': True,
            'sensor_thresholds_count': len(self.config.get_sensor_thresholds()),
            'weather_thresholds_count': len(self.config.get_weather_thresholds()),
            'last_update': datetime.now().isoformat()
        } 