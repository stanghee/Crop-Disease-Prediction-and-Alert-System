#!/usr/bin/env python3
"""
Alert Thresholds Configuration
Centralized configuration for all alert thresholds
"""

from dataclasses import dataclass
from typing import Dict, Any, List
from enum import Enum

class RiskLevel(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

@dataclass
class ThresholdRule:
    """Single threshold rule definition"""
    condition: str
    operator: str  # '>', '<', '>=', '<=', '==', '!='
    value: float
    risk_level: RiskLevel
    message_template: str

class SensorThresholds:
    """Sensor alert thresholds configuration"""
    
    TEMPERATURE_THRESHOLDS = [
        ThresholdRule("temperature", ">", 35.0, RiskLevel.HIGH, "Temperature too high: {value}°C"),
        ThresholdRule("temperature", ">", 30.0, RiskLevel.MEDIUM, "Temperature elevated: {value}°C"),
        ThresholdRule("temperature", "<", 5.0, RiskLevel.HIGH, "Temperature too low: {value}°C"),
        ThresholdRule("temperature", "<", 10.0, RiskLevel.MEDIUM, "Temperature low: {value}°C"),
    ]
    
    HUMIDITY_THRESHOLDS = [
        ThresholdRule("humidity", ">", 90.0, RiskLevel.CRITICAL, "Humidity extremely high: {value}%"),
        ThresholdRule("humidity", ">", 85.0, RiskLevel.HIGH, "Humidity too high: {value}%"),
        ThresholdRule("humidity", ">", 75.0, RiskLevel.MEDIUM, "Humidity elevated: {value}%"),
        ThresholdRule("humidity", "<", 30.0, RiskLevel.HIGH, "Humidity too low: {value}%"),
        ThresholdRule("humidity", "<", 40.0, RiskLevel.MEDIUM, "Humidity low: {value}%"),
    ]
    
    SOIL_PH_THRESHOLDS = [
        ThresholdRule("soil_ph", ">", 8.0, RiskLevel.HIGH, "Soil pH too alkaline: {value}"),
        ThresholdRule("soil_ph", "<", 5.5, RiskLevel.HIGH, "Soil pH too acidic: {value}"),
    ]
    
    VALIDATION_THRESHOLDS = [
        ThresholdRule("temperature_valid", "==", False, RiskLevel.HIGH, "Invalid temperature reading: {value}°C"),
        ThresholdRule("humidity_valid", "==", False, RiskLevel.HIGH, "Invalid humidity reading: {value}%"),
        ThresholdRule("ph_valid", "==", False, RiskLevel.HIGH, "Invalid soil pH reading: {value}"),
    ]

class WeatherThresholds:
    """Weather alert thresholds configuration"""
    
    TEMPERATURE_THRESHOLDS = [
        ThresholdRule("temp_c", ">", 40.0, RiskLevel.CRITICAL, "Temperature extremely high: {value}°C"),
        ThresholdRule("temp_c", ">", 35.0, RiskLevel.HIGH, "Temperature too high: {value}°C"),
        ThresholdRule("temp_c", "<", 0.0, RiskLevel.CRITICAL, "Temperature extremely low: {value}°C"),
        ThresholdRule("temp_c", "<", 5.0, RiskLevel.HIGH, "Temperature too low: {value}°C"),
    ]
    
    HUMIDITY_THRESHOLDS = [
        ThresholdRule("humidity", ">", 95.0, RiskLevel.CRITICAL, "Humidity extremely high: {value}%"),
        ThresholdRule("humidity", ">", 90.0, RiskLevel.HIGH, "Humidity too high: {value}%"),
        ThresholdRule("humidity", "<", 20.0, RiskLevel.CRITICAL, "Humidity extremely low: {value}%"),
        ThresholdRule("humidity", "<", 30.0, RiskLevel.HIGH, "Humidity too low: {value}%"),
    ]
    
    WIND_SPEED_THRESHOLDS = [
        ThresholdRule("wind_kph", ">", 40.0, RiskLevel.CRITICAL, "Wind speed extremely high: {value} km/h"),
        ThresholdRule("wind_kph", ">", 25.0, RiskLevel.HIGH, "Wind speed too high: {value} km/h"),
    ]

class AlertConfiguration:
    """Master alert configuration class"""
    
    def __init__(self):
        self.sensor_thresholds = SensorThresholds()
        self.weather_thresholds = WeatherThresholds()
    
    def get_sensor_thresholds(self) -> List[ThresholdRule]:
        """Get all sensor threshold rules"""
        return (
            self.sensor_thresholds.TEMPERATURE_THRESHOLDS +
            self.sensor_thresholds.HUMIDITY_THRESHOLDS +
            self.sensor_thresholds.SOIL_PH_THRESHOLDS +
            self.sensor_thresholds.VALIDATION_THRESHOLDS
        )
    
    def get_weather_thresholds(self) -> List[ThresholdRule]:
        """Get all weather threshold rules"""
        return (
            self.weather_thresholds.TEMPERATURE_THRESHOLDS +
            self.weather_thresholds.HUMIDITY_THRESHOLDS +
            self.weather_thresholds.WIND_SPEED_THRESHOLDS
        )
    
    def evaluate_threshold(self, field_name: str, value: Any, threshold_rule: ThresholdRule) -> bool:
        """Evaluate if a value violates a threshold"""
        if threshold_rule.condition != field_name:
            return False
        
        operator = threshold_rule.operator
        threshold_value = threshold_rule.value
        
        try:
            if operator == ">":
                return value > threshold_value
            elif operator == "<":
                return value < threshold_value
            elif operator == ">=":
                return value >= threshold_value
            elif operator == "<=":
                return value <= threshold_value
            elif operator == "==":
                return value == threshold_value
            elif operator == "!=":
                return value != threshold_value
            else:
                return False
        except (TypeError, ValueError):
            return False 