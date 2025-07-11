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

class EconomicImpactConfig:
    """Economic impact calculation configuration"""
    
    SENSOR_IMPACT_RATES = {
        'HIGH_TEMPERATURE': {
            RiskLevel.LOW: {'potential_loss': 200, 'treatment_cost': 50},
            RiskLevel.MEDIUM: {'potential_loss': 500, 'treatment_cost': 100},
            RiskLevel.HIGH: {'potential_loss': 1200, 'treatment_cost': 200},
            RiskLevel.CRITICAL: {'potential_loss': 2500, 'treatment_cost': 400}
        },
        'LOW_TEMPERATURE': {
            RiskLevel.LOW: {'potential_loss': 150, 'treatment_cost': 30},
            RiskLevel.MEDIUM: {'potential_loss': 400, 'treatment_cost': 80},
            RiskLevel.HIGH: {'potential_loss': 1000, 'treatment_cost': 150},
            RiskLevel.CRITICAL: {'potential_loss': 2000, 'treatment_cost': 300}
        },
        'HIGH_HUMIDITY': {
            RiskLevel.LOW: {'potential_loss': 300, 'treatment_cost': 60},
            RiskLevel.MEDIUM: {'potential_loss': 700, 'treatment_cost': 120},
            RiskLevel.HIGH: {'potential_loss': 1500, 'treatment_cost': 250},
            RiskLevel.CRITICAL: {'potential_loss': 3000, 'treatment_cost': 500}
        },
        'LOW_HUMIDITY': {
            RiskLevel.LOW: {'potential_loss': 100, 'treatment_cost': 20},
            RiskLevel.MEDIUM: {'potential_loss': 300, 'treatment_cost': 60},
            RiskLevel.HIGH: {'potential_loss': 800, 'treatment_cost': 150},
            RiskLevel.CRITICAL: {'potential_loss': 1800, 'treatment_cost': 300}
        }
    }
    
    WEATHER_IMPACT_RATES = {
        'HIGH_TEMPERATURE': {
            RiskLevel.LOW: {'potential_loss': 200, 'treatment_cost': 50},
            RiskLevel.MEDIUM: {'potential_loss': 500, 'treatment_cost': 100},
            RiskLevel.HIGH: {'potential_loss': 1200, 'treatment_cost': 200},
            RiskLevel.CRITICAL: {'potential_loss': 2500, 'treatment_cost': 400}
        },
        'HIGH_WIND': {
            RiskLevel.LOW: {'potential_loss': 100, 'treatment_cost': 20},
            RiskLevel.MEDIUM: {'potential_loss': 300, 'treatment_cost': 60},
            RiskLevel.HIGH: {'potential_loss': 800, 'treatment_cost': 150},
            RiskLevel.CRITICAL: {'potential_loss': 1800, 'treatment_cost': 300}
        }
    }

class RecommendationConfig:
    """Alert recommendations configuration"""
    
    SENSOR_RECOMMENDATIONS = {
        'HIGH_TEMPERATURE': {
            RiskLevel.MEDIUM: ["Provide shade if possible", "Increase irrigation"],
            RiskLevel.HIGH: ["URGENT ACTION REQUIRED", "Provide shade if possible", "Increase irrigation", "Monitor for heat stress"],
            RiskLevel.CRITICAL: ["IMMEDIATE ACTION REQUIRED", "Emergency cooling measures", "Contact agricultural consultant"]
        },
        'HIGH_HUMIDITY': {
            RiskLevel.MEDIUM: ["Improve air circulation", "Monitor for disease development"],
            RiskLevel.HIGH: ["URGENT ACTION REQUIRED", "Improve air circulation", "Consider fungicide application"],
            RiskLevel.CRITICAL: ["IMMEDIATE ACTION REQUIRED", "Emergency ventilation", "Apply fungicide immediately"]
        },
        'LOW_TEMPERATURE': {
            RiskLevel.MEDIUM: ["Monitor for cold damage", "Consider covering plants"],
            RiskLevel.HIGH: ["URGENT ACTION REQUIRED", "Protect crops from frost", "Cover sensitive plants"],
            RiskLevel.CRITICAL: ["IMMEDIATE ACTION REQUIRED", "Emergency frost protection", "Contact agricultural consultant"]
        }
    }
    
    WEATHER_RECOMMENDATIONS = {
        'HIGH_TEMPERATURE': {
            RiskLevel.MEDIUM: ["Provide shade if possible", "Increase irrigation"],
            RiskLevel.HIGH: ["URGENT ACTION REQUIRED", "Provide shade", "Increase irrigation", "Monitor crops"],
            RiskLevel.CRITICAL: ["IMMEDIATE ACTION REQUIRED", "Emergency cooling", "Contact consultant"]
        },
        'HIGH_WIND': {
            RiskLevel.MEDIUM: ["Secure loose structures", "Monitor for damage"],
            RiskLevel.HIGH: ["URGENT ACTION REQUIRED", "Secure structures", "Protect from wind damage"],
            RiskLevel.CRITICAL: ["IMMEDIATE ACTION REQUIRED", "Emergency protection", "Evacuate if necessary"]
        }
    }

class AlertConfiguration:
    """Master alert configuration class"""
    
    def __init__(self):
        self.sensor_thresholds = SensorThresholds()
        self.weather_thresholds = WeatherThresholds()
        self.economic_impact = EconomicImpactConfig()
        self.recommendations = RecommendationConfig()
    
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
    
    def get_economic_impact(self, alert_type: str, risk_level: RiskLevel, source: str = 'sensor') -> Dict[str, Any]:
        """Get economic impact for alert type and risk level"""
        impact_config = self.economic_impact.SENSOR_IMPACT_RATES if source == 'sensor' else self.economic_impact.WEATHER_IMPACT_RATES
        
        if alert_type in impact_config and risk_level in impact_config[alert_type]:
            impact = impact_config[alert_type][risk_level]
            roi = impact['potential_loss'] - impact['treatment_cost']
            
            return {
                'potential_loss': f"€{impact['potential_loss']:,.0f}",
                'treatment_cost': f"€{impact['treatment_cost']:,.0f}",
                'roi': f"€{roi:,.0f} savings" if roi > 0 else f"€{abs(roi):,.0f} additional cost"
            }
        
        # Default fallback
        return {
            'potential_loss': '€500',
            'treatment_cost': '€100',
            'roi': '€400 savings'
        }
    
    def get_recommendations(self, alert_type: str, risk_level: RiskLevel, source: str = 'sensor') -> List[str]:
        """Get recommendations for alert type and risk level"""
        rec_config = self.recommendations.SENSOR_RECOMMENDATIONS if source == 'sensor' else self.recommendations.WEATHER_RECOMMENDATIONS
        
        if alert_type in rec_config and risk_level in rec_config[alert_type]:
            return rec_config[alert_type][risk_level]
        
        # Default recommendations
        return [
            "Monitor conditions closely",
            "Take appropriate action based on risk level",
            "Consult agricultural specialist if needed"
        ] 