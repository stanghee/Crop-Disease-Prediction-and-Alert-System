#!/usr/bin/env python3
"""
Alert Generator - Strategic Alert System
Generates intelligent alerts based on predictions with economic impact analysis
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import random

logger = logging.getLogger(__name__)

class AlertGenerator:
    """
    Generates strategic alerts based on predictions
    Includes economic impact analysis and recommended actions
    """
    
    def __init__(self):
        self.version = "v1.0.0"
        self.is_initialized = False
        
        # Alert templates
        self.alert_templates = {
            'LOW': {
                'title': 'Low Disease Risk Detected',
                'urgency': 'Continue monitoring - no immediate action required',
                'economic_impact': 'Low potential for crop loss'
            },
            'MEDIUM': {
                'title': 'Medium Disease Risk Detected',
                'urgency': 'Monitor closely - prepare for potential action',
                'economic_impact': 'Moderate potential for crop loss'
            },
            'HIGH': {
                'title': 'High Disease Risk Detected',
                'urgency': 'Immediate action required within 48 hours',
                'economic_impact': 'High potential for significant crop loss'
            },
            'CRITICAL': {
                'title': 'Critical Disease Risk - Immediate Action Required',
                'urgency': 'Immediate action required within 24 hours',
                'economic_impact': 'Very high potential for severe crop loss'
            }
        }
        
        # Economic impact estimates (per hectare)
        self.economic_estimates = {
            'tomatoes': {
                'yield_per_hectare': 40000,  # kg
                'price_per_kg': 0.8,         # €
                'treatment_cost_per_hectare': 200,  # €
                'disease_loss_percentage': {
                    'LOW': 0.05,      # 5% loss
                    'MEDIUM': 0.15,   # 15% loss
                    'HIGH': 0.35,     # 35% loss
                    'CRITICAL': 0.60  # 60% loss
                }
            },
            'potatoes': {
                'yield_per_hectare': 35000,  # kg
                'price_per_kg': 0.6,         # €
                'treatment_cost_per_hectare': 180,  # €
                'disease_loss_percentage': {
                    'LOW': 0.05,
                    'MEDIUM': 0.20,
                    'HIGH': 0.40,
                    'CRITICAL': 0.70
                }
            },
            'wheat': {
                'yield_per_hectare': 8000,   # kg
                'price_per_kg': 0.25,        # €
                'treatment_cost_per_hectare': 150,  # €
                'disease_loss_percentage': {
                    'LOW': 0.05,
                    'MEDIUM': 0.15,
                    'HIGH': 0.30,
                    'CRITICAL': 0.50
                }
            }
        }
        
        # Treatment recommendations
        self.treatment_recommendations = {
            'Late_Blight': {
                'HIGH': [
                    'Apply copper-based fungicide',
                    'Improve field drainage',
                    'Remove infected plant material',
                    'Monitor weather conditions closely'
                ],
                'CRITICAL': [
                    'Apply systemic fungicide immediately',
                    'Consider crop rotation planning',
                    'Implement strict quarantine measures',
                    'Contact agricultural consultant'
                ]
            },
            'Early_Blight': {
                'HIGH': [
                    'Apply preventive fungicide',
                    'Maintain proper plant spacing',
                    'Remove lower leaves',
                    'Monitor humidity levels'
                ],
                'CRITICAL': [
                    'Apply curative fungicide treatment',
                    'Remove all infected plant parts',
                    'Improve air circulation',
                    'Consider early harvest if possible'
                ]
            }
        }
    
    def initialize(self):
        """Initialize the alert generator"""
        try:
            logger.info("Initializing Alert Generator...")
            self.is_initialized = True
            logger.info("Alert Generator initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Alert Generator: {e}")
            raise
    
    def generate_alert(self, prediction: Dict[str, Any], field_id: str) -> Dict[str, Any]:
        """Generate strategic alert based on prediction"""
        
        if not self.is_initialized:
            raise RuntimeError("Alert Generator not initialized")
        
        try:
            risk_level = prediction.get('risk_level', 'MEDIUM')
            disease_type = prediction.get('disease_type', 'Unknown')
            probability = prediction.get('disease_probability', 0.5)
            
            # Generate alerts for all risk levels
            # All risk levels will generate alerts for dashboard visibility
            
            # Generate alert content
            alert = {
                'field_id': field_id,
                'alert_timestamp': datetime.now().isoformat(),
                'alert_type': 'DISEASE_DETECTED',
                'severity': risk_level,
                'prediction_id': None,  # Will be set by caller
                'status': 'ACTIVE',
                
                # Alert details
                'message': self._generate_alert_message(risk_level, disease_type, probability),
                'details': self._generate_alert_details(prediction, field_id)
            }
            
            logger.info(f"Generated {risk_level} alert for field {field_id}")
            return alert
            
        except Exception as e:
            logger.error(f"Error generating alert for field {field_id}: {e}")
            return self._fallback_alert(field_id, prediction)
    
    def generate_threshold_alert(self, alert_type: str, data: Dict[str, Any], field_id: str = None) -> Dict[str, Any]:
        """Generate threshold-based alert (sensor or weather)"""
        
        if not self.is_initialized:
            raise RuntimeError("Alert Generator not initialized")
        
        try:
            if alert_type == "SENSOR_ANOMALY":
                return self._generate_sensor_alert(data, field_id)
            elif alert_type == "WEATHER_ALERT":
                return self._generate_weather_alert(data)
            else:
                raise ValueError(f"Unknown alert type: {alert_type}")
                
        except Exception as e:
            logger.error(f"Error generating threshold alert: {e}")
            return None
    
    def _generate_sensor_alert(self, sensor_data: Dict[str, Any], field_id: str) -> Dict[str, Any]:
        """Generate sensor-based threshold alert"""
        
        # Calculate risk based on sensor thresholds
        risk_level = self._calculate_sensor_risk(sensor_data)
        
        alert = {
            'field_id': field_id,
            'alert_timestamp': datetime.now().isoformat(),
            'alert_type': 'SENSOR_ANOMALY',
            'severity': risk_level,
            'prediction_id': None,
            'status': 'ACTIVE',
            'message': self._generate_sensor_message(risk_level, sensor_data),
            'details': {
                'current_conditions': sensor_data,
                'alert_source': 'sensor_threshold',
                'economic_impact': self._calculate_sensor_economic_impact(risk_level),
                'recommendations': self._get_sensor_recommendations(risk_level),
                'alert_priority': risk_level
            }
        }
        
        return alert
    
    def _generate_weather_alert(self, weather_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate weather-based threshold alert"""
        
        alert_type = weather_data.get('type', 'UNKNOWN')
        severity = weather_data.get('severity', 'WARNING')
        message = weather_data.get('message', 'Weather condition detected')
        location = weather_data.get('location', 'Unknown')
        
        alert = {
            'field_id': None,  # Weather alerts are regional
            'alert_timestamp': datetime.now().isoformat(),
            'alert_type': 'WEATHER_ALERT',
            'severity': severity,
            'prediction_id': None,
            'status': 'ACTIVE',
            'message': message,
            'details': {
                'weather_condition': alert_type,
                'location': location,
                'alert_source': 'weather_threshold',
                'recommendations': self._get_weather_recommendations(alert_type),
                'alert_priority': severity
            }
        }
        
        return alert
    
    def _calculate_sensor_risk(self, sensor_data: Dict[str, Any]) -> str:
        """Calculate risk level based on sensor thresholds"""
        
        risk_score = 0
        temp = sensor_data.get('temperature', 0)
        humidity = sensor_data.get('humidity', 0)
        soil_ph = sensor_data.get('soil_ph', 7.0)
        
        # Temperature risk (20-25°C is optimal for many diseases)
        if 20 <= temp <= 25:
            risk_score += 2
        elif 18 <= temp <= 27:
            risk_score += 1
        
        # Humidity risk (high humidity favors disease)
        if humidity > 85:
            risk_score += 3
        elif humidity > 75:
            risk_score += 2
        elif humidity > 65:
            risk_score += 1
        
        # Soil pH risk (neutral pH is generally good)
        if not (6.0 <= soil_ph <= 7.0):
            risk_score += 1
        
        # Determine risk level
        if risk_score >= 5:
            return "HIGH"
        elif risk_score >= 3:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _generate_sensor_message(self, risk_level: str, sensor_data: Dict[str, Any]) -> str:
        """Generate sensor alert message"""
        
        temp = sensor_data.get('temperature', 0)
        humidity = sensor_data.get('humidity', 0)
        
        if risk_level == "HIGH":
            return f"High risk conditions detected. Temperature: {temp}°C, Humidity: {humidity}%. Monitor closely for disease symptoms."
        elif risk_level == "MEDIUM":
            return f"Moderate risk conditions. Temperature: {temp}°C, Humidity: {humidity}%. Check field in next 2 hours."
        else:
            return f"Low risk conditions. Temperature: {temp}°C, Humidity: {humidity}%. No immediate action needed."
    
    def _calculate_sensor_economic_impact(self, risk_level: str) -> Dict[str, Any]:
        """Calculate economic impact for sensor alerts"""
        
        loss_mapping = {
            'LOW': 50.0,
            'MEDIUM': 200.0,
            'HIGH': 500.0,
            'CRITICAL': 1000.0
        }
        
        potential_loss = loss_mapping.get(risk_level, 100.0)
        
        return {
            'crop_type': 'unknown',
            'field_size_hectares': 1.0,
            'potential_loss_euro': potential_loss,
            'treatment_cost_euro': 100.0,
            'roi_euro': potential_loss - 100.0,
            'recommendation': 'Monitor field conditions closely'
        }
    
    def _get_sensor_recommendations(self, risk_level: str) -> List[str]:
        """Get recommendations for sensor alerts"""
        
        if risk_level == "HIGH":
            return [
                "Immediate monitoring required",
                "Check for disease symptoms",
                "Prepare treatment plan",
                "Monitor weather forecast"
            ]
        elif risk_level == "MEDIUM":
            return [
                "Monitor conditions",
                "Check field in next 2 hours",
                "Prepare for potential action"
            ]
        else:
            return [
                "Continue normal operations",
                "Maintain regular monitoring schedule"
            ]
    
    def _get_weather_recommendations(self, weather_type: str) -> List[str]:
        """Get recommendations for weather alerts"""
        
        recommendations = {
            'HIGH_TEMPERATURE': [
                "Provide shade if possible",
                "Increase irrigation",
                "Monitor for heat stress"
            ],
            'LOW_TEMPERATURE': [
                "Protect crops from frost",
                "Consider covering sensitive plants",
                "Monitor for cold damage"
            ],
            'HIGH_HUMIDITY': [
                "Improve air circulation",
                "Monitor for disease development",
                "Check field drainage"
            ],
            'LOW_HUMIDITY': [
                "Increase irrigation",
                "Monitor for drought stress",
                "Consider misting systems"
            ],
            'HIGH_WIND': [
                "Secure any loose structures",
                "Protect from wind damage",
                "Monitor for physical damage"
            ]
        }
        
        return recommendations.get(weather_type, ["Monitor conditions closely"])
    
    def _generate_alert_message(self, risk_level: str, disease_type: str, probability: float) -> str:
        """Generate alert message"""
        
        template = self.alert_templates.get(risk_level, {})
        title = template.get('title', 'Disease Risk Alert')
        urgency = template.get('urgency', 'Action required')
        
        probability_percent = int(probability * 100)
        
        message = f"{title}: {disease_type} detected with {probability_percent}% probability. {urgency}."
        
        return message
    
    def _generate_alert_details(self, prediction: Dict[str, Any], field_id: str) -> Dict[str, Any]:
        """Generate detailed alert information"""
        
        risk_level = prediction.get('risk_level', 'MEDIUM')
        disease_type = prediction.get('disease_type', 'Unknown')
        probability = prediction.get('disease_probability', 0.5)
        expected_onset = prediction.get('expected_onset', 'Unknown')
        triggering_factors = prediction.get('triggering_factors', [])
        
        # Estimate economic impact
        economic_impact = self._calculate_economic_impact(field_id, risk_level, disease_type)
        
        # Get treatment recommendations
        treatments = self._get_treatment_recommendations(disease_type, risk_level)
        
        # Generate strategic recommendations
        strategic_recommendations = self._generate_strategic_recommendations(
            risk_level, probability, economic_impact
        )
        
        return {
            'prediction_summary': {
                'disease_type': disease_type,
                'probability': probability,
                'expected_onset': expected_onset,
                'triggering_factors': triggering_factors
            },
            'economic_impact': economic_impact,
            'treatments': treatments,
            'strategic_recommendations': strategic_recommendations,
            'alert_priority': self._calculate_alert_priority(risk_level, probability, economic_impact)
        }
    
    def _calculate_economic_impact(self, field_id: str, risk_level: str, disease_type: str) -> Dict[str, Any]:
        """Calculate economic impact of disease risk"""
        
        # Assume field size (in real implementation, this would come from field metadata)
        field_size_hectares = 2.0  # Default 2 hectares
        
        # Determine crop type from field ID (simplified)
        crop_type = self._determine_crop_type(field_id)
        
        # Get economic parameters
        crop_params = self.economic_estimates.get(crop_type, self.economic_estimates['tomatoes'])
        
        # Calculate potential loss
        loss_percentage = crop_params['disease_loss_percentage'].get(risk_level, 0.2)
        potential_loss = (
            crop_params['yield_per_hectare'] * 
            crop_params['price_per_kg'] * 
            field_size_hectares * 
            loss_percentage
        )
        
        # Calculate treatment cost
        treatment_cost = crop_params['treatment_cost_per_hectare'] * field_size_hectares
        
        # Calculate ROI
        roi = potential_loss - treatment_cost
        
        return {
            'crop_type': crop_type,
            'field_size_hectares': field_size_hectares,
            'potential_loss_euro': round(potential_loss, 2),
            'treatment_cost_euro': round(treatment_cost, 2),
            'roi_euro': round(roi, 2),
            'loss_percentage': round(loss_percentage * 100, 1),
            'recommendation': 'Apply treatment' if roi > 0 else 'Monitor closely'
        }
    
    def _determine_crop_type(self, field_id: str) -> str:
        """Determine crop type from field ID"""
        # Simplified logic - in real implementation, this would query field metadata
        if 'tomato' in field_id.lower():
            return 'tomatoes'
        elif 'potato' in field_id.lower():
            return 'potatoes'
        elif 'wheat' in field_id.lower():
            return 'wheat'
        else:
            # Default to tomatoes
            return 'tomatoes'
    
    def _get_treatment_recommendations(self, disease_type: str, risk_level: str) -> List[str]:
        """Get treatment recommendations for disease and risk level"""
        
        treatments = self.treatment_recommendations.get(disease_type, {})
        recommendations = treatments.get(risk_level, [])
        
        if not recommendations:
            # Default recommendations for all risk levels
            if risk_level == 'LOW':
                recommendations = [
                    'Continue normal monitoring schedule',
                    'Maintain good field hygiene',
                    'Monitor weather conditions',
                    'Document field observations'
                ]
            elif risk_level == 'MEDIUM':
                recommendations = [
                    'Increase monitoring frequency',
                    'Prepare treatment plan',
                    'Monitor weather forecast',
                    'Check field drainage'
                ]
            elif risk_level == 'HIGH':
                recommendations = [
                    'Apply appropriate fungicide',
                    'Monitor field conditions',
                    'Remove infected plant material',
                    'Improve field drainage'
                ]
            elif risk_level == 'CRITICAL':
                recommendations = [
                    'Apply curative treatment immediately',
                    'Contact agricultural consultant',
                    'Consider early harvest',
                    'Implement strict quarantine measures'
                ]
        
        return recommendations
    
    def _generate_strategic_recommendations(self, risk_level: str, probability: float, economic_impact: Dict[str, Any]) -> List[str]:
        """Generate strategic recommendations"""
        
        recommendations = []
        
        # Time-based recommendations
        if risk_level == 'CRITICAL':
            recommendations.append("Take immediate action within 24 hours")
        elif risk_level == 'HIGH':
            recommendations.append("Take action within 48 hours")
        elif risk_level == 'MEDIUM':
            recommendations.append("Monitor closely and prepare for action within 1 week")
        elif risk_level == 'LOW':
            recommendations.append("Continue normal operations - no immediate action needed")
        
        # Economic-based recommendations
        if economic_impact['roi_euro'] > 100:
            recommendations.append(f"Treatment recommended - potential savings: €{economic_impact['roi_euro']}")
        elif economic_impact['roi_euro'] > 0:
            recommendations.append(f"Treatment marginally beneficial - savings: €{economic_impact['roi_euro']}")
        else:
            recommendations.append("Monitor closely - treatment cost exceeds potential savings")
        
        # Probability-based recommendations
        if probability > 0.8:
            recommendations.append("Very high confidence in prediction - act decisively")
        elif probability > 0.6:
            recommendations.append("High confidence in prediction - recommended action")
        else:
            recommendations.append("Moderate confidence - monitor and prepare for action")
        
        # Weather-based recommendations
        recommendations.append("Monitor weather forecast for next 7 days")
        recommendations.append("Prepare backup treatment plan")
        
        return recommendations
    
    def _calculate_alert_priority(self, risk_level: str, probability: float, economic_impact: Dict[str, Any]) -> str:
        """Calculate alert priority"""
        
        priority_score = 0
        
        # Risk level score
        risk_scores = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3, 'CRITICAL': 4}
        priority_score += risk_scores.get(risk_level, 2)
        
        # Probability score
        priority_score += int(probability * 2)
        
        # Economic impact score
        if economic_impact['roi_euro'] > 500:
            priority_score += 3
        elif economic_impact['roi_euro'] > 100:
            priority_score += 2
        elif economic_impact['roi_euro'] > 0:
            priority_score += 1
        
        # Determine priority
        if priority_score >= 8:
            return 'CRITICAL'
        elif priority_score >= 6:
            return 'HIGH'
        elif priority_score >= 4:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _fallback_alert(self, field_id: str, prediction: Dict[str, Any]) -> Dict[str, Any]:
        """Generate fallback alert when normal generation fails"""
        
        return {
            'field_id': field_id,
            'alert_timestamp': datetime.now().isoformat(),
            'alert_type': 'DISEASE_DETECTED',
            'severity': 'MEDIUM',
            'prediction_id': None,
            'status': 'ACTIVE',
            'message': f'Disease risk detected in field {field_id}. Please monitor field conditions.',
            'details': {
                'prediction_summary': prediction,
                'economic_impact': {
                    'crop_type': 'unknown',
                    'field_size_hectares': 1.0,
                    'potential_loss_euro': 500.0,
                    'treatment_cost_euro': 200.0,
                    'roi_euro': 300.0,
                    'loss_percentage': 20.0,
                    'recommendation': 'Monitor field conditions'
                },
                'treatments': ['Monitor field conditions', 'Check for disease symptoms'],
                'strategic_recommendations': ['Monitor closely', 'Prepare for potential treatment'],
                'alert_priority': 'MEDIUM'
            }
        }
    
    def update_alert_status(self, alert_id: str, new_status: str, user_id: str = None) -> Dict[str, Any]:
        """Update alert status (acknowledged, resolved, etc.)"""
        
        # In a real implementation, this would update the database
        return {
            'alert_id': alert_id,
            'new_status': new_status,
            'updated_by': user_id,
            'updated_at': datetime.now().isoformat(),
            'status': 'success'
        }
    
 