import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

class AlertGenerator:
    """
    Generate alerts and recommendations for crop disease prevention
    """
    
    def __init__(self):
        # Define intervention recommendations for each anomaly type
        self.intervention_recommendations = {
            'fungal_risk': {
                'severity': 'HIGH',
                'title': 'Fungal Disease Risk Detected',
                'description': 'High humidity and optimal temperature conditions favor fungal growth',
                'interventions': [
                    'Apply preventive fungicide spray',
                    'Improve field drainage',
                    'Increase air circulation between plants',
                    'Reduce irrigation frequency temporarily',
                    'Monitor for early signs of fungal infections'
                ],
                'urgency': 'immediate',
                'color': '#FF4444'
            },
            'bacterial_risk': {
                'severity': 'HIGH',
                'title': 'Bacterial Infection Risk',
                'description': 'Warm and humid conditions promote bacterial diseases',
                'interventions': [
                    'Apply copper-based bactericide',
                    'Avoid overhead irrigation',
                    'Remove and destroy infected plant material',
                    'Improve field sanitation',
                    'Consider resistant crop varieties'
                ],
                'urgency': 'immediate',
                'color': '#FF6644'
            },
            'pest_risk': {
                'severity': 'MEDIUM',
                'title': 'Increased Pest Activity Risk',
                'description': 'Temperature and humidity conditions favor pest reproduction',
                'interventions': [
                    'Deploy pest monitoring traps',
                    'Consider biological control agents',
                    'Apply targeted insecticide if needed',
                    'Scout fields more frequently',
                    'Maintain beneficial insect habitats'
                ],
                'urgency': 'within_24h',
                'color': '#FF8844'
            },
            'nutrient_stress': {
                'severity': 'MEDIUM',
                'title': 'Soil Acidity Alert',
                'description': 'Low soil pH may limit nutrient availability',
                'interventions': [
                    'Test soil pH and nutrient levels',
                    'Apply lime to raise pH if needed',
                    'Consider acid-tolerant fertilizers',
                    'Monitor plant health closely',
                    'Adjust fertilization program'
                ],
                'urgency': 'within_week',
                'color': '#FFAA44'
            },
            'alkaline_stress': {
                'severity': 'MEDIUM',
                'title': 'Soil Alkalinity Alert',
                'description': 'High soil pH may reduce nutrient uptake',
                'interventions': [
                    'Test soil pH and nutrient levels',
                    'Apply sulfur or organic matter to lower pH',
                    'Use acidifying fertilizers',
                    'Monitor for nutrient deficiency symptoms',
                    'Consider foliar feeding'
                ],
                'urgency': 'within_week',
                'color': '#FFCC44'
            },
            'extreme_cold': {
                'severity': 'HIGH',
                'title': 'Frost/Cold Damage Risk',
                'description': 'Extremely low temperatures detected',
                'interventions': [
                    'Activate frost protection measures',
                    'Cover sensitive plants if possible',
                    'Run irrigation system for heat retention',
                    'Monitor for cold damage symptoms',
                    'Plan post-frost recovery treatments'
                ],
                'urgency': 'immediate',
                'color': '#4444FF'
            },
            'extreme_heat': {
                'severity': 'HIGH',
                'title': 'Heat Stress Alert',
                'description': 'Dangerously high temperatures detected',
                'interventions': [
                    'Increase irrigation frequency',
                    'Provide shade if possible',
                    'Apply anti-transpirant sprays',
                    'Avoid field operations during peak heat',
                    'Monitor for heat stress symptoms'
                ],
                'urgency': 'immediate',
                'color': '#FF0000'
            },
            'extreme_dry': {
                'severity': 'HIGH',
                'title': 'Severe Drought Conditions',
                'description': 'Critically low humidity levels detected',
                'interventions': [
                    'Increase irrigation immediately',
                    'Apply mulch to retain moisture',
                    'Reduce plant density if possible',
                    'Monitor soil moisture levels',
                    'Consider drought stress treatments'
                ],
                'urgency': 'immediate',
                'color': '#8B4513'
            },
            'extreme_wet': {
                'severity': 'HIGH',
                'title': 'Excess Moisture Alert',
                'description': 'Extremely high humidity may cause multiple issues',
                'interventions': [
                    'Improve field drainage',
                    'Reduce irrigation',
                    'Increase air circulation',
                    'Apply preventive fungicide',
                    'Monitor for root rot and fungal diseases'
                ],
                'urgency': 'immediate',
                'color': '#0066CC'
            },
            'statistical_anomaly': {
                'severity': 'MEDIUM',
                'title': 'Unusual Sensor Reading',
                'description': 'Sensor values deviate from normal patterns',
                'interventions': [
                    'Verify sensor calibration',
                    'Inspect field conditions manually',
                    'Check for equipment malfunction',
                    'Monitor trend over next readings',
                    'Consider environmental factors'
                ],
                'urgency': 'within_24h',
                'color': '#666666'
            }
        }
    
    def generate_alert(self, field_id: str, anomaly_type: str, anomaly_score: float, 
                      sensor_data: Dict, timestamp: str) -> str:
        """
        Generate a comprehensive alert with recommendations
        """
        try:
            # Get base recommendation
            recommendation = self.intervention_recommendations.get(
                anomaly_type, 
                self.intervention_recommendations['statistical_anomaly']
            )
            
            # Create alert data structure
            alert = {
                'alert_id': f"ALERT_{field_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'timestamp': timestamp,
                'field_id': field_id,
                'anomaly_type': anomaly_type,
                'anomaly_score': round(anomaly_score, 3),
                'severity': recommendation['severity'],
                'urgency': recommendation['urgency'],
                'title': recommendation['title'],
                'description': recommendation['description'],
                'sensor_readings': {
                    'temperature': sensor_data['temperature'],
                    'humidity': sensor_data['humidity'],
                    'soil_ph': sensor_data['soil_ph']
                },
                'interventions': recommendation['interventions'],
                'visual': {
                    'color': recommendation['color'],
                    'icon': self._get_icon_for_anomaly(anomaly_type)
                },
                'estimated_risk_level': self._calculate_risk_level(anomaly_score, anomaly_type),
                'recommended_timeline': self._get_timeline_recommendation(recommendation['urgency']),
                'economic_impact': self._estimate_economic_impact(anomaly_type, anomaly_score),
                'monitoring_suggestions': self._get_monitoring_suggestions(anomaly_type)
            }
            
            # Add specific contextual information
            alert['context'] = self._add_contextual_information(sensor_data, anomaly_type)
            
            logger.info(f"🚨 Generated alert for {field_id}: {alert['title']}")
            
            return json.dumps(alert, indent=2)
            
        except Exception as e:
            logger.error(f"❌ Error generating alert: {e}")
            return json.dumps({
                'error': 'Failed to generate alert',
                'field_id': field_id,
                'timestamp': timestamp
            })
    
    def _get_icon_for_anomaly(self, anomaly_type: str) -> str:
        """Get appropriate icon for anomaly type"""
        icon_map = {
            'fungal_risk': '🍄',
            'bacterial_risk': '🦠', 
            'pest_risk': '🐛',
            'nutrient_stress': '🌱',
            'alkaline_stress': '🌿',
            'extreme_cold': '❄️',
            'extreme_heat': '🔥',
            'extreme_dry': '🏜️',
            'extreme_wet': '💧',
            'statistical_anomaly': '⚠️'
        }
        return icon_map.get(anomaly_type, '⚠️')
    
    def _calculate_risk_level(self, anomaly_score: float, anomaly_type: str) -> str:
        """Calculate overall risk level based on score and type"""
        high_risk_types = ['fungal_risk', 'bacterial_risk', 'extreme_cold', 'extreme_heat']
        
        if anomaly_type in high_risk_types:
            if anomaly_score > 0.8:
                return 'CRITICAL'
            elif anomaly_score > 0.6:
                return 'HIGH'
            else:
                return 'MODERATE'
        else:
            if anomaly_score > 0.7:
                return 'HIGH'
            elif anomaly_score > 0.5:
                return 'MODERATE'
            else:
                return 'LOW'
    
    def _get_timeline_recommendation(self, urgency: str) -> str:
        """Get specific timeline recommendation"""
        timeline_map = {
            'immediate': 'Take action within 2-4 hours',
            'within_24h': 'Address within 24 hours',
            'within_week': 'Plan intervention within 1 week'
        }
        return timeline_map.get(urgency, 'Monitor and assess')
    
    def _estimate_economic_impact(self, anomaly_type: str, anomaly_score: float) -> Dict:
        """Estimate potential economic impact"""
        impact_factors = {
            'fungal_risk': {'yield_loss': 15, 'cost_per_hectare': 200},
            'bacterial_risk': {'yield_loss': 20, 'cost_per_hectare': 250},
            'pest_risk': {'yield_loss': 10, 'cost_per_hectare': 150},
            'extreme_cold': {'yield_loss': 25, 'cost_per_hectare': 300},
            'extreme_heat': {'yield_loss': 18, 'cost_per_hectare': 180},
            'extreme_dry': {'yield_loss': 30, 'cost_per_hectare': 400},
            'extreme_wet': {'yield_loss': 12, 'cost_per_hectare': 160}
        }
        
        factor = impact_factors.get(anomaly_type, {'yield_loss': 5, 'cost_per_hectare': 100})
        
        return {
            'potential_yield_loss_percent': round(factor['yield_loss'] * anomaly_score, 1),
            'estimated_intervention_cost_per_hectare': round(factor['cost_per_hectare'] * anomaly_score),
            'cost_of_inaction_multiplier': round(3 * anomaly_score, 1)
        }
    
    def _get_monitoring_suggestions(self, anomaly_type: str) -> List[str]:
        """Get specific monitoring suggestions"""
        monitoring_map = {
            'fungal_risk': [
                'Check leaf undersides for fungal spores',
                'Monitor humidity levels hourly',
                'Inspect for leaf spots or discoloration'
            ],
            'bacterial_risk': [
                'Look for water-soaked lesions',
                'Check for bacterial ooze',
                'Monitor plant wilting patterns'
            ],
            'pest_risk': [
                'Set up sticky traps',
                'Check for egg masses',
                'Monitor beneficial insect populations'
            ],
            'extreme_cold': [
                'Check for frost damage on leaves',
                'Monitor soil temperature',
                'Assess plant recovery after warming'
            ]
        }
        
        return monitoring_map.get(anomaly_type, [
            'Conduct regular field inspections',
            'Monitor sensor readings trend',
            'Document any visible changes'
        ])
    
    def _add_contextual_information(self, sensor_data: Dict, anomaly_type: str) -> Dict:
        """Add contextual information to help farmers understand the situation"""
        context = {
            'current_conditions': f"T: {sensor_data['temperature']}°C, H: {sensor_data['humidity']}%, pH: {sensor_data['soil_ph']}",
            'season_relevance': self._get_seasonal_context(sensor_data),
            'historical_comparison': 'Conditions significantly different from recent patterns'
        }
        
        # Add specific context based on anomaly type
        if 'risk' in anomaly_type:
            context['prevention_note'] = 'Early intervention is key to preventing crop losses'
        elif 'extreme' in anomaly_type:
            context['damage_assessment'] = 'Immediate damage assessment recommended'
        
        return context
    
    def _get_seasonal_context(self, sensor_data: Dict) -> str:
        """Provide seasonal context for the readings"""
        # This is a simplified version - in reality, you'd want to consider
        # actual calendar dates, crop growth stages, and regional patterns
        temp = sensor_data['temperature']
        humidity = sensor_data['humidity']
        
        if temp < 15:
            return 'Cool conditions - typical for late fall/winter/early spring'
        elif temp > 30:
            return 'Hot conditions - typical for summer periods'
        else:
            return 'Moderate conditions - typical for spring/fall growing seasons' 