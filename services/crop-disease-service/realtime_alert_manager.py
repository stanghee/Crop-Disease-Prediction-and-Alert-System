#!/usr/bin/env python3
"""
Real-time Alert Manager
Centralized management for real-time sensor and weather alerts
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from alert_handlers.sensor_alert_handler import SensorAlertHandler
from alert_handlers.weather_alert_handler import WeatherAlertHandler
from alert_factory import AlertFactory
from database.alert_repository import AlertRepository

logger = logging.getLogger(__name__)

@dataclass
class AlertProcessingResult:
    """Result of alert processing operation"""
    success: bool
    alerts_generated: int
    errors: List[str]
    processing_time_ms: int

class RealtimeAlertManager:
    """
    Centralized manager for real-time alert processing
    Handles both sensor and weather alerts with unified logic
    """
    
    def __init__(self, data_loader, spark_session):
        self.data_loader = data_loader
        self.spark_session = spark_session
        
        # Initialize handlers
        self.sensor_handler = SensorAlertHandler(data_loader)
        self.weather_handler = WeatherAlertHandler(data_loader)
        
        # Initialize factory and repository
        self.alert_factory = AlertFactory()
        self.alert_repository = AlertRepository(spark_session)
        
        # Statistics
        self.stats = {
            'sensor_alerts_processed': 0,
            'weather_alerts_processed': 0,
            'total_alerts_generated': 0,
            'last_processing_time': None
        }
    
    def process_sensor_alerts(self) -> AlertProcessingResult:
        """Process sensor alerts from Silver layer"""
        start_time = datetime.now()
        errors = []
        
        try:
            # 1. Get raw sensor conditions
            raw_conditions = self.sensor_handler.get_threshold_violations()
            
            if not raw_conditions:
                return AlertProcessingResult(
                    success=True,
                    alerts_generated=0,
                    errors=[],
                    processing_time_ms=0
                )
            
            # 2. Generate enhanced alerts
            enhanced_alerts = []
            for condition in raw_conditions:
                try:
                    alert = self.alert_factory.create_sensor_alert(condition)
                    if alert:
                        enhanced_alerts.append(alert)
                except Exception as e:
                    errors.append(f"Error creating sensor alert: {e}")
            
            # 3. Save to database
            if enhanced_alerts:
                self.alert_repository.save_alerts(enhanced_alerts)
                self.stats['sensor_alerts_processed'] += len(enhanced_alerts)
                self.stats['total_alerts_generated'] += len(enhanced_alerts)
            
            # 4. Update stats
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            self.stats['last_processing_time'] = datetime.now().isoformat()
            
            logger.info(f"Processed {len(enhanced_alerts)} sensor alerts in {processing_time}ms")
            
            return AlertProcessingResult(
                success=True,
                alerts_generated=len(enhanced_alerts),
                errors=errors,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"Error processing sensor alerts: {e}")
            return AlertProcessingResult(
                success=False,
                alerts_generated=0,
                errors=[str(e)],
                processing_time_ms=processing_time
            )
    
    def process_weather_alerts(self) -> AlertProcessingResult:
        """Process weather alerts from Silver layer"""
        start_time = datetime.now()
        errors = []
        
        try:
            # 1. Get raw weather conditions
            raw_conditions = self.weather_handler.get_threshold_violations()
            
            if not raw_conditions:
                return AlertProcessingResult(
                    success=True,
                    alerts_generated=0,
                    errors=[],
                    processing_time_ms=0
                )
            
            # 2. Generate enhanced alerts
            enhanced_alerts = []
            for condition in raw_conditions:
                try:
                    alert = self.alert_factory.create_weather_alert(condition)
                    if alert:
                        enhanced_alerts.append(alert)
                except Exception as e:
                    errors.append(f"Error creating weather alert: {e}")
            
            # 3. Save to database
            if enhanced_alerts:
                self.alert_repository.save_alerts(enhanced_alerts)
                self.stats['weather_alerts_processed'] += len(enhanced_alerts)
                self.stats['total_alerts_generated'] += len(enhanced_alerts)
            
            # 4. Update stats
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            self.stats['last_processing_time'] = datetime.now().isoformat()
            
            logger.info(f"Processed {len(enhanced_alerts)} weather alerts in {processing_time}ms")
            
            return AlertProcessingResult(
                success=True,
                alerts_generated=len(enhanced_alerts),
                errors=errors,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            logger.error(f"Error processing weather alerts: {e}")
            return AlertProcessingResult(
                success=False,
                alerts_generated=0,
                errors=[str(e)],
                processing_time_ms=processing_time
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return {
            'timestamp': datetime.now().isoformat(),
            'statistics': self.stats,
            'handlers': {
                'sensor': self.sensor_handler.get_stats(),
                'weather': self.weather_handler.get_stats()
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Health check for the alert manager"""
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'components': {
                'sensor_handler': self.sensor_handler.is_healthy(),
                'weather_handler': self.weather_handler.is_healthy(),
                'alert_factory': self.alert_factory.is_healthy(),
                'alert_repository': self.alert_repository.is_healthy()
            }
        } 