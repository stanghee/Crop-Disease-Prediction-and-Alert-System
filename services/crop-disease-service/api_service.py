#!/usr/bin/env python3
"""
API Service - REST API for Threshold-Based Alert Service
Provides endpoints for real-time threshold monitoring and alert generation
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class AlertUpdateRequest(BaseModel):
    alert_id: str
    new_status: str
    user_id: Optional[str] = None

class APIService:
    """
    REST API service for threshold-based alert system
    """
    
    def __init__(self, threshold_service):
        self.threshold_service = threshold_service
        self.router = APIRouter(prefix="/api/v1", tags=["Threshold Alert Service"])
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup API routes"""
        ###################### ALERTS ENDPOINTS ######################
        
        # Alerts endpoints
        @self.router.get("/alerts")
        async def get_alerts(active_only: bool = True, limit: int = 20):
            """Get alerts"""
            try:
                alerts = self.threshold_service.get_active_alerts()
                all_alerts = alerts["alerts"][:limit]
                if active_only:
                    all_alerts = [alert for alert in all_alerts if alert.get('status') == 'ACTIVE']
                return {
                    "status": "success",
                    "data": {
                        "alerts": all_alerts,
                        "total": len(all_alerts)
                    },
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/alerts/weather")
        async def get_weather_alerts():
            """Get weather-based alerts using threshold system"""
            try:
                if (self.threshold_service.continuous_monitor and 
                    self.threshold_service.continuous_monitor.alert_consumer and
                    self.threshold_service.continuous_monitor.alert_consumer.alert_repository):
                    
                    # Get all active alerts and filter for weather alerts
                    all_alerts = self.threshold_service.continuous_monitor.alert_consumer.alert_repository.get_active_alerts(limit=100)
                    weather_alerts = [alert for alert in all_alerts if alert.get('alert_type') == 'WEATHER_ALERT']
                    
                    return {
                        "status": "success",
                        "data": {
                            "weather_alerts": weather_alerts,
                            "total_alerts": len(weather_alerts),
                            "data_source": "threshold_system"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Threshold system not available",
                        "data": {
                            "weather_alerts": [],
                            "total_alerts": 0,
                            "data_source": "system_unavailable"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/alerts/sensors")
        async def get_sensor_alerts():
            """Get sensor alerts using threshold system"""
            try:
                if (self.threshold_service.continuous_monitor and 
                    self.threshold_service.continuous_monitor.alert_consumer and
                    self.threshold_service.continuous_monitor.alert_consumer.alert_repository):
                    
                    # Get all active alerts and filter for sensor alerts
                    all_alerts = self.threshold_service.continuous_monitor.alert_consumer.alert_repository.get_active_alerts(limit=100)
                    sensor_alerts = [alert for alert in all_alerts if alert.get('alert_type') == 'SENSOR_ANOMALY']
                    
                    return {
                        "status": "success",
                        "data": {
                            "sensor_alerts": sensor_alerts,
                            "total_alerts": len(sensor_alerts),
                            "data_source": "threshold_system"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Threshold system not available",
                        "data": {
                            "sensor_alerts": [],
                            "total_alerts": 0,
                            "data_source": "system_unavailable"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
 
        # Refactored Alert System endpoints
        @self.router.get("/alerts/statistics")
        async def get_alert_statistics():
            """Get alert statistics from threshold system"""
            try:
                if (self.threshold_service.continuous_monitor and 
                    self.threshold_service.continuous_monitor.alert_consumer and
                    self.threshold_service.continuous_monitor.alert_consumer.alert_repository):
                    stats = self.threshold_service.continuous_monitor.alert_consumer.alert_repository.get_alert_statistics()
                    return {
                        "status": "success",
                        "data": stats,
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Threshold system not available",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/alerts/configuration")
        async def get_alert_configuration():
            """Get alert configuration information"""
            try:
                if (self.threshold_service.continuous_monitor and 
                    self.threshold_service.continuous_monitor.alert_consumer and
                    self.threshold_service.continuous_monitor.alert_consumer.alert_factory):
                    config_info = self.threshold_service.continuous_monitor.alert_consumer.alert_factory.get_configuration_info()
                    return {
                        "status": "success",
                        "data": config_info,
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Threshold system not available",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.post("/alerts/cleanup")
        async def cleanup_old_alerts(days_old: int = 30):
            """Clean up old alerts"""
            try:
                if (self.threshold_service.continuous_monitor and 
                    self.threshold_service.continuous_monitor.alert_consumer and
                    self.threshold_service.continuous_monitor.alert_consumer.alert_repository):
                    deleted_count = self.threshold_service.continuous_monitor.alert_consumer.alert_repository.cleanup_old_alerts(days_old)
                    return {
                        "status": "success",
                        "data": {
                            "deleted_alerts": deleted_count,
                            "days_old": days_old
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Threshold system not available",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        ###################### SYSTEM ENDPOINTS ######################

        # System endpoints
        @self.router.get("/system/status")
        async def get_system_status():
            """Get overall system status"""
            try:
                system_status = self.threshold_service.get_status()
                return {
                    "status": "success",
                    "data": system_status,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/system/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Check threshold system health
                if (self.threshold_service.continuous_monitor):
                    monitor_health = self.threshold_service.continuous_monitor.health_check()
                    return {
                        "status": monitor_health['status'],
                        "service": "threshold-alert-service",
                        "mode": "threshold-based-only",
                        "components": monitor_health['components'],
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "unhealthy",
                        "service": "threshold-alert-service",
                        "error": "Threshold system not available",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                return {
                    "status": "unhealthy",
                    "service": "threshold-alert-service",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
    

    
    def add_routes(self, app):
        """Add routes to FastAPI app"""
        app.include_router(self.router) 