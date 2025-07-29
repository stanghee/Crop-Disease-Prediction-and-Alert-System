#!/usr/bin/env python3
"""
API Service - REST API for Spark Streaming Alert Service
Provides endpoints for real-time alert monitoring and retrieval
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from database.alert_repository import AlertRepository

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class AlertUpdateRequest(BaseModel):
    alert_id: str
    new_status: str
    user_id: Optional[str] = None

class APIService:
    """
    REST API service for Spark Streaming alert system
    """
    
    def __init__(self, alert_repository: AlertRepository):
        self.alert_repository = alert_repository
        self.router = APIRouter(prefix="/api/v1", tags=["Spark Streaming Alert Service"])
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup API routes"""
        ###################### ALERTS ENDPOINTS ######################
        
        # Alerts endpoints
        @self.router.get("/alerts")
        async def get_alerts(active_only: bool = True, limit: int = 20):
            """Get alerts"""
            try:
                if active_only:
                    all_alerts = self.alert_repository.get_active_alerts(limit=limit)
                else:
                    all_alerts = self.alert_repository.get_active_alerts(limit=limit)  # Use same method for now
                
                return {
                    "status": "success",
                    "data": {
                        "alerts": all_alerts,
                        "total": len(all_alerts)
                    },
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting alerts: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/alerts/weather")
        async def get_weather_alerts():
            """Get weather-based alerts using Spark Streaming system"""
            try:
                all_alerts = self.alert_repository.get_active_alerts(limit=100)
                weather_alerts = [alert for alert in all_alerts if alert.get('alert_type') == 'WEATHER_ALERT']
                
                return {
                    "status": "success",
                    "data": {
                        "weather_alerts": weather_alerts,
                        "total_alerts": len(weather_alerts),
                        "data_source": "spark_streaming_system"
                    },
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting weather alerts: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/alerts/sensors")
        async def get_sensor_alerts():
            """Get sensor alerts using Spark Streaming system"""
            try:
                all_alerts = self.alert_repository.get_active_alerts(limit=100)
                sensor_alerts = [alert for alert in all_alerts if alert.get('alert_type') == 'SENSOR_ANOMALY']
                
                return {
                    "status": "success",
                    "data": {
                        "sensor_alerts": sensor_alerts,
                        "total_alerts": len(sensor_alerts),
                        "data_source": "spark_streaming_system"
                    },
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting sensor alerts: {e}")
                raise HTTPException(status_code=500, detail=str(e))
 
        # Spark Streaming Alert System endpoints
        @self.router.get("/alerts/statistics")
        async def get_alert_statistics():
            """Get alert statistics from Spark Streaming system"""
            try:
                stats = self.alert_repository.get_alert_statistics()
                return {
                    "status": "success",
                    "data": stats,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting alert statistics: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        
        @self.router.get("/alerts/historical")
        async def get_historical_alerts(days_back: int = 7, zone_id: str = None):
            """Get historical alerts for Data Insights"""
            try:
                alerts = self.alert_repository.get_historical_alerts(days_back=days_back, zone_id=zone_id)
                return {
                    "status": "success",
                    "data": {
                        "alerts": alerts,
                        "total": len(alerts),
                        "days_back": days_back,
                        "zone_id": zone_id
                    },
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting historical alerts: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post("/alerts/cleanup")
        async def cleanup_old_alerts(days_old: int = 30):
            """Clean up old alerts"""
            try:
                deleted_count = self.alert_repository.cleanup_old_alerts(days_old)
                return {
                    "status": "success",
                    "data": {
                        "deleted_alerts": deleted_count,
                        "days_old": days_old
                    },
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error cleaning up alerts: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        ###################### ML PREDICTIONS ENDPOINTS ######################

        @self.router.get("/predictions")
        async def get_ml_predictions(days_back: int = 7, field_id: str = None):
            """Get ML predictions for Data Insights"""
            try:
                predictions = self.alert_repository.get_ml_predictions(days_back=days_back, field_id=field_id)
                return {
                    "status": "success",
                    "data": {
                        "predictions": predictions,
                        "total": len(predictions),
                        "days_back": days_back,
                        "field_id": field_id
                    },
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting ML predictions: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get("/predictions/statistics")
        async def get_ml_statistics():
            """Get ML prediction statistics for Data Insights"""
            try:
                stats = self.alert_repository.get_ml_statistics()
                return {
                    "status": "success",
                    "data": stats,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting ML statistics: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        ###################### SYSTEM ENDPOINTS ######################

        # System endpoints
        @self.router.get("/system/status")
        async def get_system_status():
            """Get overall system status"""
            try:
                # Basic system status
                system_status = {
                    "service": "spark-streaming-alert-service",
                    "mode": "real-time",
                    "processing_engine": "apache_spark_streaming",
                    "cluster": "spark://spark-master:7077",
                    "status": "running",
                    "database_connected": self._check_database_connection(),
                    "last_alert_check": None  # This will be updated by streaming queries
                }
                
                # Add alert repository stats if available
                try:
                    repo_stats = self.alert_repository.get_statistics()
                    system_status.update(repo_stats)
                except:
                    pass
                
                return {
                    "status": "success",
                    "data": system_status,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting system status: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/system/health")
        async def health_check():
            """Health check endpoint"""
            try:
                db_connected = self._check_database_connection()
                
                health_status = "healthy" if db_connected else "unhealthy"
                
                return {
                    "status": health_status,
                    "service": "spark-streaming-alert-service",
                    "mode": "real-time-streaming",
                    "database_connected": db_connected,
                    "processing_engine": "apache_spark_streaming",
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Error in health check: {e}")
                return {
                    "status": "unhealthy",
                    "service": "spark-streaming-alert-service",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
    
    def _check_database_connection(self) -> bool:
        """Check if database connection is working"""
        try:
            # Simple test query
            stats = self.alert_repository.get_alert_statistics()
            return True
        except Exception as e:
            logger.warning(f"Database connection check failed: {e}")
            return False
    
    def add_routes(self, app):
        """Add routes to FastAPI app"""
        app.include_router(self.router) 