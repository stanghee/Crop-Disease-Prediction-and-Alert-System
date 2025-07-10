#!/usr/bin/env python3
"""
API Service - REST API for Crop Disease Service
Provides endpoints for real-time and batch functionality
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class PredictionRequest(BaseModel):
    field_id: str
    features: Dict[str, float]

class AlertUpdateRequest(BaseModel):
    alert_id: str
    new_status: str
    user_id: Optional[str] = None

class ScheduleUpdateRequest(BaseModel):
    batch_interval_hours: Optional[int] = None
    batch_start_hour: Optional[int] = None

class APIService:
    """
    REST API service for crop disease functionality
    Provides endpoints for real-time and batch operations
    """
    
    def __init__(self, ml_service, scheduler_service=None):
        self.ml_service = ml_service
        self.scheduler_service = scheduler_service
        self.router = APIRouter(prefix="/api/v1", tags=["Crop Disease Service"])
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup API routes"""
        

        
        # Batch endpoints
        @self.router.get("/batch/status")
        async def get_batch_status():
            """Get batch processing status"""
            try:
                if self.scheduler_service:
                    scheduler_status = self.scheduler_service.get_scheduler_status()
                else:
                    scheduler_status = {"status": "scheduler_not_available"}
                return {
                    "status": "success",
                    "data": scheduler_status,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.post("/batch/predict")
        async def trigger_batch_prediction(background_tasks: BackgroundTasks):
            """Trigger batch prediction manually"""
            try:
                background_tasks.add_task(self.ml_service.run_batch_prediction)
                return {
                    "status": "success",
                    "message": "Batch prediction triggered",
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/batch/schedule")
        async def get_batch_schedule():
            """Get batch processing schedule"""
            try:
                if self.scheduler_service:
                    upcoming_runs = self.scheduler_service.get_upcoming_runs()
                else:
                    upcoming_runs = {"status": "scheduler_not_available"}
                return {
                    "status": "success",
                    "data": upcoming_runs,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.put("/batch/schedule")
        async def update_batch_schedule(request: ScheduleUpdateRequest):
            """Update batch processing schedule"""
            try:
                if self.scheduler_service:
                    result = self.scheduler_service.update_schedule(
                        batch_interval_hours=request.batch_interval_hours,
                        batch_start_hour=request.batch_start_hour
                    )
                else:
                    result = {"status": "error", "message": "scheduler_not_available"}
                
                if result.get('status') == 'success':
                    return result
                else:
                    raise HTTPException(status_code=400, detail=result.get('message'))
                    
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        # Predictions endpoints
        @self.router.get("/predictions")
        async def get_predictions(limit: int = 10, field_id: Optional[str] = None):
            """Get recent predictions"""
            try:
                predictions = self.ml_service.get_recent_predictions()
                
                if field_id:
                    # Filter by field_id
                    filtered_predictions = [
                        p for p in predictions.get('predictions', [])
                        if p.get('field_id') == field_id
                    ]
                    predictions['predictions'] = filtered_predictions[:limit]
                else:
                    predictions['predictions'] = predictions.get('predictions', [])[:limit]
                
                return {
                    "status": "success",
                    "data": predictions,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.post("/predictions/predict")
        async def make_prediction(request: PredictionRequest):
            """Make prediction for specific field"""
            try:
                # Create a DataFrame with the provided features
                import pandas as pd
                features_df = pd.DataFrame([request.features])
                
                # Make prediction
                prediction = self.ml_service.disease_predictor.predict(features_df, request.field_id)
                
                return {
                    "status": "success",
                    "data": prediction,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        # Alerts endpoints
        @self.router.get("/alerts")
        async def get_alerts(active_only: bool = True, limit: int = 20):
            """Get alerts"""
            try:
                alerts = self.ml_service.get_active_alerts()
                
                if active_only:
                    alerts['alerts'] = [
                        alert for alert in alerts.get('alerts', [])
                        if alert.get('status') == 'ACTIVE'
                    ]
                
                alerts['alerts'] = alerts.get('alerts', [])[:limit]
                
                return {
                    "status": "success",
                    "data": alerts,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        

        
        @self.router.get("/alerts/weather")
        async def get_weather_alerts():
            """Get weather-based alerts using refactored centralized system"""
            try:
                # Use the new centralized alert manager if available
                if self.ml_service.alert_manager:
                    # Get weather violations from refactored system
                    weather_violations = self.ml_service.alert_manager.weather_handler.get_threshold_violations()
                    
                    return {
                        "status": "success",
                        "data": {
                            "weather_alerts": [violation.__dict__ for violation in weather_violations],
                            "total_alerts": len(weather_violations),
                            "data_source": "refactored_centralized_system"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Refactored alert system not available",
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
            """Get sensor alerts using refactored centralized system"""
            try:
                # Use the new centralized alert manager if available
                if self.ml_service.alert_manager:
                    # Get sensor violations from refactored system
                    sensor_violations = self.ml_service.alert_manager.sensor_handler.get_threshold_violations()
                    
                    return {
                        "status": "success",
                        "data": {
                            "sensor_alerts": [violation.__dict__ for violation in sensor_violations],
                            "total_alerts": len(sensor_violations),
                            "data_source": "refactored_centralized_system"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Refactored alert system not available",
                        "data": {
                            "sensor_alerts": [],
                            "total_alerts": 0,
                            "data_source": "system_unavailable"
                        },
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.put("/alerts/{alert_id}")
        async def update_alert(alert_id: str, request: AlertUpdateRequest):
            """Update alert status using refactored centralized system"""
            try:
                # Use the new centralized alert repository if available
                if self.ml_service.alert_manager:
                    success = self.ml_service.alert_manager.alert_repository.update_alert_status(
                        int(alert_id), request.new_status, request.user_id
                    )
                    
                    if success:
                        result = {
                            'alert_id': alert_id,
                            'new_status': request.new_status,
                            'updated_by': request.user_id,
                            'updated_at': datetime.now().isoformat(),
                            'status': 'success',
                            'system': 'refactored_centralized'
                        }
                    else:
                        result = {
                            'alert_id': alert_id,
                            'status': 'error',
                            'message': 'Failed to update alert status',
                            'system': 'refactored_centralized'
                        }
                else:
                    # Fallback to old system
                    result = self.ml_service.alert_generator.update_alert_status(
                        alert_id, request.new_status, request.user_id
                    )
                    result['system'] = 'legacy'
                
                return {
                    "status": "success",
                    "data": result,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        # Refactored Alert System endpoints
        @self.router.get("/alerts/statistics")
        async def get_alert_statistics():
            """Get alert statistics from refactored system"""
            try:
                if self.ml_service.alert_manager:
                    stats = self.ml_service.alert_manager.alert_repository.get_alert_statistics()
                    return {
                        "status": "success",
                        "data": stats,
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Refactored alert system not available",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/alerts/configuration")
        async def get_alert_configuration():
            """Get alert configuration information"""
            try:
                if self.ml_service.alert_manager:
                    config_info = self.ml_service.alert_manager.alert_factory.get_configuration_info()
                    return {
                        "status": "success",
                        "data": config_info,
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Refactored alert system not available",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.post("/alerts/cleanup")
        async def cleanup_old_alerts(days_old: int = 30):
            """Clean up old alerts"""
            try:
                if self.ml_service.alert_manager:
                    deleted_count = self.ml_service.alert_manager.alert_repository.cleanup_old_alerts(days_old)
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
                        "message": "Refactored alert system not available",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        # Models endpoints
        @self.router.get("/models")
        async def get_models():
            """Get ML models status"""
            try:
                models_status = self.ml_service.get_models_status()
                return {
                    "status": "success",
                    "data": models_status,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.post("/models/retrain")
        async def retrain_models():
            """Retrain ML models with synthetic data"""
            try:
                # Force retraining of disease predictor
                self.ml_service.disease_predictor._train_initial_model()
                return {
                    "status": "success",
                    "message": "Models retrained successfully",
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        

        
        # System endpoints
        @self.router.get("/system/status")
        async def get_system_status():
            """Get overall system status"""
            try:
                system_status = self.ml_service.get_status()
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
                # Check if ML service is initialized
                if self.ml_service.is_initialized:
                    return {
                        "status": "healthy",
                        "service": "ml-service",
                        "mode": "dual-mode",
                        "initialized": True,
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "status": "initializing",
                        "service": "ml-service",
                        "mode": "dual-mode",
                        "initialized": False,
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                return {
                    "status": "unhealthy",
                    "service": "ml-service",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
    

    
    def add_routes(self, app):
        """Add routes to FastAPI app"""
        app.include_router(self.router) 