#!/usr/bin/env python3
"""
ML Service Main Entry Point
Dual-Mode Architecture: Real-time Analysis + Batch Predictions
"""

import os
import logging
import threading
import time
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Import ML service components
from ml_service import MLService
from api_service import APIService
from scheduler_service import SchedulerService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class MLServiceOrchestrator:
    """
    Main orchestrator for the ML service implementing dual-mode architecture
    """
    
    def __init__(self):
        self.ml_service = MLService()
        self.scheduler_service = SchedulerService(self.ml_service)
        self.api_service = APIService(self.ml_service, self.scheduler_service)
        
        # FastAPI app
        self.app = FastAPI(
            title="Crop Disease Service",
            description="Dual-Mode Service for Real-time Analysis and Batch Predictions",
            version="1.0.0"
        )
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Setup routes
        self._setup_routes()
        
        # Service state
        self.is_running = True
    
    def _setup_routes(self):
        """Setup FastAPI routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "service": "crop-disease-service",
                "mode": "dual-mode"
            }
        
        @self.app.get("/status")
        async def get_status():
            """Get service status"""
            return self.ml_service.get_status()
        
        @self.app.post("/predict/batch")
        async def trigger_batch_prediction(background_tasks: BackgroundTasks):
            """Trigger batch prediction manually"""
            background_tasks.add_task(self.ml_service.run_batch_prediction)
            return {"message": "Batch prediction triggered", "timestamp": datetime.now().isoformat()}
        
        @self.app.get("/predictions/recent")
        async def get_recent_predictions():
            """Get recent predictions for dashboard"""
            return self.ml_service.get_recent_predictions()
        
        @self.app.get("/alerts/active")
        async def get_active_alerts():
            """Get active alerts"""
            return self.ml_service.get_active_alerts()
        
        @self.app.get("/models/status")
        async def get_models_status():
            """Get ML models status"""
            return self.ml_service.get_models_status()
        
        # Add API service routes
        self.api_service.add_routes(self.app)
    
    def start_background_services(self):
        """Start background services"""
        logger.info("Starting background services...")
        
        # Start scheduler for batch processing
        scheduler_thread = threading.Thread(
            target=self.scheduler_service.start,
            daemon=True
        )
        scheduler_thread.start()
        logger.info("Scheduler service started")
        
        # Start real-time processing
        realtime_thread = threading.Thread(
            target=self._run_realtime_processing,
            daemon=True
        )
        realtime_thread.start()
        logger.info("Real-time processing started")
    
    def _run_realtime_processing(self):
        """Run real-time processing loop"""
        while self.is_running:
            try:
                # Process real-time data every 5 minutes
                self.ml_service.process_realtime_data()
                time.sleep(300)  # 5 minutes
            except Exception as e:
                logger.error(f"Error in real-time processing: {e}")
                time.sleep(60)
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Crop Disease service...")
        self.is_running = False
        self.scheduler_service.stop()
        self.ml_service.shutdown()

def main():
    """Main entry point"""
    logger.info("Starting Crop Disease Service (Dual-Mode Architecture)...")
    
    # Create orchestrator
    orchestrator = MLServiceOrchestrator()
    
    # Start background services
    orchestrator.start_background_services()
    
    # Get configuration
    host = os.getenv("CROP_DISEASE_SERVICE_HOST", "0.0.0.0")
    port = int(os.getenv("CROP_DISEASE_SERVICE_PORT", "8000"))
    
    logger.info(f"Starting FastAPI server on {host}:{port}")
    
    try:
        # Start FastAPI server
        uvicorn.run(
            orchestrator.app,
            host=host,
            port=port,
            log_level="info"
        )
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Error starting server: {e}")
    finally:
        orchestrator.shutdown()

if __name__ == "__main__":
    main() 