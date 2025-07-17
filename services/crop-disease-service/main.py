#!/usr/bin/env python3
"""
Crop Disease Service Main Entry Point
Threshold-Based Alert System Architecture
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

# Import service components
from api_service import APIService
import sys
sys.path.append('Threshold-alert')
from continuous_monitor_refactored import ContinuousMonitorRefactored

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class CropDiseaseServiceOrchestrator:
    """
    Main orchestrator for the Threshold-Based Alert Service
    """
    
    def __init__(self):
        # Create a simple threshold service for compatibility
        class SimpleThresholdService:
            def __init__(self, continuous_monitor=None):
                self.spark = None  # No Spark session needed for threshold alerts
                self.continuous_monitor = continuous_monitor
            
            def get_status(self):
                if self.continuous_monitor:
                    return self.continuous_monitor.get_status()
                else:
                    return {"status": "threshold-based-only", "timestamp": datetime.now().isoformat()}
            
            def get_active_alerts(self):
                if (self.continuous_monitor and 
                    self.continuous_monitor.alert_consumer and 
                    self.continuous_monitor.alert_consumer.alert_repository):
                    alerts_list = self.continuous_monitor.alert_consumer.alert_repository.get_active_alerts()
                    return {
                        "alerts": alerts_list,
                        "total": len(alerts_list),
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {"alerts": [], "total": 0, "timestamp": datetime.now().isoformat()}
            
            def get_recent_predictions(self):
                return {"predictions": [], "total": 0, "timestamp": datetime.now().isoformat()}
            
            def shutdown(self):
                pass
        
        # Initialize threshold-based alert system
        self.threshold_service = SimpleThresholdService()
        self.continuous_monitor = ContinuousMonitorRefactored(self.threshold_service)
        
        # Update threshold service with continuous monitor reference
        self.threshold_service.continuous_monitor = self.continuous_monitor
        
        # Initialize API service
        self.api_service = APIService(self.threshold_service)
        
        # FastAPI app
        self.app = FastAPI(
            title="Threshold-Based Alert Service",
            description="Real-time Agricultural Monitoring with Threshold-Based Alerts",
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
        
        @self.app.get("/monitor/status")
        async def get_monitor_status():
            """Get continuous monitor status"""
            return self.continuous_monitor.get_status()
        
        @self.app.post("/monitor/check")
        async def trigger_manual_check():
            """Trigger manual alert check (for testing/debugging)"""
            return self.continuous_monitor.trigger_manual_check()
        
        @self.app.get("/monitor/health")
        async def get_monitor_health():
            """Get monitor health check"""
            return self.continuous_monitor.health_check()
        
        # Add API service routes
        self.api_service.add_routes(self.app)
    
    def start_background_services(self):
        """Start background services"""
        logger.info("Starting background services...")
        
        # Start continuous monitor for threshold-based alerts
        monitor_thread = threading.Thread(
            target=self.continuous_monitor.start,
            daemon=True
        )
        monitor_thread.start()
        logger.info("Continuous monitor service started")
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Crop Disease service...")
        self.is_running = False
        self.continuous_monitor.stop()

def main():
    """Main entry point"""
    logger.info("Starting Crop Disease Service (Threshold-Based Alert System)...")
    
    # Create orchestrator
    orchestrator = CropDiseaseServiceOrchestrator()
    
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