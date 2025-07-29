#!/usr/bin/env python3
"""
Crop Disease Service Main Entry Point
Spark Streaming Alert System Architecture
"""

import os
import logging
import time
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Import service components
from spark_streaming_service import SparkStreamingAlertService
from api_service import APIService
from database.alert_repository import AlertRepository

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class CropDiseaseServiceOrchestrator:
    """
    Main orchestrator for the Spark Streaming Alert Service
    """
    
    def __init__(self):
        logger.info("Initializing Crop Disease Service with Spark Streaming...")
        
        # Initialize Spark Streaming Alert Service
        self.spark_service = SparkStreamingAlertService()
        
        # Initialize Alert Repository for API access
        self.alert_repository = AlertRepository(self.spark_service.spark)
        
        # Initialize API service with direct repository access
        self.api_service = APIService(self.alert_repository)
        
        # FastAPI app
        self.app = FastAPI(
            title="Spark Streaming Alert Service",
            description="Real-time Agricultural Monitoring with Spark Streaming",
            version="2.0.0"
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
            """Get Spark Streaming monitor status"""
            try:
                return self.spark_service.get_streaming_status()
            except Exception as e:
                logger.error(f"Error getting monitor status: {e}")
                return {
                    "error": str(e),
                    "is_running": False,
                    "timestamp": datetime.now().isoformat()
                }
        
        @self.app.get("/monitor/health")
        async def get_monitor_health():
            """Get monitor health check"""
            try:
                status = self.spark_service.get_streaming_status()
                health_status = "healthy" if status["is_running"] and status["active_queries"] > 0 else "unhealthy"
                
                return {
                    "status": health_status,
                    "service": "spark-streaming-alert-service",
                    "spark_cluster": "spark://spark-master:7077",
                    "active_streaming_queries": status["active_queries"],
                    "is_running": status["is_running"],
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
        
        @self.app.get("/health")
        async def health_check():
            """Basic health check endpoint"""
            return await get_monitor_health()
        
        # Add API service routes
        self.api_service.add_routes(self.app)
    
    def start_services(self):
        """Start Spark Streaming services"""
        logger.info("Starting Spark Streaming services...")
        
        try:
            # Start Spark Streaming in background
            self.spark_service.start_streaming()
            logger.info("Spark Streaming Alert Service started successfully")
            
        except Exception as e:
            logger.error(f"Error starting Spark Streaming services: {e}")
            raise
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Crop Disease service...")
        self.is_running = False
        
        # Stop Spark Streaming
        try:
            self.spark_service.stop_streaming()
        except Exception as e:
            logger.error(f"Error stopping Spark service: {e}")
        
        logger.info("Crop Disease service shutdown completed")

def main():
    """Main entry point"""
    logger.info("Starting Crop Disease Service (Spark Streaming Alert System)...")
    
    # Create orchestrator
    orchestrator = CropDiseaseServiceOrchestrator()
    
    # Start Spark Streaming services
    orchestrator.start_services()
    
    # Get configuration
    host = os.getenv("CROP_DISEASE_SERVICE_HOST", "0.0.0.0")
    port = int(os.getenv("CROP_DISEASE_SERVICE_PORT", "8000"))
    
    logger.info(f"Starting FastAPI server on {host}:{port}")
    logger.info("Spark UI available at http://localhost:4042")
    
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