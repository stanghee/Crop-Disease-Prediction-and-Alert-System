#!/usr/bin/env python3
"""
Continuous Monitor Service (Refactored)
Clean, centralized monitoring using the new alert management system
"""

import os
import logging
import time
import threading
from datetime import datetime
from typing import Dict, Any, List, Set
import hashlib

from realtime_alert_manager import RealtimeAlertManager

logger = logging.getLogger(__name__)

class ContinuousMonitorRefactored:
    """
    Refactored continuous monitoring service
    Uses centralized alert management for clean, maintainable code
    """
    
    def __init__(self, ml_service):
        self.ml_service = ml_service
        self.is_running = False
        self.monitor_thread = None
        
        # Initialize the centralized alert manager
        self.alert_manager = RealtimeAlertManager(
            ml_service.data_loader,
            ml_service.spark
        )
        
        # Track processed records to avoid duplicates
        self.processed_records: Set[str] = set()
        
        # Statistics
        self.stats = {
            'total_checks': 0,
            'sensor_alerts_generated': 0,
            'weather_alerts_generated': 0,
            'last_check_time': None,
            'last_processing_time_ms': 0
        }
    
    def start(self):
        """Start the continuous monitoring service"""
        if self.is_running:
            logger.warning("Continuous monitor is already running")
            return
        
        logger.info("Starting refactored continuous monitor service...")
        self.is_running = True
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self.monitor_thread.start()
        
        logger.info("Refactored continuous monitor service started successfully")
    
    def stop(self):
        """Stop the continuous monitoring service"""
        logger.info("Stopping refactored continuous monitor service...")
        self.is_running = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        logger.info("Refactored continuous monitor service stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop - runs every minute"""
        while self.is_running:
            try:
                start_time = time.time()
                
                # Process sensor alerts using centralized manager
                sensor_result = self.alert_manager.process_sensor_alerts()
                
                # Process weather alerts using centralized manager
                weather_result = self.alert_manager.process_weather_alerts()
                
                # Update local statistics
                self._update_stats(sensor_result, weather_result)
                
                # Calculate sleep time to maintain 1-minute intervals
                elapsed = time.time() - start_time
                sleep_time = max(60 - elapsed, 1)  # Minimum 1 second sleep
                
                logger.debug(f"Monitor check completed in {elapsed:.2f}s, sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def _update_stats(self, sensor_result, weather_result):
        """Update monitoring statistics"""
        self.stats['total_checks'] += 1
        self.stats['sensor_alerts_generated'] += sensor_result.alerts_generated
        self.stats['weather_alerts_generated'] += weather_result.alerts_generated
        self.stats['last_check_time'] = datetime.now().isoformat()
        self.stats['last_processing_time_ms'] = (
            sensor_result.processing_time_ms + weather_result.processing_time_ms
        )
        
        # Log results
        if sensor_result.alerts_generated > 0:
            logger.info(f"Generated {sensor_result.alerts_generated} sensor alerts")
        
        if weather_result.alerts_generated > 0:
            logger.info(f"Generated {weather_result.alerts_generated} weather alerts")
        
        # Log any errors
        if sensor_result.errors:
            logger.warning(f"Sensor processing errors: {sensor_result.errors}")
        
        if weather_result.errors:
            logger.warning(f"Weather processing errors: {weather_result.errors}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get detailed status of the monitoring service"""
        return {
            'service_status': {
                'running': self.is_running,
                'thread_alive': self.monitor_thread.is_alive() if self.monitor_thread else False,
                'last_check': self.stats['last_check_time'],
                'uptime_checks': self.stats['total_checks']
            },
            'alert_statistics': {
                'sensor_alerts_generated': self.stats['sensor_alerts_generated'],
                'weather_alerts_generated': self.stats['weather_alerts_generated'],
                'total_alerts': self.stats['sensor_alerts_generated'] + self.stats['weather_alerts_generated'],
                'last_processing_time_ms': self.stats['last_processing_time_ms']
            },
            'alert_manager_stats': self.alert_manager.get_stats(),
            'health_check': self.alert_manager.health_check(),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_simple_status(self) -> Dict[str, Any]:
        """Get simple status for API endpoints"""
        return {
            'status': 'running' if self.is_running else 'stopped',
            'total_checks': self.stats['total_checks'],
            'alerts_generated': {
                'sensor': self.stats['sensor_alerts_generated'],
                'weather': self.stats['weather_alerts_generated']
            },
            'last_check': self.stats['last_check_time'],
            'timestamp': datetime.now().isoformat()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        is_healthy = (
            self.is_running and
            self.monitor_thread and
            self.monitor_thread.is_alive() and
            self.alert_manager.health_check()['status'] == 'healthy'
        )
        
        return {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'components': {
                'monitor_service': self.is_running,
                'monitor_thread': self.monitor_thread.is_alive() if self.monitor_thread else False,
                'alert_manager': self.alert_manager.health_check()['status'] == 'healthy'
            },
            'last_successful_check': self.stats['last_check_time'],
            'timestamp': datetime.now().isoformat()
        }
    
    def trigger_manual_check(self) -> Dict[str, Any]:
        """Trigger a manual check (for testing/debugging)"""
        logger.info("Triggering manual check...")
        
        try:
            start_time = time.time()
            
            # Process alerts manually
            sensor_result = self.alert_manager.process_sensor_alerts()
            weather_result = self.alert_manager.process_weather_alerts()
            
            # Update stats
            self._update_stats(sensor_result, weather_result)
            
            processing_time = int((time.time() - start_time) * 1000)
            
            return {
                'status': 'success',
                'results': {
                    'sensor_alerts': sensor_result.alerts_generated,
                    'weather_alerts': weather_result.alerts_generated,
                    'processing_time_ms': processing_time
                },
                'errors': sensor_result.errors + weather_result.errors,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in manual check: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            } 