#!/usr/bin/env python3
"""
Scheduler Service - Manages batch processing scheduling
Implements 6-hour batch processing cycle for Crop Disease Service
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

logger = logging.getLogger(__name__)

class SchedulerService:
    """
    Manages scheduling for batch processing (Mode 2)
    Runs crop disease predictions every 6 hours
    """
    
    def __init__(self, ml_service):
        self.ml_service = ml_service
        self.scheduler = BackgroundScheduler()
        self.is_running = False
        
        # Schedule configuration
        self.batch_interval_hours = 6
        self.batch_start_hour = 0  # Start at midnight
        
        # Job tracking
        self.last_batch_run = None
        self.next_batch_run = None
        self.batch_run_count = 0
        self.batch_success_count = 0
        self.batch_error_count = 0
        
        # Setup scheduler
        self._setup_scheduler()
    
    def _setup_scheduler(self):
        """Setup the scheduler with batch processing jobs"""
        
        # Add event listeners
        self.scheduler.add_listener(self._job_executed_listener, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
        
        # Add batch prediction job - run every 6 hours starting at midnight
        self.scheduler.add_job(
            func=self._run_batch_prediction_job,
            trigger=CronTrigger(
                hour=f"{self.batch_start_hour}/{self.batch_interval_hours}",
                minute=0,
                second=0
            ),
            id='batch_prediction',
            name='Batch ML Prediction',
            replace_existing=True,
            max_instances=1
        )
        
        # Add model retraining job - run weekly on Sunday at 2 AM
        self.scheduler.add_job(
            func=self._run_model_retraining_job,
            trigger=CronTrigger(
                day_of_week='sun',
                hour=2,
                minute=0,
                second=0
            ),
            id='model_retraining',
            name='Model Retraining',
            replace_existing=True,
            max_instances=1
        )
        
        # Add data quality check job - run daily at 6 AM
        self.scheduler.add_job(
            func=self._run_data_quality_check_job,
            trigger=CronTrigger(
                hour=6,
                minute=0,
                second=0
            ),
            id='data_quality_check',
            name='Data Quality Check',
            replace_existing=True,
            max_instances=1
        )
        
        logger.info("Scheduler setup completed")
        logger.info(f"Batch prediction scheduled every {self.batch_interval_hours} hours starting at {self.batch_start_hour}:00")
        logger.info("Model retraining scheduled weekly on Sunday at 2:00 AM")
        logger.info("Data quality check scheduled daily at 6:00 AM")
    
    def start(self):
        """Start the scheduler"""
        try:
            if not self.is_running:
                self.scheduler.start()
                self.is_running = True
                
                # Calculate next batch run
                self._calculate_next_batch_run()
                
                logger.info("Scheduler started successfully")
                logger.info(f"Next batch prediction: {self.next_batch_run}")
                
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler"""
        try:
            if self.is_running:
                self.scheduler.shutdown(wait=False)
                self.is_running = False
                logger.info("Scheduler stopped")
                
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def _run_batch_prediction_job(self):
        """Run batch prediction job"""
        try:
            logger.info("Starting scheduled batch prediction...")
            
            start_time = datetime.now()
            
            # Run batch prediction
            result = self.ml_service.run_batch_prediction()
            
            # Update tracking
            self.last_batch_run = start_time
            self.batch_run_count += 1
            
            if 'error' not in result:
                self.batch_success_count += 1
                logger.info(f"Batch prediction completed successfully: {result.get('predictions_count', 0)} predictions, {result.get('alerts_generated', 0)} alerts")
            else:
                self.batch_error_count += 1
                logger.error(f"Batch prediction failed: {result.get('error')}")
            
            # Calculate next run
            self._calculate_next_batch_run()
            
        except Exception as e:
            logger.error(f"Error in batch prediction job: {e}")
            self.batch_error_count += 1
    
    def _run_model_retraining_job(self):
        """Run model retraining job"""
        try:
            logger.info("Starting scheduled model retraining...")
            
            # Load historical data for retraining
            historical_data = self.ml_service.data_loader.load_historical_data(days=30)
            
            if not historical_data.empty:
                # Retrain model
                result = self.ml_service.disease_predictor.retrain_model(historical_data)
                
                if result.get('status') == 'success':
                    logger.info(f"Model retraining completed successfully. New version: {result.get('new_version')}")
                else:
                    logger.error(f"Model retraining failed: {result.get('message')}")
            else:
                logger.warning("No historical data available for model retraining")
                
        except Exception as e:
            logger.error(f"Error in model retraining job: {e}")
    
    def _run_data_quality_check_job(self):
        """Run data quality check job"""
        try:
            logger.info("Starting scheduled data quality check...")
            
            # Get data quality metrics
            quality_metrics = self.ml_service.data_loader.get_data_quality_metrics()
            
            # Check for quality issues
            issues = []
            
            if quality_metrics.get('completeness', 1.0) < 0.8:
                issues.append(f"Low data completeness: {quality_metrics.get('completeness')}")
            
            if quality_metrics.get('timeliness', 1.0) < 0.9:
                issues.append(f"Low data timeliness: {quality_metrics.get('timeliness')}")
            
            if quality_metrics.get('total_records', 0) < 10:
                issues.append(f"Low data volume: {quality_metrics.get('total_records')} records")
            
            if issues:
                logger.warning(f"Data quality issues detected: {issues}")
                # In a real implementation, this would trigger alerts
            else:
                logger.info("Data quality check passed")
                
        except Exception as e:
            logger.error(f"Error in data quality check job: {e}")
    
    def _job_executed_listener(self, event):
        """Listener for successful job execution"""
        job_id = event.job_id
        logger.info(f"Job {job_id} executed successfully at {event.scheduled_run_time}")
    
    def _job_error_listener(self, event):
        """Listener for job execution errors"""
        job_id = event.job_id
        exception = event.exception
        logger.error(f"Job {job_id} failed with exception: {exception}")
    
    def _calculate_next_batch_run(self):
        """Calculate next batch run time"""
        now = datetime.now()
        
        # Find next run time based on 6-hour intervals starting at midnight
        current_hour = now.hour
        hours_since_midnight = current_hour - self.batch_start_hour
        
        # Calculate how many hours until next run
        hours_until_next = self.batch_interval_hours - (hours_since_midnight % self.batch_interval_hours)
        
        if hours_until_next == self.batch_interval_hours:
            hours_until_next = 0
        
        # Calculate next run time
        next_run = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=hours_until_next)
        
        self.next_batch_run = next_run
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get scheduler status"""
        return {
            'is_running': self.is_running,
            'batch_interval_hours': self.batch_interval_hours,
            'last_batch_run': self.last_batch_run.isoformat() if self.last_batch_run else None,
            'next_batch_run': self.next_batch_run.isoformat() if self.next_batch_run else None,
            'batch_run_count': self.batch_run_count,
            'batch_success_count': self.batch_success_count,
            'batch_error_count': self.batch_error_count,
            'success_rate': (self.batch_success_count / self.batch_run_count * 100) if self.batch_run_count > 0 else 0,
            'jobs': self._get_jobs_status()
        }
    
    def _get_jobs_status(self) -> Dict[str, Any]:
        """Get status of all scheduled jobs"""
        jobs = {}
        
        for job in self.scheduler.get_jobs():
            jobs[job.id] = {
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            }
        
        return jobs
    

    
    def update_schedule(self, batch_interval_hours: int = None, batch_start_hour: int = None) -> Dict[str, Any]:
        """Update scheduler configuration"""
        try:
            if batch_interval_hours is not None:
                self.batch_interval_hours = batch_interval_hours
            
            if batch_start_hour is not None:
                self.batch_start_hour = batch_start_hour
            
            # Restart scheduler with new configuration
            if self.is_running:
                self.stop()
                self._setup_scheduler()
                self.start()
            
            return {
                'status': 'success',
                'message': 'Schedule updated successfully',
                'new_config': {
                    'batch_interval_hours': self.batch_interval_hours,
                    'batch_start_hour': self.batch_start_hour
                }
            }
            
        except Exception as e:
            logger.error(f"Error updating schedule: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_upcoming_runs(self, count: int = 5) -> Dict[str, Any]:
        """Get upcoming scheduled runs"""
        try:
            upcoming_runs = []
            
            for job in self.scheduler.get_jobs():
                next_runs = []
                trigger = job.trigger
                
                # Get next few run times
                run_time = trigger.next_fire_time
                for i in range(count):
                    if run_time:
                        next_runs.append(run_time.isoformat())
                        run_time = trigger.next_fire_time(run_time)
                    else:
                        break
                
                upcoming_runs.append({
                    'job_id': job.id,
                    'job_name': job.name,
                    'next_runs': next_runs
                })
            
            return {
                'upcoming_runs': upcoming_runs,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting upcoming runs: {e}")
            return {
                'error': str(e),
                'upcoming_runs': []
            } 