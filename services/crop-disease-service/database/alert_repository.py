#!/usr/bin/env python3
"""
Alert Repository
Centralized database operations for alerts
"""

import os
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

class AlertRepository:
    """
    Centralized repository for alert database operations
    Replaces scattered database logic from ml_service.py and other files
    """
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.connection_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'crop_disease_ml'),
            'user': os.getenv('POSTGRES_USER', 'ml_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'ml_password')
        }
        
        # Statistics
        self.stats = {
            'alerts_saved': 0,
            'alerts_retrieved': 0,
            'database_errors': 0,
            'last_operation_time': None
        }
    
    def save_alerts(self, alerts: List[Dict[str, Any]]) -> bool:
        """
        Save multiple alerts to PostgreSQL database
        Replaces _save_realtime_alerts() from ml_service.py
        
        Note: zone_id is used for both:
        - Sensor alerts: contains the actual field_id (e.g., "field_001")
        - Weather alerts: contains the location (e.g., "Verona", "Milan")
        """
        if not alerts:
            logger.debug("No alerts to save")
            return True
        
        start_time = time.time()
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Prepare batch insert
            insert_query = """
                INSERT INTO alerts (
                    zone_id, alert_timestamp, alert_type, severity, message,
                    details, prediction_id, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Prepare data for batch insert
            alert_data = []
            for alert in alerts:
                alert_data.append((
                    alert.get('zone_id'),
                    alert.get('alert_timestamp'),
                    alert.get('alert_type'),
                    alert.get('severity'),
                    alert.get('message'),
                    json.dumps(alert.get('details', {})),
                    alert.get('prediction_id'),
                    alert.get('status', 'ACTIVE')
                ))
            
            # Execute batch insert
            cursor.executemany(insert_query, alert_data)
            conn.commit()
            
            # Update statistics
            self.stats['alerts_saved'] += len(alerts)
            self.stats['last_operation_time'] = datetime.now().isoformat()
            
            cursor.close()
            conn.close()
            
            processing_time = int((time.time() - start_time) * 1000)
            logger.info(f"Successfully saved {len(alerts)} alerts to database in {processing_time}ms")
            
            return True
            
        except Exception as e:
            self.stats['database_errors'] += 1
            logger.error(f"Error saving alerts to database: {e}")
            return False
    
    def save_single_alert(self, alert: Dict[str, Any]) -> bool:
        """Save a single alert to database"""
        return self.save_alerts([alert])
    
    def get_active_alerts(self, limit: int = 100, zone_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get active alerts from database
        Replaces _query_alerts_from_database() from ml_service.py
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with optional zone filter
            base_query = """
                SELECT 
                    id,
                    zone_id,
                    alert_timestamp,
                    alert_type,
                    severity,
                    message,
                    details,
                    prediction_id,
                    status,
                    created_at
                FROM alerts 
                WHERE status = 'ACTIVE' 
                AND alert_timestamp >= NOW() - INTERVAL '24 hours'
            """
            
            params = []
            if zone_id:
                base_query += " AND zone_id = %s"
                params.append(zone_id)
            
            base_query += " ORDER BY alert_timestamp DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(base_query, params)
            
            alerts = []
            for row in cursor.fetchall():
                alert = dict(row)
                # Convert datetime objects to strings for JSON serialization
                alert['alert_timestamp'] = alert['alert_timestamp'].isoformat() if alert['alert_timestamp'] else None
                alert['created_at'] = alert['created_at'].isoformat() if alert['created_at'] else None
                alerts.append(alert)
            
            cursor.close()
            conn.close()
            
            self.stats['alerts_retrieved'] += len(alerts)
            logger.debug(f"Retrieved {len(alerts)} active alerts from database")
            
            return alerts
            
        except Exception as e:
            self.stats['database_errors'] += 1
            logger.error(f"Error retrieving alerts from database: {e}")
            return []
    
    def update_alert_status(self, alert_id: int, new_status: str, user_id: Optional[str] = None) -> bool:
        """Update alert status (acknowledged, resolved, etc.)"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            update_query = """
                UPDATE alerts 
                SET status = %s, 
                    acknowledged_by = %s,
                    acknowledged_at = CASE 
                        WHEN %s = 'ACKNOWLEDGED' THEN NOW() 
                        ELSE acknowledged_at 
                    END,
                    resolved_at = CASE 
                        WHEN %s = 'RESOLVED' THEN NOW() 
                        ELSE resolved_at 
                    END
                WHERE id = %s
            """
            
            cursor.execute(update_query, (new_status, user_id, new_status, new_status, alert_id))
            conn.commit()
            
            rows_affected = cursor.rowcount
            cursor.close()
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"Updated alert {alert_id} status to {new_status}")
                return True
            else:
                logger.warning(f"No alert found with id {alert_id}")
                return False
                
        except Exception as e:
            self.stats['database_errors'] += 1
            logger.error(f"Error updating alert status: {e}")
            return False
    
    def get_alert_statistics(self) -> Dict[str, Any]:
        """Get alert statistics from database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get counts by type and severity
            cursor.execute("""
                SELECT 
                    alert_type,
                    severity,
                    status,
                    COUNT(*) as count
                FROM alerts
                WHERE alert_timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY alert_type, severity, status
                ORDER BY alert_type, severity
            """)
            
            stats_data = cursor.fetchall()
            
            # Get recent activity
            cursor.execute("""
                SELECT 
                    DATE_TRUNC('hour', alert_timestamp) as hour,
                    COUNT(*) as alerts_count
                FROM alerts
                WHERE alert_timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY DATE_TRUNC('hour', alert_timestamp)
                ORDER BY hour DESC
            """)
            
            activity_data = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return {
                'statistics_by_type': [dict(row) for row in stats_data],
                'hourly_activity': [dict(row) for row in activity_data],
                'repository_stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting alert statistics: {e}")
            return {
                'error': str(e),
                'repository_stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
    
    def cleanup_old_alerts(self, days_old: int = 30) -> int:
        """Clean up old alerts from database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Delete alerts older than specified days
            delete_query = """
                DELETE FROM alerts 
                WHERE alert_timestamp < NOW() - INTERVAL '%s days'
                AND status IN ('RESOLVED', 'IGNORED')
            """
            
            cursor.execute(delete_query, (days_old,))
            deleted_count = cursor.rowcount
            conn.commit()
            
            cursor.close()
            conn.close()
            
            logger.info(f"Cleaned up {deleted_count} old alerts")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old alerts: {e}")
            return 0
    
    def _get_connection(self):
        """Get PostgreSQL connection"""
        try:
            return psycopg2.connect(**self.connection_config)
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def is_healthy(self) -> bool:
        """Check if repository is healthy (can connect to database)"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
        except Exception:
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get repository statistics"""
        return {
            'repository_type': 'postgresql',
            'statistics': self.stats,
            'connection_config': {
                'host': self.connection_config['host'],
                'port': self.connection_config['port'],
                'database': self.connection_config['database'],
                'user': self.connection_config['user']
                # Password omitted for security
            },
            'timestamp': datetime.now().isoformat()
        } 