#!/usr/bin/env python3
"""
Data Loader - Loads data from Gold zone for crop disease processing
Supports both real-time and batch data loading
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, when

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Loads data from Gold zone for crop disease processing
    Supports real-time and batch data loading
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.gold_path = "s3a://gold/"
        self.silver_path = "s3a://silver/"
        
        # Data paths
        self.paths = {
            'sensor_metrics': f"{self.gold_path}sensor_metrics/recent/",  # Use Gold layer for sensor data
            'weather_features': f"{self.silver_path}weather/",  # Use Silver layer for weather data
            'ml_features': f"{self.gold_path}ml_features/integrated/",  # Use integrated features for ML
            'dashboard_metrics': f"{self.gold_path}dashboard_metrics/"
        }
    
    def load_realtime_data(self) -> pd.DataFrame:
        """
        Load real-time data for current analysis (Mode 1)
        Returns recent data for immediate insights
        """
        try:
            logger.info("Loading real-time data from Gold zone...")
            
            # Load recent sensor metrics from Gold layer
            sensor_df = self._load_recent_sensor_data()
            
            # For ML processing, we only use sensor data
            # Weather data will be used separately for simple threshold alerts
            if not sensor_df.empty:
                merged_df = sensor_df.copy()
                # Rename sensor columns to match expected format for ML
                merged_df['avg_temperature'] = merged_df['current_temperature']
                merged_df['avg_humidity'] = merged_df['current_humidity']
                merged_df['avg_soil_ph'] = merged_df['current_soil_ph']
            else:
                merged_df = pd.DataFrame()
            
            logger.info(f"Loaded real-time data: {len(merged_df)} records")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error loading real-time data: {e}")
            return pd.DataFrame()
    
    def load_ml_features(self) -> pd.DataFrame:
        """
        Load ML features for batch prediction (Mode 2)
        Returns historical data for ML model training and prediction
        """
        try:
            logger.info("Loading ML features from Gold zone...")
            
            # Load integrated ML features
            ml_features_path = self.paths['ml_features']
            
            try:
                # Load from Parquet format (faster and more reliable)
                ml_df = self.spark.read.parquet(ml_features_path)
                pandas_df = ml_df.toPandas()
            except Exception as e:
                logger.warning(f"Could not load from Parquet format: {e}")
                # Return empty DataFrame
                pandas_df = pd.DataFrame()
            
            if not pandas_df.empty:
                # Ensure we have the required columns (including weather features)
                required_columns = [
                    'field_id', 'avg_temperature', 'avg_humidity', 'avg_soil_ph',
                    'temperature_range', 'humidity_range', 'anomaly_rate',
                    'data_quality_score', 'weather_avg_temperature',
                    'weather_avg_humidity', 'weather_avg_wind_speed'
                ]
                
                missing_columns = [column_name for column_name in required_columns if column_name not in pandas_df.columns]
                if missing_columns:
                    logger.warning(f"Missing columns in ML features: {missing_columns}")
                    # Add missing columns with default values
                    for column_name in missing_columns:
                        pandas_df[column_name] = 0.0
            
            logger.info(f"Loaded ML features: {len(pandas_df)} records")
            return pandas_df
            
        except Exception as e:
            logger.error(f"Error loading ML features: {e}")
            return pd.DataFrame()
    
    def _load_recent_sensor_data(self) -> pd.DataFrame:
        """Load recent sensor data for real-time processing from Gold layer (for ML analysis only)"""
        try:
            sensor_path = self.paths['sensor_metrics']
            
            # Load from Parquet format
            sensor_df = self.spark.read.parquet(sensor_path)
            
            # Get recent data (last 24 hours)
            recent_df = sensor_df.filter(
                col("created_at") >= datetime.now() - timedelta(hours=24)
            )
            
            # Convert to pandas
            pandas_df = recent_df.toPandas()
            
            # Ensure we have required columns
            required_columns = ['field_id', 'current_temperature', 'current_humidity', 'current_soil_ph']
            for column_name in required_columns:
                if column_name not in pandas_df.columns:
                    pandas_df[column_name] = 0.0
            
            return pandas_df
            
        except Exception as e:
            logger.warning(f"Could not load recent sensor data from Gold layer: {e}")
            return pd.DataFrame()
    
    def _load_recent_weather_data(self) -> pd.DataFrame:
        """Load recent weather data for real-time processing"""
        try:
            weather_path = self.paths['weather_features']
            
            # Load from Parquet format
            weather_df = self.spark.read.parquet(weather_path)
            
            # Get recent data (last 24 hours)
            recent_df = weather_df.filter(
                col("date") >= (datetime.now() - timedelta(hours=24)).date()
            )
            
            # Convert to pandas
            pandas_df = recent_df.toPandas()
            
            # Ensure we have required columns (fill with 0.0 if missing)
            required_columns = ['region', 'temp_c', 'humidity', 'wind_kph']
            for column_name in required_columns:
                if column_name not in pandas_df.columns:
                    pandas_df[column_name] = 0.0
            
            return pandas_df
            
        except Exception as e:
            logger.warning(f"Could not load recent weather data: {e}")
            return pd.DataFrame()
    
    def load_historical_data(self, days: int = 30) -> pd.DataFrame:
        """Load historical data for model training"""
        try:
            logger.info(f"Loading historical data for last {days} days...")
            
            # Load ML features for the specified period
            ml_features_path = self.paths['ml_features']
            
            ml_df = self.spark.read.parquet(ml_features_path)
            
            # Filter for historical period
            historical_df = ml_df.filter(
                col("date") >= (datetime.now() - timedelta(days=days)).date()
            )
            
            # Convert to pandas
            pandas_df = historical_df.toPandas()
            
            logger.info(f"Loaded historical data: {len(pandas_df)} records")
            return pandas_df
            
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            return pd.DataFrame()
    
    def load_field_metadata(self) -> Dict[str, Any]:
        """Load field metadata (crop type, size, etc.)"""
        # In a real implementation, this would load from a metadata table
        # For now, return default metadata
        return {
            'field_01': {
                'crop_type': 'tomatoes',
                'size_hectares': 2.0,
                'planting_date': '2024-03-15',
                'expected_harvest': '2024-07-15'
            },
            'field_02': {
                'crop_type': 'tomatoes',
                'size_hectares': 1.5,
                'planting_date': '2024-03-20',
                'expected_harvest': '2024-07-20'
            },
            'field_03': {
                'crop_type': 'potatoes',
                'size_hectares': 3.0,
                'planting_date': '2024-04-01',
                'expected_harvest': '2024-08-15'
            }
        }
    
    def get_data_quality_metrics(self) -> Dict[str, Any]:
        """Get data quality metrics for monitoring"""
        try:
            # Load recent data
            recent_data = self.load_realtime_data()
            
            if recent_data.empty:
                return {
                    'total_records': 0,
                    'completeness': 0.0,
                    'timeliness': 0.0,
                    'accuracy': 0.0,
                    'last_update': None
                }
            
            # Calculate completeness (non-null values)
            total_fields = len(recent_data) * 3  # temperature, humidity, soil_ph
            non_null_fields = (
                recent_data['current_temperature'].notna().sum() +
                recent_data['current_humidity'].notna().sum() +
                recent_data['current_soil_ph'].notna().sum()
            )
            completeness = non_null_fields / total_fields if total_fields > 0 else 0.0
            
            # Calculate timeliness (data from last 6 hours)
            recent_timestamp = datetime.now() - timedelta(hours=6)
            recent_records = len(recent_data)
            timeliness = 1.0 if recent_records > 0 else 0.0
            
            # Calculate accuracy (values within expected ranges)
            temp_accurate = ((recent_data['current_temperature'] >= 0) & 
                           (recent_data['current_temperature'] <= 50)).sum()
            humidity_accurate = ((recent_data['current_humidity'] >= 0) & 
                               (recent_data['current_humidity'] <= 100)).sum()
            ph_accurate = ((recent_data['current_soil_ph'] >= 0) & 
                          (recent_data['current_soil_ph'] <= 14)).sum()
            
            total_accurate = temp_accurate + humidity_accurate + ph_accurate
            accuracy = total_accurate / total_fields if total_fields > 0 else 0.0
            
            return {
                'total_records': len(recent_data),
                'completeness': completeness,
                'timeliness': timeliness,
                'accuracy': accuracy,
                'last_update': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error calculating data quality metrics: {e}")
            return {
                'total_records': 0,
                'completeness': 0.0,
                'timeliness': 0.0,
                'accuracy': 0.0,
                'last_update': None
            }
    

    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of available data"""
        try:
            # Load recent data
            recent_data = self.load_realtime_data()
            
            # Load ML features
            ml_features = self.load_ml_features()
            
            # Load weather data
            weather_data = self._load_recent_weather_data()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'realtime_data': {
                    'total_records': len(recent_data),
                    'fields': list(recent_data['field_id'].unique()) if not recent_data.empty else [],
                    'last_update': datetime.now().isoformat()
                },
                'ml_features': {
                    'total_records': len(ml_features),
                    'fields': list(ml_features['field_id'].unique()) if not ml_features.empty else [],
                    'last_update': datetime.now().isoformat()
                },
                'weather_data': {
                    'total_records': len(weather_data),
                    'regions': list(weather_data['region'].unique()) if not weather_data.empty else [],
                    'last_update': datetime.now().isoformat()
                },
                'data_quality': self.get_data_quality_metrics()
            }
            
        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            } 

    def load_sensor_data_from_silver(self) -> pd.DataFrame:
        """
        Load sensor data directly from Silver layer for alert generation
        Returns raw sensor readings with validation flags
        """
        try:
            logger.info("Loading sensor data from Silver layer for alerts...")
            
            # Load from Silver layer
            silver_sensor_path = f"{self.silver_path}iot/"
            
            try:
                # Load from Parquet format
                sensor_df = self.spark.read.parquet(silver_sensor_path)
                
                # Get recent data (last 2 hours for real-time alerts)
                cutoff_time = datetime.now() - timedelta(hours=2)
                recent_df = sensor_df.filter(
                    col("timestamp_parsed") >= cutoff_time
                )
                
                # Convert to pandas
                pandas_df = recent_df.toPandas()
                
                # Log the data we found
                if not pandas_df.empty:
                    logger.info(f"Found {len(pandas_df)} records in last 2 hours (since {cutoff_time})")
                    # Show the latest timestamps for each field
                    latest_timestamps = pandas_df.groupby('field_id')['timestamp_parsed'].max()
                    for field_id, timestamp in latest_timestamps.items():
                        logger.info(f"  Latest data for {field_id}: {timestamp}")
                else:
                    logger.warning(f"No data found in Silver layer in last 2 hours (since {cutoff_time})")
                
                # Ensure we have required columns
                required_columns = [
                    'field_id', 'temperature', 'humidity', 'soil_ph',
                    'timestamp_parsed', 'temperature_valid', 'humidity_valid', 'ph_valid'
                ]
                
                for column_name in required_columns:
                    if column_name not in pandas_df.columns:
                        if 'valid' in column_name:
                            pandas_df[column_name] = True  # Default to valid
                        else:
                            pandas_df[column_name] = 0.0
                
                logger.info(f"Loaded {len(pandas_df)} sensor records from Silver layer")
                return pandas_df
                
            except Exception as e:
                logger.warning(f"Could not load sensor data from Silver layer: {e}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error loading sensor data from Silver layer: {e}")
            return pd.DataFrame()


                
 

    def _get_recent_silver_sensor_data(self) -> List[Dict[str, Any]]:
        """Get recent sensor data from Silver layer for real-time alerts"""
        try:
            # Use existing method that loads from Silver layer
            sensor_df = self.load_sensor_data_from_silver()
            
            if sensor_df.empty:
                return []
            
            # Convert to list of dictionaries
            sensor_data = []
            for _, row in sensor_df.iterrows():
                sensor_data.append(row.to_dict())
            
            return sensor_data
            
        except Exception as e:
            logger.error(f"Error getting recent silver sensor data: {e}")
            return []
    
    def _get_recent_silver_weather_data(self) -> List[Dict[str, Any]]:
        """Get recent weather data from Silver layer for real-time alerts"""
        try:
            # Query Silver layer directly for recent weather data
            spark_df = self.spark.sql("""
                SELECT 
                    location,
                    temp_c,
                    humidity,
                    wind_kph,
                    uv_index,
                    condition_text,
                    timestamp
                FROM silver.weather_data
                WHERE timestamp >= current_timestamp() - INTERVAL 10 MINUTES
                ORDER BY timestamp DESC
            """)
            
            # Convert to list of dictionaries
            weather_data = []
            for row in spark_df.collect():
                weather_data.append(row.asDict())
            
            return weather_data
            
        except Exception as e:
            logger.error(f"Error getting recent silver weather data: {e}")
            return [] 