import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from zoneinfo import ZoneInfo

# Logging configuration
logger = logging.getLogger(__name__)

class DataPreprocessor:
    """
    Central data preprocessing service for crop disease prediction system.
    Handles validation, cleaning, and enrichment of all data types.
    """
    
    def __init__(self, timezone: str = "Europe/Rome"):
        self.timezone = timezone
        logger.info("🧹 DataPreprocessor initialized")
    
    def preprocess_sensor_data(self, raw_data: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Preprocess sensor data with validation and enrichment
        
        Returns:
            Tuple[processed_data, status] where status is 'valid', 'invalid', or 'anomaly'
        """
        try:
            # Required fields validation
            required_fields = ["timestamp", "field_id", "temperature", "humidity", "soil_ph"]
            if not all(field in raw_data for field in required_fields):
                return None, "invalid"
            
            # Extract and validate values
            temperature = raw_data.get("temperature")
            humidity = raw_data.get("humidity")
            soil_ph = raw_data.get("soil_ph")
            
            status = "valid"
            
            # Temperature validation (-10°C to 50°C normal range)
            if temperature is None or not isinstance(temperature, (int, float)):
                return None, "invalid"
            if temperature < -20 or temperature > 60:
                return None, "invalid"
            if temperature < -10 or temperature > 50:
                status = "anomaly"
            
            # Humidity validation (0% to 100%)
            if humidity is None or not isinstance(humidity, (int, float)):
                return None, "invalid"
            if humidity < 0 or humidity > 100:
                return None, "invalid"
            if humidity < 10 or humidity > 95:
                status = "anomaly"
            
            # Soil pH validation (typically 4.0 to 8.5)
            if soil_ph is None or not isinstance(soil_ph, (int, float)):
                return None, "invalid"
            if soil_ph < 3.0 or soil_ph > 10.0:
                return None, "invalid"
            if soil_ph < 4.5 or soil_ph > 8.0:
                status = "anomaly"
            
            # Timestamp validation and normalization
            try:
                timestamp = datetime.fromisoformat(raw_data["timestamp"].rstrip("Z"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=ZoneInfo(self.timezone))
            except (ValueError, AttributeError):
                return None, "invalid"
            
            # Create processed data with enrichment
            processed_data = {
                "timestamp": timestamp.isoformat(),
                "field_id": raw_data["field_id"],
                "temperature": round(float(temperature), 2),
                "humidity": round(float(humidity), 2),
                "soil_ph": round(float(soil_ph), 3),
                "data_quality": status
            }
            
            # Add quality flags
            processed_data["quality_flags"] = {
                "temp_anomaly": temperature < -10 or temperature > 50,
                "humidity_anomaly": humidity < 10 or humidity > 95,
                "ph_anomaly": soil_ph < 4.5 or soil_ph > 8.0
            }
            
            return processed_data, status
            
        except Exception as e:
            logger.error(f"❌ Error preprocessing sensor data: {e}")
            return None, "invalid"
    
    def preprocess_weather_data(self, raw_data: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Preprocess weather data with validation and enrichment
        
        Returns:
            Tuple[processed_data, status] where status is 'valid', 'invalid', or 'anomaly'
        """
        try:
            # Required fields validation (più permissivo per API esterne)
            if not raw_data.get("location"):
                return None, "invalid"
            
            status = "valid"
            processed_data = {}
            
            # Timestamp validation and normalization
            try:
                timestamp = datetime.fromisoformat(raw_data["timestamp"].rstrip("Z"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=ZoneInfo(self.timezone))
                processed_data["timestamp"] = timestamp.isoformat()
            except (ValueError, AttributeError):
                return None, "invalid"
            
            # Location data (required)
            processed_data["location"] = raw_data.get("location")
            processed_data["region"] = raw_data.get("region")
            processed_data["country"] = raw_data.get("country")
            processed_data["lat"] = raw_data.get("lat")
            processed_data["lon"] = raw_data.get("lon")
            
            # Temperature validation
            temp_c = raw_data.get("temp_c")
            if temp_c is not None:
                if not isinstance(temp_c, (int, float)) or temp_c < -60 or temp_c > 70:
                    temp_c = None
                elif temp_c < -40 or temp_c > 60:
                    status = "anomaly"
            processed_data["temp_c"] = round(float(temp_c), 1) if temp_c is not None else None
            
            # Humidity validation
            humidity = raw_data.get("humidity")
            if humidity is not None:
                if not isinstance(humidity, (int, float)) or humidity < 0 or humidity > 100:
                    humidity = None
                elif humidity < 5 or humidity > 98:
                    status = "anomaly"
            processed_data["humidity"] = int(humidity) if humidity is not None else None
            
            # Wind validation
            wind_kph = raw_data.get("wind_kph")
            if wind_kph is not None:
                if not isinstance(wind_kph, (int, float)) or wind_kph < 0:
                    wind_kph = 0
                elif wind_kph > 200:
                    status = "anomaly"
            processed_data["wind_kph"] = round(float(wind_kph), 1) if wind_kph is not None else 0
            
            # UV validation
            uv = raw_data.get("uv")
            if uv is not None:
                if not isinstance(uv, (int, float)) or uv < 0 or uv > 20:
                    uv = None
                elif uv > 15:
                    status = "anomaly"
            processed_data["uv"] = round(float(uv), 1) if uv is not None else None
            
            # Condition
            processed_data["condition"] = raw_data.get("condition", "Unknown")
            processed_data["message_id"] = raw_data.get("message_id")
            
            # Add metadata
            processed_data["data_quality"] = status
            
            # Add quality flags
            processed_data["quality_flags"] = {
                "temp_anomaly": temp_c is not None and (temp_c < -40 or temp_c > 60),
                "humidity_anomaly": humidity is not None and (humidity < 5 or humidity > 98),
                "wind_anomaly": wind_kph is not None and wind_kph > 200,
                "uv_anomaly": uv is not None and uv > 15,
                "missing_data": any(v is None for v in [temp_c, humidity, uv])
            }
            
            return processed_data, status
            
        except Exception as e:
            logger.error(f"❌ Error preprocessing weather data: {e}")
            return None, "invalid"
    
    def preprocess_satellite_data(self, raw_data: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Preprocess satellite data with validation and enrichment
        
        Returns:
            Tuple[processed_data, status] where status is 'valid', 'invalid', or 'anomaly'
        """
        try:
            # Required fields validation
            if not raw_data.get("image_base64") or not raw_data.get("location"):
                return None, "invalid"
            
            # Timestamp validation
            try:
                timestamp = datetime.fromisoformat(raw_data["timestamp"].rstrip("Z"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=ZoneInfo(self.timezone))
            except (ValueError, AttributeError):
                return None, "invalid"
            
            # Basic image validation (check if base64 is reasonable)
            image_base64 = raw_data.get("image_base64", "")
            if len(image_base64) < 1000:  # Too small to be a real image
                return None, "invalid"
            
            processed_data = {
                "timestamp": timestamp.isoformat(),
                "image_base64": image_base64,
                "location": raw_data.get("location"),
                "image_size_kb": len(image_base64) // 1024,
                "data_quality": "valid"
            }
            
            # Add quality flags
            processed_data["quality_flags"] = {
                "large_image": len(image_base64) > 1000000,  # > 1MB
                "small_image": len(image_base64) < 10000     # < 10KB
            }
            
            return processed_data, "valid"
            
        except Exception as e:
            logger.error(f"❌ Error preprocessing satellite data: {e}")
            return None, "invalid" 