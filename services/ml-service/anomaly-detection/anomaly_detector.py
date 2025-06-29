import os
import json
import logging
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
from sklearn.model_selection import train_test_split
import psycopg2
import joblib

logger = logging.getLogger(__name__)

class AnomalyDetector:
    """
    Machine Learning based anomaly detector for crop sensor data
    """
    
    def __init__(self, model_path="models/"):
        self.model_path = model_path
        self.models = {}
        self.scalers = {}
        self.field_configs = {}
        
        # Ensure model directory exists
        os.makedirs(model_path, exist_ok=True)
        
        # Load or initialize models
        self._load_or_create_models()
        
        # Thresholds for different types of anomalies
        self.anomaly_thresholds = {
            'temperature': {'min': 5, 'max': 45},
            'humidity': {'min': 20, 'max': 95},
            'soil_ph': {'min': 4.0, 'max': 8.5}
        }
        
        # Disease prediction patterns
        self.disease_patterns = {
            'fungal_risk': {
                'conditions': {'humidity': (80, 100), 'temperature': (20, 30)},
                'description': 'High humidity and moderate temperature favor fungal diseases'
            },
            'bacterial_risk': {
                'conditions': {'humidity': (70, 90), 'temperature': (25, 35)},
                'description': 'Warm humid conditions favor bacterial infections'
            },
            'pest_risk': {
                'conditions': {'temperature': (25, 35), 'humidity': (40, 70)},
                'description': 'Optimal conditions for pest activity'
            },
            'nutrient_stress': {
                'conditions': {'soil_ph': (4.0, 5.5)},
                'description': 'Acidic soil may indicate nutrient deficiency'
            },
            'alkaline_stress': {
                'conditions': {'soil_ph': (7.5, 8.5)},
                'description': 'Alkaline soil may limit nutrient uptake'
            }
        }
    
    def _load_or_create_models(self):
        """Load existing models or create new ones"""
        try:
            # Try to load existing models
            isolation_forest_path = os.path.join(self.model_path, "isolation_forest.pkl")
            scaler_path = os.path.join(self.model_path, "scaler.pkl")
            
            if os.path.exists(isolation_forest_path) and os.path.exists(scaler_path):
                self.models['isolation_forest'] = joblib.load(isolation_forest_path)
                self.scalers['main'] = joblib.load(scaler_path)
                logger.info("✅ Loaded existing ML models")
            else:
                self._create_new_models()
                
        except Exception as e:
            logger.warning(f"⚠️ Error loading models: {e}. Creating new models...")
            self._create_new_models()
    
    def _create_new_models(self):
        """Create and initialize new ML models"""
        # Isolation Forest for anomaly detection
        self.models['isolation_forest'] = IsolationForest(
            contamination=0.1,  # Expect 10% anomalies
            random_state=42,
            n_estimators=100
        )
        
        # Standard scaler for feature normalization
        self.scalers['main'] = StandardScaler()
        
        # Try to train on existing data
        self._train_on_historical_data()
        
        logger.info("✅ Created new ML models")
    
    def _train_on_historical_data(self):
        """Train models on historical data from database"""
        try:
            # Connect to database
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                database=os.getenv("POSTGRES_DB", "sensordb"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres")
            )
            
            # Query recent historical data
            query = """
                SELECT temperature, humidity, soil_ph, field_id, timestamp
                FROM sensor_data 
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                ORDER BY timestamp DESC 
                LIMIT 1000
            """
            
            df = pd.read_sql(query, conn)
            conn.close()
            
            if len(df) > 50:  # Need minimum data for training
                self._train_models(df)
                logger.info(f"🎯 Trained models on {len(df)} historical records")
            else:
                logger.info("⚠️ Insufficient historical data for training. Using default models.")
                
        except Exception as e:
            logger.warning(f"⚠️ Could not train on historical data: {e}")
    
    def _train_models(self, df):
        """Train ML models on provided data"""
        # Prepare features
        features = ['temperature', 'humidity', 'soil_ph']
        X = df[features].values
        
        # Remove any NaN values
        X = X[~np.isnan(X).any(axis=1)]
        
        if len(X) < 10:
            logger.warning("⚠️ Insufficient clean data for training")
            return
        
        # Fit scaler and transform data
        X_scaled = self.scalers['main'].fit_transform(X)
        
        # Train Isolation Forest
        self.models['isolation_forest'].fit(X_scaled)
        
        # Save models
        self._save_models()
        
        logger.info("✅ Models trained successfully")
    
    def _save_models(self):
        """Save trained models to disk"""
        try:
            joblib.dump(
                self.models['isolation_forest'], 
                os.path.join(self.model_path, "isolation_forest.pkl")
            )
            joblib.dump(
                self.scalers['main'], 
                os.path.join(self.model_path, "scaler.pkl")
            )
            logger.info("💾 Models saved to disk")
        except Exception as e:
            logger.error(f"❌ Error saving models: {e}")
    
    def detect_anomaly(self, sensor_data):
        """
        Detect anomalies in sensor data
        Returns: (is_anomaly, anomaly_score, anomaly_type)
        """
        try:
            # Extract numerical features
            features = np.array([
                sensor_data['temperature'],
                sensor_data['humidity'],
                sensor_data['soil_ph']
            ]).reshape(1, -1)
            
            # Check for extreme values (rule-based)
            rule_based_anomaly = self._check_rule_based_anomalies(sensor_data)
            
            # ML-based anomaly detection
            ml_anomaly_score = 0.0
            ml_is_anomaly = False
            
            if 'isolation_forest' in self.models:
                try:
                    # Scale features
                    features_scaled = self.scalers['main'].transform(features)
                    
                    # Predict anomaly
                    prediction = self.models['isolation_forest'].predict(features_scaled)
                    anomaly_score = self.models['isolation_forest'].decision_function(features_scaled)[0]
                    
                    ml_is_anomaly = prediction[0] == -1
                    ml_anomaly_score = abs(anomaly_score)
                    
                except Exception as e:
                    logger.warning(f"⚠️ ML prediction error: {e}")
            
            # Disease pattern detection
            disease_risk = self._check_disease_patterns(sensor_data)
            
            # Combine all detection methods
            is_anomaly = rule_based_anomaly[0] or ml_is_anomaly or disease_risk[0]
            
            # Determine anomaly type
            anomaly_type = "normal"
            if rule_based_anomaly[0]:
                anomaly_type = rule_based_anomaly[1]
            elif disease_risk[0]:
                anomaly_type = disease_risk[1]
            elif ml_is_anomaly:
                anomaly_type = "statistical_anomaly"
            
            # Calculate combined score
            combined_score = max(rule_based_anomaly[2], ml_anomaly_score, disease_risk[2])
            
            return is_anomaly, combined_score, anomaly_type
            
        except Exception as e:
            logger.error(f"❌ Anomaly detection error: {e}")
            return False, 0.0, "error"
    
    def _check_rule_based_anomalies(self, sensor_data):
        """Check for rule-based anomalies (extreme values)"""
        temperature = sensor_data['temperature']
        humidity = sensor_data['humidity']
        soil_ph = sensor_data['soil_ph']
        
        # Check temperature
        if temperature < self.anomaly_thresholds['temperature']['min']:
            return True, "extreme_cold", 0.9
        elif temperature > self.anomaly_thresholds['temperature']['max']:
            return True, "extreme_heat", 0.9
        
        # Check humidity
        if humidity < self.anomaly_thresholds['humidity']['min']:
            return True, "extreme_dry", 0.8
        elif humidity > self.anomaly_thresholds['humidity']['max']:
            return True, "extreme_wet", 0.8
        
        # Check soil pH
        if soil_ph < self.anomaly_thresholds['soil_ph']['min']:
            return True, "extreme_acidic", 0.7
        elif soil_ph > self.anomaly_thresholds['soil_ph']['max']:
            return True, "extreme_alkaline", 0.7
        
        return False, "normal", 0.0
    
    def _check_disease_patterns(self, sensor_data):
        """Check for disease-favorable conditions"""
        temperature = sensor_data['temperature']
        humidity = sensor_data['humidity']
        soil_ph = sensor_data['soil_ph']
        
        for disease, pattern in self.disease_patterns.items():
            conditions = pattern['conditions']
            risk_score = 0.0
            
            # Check each condition
            condition_met = True
            for param, (min_val, max_val) in conditions.items():
                value = sensor_data.get(param)
                if value is not None:
                    if min_val <= value <= max_val:
                        # Calculate how close to the center of the risk range
                        center = (min_val + max_val) / 2
                        range_size = max_val - min_val
                        deviation = abs(value - center) / (range_size / 2)
                        risk_score += (1 - deviation) * 0.6  # 0.6 max per condition
                    else:
                        condition_met = False
                        break
            
            if condition_met and risk_score > 0.4:
                return True, disease, risk_score
        
        return False, "normal", 0.0
    
    def retrain_model(self):
        """Retrain the model with recent data"""
        logger.info("🔄 Retraining anomaly detection model...")
        self._train_on_historical_data() 