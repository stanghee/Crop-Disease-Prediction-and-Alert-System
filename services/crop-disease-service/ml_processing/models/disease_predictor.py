#!/usr/bin/env python3
"""
Disease Predictor - ML Model for Crop Disease Prediction
Implements various ML algorithms for disease risk assessment
"""

import os
import logging
import joblib
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.preprocessing import StandardScaler
import xgboost as xgb

logger = logging.getLogger(__name__)

class DiseasePredictor:
    """
    ML model for predicting crop disease risk
    Supports multiple algorithms and provides confidence scores
    """
    
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.model_type = "RandomForest"  # Default model
        self.model_version = "v1.0.0"
        self.is_initialized = False
        self.last_training_date = None
        
        # Model parameters
        self.feature_columns = [
            'avg_temperature', 'avg_humidity', 'avg_soil_ph',
            'temperature_range', 'humidity_range', 'anomaly_rate',
            'data_quality_score', 'weather_avg_temperature',
            'weather_avg_humidity', 'weather_avg_wind_speed'
        ]
        
        # Risk thresholds
        self.risk_thresholds = {
            'LOW': 0.3,
            'MEDIUM': 0.6,
            'HIGH': 0.8,
            'CRITICAL': 0.9
        }
    
    def initialize(self):
        """Initialize the model - load existing or train new"""
        try:
            model_path = os.path.join(os.getcwd(), 'models', 'disease_predictor.joblib')
            
            if os.path.exists(model_path):
                # Load existing model
                self._load_model(model_path)
                logger.info(f"Loaded existing model: {self.model_type} v{self.model_version}")
            else:
                # Train new model with synthetic data
                self._train_initial_model()
                logger.info(f"Trained new model: {self.model_type} v{self.model_version}")
            
            self.is_initialized = True
            
        except Exception as e:
            logger.error(f"Error initializing disease predictor: {e}")
            # Create a simple rule-based model as fallback
            self._create_fallback_model()
            self.is_initialized = True
    
    def _load_model(self, model_path: str):
        """Load trained model from file"""
        try:
            model_data = joblib.load(model_path)
            self.model = model_data['model']
            self.scaler = model_data['scaler']
            self.model_type = model_data['model_type']
            self.model_version = model_data['version']
            self.last_training_date = model_data['training_date']
            
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise
    
    def _train_initial_model(self):
        """Train initial model with synthetic data"""
        try:
            # Generate synthetic training data
            X_train, y_train = self._generate_synthetic_data()
            
            # Train model
            self._train_model(X_train, y_train)
            
            # Save model
            self._save_model()
            
        except Exception as e:
            logger.error(f"Error training initial model: {e}")
            raise
    
    def _generate_synthetic_data(self, n_samples: int = 1000) -> tuple:
        """Generate synthetic training data for initial model"""
        
        np.random.seed(42)
        
        # Generate features
        data = {
            'avg_temperature': np.random.normal(22, 5, n_samples),  # 17-27°C
            'avg_humidity': np.random.normal(70, 15, n_samples),    # 40-100%
            'avg_soil_ph': np.random.normal(6.5, 0.5, n_samples),   # 5.5-7.5
            'temperature_range': np.random.exponential(3, n_samples), # 0-15°C
            'humidity_range': np.random.exponential(10, n_samples),   # 0-30%
            'anomaly_rate': np.random.beta(2, 8, n_samples),         # 0-1
            'data_quality_score': np.random.beta(8, 2, n_samples)    # 0-1
        }
        
        X = pd.DataFrame(data)
        
        # Generate labels based on disease risk rules
        y = self._generate_disease_labels(X)
        
        return X, y
    
    def _generate_disease_labels(self, X: pd.DataFrame) -> np.ndarray:
        """Generate disease labels based on agricultural knowledge"""
        
        # Disease risk scoring
        risk_scores = np.zeros(len(X))
        
        # Temperature risk (20-25°C optimal for many diseases)
        temp_risk = np.where(
            (X['avg_temperature'] >= 20) & (X['avg_temperature'] <= 25),
            2,  # High risk
            np.where(
                (X['avg_temperature'] >= 18) & (X['avg_temperature'] <= 27),
                1,  # Medium risk
                0   # Low risk
            )
        )
        risk_scores += temp_risk
        
        # Humidity risk (high humidity favors disease)
        humidity_risk = np.where(
            X['avg_humidity'] > 85,
            3,  # Very high risk
            np.where(
                X['avg_humidity'] > 75,
                2,  # High risk
                np.where(
                    X['avg_humidity'] > 65,
                    1,  # Medium risk
                    0   # Low risk
                )
            )
        )
        risk_scores += humidity_risk
        
        # Soil pH risk (neutral pH is generally good)
        ph_risk = np.where(
            (X['avg_soil_ph'] < 6.0) | (X['avg_soil_ph'] > 7.0),
            1,  # Medium risk
            0   # Low risk
        )
        risk_scores += ph_risk
        
        # Anomaly rate risk
        anomaly_risk = np.where(
            X['anomaly_rate'] > 0.3,
            2,  # High risk
            np.where(
                X['anomaly_rate'] > 0.1,
                1,  # Medium risk
                0   # Low risk
            )
        )
        risk_scores += anomaly_risk
        
        # Convert to binary labels (disease present/absent)
        # Threshold: risk score >= 4 indicates disease risk
        y = (risk_scores >= 4).astype(int)
        
        # Add some noise to make it more realistic
        noise = np.random.random(len(y)) < 0.1  # 10% noise
        y[noise] = 1 - y[noise]
        
        return y
    
    def _train_model(self, X: pd.DataFrame, y: np.ndarray):
        """Train the ML model"""
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Choose and train model
        if self.model_type == "RandomForest":
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                class_weight='balanced'
            )
        elif self.model_type == "XGBoost":
            self.model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42,
                scale_pos_weight=1
            )
        elif self.model_type == "GradientBoosting":
            self.model = GradientBoostingClassifier(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            )
        else:
            self.model = LogisticRegression(random_state=42, class_weight='balanced')
        
        # Train model
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate model
        y_pred = self.model.predict(X_test_scaled)
        y_pred_proba = self.model.predict_proba(X_test_scaled)[:, 1]
        
        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        auc = roc_auc_score(y_test, y_pred_proba)
        
        logger.info(f"Model training completed:")
        logger.info(f"  Accuracy: {accuracy:.3f}")
        logger.info(f"  Precision: {precision:.3f}")
        logger.info(f"  Recall: {recall:.3f}")
        logger.info(f"  F1-Score: {f1:.3f}")
        logger.info(f"  AUC: {auc:.3f}")
        
        # Update training date
        self.last_training_date = datetime.now().isoformat()
    
    def _save_model(self):
        """Save trained model to file"""
        try:
            model_dir = os.path.join(os.getcwd(), 'models')
            os.makedirs(model_dir, exist_ok=True)
            
            model_path = os.path.join(model_dir, 'disease_predictor.joblib')
            
            model_data = {
                'model': self.model,
                'scaler': self.scaler,
                'model_type': self.model_type,
                'version': self.model_version,
                'training_date': self.last_training_date,
                'feature_columns': self.feature_columns,
                'risk_thresholds': self.risk_thresholds
            }
            
            joblib.dump(model_data, model_path)
            logger.info(f"Model saved to {model_path}")
            
        except Exception as e:
            logger.error(f"Error saving model: {e}")
    
    def _create_fallback_model(self):
        """Create a simple rule-based model as fallback"""
        self.model_type = "RuleBased"
        self.model_version = "v1.0.0-fallback"
        self.last_training_date = datetime.now().isoformat()
        logger.info("Created fallback rule-based model")
    
    def predict(self, field_features: pd.DataFrame, field_id: str) -> Dict[str, Any]:
        """Make disease prediction for a field"""
        
        if not self.is_initialized:
            raise RuntimeError("Model not initialized")
        
        try:
            # Prepare features
            features = self._prepare_features(field_features)
            
            if self.model_type == "RuleBased":
                # Use rule-based prediction
                prediction = self._rule_based_prediction(features)
            else:
                # Use ML model prediction
                prediction = self._ml_model_prediction(features)
            
            # Add metadata
            prediction.update({
                'field_id': field_id,
                'prediction_timestamp': datetime.now().isoformat(),
                'model_version': self.model_version,
                'model_type': self.model_type,
                'features_used': self.feature_columns,
                'processing_time_ms': 0  # Will be set by caller
            })
            
            return prediction
            
        except Exception as e:
            logger.error(f"Error making prediction for field {field_id}: {e}")
            # Return fallback prediction
            return self._fallback_prediction(field_id)
    
    def _prepare_features(self, field_features: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for prediction"""
        
        # Select and order features
        features = field_features[self.feature_columns].copy()
        
        # Handle missing values
        features = features.fillna(features.mean())
        
        # Ensure all features are numeric
        for col in features.columns:
            features[col] = pd.to_numeric(features[col], errors='coerce')
        
        # Fill any remaining NaN with 0
        features = features.fillna(0)
        
        return features
    
    def _ml_model_prediction(self, features: pd.DataFrame) -> Dict[str, Any]:
        """Make prediction using ML model"""
        
        # Scale features
        features_scaled = self.scaler.transform(features)
        
        # Get prediction probability
        disease_probability = self.model.predict_proba(features_scaled)[0, 1]
        
        # Determine risk level
        risk_level = self._probability_to_risk_level(disease_probability)
        
        # Calculate confidence score
        confidence_score = self._calculate_confidence(features_scaled[0])
        
        return {
            'disease_probability': float(disease_probability),
            'disease_type': 'Late_Blight',  # Default disease type
            'confidence_score': float(confidence_score),
            'risk_level': risk_level,
            'expected_onset': self._predict_onset_time(risk_level),
            'triggering_factors': self._identify_triggering_factors(features.iloc[0])
        }
    
    def _rule_based_prediction(self, features: pd.DataFrame) -> Dict[str, Any]:
        """Make prediction using rule-based approach"""
        
        # Calculate risk score based on rules
        risk_score = 0
        feature_values = features.iloc[0]
        
        # Temperature rules
        if 20 <= feature_values['avg_temperature'] <= 25:
            risk_score += 2
        elif 18 <= feature_values['avg_temperature'] <= 27:
            risk_score += 1
        
        # Humidity rules
        if feature_values['avg_humidity'] > 85:
            risk_score += 3
        elif feature_values['avg_humidity'] > 75:
            risk_score += 2
        elif feature_values['avg_humidity'] > 65:
            risk_score += 1
        
        # Soil pH rules
        if not (6.0 <= feature_values['avg_soil_ph'] <= 7.0):
            risk_score += 1
        
        # Anomaly rules
        if feature_values['anomaly_rate'] > 0.3:
            risk_score += 2
        elif feature_values['anomaly_rate'] > 0.1:
            risk_score += 1
        
        # Convert to probability (0-1 scale)
        max_possible_score = 8
        disease_probability = min(risk_score / max_possible_score, 1.0)
        
        # Determine risk level
        risk_level = self._probability_to_risk_level(disease_probability)
        
        return {
            'disease_probability': float(disease_probability),
            'disease_type': 'Late_Blight',
            'confidence_score': 0.7,  # Fixed confidence for rule-based
            'risk_level': risk_level,
            'expected_onset': self._predict_onset_time(risk_level),
            'triggering_factors': self._identify_triggering_factors(feature_values)
        }
    
    def _probability_to_risk_level(self, probability: float) -> str:
        """Convert probability to risk level"""
        if probability >= self.risk_thresholds['CRITICAL']:
            return 'CRITICAL'
        elif probability >= self.risk_thresholds['HIGH']:
            return 'HIGH'
        elif probability >= self.risk_thresholds['MEDIUM']:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _calculate_confidence(self, features_scaled: np.ndarray) -> float:
        """Calculate confidence score for prediction"""
        # Simple confidence based on feature values
        # In a real implementation, this could use model uncertainty
        confidence = 0.8  # Base confidence
        
        # Adjust based on feature quality
        if hasattr(self.model, 'feature_importances_'):
            # Use feature importance for confidence
            importance_sum = np.sum(self.model.feature_importances_)
            if importance_sum > 0:
                confidence = min(0.95, confidence + 0.1)
        
        return confidence
    
    def _predict_onset_time(self, risk_level: str) -> str:
        """Predict disease onset time based on risk level"""
        onset_times = {
            'CRITICAL': '1-2 days',
            'HIGH': '3-5 days',
            'MEDIUM': '1-2 weeks',
            'LOW': '2-4 weeks'
        }
        return onset_times.get(risk_level, 'Unknown')
    
    def _identify_triggering_factors(self, features: pd.Series) -> List[str]:
        """Identify factors contributing to disease risk"""
        factors = []
        
        if 20 <= features['avg_temperature'] <= 25:
            factors.append("Optimal temperature for disease development")
        
        if features['avg_humidity'] > 85:
            factors.append("High humidity favorable for disease")
        elif features['avg_humidity'] > 75:
            factors.append("Moderate humidity supporting disease")
        
        if not (6.0 <= features['avg_soil_ph'] <= 7.0):
            factors.append("Suboptimal soil pH")
        
        if features['anomaly_rate'] > 0.3:
            factors.append("High sensor anomaly rate")
        
        if len(factors) == 0:
            factors.append("No significant risk factors identified")
        
        return factors
    
    def _fallback_prediction(self, field_id: str) -> Dict[str, Any]:
        """Return fallback prediction when model fails"""
        return {
            'field_id': field_id,
            'prediction_timestamp': datetime.now().isoformat(),
            'disease_probability': 0.5,
            'disease_type': 'Unknown',
            'confidence_score': 0.3,
            'risk_level': 'MEDIUM',
            'expected_onset': 'Unknown',
            'triggering_factors': ['Model prediction failed'],
            'model_version': 'fallback',
            'model_type': 'fallback',
            'features_used': [],
            'processing_time_ms': 0
        }
    
    def retrain_model(self, new_data: pd.DataFrame) -> Dict[str, Any]:
        """Retrain model with new data"""
        try:
            logger.info("Starting model retraining...")
            
            # Prepare new training data
            X_new = new_data[self.feature_columns]
            y_new = new_data['disease_present']  # Assuming this column exists
            
            # Combine with existing model if available
            if self.model is not None:
                # In a real implementation, you'd combine with existing data
                X_combined = X_new
                y_combined = y_new
            else:
                X_combined = X_new
                y_combined = y_new
            
            # Retrain model
            self._train_model(X_combined, y_combined)
            
            # Update version
            self.model_version = f"v{self.model_version.split('.')[0]}.{int(self.model_version.split('.')[1]) + 1}.0"
            
            # Save updated model
            self._save_model()
            
            logger.info(f"Model retraining completed. New version: {self.model_version}")
            
            return {
                'status': 'success',
                'new_version': self.model_version,
                'training_date': self.last_training_date
            }
            
        except Exception as e:
            logger.error(f"Error retraining model: {e}")
            return {'status': 'error', 'message': str(e)} 