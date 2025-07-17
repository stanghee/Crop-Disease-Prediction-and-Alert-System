-- =====================================================
-- Crop Disease Prediction ML Database Schema
-- =====================================================

-- Database is already created by Docker environment variables
-- We're already connected to the crop_disease_ml database

-- =====================================================
-- ML PREDICTIONS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS ml_predictions (
    id SERIAL PRIMARY KEY,
    field_id VARCHAR(50) NOT NULL,
    prediction_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    disease_probability DECIMAL(5,4) NOT NULL CHECK (disease_probability >= 0 AND disease_probability <= 1),
    disease_type VARCHAR(100),
    confidence_score DECIMAL(5,4) CHECK (confidence_score >= 0 AND confidence_score <= 1),
    risk_level VARCHAR(20) CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    
    -- Input features used for prediction
    avg_temperature DECIMAL(5,2),
    avg_humidity DECIMAL(5,2),
    avg_soil_ph DECIMAL(4,2),
    temperature_range DECIMAL(5,2),
    humidity_range DECIMAL(5,2),
    anomaly_rate DECIMAL(5,4),
    data_quality_score DECIMAL(5,4),
    
    -- Weather features
    weather_avg_temperature DECIMAL(5,2),
    weather_avg_humidity DECIMAL(5,2),
    weather_avg_wind_speed DECIMAL(5,2),
    weather_avg_uv_index DECIMAL(4,2),
    
    -- Model metadata
    model_version VARCHAR(50),
    model_type VARCHAR(50),
    features_used TEXT,
    
    -- Processing metadata
    processing_time_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ALERTS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    zone_id VARCHAR(50) NOT NULL,  -- Contains field_id for sensors, location for weather
    alert_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    alert_type VARCHAR(50) NOT NULL CHECK (alert_type IN ('DISEASE_DETECTED', 'HIGH_RISK', 'DATA_QUALITY', 'SENSOR_ANOMALY', 'WEATHER_ALERT')),
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    message TEXT NOT NULL,
    
    -- Alert status
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ACKNOWLEDGED', 'RESOLVED', 'IGNORED')),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);







-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- ML Predictions indexes
CREATE INDEX IF NOT EXISTS idx_ml_predictions_field_id ON ml_predictions(field_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_timestamp ON ml_predictions(prediction_timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_risk_level ON ml_predictions(risk_level);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_model_version ON ml_predictions(model_version);

-- Alerts indexes
CREATE INDEX IF NOT EXISTS idx_alerts_zone_id ON alerts(zone_id);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(alert_timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);



-- =====================================================
-- VIEWS FOR COMMON QUERIES
-- =====================================================

-- Recent predictions view
CREATE OR REPLACE VIEW recent_predictions AS
SELECT 
    field_id,
    prediction_timestamp,
    disease_probability,
    disease_type,
    risk_level,
    confidence_score,
    model_version
FROM ml_predictions 
WHERE prediction_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY prediction_timestamp DESC;

-- Active alerts view
CREATE OR REPLACE VIEW active_alerts AS
SELECT 
    a.id,
    a.zone_id,
    a.alert_timestamp,
    a.alert_type,
    a.severity,
    a.message,
    a.status
FROM alerts a
WHERE a.status = 'ACTIVE'
ORDER BY a.alert_timestamp DESC;

-- =====================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================

COMMENT ON TABLE ml_predictions IS 'Stores real-time ML predictions for crop disease detection';
COMMENT ON TABLE alerts IS 'Stores system alerts and notifications for users';

COMMENT ON COLUMN ml_predictions.disease_probability IS 'Probability of disease presence (0-1)';
COMMENT ON COLUMN ml_predictions.risk_level IS 'Risk assessment level based on prediction and features';
COMMENT ON COLUMN alerts.severity IS 'Alert severity level for prioritization'; 