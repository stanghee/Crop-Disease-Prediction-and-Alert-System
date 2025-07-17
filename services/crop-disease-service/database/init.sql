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
    location VARCHAR(100),
    prediction_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Anomaly detection results
    anomaly_score FLOAT NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    severity VARCHAR(20) CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    recommendations TEXT,
    
    -- Model metadata
    model_version VARCHAR(50),
    
    -- Features stored as JSONB for flexibility
    features JSONB,
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
CREATE INDEX IF NOT EXISTS idx_ml_predictions_anomaly ON ml_predictions(is_anomaly, prediction_timestamp);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_severity ON ml_predictions(severity);
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
    location,
    prediction_timestamp,
    anomaly_score,
    is_anomaly,
    severity,
    recommendations,
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


COMMENT ON COLUMN alerts.severity IS 'Alert severity level for prioritization'; 