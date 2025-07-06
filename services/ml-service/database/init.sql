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
    field_id VARCHAR(50) NOT NULL,
    alert_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    alert_type VARCHAR(50) NOT NULL CHECK (alert_type IN ('DISEASE_DETECTED', 'HIGH_RISK', 'DATA_QUALITY', 'SENSOR_ANOMALY', 'WEATHER_ALERT')),
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    message TEXT NOT NULL,
    details JSONB,
    
    -- Related prediction (if applicable)
    prediction_id INTEGER REFERENCES ml_predictions(id),
    
    -- Alert status
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ACKNOWLEDGED', 'RESOLVED', 'IGNORED')),
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- USER PREFERENCES TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS user_preferences (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) UNIQUE NOT NULL,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    
    -- Alert preferences
    email_alerts BOOLEAN DEFAULT true,
    sms_alerts BOOLEAN DEFAULT false,
    alert_frequency VARCHAR(20) DEFAULT 'IMMEDIATE' CHECK (alert_frequency IN ('IMMEDIATE', 'HOURLY', 'DAILY', 'WEEKLY')),
    min_severity VARCHAR(20) DEFAULT 'MEDIUM' CHECK (min_severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    
    -- Dashboard preferences
    dashboard_layout JSONB,
    favorite_fields TEXT[],
    default_time_range VARCHAR(20) DEFAULT '24H' CHECK (default_time_range IN ('1H', '6H', '24H', '7D', '30D')),
    
    -- ML preferences
    ml_model_preference VARCHAR(50) DEFAULT 'AUTO',
    confidence_threshold DECIMAL(5,4) DEFAULT 0.7,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- MODEL METADATA TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS model_metadata (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50) NOT NULL,
    
    -- Model performance metrics
    accuracy DECIMAL(5,4),
    precision DECIMAL(5,4),
    recall DECIMAL(5,4),
    f1_score DECIMAL(5,4),
    auc_score DECIMAL(5,4),
    
    -- Training metadata
    training_date TIMESTAMP WITH TIME ZONE,
    training_duration_minutes INTEGER,
    training_samples INTEGER,
    validation_samples INTEGER,
    
    -- Model file information
    model_file_path VARCHAR(500),
    model_file_size_bytes BIGINT,
    model_hash VARCHAR(64),
    
    -- Feature information
    features_used TEXT[],
    feature_importance JSONB,
    
    -- Deployment status
    status VARCHAR(20) DEFAULT 'TRAINED' CHECK (status IN ('TRAINING', 'TRAINED', 'DEPLOYED', 'DEPRECATED', 'FAILED')),
    deployed_at TIMESTAMP WITH TIME ZONE,
    deprecated_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DATA SYNC LOG TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS data_sync_log (
    id SERIAL PRIMARY KEY,
    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    sync_type VARCHAR(50) NOT NULL CHECK (sync_type IN ('MINIO_TO_POSTGRES', 'POSTGRES_TO_MINIO', 'BATCH_SYNC', 'REAL_TIME_SYNC')),
    source_table VARCHAR(100),
    target_location VARCHAR(100),
    
    -- Sync statistics
    records_processed INTEGER,
    records_synced INTEGER,
    records_failed INTEGER,
    sync_duration_ms INTEGER,
    
    -- Error information
    error_message TEXT,
    error_details JSONB,
    
    -- Status
    status VARCHAR(20) NOT NULL CHECK (status IN ('SUCCESS', 'PARTIAL', 'FAILED')),
    
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
CREATE INDEX IF NOT EXISTS idx_alerts_field_id ON alerts(field_id);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(alert_timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_prediction_id ON alerts(prediction_id);

-- User preferences indexes
CREATE INDEX IF NOT EXISTS idx_user_preferences_user_id ON user_preferences(user_id);
CREATE INDEX IF NOT EXISTS idx_user_preferences_email ON user_preferences(email);

-- Model metadata indexes
CREATE INDEX IF NOT EXISTS idx_model_metadata_name_version ON model_metadata(model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_model_metadata_status ON model_metadata(status);
CREATE INDEX IF NOT EXISTS idx_model_metadata_training_date ON model_metadata(training_date);

-- Data sync log indexes
CREATE INDEX IF NOT EXISTS idx_data_sync_log_timestamp ON data_sync_log(sync_timestamp);
CREATE INDEX IF NOT EXISTS idx_data_sync_log_type ON data_sync_log(sync_type);
CREATE INDEX IF NOT EXISTS idx_data_sync_log_status ON data_sync_log(status);

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
    a.field_id,
    a.alert_timestamp,
    a.alert_type,
    a.severity,
    a.message,
    a.status,
    p.disease_probability,
    p.risk_level
FROM alerts a
LEFT JOIN ml_predictions p ON a.prediction_id = p.id
WHERE a.status = 'ACTIVE'
ORDER BY a.alert_timestamp DESC;

-- Model performance summary view
CREATE OR REPLACE VIEW model_performance_summary AS
SELECT 
    model_name,
    model_version,
    model_type,
    accuracy,
    precision,
    recall,
    f1_score,
    status,
    training_date,
    deployed_at
FROM model_metadata
WHERE status IN ('DEPLOYED', 'TRAINED')
ORDER BY training_date DESC;

-- =====================================================
-- SAMPLE DATA INSERTION
-- =====================================================

-- Insert sample user preferences
INSERT INTO user_preferences (user_id, username, email, alert_frequency, min_severity) 
VALUES 
    ('admin', 'System Administrator', 'admin@cropdisease.com', 'IMMEDIATE', 'MEDIUM'),
    ('farmer1', 'John Farmer', 'john@farm.com', 'HOURLY', 'HIGH'),
    ('analyst1', 'Data Analyst', 'analyst@cropdisease.com', 'DAILY', 'LOW')
ON CONFLICT (user_id) DO NOTHING;

-- Insert sample model metadata
INSERT INTO model_metadata (model_name, model_version, model_type, accuracy, precision, recall, f1_score, status, training_date, features_used) 
VALUES 
    ('crop_disease_classifier', 'v1.0.0', 'RandomForest', 0.85, 0.83, 0.87, 0.85, 'DEPLOYED', CURRENT_TIMESTAMP - INTERVAL '7 days', ARRAY['avg_temperature', 'avg_humidity', 'avg_soil_ph', 'anomaly_rate']),
    ('crop_disease_classifier', 'v1.1.0', 'XGBoost', 0.88, 0.86, 0.89, 0.87, 'TRAINED', CURRENT_TIMESTAMP - INTERVAL '1 day', ARRAY['avg_temperature', 'avg_humidity', 'avg_soil_ph', 'anomaly_rate', 'weather_avg_temperature'])
ON CONFLICT DO NOTHING;

-- =====================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================

COMMENT ON TABLE ml_predictions IS 'Stores real-time ML predictions for crop disease detection';
COMMENT ON TABLE alerts IS 'Stores system alerts and notifications for users';
COMMENT ON TABLE user_preferences IS 'Stores user preferences and alert settings';
COMMENT ON TABLE model_metadata IS 'Stores ML model metadata and performance metrics';
COMMENT ON TABLE data_sync_log IS 'Logs data synchronization activities between MinIO and PostgreSQL';

COMMENT ON COLUMN ml_predictions.disease_probability IS 'Probability of disease presence (0-1)';
COMMENT ON COLUMN ml_predictions.risk_level IS 'Risk assessment level based on prediction and features';
COMMENT ON COLUMN alerts.severity IS 'Alert severity level for prioritization';
COMMENT ON COLUMN user_preferences.alert_frequency IS 'How often user wants to receive alerts';
COMMENT ON COLUMN model_metadata.status IS 'Current deployment status of the model'; 