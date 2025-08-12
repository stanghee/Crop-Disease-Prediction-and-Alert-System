# Dashboard Service

## Overview

The Dashboard Service provides a comprehensive web-based interface for monitoring the Crop Disease Prediction and Alert System. Built with Streamlit, it offers real-time visualization of sensor data, ML insights, threshold alerts, satellite imagery, and analytical reports for agricultural monitoring and decision support.

## Key Features

- **Real-time Data Visualization**: Live monitoring of sensor and weather data with field-specific filtering
- **ML Insights Dashboard**: Detailed analysis of machine learning predictions 
- **Threshold Alerts Management**: Monitoring and management of environmental alerts 
- **Satellite Map Integration**: Geospatial visualization with satellite imagery and field location mapping
- **Data Analytics**: Comprehensive reporting with temporal analysis and statistical insights

## Architecture

```
dashboard/
├── app.py                    # Main Streamlit application
├── pages/
│   ├── 01_Real-time_data.py # Real-time sensor and weather monitoring
│   ├── 02_ML_Insights.py    # Machine learning predictions dashboard
│   ├── 03_Threshold_Alerts.py # Alert management and monitoring
│   ├── 04_Satellite_Map.py  # Geospatial visualization
│   └── 05_Data_Insights.py  # Analytical reporting and statistics
├── Logo/
│   └── Logo Crop.png        # Application logo
├── Dockerfile               # Container configuration
└── requirements.txt         # Python dependencies
```

## Dashboard Pages

### 1. Real-time Data
- Live sensor data visualization with field-specific filtering
- Weather condition monitoring with location-based data
- Environmental parameter tracking (temperature, humidity, soil pH)
- Data quality indicators and validation status
- Real-time updates with 10-second cache refresh

### 2. ML Insights
- Anomaly detection results with anomaly scores
- Disease risk predictions by field and time period
- Feature importance analysis for understanding predictions
- Severity classification (LOW, MEDIUM, HIGH, CRITICAL)
- Interactive filtering by field and time range

### 3. Threshold Alerts
- Alert severity filtering (high, medium)
- Field-specific alert visualization
- Alert information including thresholds and timestamps
- Statistical analysis of alerts by type and severity
- Real-time alert monitoring and management

### 4. Satellite Map
- Field location mapping with sensor placement
- Environmental overlay showing field data
- Interactive field selection for detailed analysis
- Color-coded field status based on alerts

### 5. Data Insights
- Alert count analysis for ML predictions and threshold alerts
- Temporal analysis with time-series charts
- Descriptive statistics for ML predictions
- Average risk score distribution
- Comprehensive analytical reporting

## Data Sources

The dashboard integrates data from multiple sources:

- **Redis Cache**: Real-time sensor data, weather information, and ML predictions
- **PostgreSQL**: Historical ML predictions and alert data for analytics
- **MinIO**: Satellite imagery for geospatial visualization
- **Crop Disease Service API**: Additional alert and field information


### Caching Strategy
- **Real-time data**: 10-second cache for live updates
- **ML insights**: 10-second cache for predictions
- **Satellite images**: 60-second cache for imagery
- **Analytics data**: 300-second cache for reports

## Monitoring

- **Dashboard URL**: http://localhost:8501 - Main application interface
