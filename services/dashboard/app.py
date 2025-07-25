#!/usr/bin/env python3
"""
Crop Disease Dashboard - Streamlit Application
Main dashboard for monitoring the agricultural system
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import time
import json

# Page configuration
st.set_page_config(
    page_title="🌾 Crop Disease Dashboard",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API configuration
API_BASE_URL = "http://crop-disease-service:8000/api/v1"

class CropDiseaseAPI:
    """Client for the crop disease system APIs"""
    
    def __init__(self, base_url):
        self.base_url = base_url
    
    def get_system_status(self):
        """Get overall system status"""
        try:
            response = requests.get(f"{self.base_url}/system/status", timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            st.error(f"API connection error: {e}")
            return None
    
    def get_sensor_alerts(self):
        """Get sensor alerts"""
        try:
            response = requests.get(f"{self.base_url}/alerts/sensors", timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            return None
    
    def get_weather_alerts(self):
        """Get weather alerts"""
        try:
            response = requests.get(f"{self.base_url}/alerts/weather", timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            return None
    
    def get_predictions(self):
        """Get recent predictions"""
        try:
            response = requests.get(f"{self.base_url}/predictions", timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            return None
    
    def get_models_status(self):
        """Get ML models status"""
        try:
            response = requests.get(f"{self.base_url}/models", timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            return None

# Initialize API client
api = CropDiseaseAPI(API_BASE_URL)

def main():
    """Main dashboard"""
    
    # Header
    st.title("🌾 Crop Disease Monitoring System")
    st.markdown("---")
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Controls")
        
        # Auto-refresh
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=True)
        if auto_refresh:
            time.sleep(30)
            st.rerun()
        
        # Manual refresh
        if st.button("🔄 Refresh Data"):
            st.rerun()
        
        # Filters
        st.subheader("🔍 Filters")
        show_healthy = st.checkbox("Show healthy fields", value=True)
        show_alerts = st.checkbox("Show alerts", value=True)
        
        # System info
        st.subheader("ℹ️ System Info")
        st.info(f"Last update: {datetime.now().strftime('%H:%M:%S')}")
    
    # Load data
    system_status = api.get_system_status()
    sensor_alerts = api.get_sensor_alerts()
    weather_alerts = api.get_weather_alerts()
    predictions = api.get_predictions()
    models_status = api.get_models_status()
    
    # KPI Overview
    st.header("📊 System Overview")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if system_status and 'data' in system_status:
            healthy_fields = system_status['data'].get('healthy_fields', 0)
            total_fields = system_status['data'].get('total_fields', 0)
        else:
            healthy_fields, total_fields = 0, 0
        
        st.metric(
            label="🌱 Active Fields",
            value=f"{total_fields}",
            delta=f"{healthy_fields} healthy"
        )
    
    with col2:
        if sensor_alerts and 'data' in sensor_alerts:
            sensor_count = sensor_alerts['data'].get('total_alerts', 0)
        else:
            sensor_count = 0
        
        st.metric(
            label="⚠️ Sensor Alerts",
            value=sensor_count,
            delta="Last 30min" if sensor_count > 0 else "No alerts"
        )
    
    with col3:
        if weather_alerts and 'data' in weather_alerts:
            weather_count = weather_alerts['data'].get('total_alerts', 0)
        else:
            weather_count = 0
        
        st.metric(
            label="🌦️ Weather Alerts",
            value=weather_count,
            delta="Current"
        )
    
    with col4:
        if predictions and 'data' in predictions:
            pred_count = len(predictions['data'].get('predictions', []))
        else:
            pred_count = 0
        
        st.metric(
            label="🔮 ML Predictions",
            value=pred_count,
            delta="Last 24h"
        )
    
    with col5:
        if models_status and 'data' in models_status:
            models_health = "🟢 Operational" if models_status['data'].get('initialized', False) else "🔴 Error"
        else:
            models_health = "🔴 Not available"
        
        st.metric(
            label="🤖 ML Models",
            value=models_health,
            delta=""
        )
    
    st.markdown("---")
    
    # Main dashboard in two columns
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.header("📈 Real-time Monitoring")
        
        # Field status chart
        if system_status and 'data' in system_status:
            data = system_status['data']
            
            # Status distribution
            status_data = {
                'Status': ['Healthy', 'Medium Risk', 'High Risk', 'Data Issues'],
                'Fields': [
                    data.get('healthy_fields', 0),
                    data.get('medium_risk_fields', 0), 
                    data.get('high_risk_fields', 0),
                    data.get('data_issue_fields', 0)
                ],
                'Colors': ['#28a745', '#ffc107', '#fd7e14', '#dc3545']
            }
            
            fig_status = px.bar(
                x=status_data['Fields'],
                y=status_data['Status'],
                orientation='h',
                color=status_data['Status'],
                color_discrete_map={
                    'Healthy': '#28a745',
                    'Medium Risk': '#ffc107', 
                    'High Risk': '#fd7e14',
                    'Data Issues': '#dc3545'
                },
                title="Field Status Distribution"
            )
            fig_status.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig_status, use_container_width=True)
            
            # Environmental metrics
            st.subheader("🌡️ Average Environmental Conditions")
            
            col_temp, col_hum, col_ph = st.columns(3)
            
            with col_temp:
                avg_temp = data.get('avg_temperature', 0)
                st.metric(
                    label="Temperature",
                    value=f"{avg_temp:.1f}°C",
                    delta=f"{'🔥' if avg_temp > 30 else '❄️' if avg_temp < 10 else '✅'}"
                )
            
            with col_hum:
                avg_hum = data.get('avg_humidity', 0)
                st.metric(
                    label="Humidity",
                    value=f"{avg_hum:.1f}%",
                    delta=f"{'💧' if avg_hum > 80 else '🏜️' if avg_hum < 30 else '✅'}"
                )
            
            with col_ph:
                avg_ph = data.get('avg_soil_ph', 0)
                st.metric(
                    label="Soil pH",
                    value=f"{avg_ph:.1f}",
                    delta=f"{'⚗️' if avg_ph < 6 or avg_ph > 8 else '✅'}"
                )
    
    with col_right:
        st.header("🚨 Active Alerts")
        
        # Sensor alerts
        if sensor_alerts and 'data' in sensor_alerts and sensor_alerts['data'].get('sensor_alerts'):
            st.subheader("📡 Sensors")
            for alert in sensor_alerts['data']['sensor_alerts'][:5]:  # Show first 5
                severity = alert.get('severity', 'INFO')
                field_id = alert.get('field_id', 'N/A')
                message = alert.get('message', 'No message')
                
                if severity == 'HIGH':
                    st.error(f"🔴 **{field_id}**: {message}")
                elif severity == 'MEDIUM':
                    st.warning(f"🟡 **{field_id}**: {message}")
                else:
                    st.info(f"🔵 **{field_id}**: {message}")
        else:
            st.success("✅ No active sensor alerts")
        
        # Weather alerts
        if weather_alerts and 'data' in weather_alerts and weather_alerts['data'].get('weather_alerts'):
            st.subheader("🌦️ Weather")
            for alert in weather_alerts['data']['weather_alerts'][:3]:  # Show first 3
                severity = alert.get('severity', 'INFO')
                message = alert.get('message', 'No message')
                
                if severity == 'CRITICAL':
                    st.error(f"🔴 **Critical**: {message}")
                elif severity == 'HIGH':
                    st.warning(f"🟡 **High**: {message}")
                else:
                    st.info(f"🔵 **Info**: {message}")
        else:
            st.success("✅ No active weather alerts")
        
        # Recent predictions
        st.subheader("🔮 ML Predictions")
        if predictions and 'data' in predictions and predictions['data'].get('predictions'):
            for pred in predictions['data']['predictions'][:3]:  # Show first 3
                field_id = pred.get('field_id', 'N/A')
                disease_prob = pred.get('disease_probability', 0)
                risk_level = pred.get('risk_level', 'LOW')
                
                if risk_level == 'HIGH':
                    st.error(f"🔴 **{field_id}**: Risk {disease_prob:.1%}")
                elif risk_level == 'MEDIUM':
                    st.warning(f"🟡 **{field_id}**: Risk {disease_prob:.1%}")
                else:
                    st.success(f"🟢 **{field_id}**: Risk {disease_prob:.1%}")
        else:
            st.info("ℹ️ No recent predictions available")
    
    # Footer with system info
    st.markdown("---")
    
    col_info1, col_info2, col_info3 = st.columns(3)
    
    with col_info1:
        st.caption(f"🕐 **Last update**: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    with col_info2:
        if system_status:
            st.caption(f"📡 **API Status**: {'🟢 Online' if system_status.get('status') == 'success' else '🔴 Offline'}")
        else:
            st.caption("📡 **API Status**: 🔴 Not available")
    
    with col_info3:
        st.caption("💾 **Data Source**: Gold Zone + Real-time APIs")

if __name__ == "__main__":
    main() 

#TODO: clean this file 