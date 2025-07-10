#!/usr/bin/env python3
"""
Analytics Page - Advanced analytics and historical trends
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer
import threading
import time
import os
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="📊 Analytics - Crop Disease Dashboard",
    page_icon="📊",
    layout="wide"
)

# API Configuration
API_BASE_URL = "http://crop-disease-service:8000/api/v1"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

st.title("📊 Analytics and Historical Trends")
st.markdown("---")

# Sidebar controls
with st.sidebar:
    st.header("🔧 Analytics Settings")
    
    # Field filters
    st.subheader("🌾 Field Selection")
    show_all_fields = st.checkbox("All fields", value=True)
    
    if not show_all_fields:
        selected_fields = st.multiselect(
            "Select specific fields",
            ["field_01", "field_02", "field_03", "field_04", "field_05"],
            default=["field_01", "field_02"]
        )
    
    # Weather location selection
    st.subheader("🌤️ Weather Location Selection")
    show_all_locations = st.checkbox("All locations", value=True)
    
    if not show_all_locations:
        selected_locations = st.multiselect(
            "Select specific locations",
            ["Verona", "Milan", "Rome", "Naples", "Palermo"],
            default=["Verona"]
        )
    
    # Analysis type
    st.subheader("📈 Analysis Type")
    analysis_type = st.selectbox(
        "Select analysis",
        ["Real-time Data"]
    )

# Cache for Kafka data
@st.cache_data(ttl=60)  # Cache for 1 minute
def get_latest_kafka_data():
    """Get the latest data from Kafka topics"""
    try:
        # IoT data consumer
        iot_consumer = KafkaConsumer(
            'iot_valid_data',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='dashboard_iot_consumer',
            consumer_timeout_ms=5000  # 5 seconds timeout
        )
        if not iot_consumer.bootstrap_connected():
            st.error("[DEBUG] Kafka (IoT) connection FAILED!")
            return [], []
        # Weather data consumer
        weather_consumer = KafkaConsumer(
            'weather_valid_data',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='dashboard_weather_consumer',
            consumer_timeout_ms=5000  # 5 seconds timeout
        )
        if not weather_consumer.bootstrap_connected():
            st.error("[DEBUG] Kafka (Weather) connection FAILED!")
            return [], []
        # Collect IoT data
        iot_data = []
        try:
            for message in iot_consumer:
                iot_data.append(message.value)
                if len(iot_data) >= 500:  # Limit to 500 messages
                    break
        except Exception as e:
            st.error(f"Error reading IoT data: {e}")
        finally:
            iot_consumer.close()
        # Collect weather data
        weather_data = []
        try:
            for message in weather_consumer:
                weather_data.append(message.value)
                if len(weather_data) >= 500:  # Limit to 500 messages
                    break
        except Exception as e:
            st.error(f"Error reading Weather data: {e}")
        finally:
            weather_consumer.close()
        return iot_data, weather_data
    except Exception as e:
        st.error(f"Error retrieving Kafka data: {e}")
        import traceback
        st.error(traceback.format_exc())
        return [], []

# Load data
# (Other data loading functions removed as only real-time data is now used)

# Auto-refresh every 60 seconds
st_autorefresh(interval=60 * 1000, key="realtime_data_autorefresh")

# Main layout
if analysis_type == "Real-time Data":
    st.header("🌐 Real-time Data")
    
    # Get real data from Kafka
    iot_data, weather_data = get_latest_kafka_data()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📡 IoT Sensor Data")
        
        if iot_data:
            # Convert to DataFrame
            iot_df = pd.DataFrame(iot_data)
            
            # Filter by selected fields
            if not show_all_fields and 'selected_fields' in locals():
                iot_df = iot_df[iot_df['field_id'].isin(selected_fields)]
            
            # Sort by timestamp descending and get the most recent for each field
            if 'timestamp' in iot_df.columns:
                iot_df['timestamp'] = pd.to_datetime(iot_df['timestamp'])
                iot_df = iot_df.sort_values('timestamp', ascending=False)
                latest_sensors = iot_df.groupby('field_id', as_index=False).first()
                # Also show the max timestamp found for each field
                max_timestamps = iot_df.groupby('field_id')['timestamp'].max().reset_index()
            else:
                latest_sensors = iot_df.groupby('field_id').last().reset_index()
            
            if not latest_sensors.empty:
                # Temperature chart
                fig_temp = px.bar(
                    latest_sensors,
                    x='field_id',
                    y='temperature',
                    title="Current Temperature per Field (°C)",
                    color='temperature',
                    color_continuous_scale='RdYlBu_r'
                )
                fig_temp.update_layout(height=300)
                st.plotly_chart(fig_temp, use_container_width=True)
                
                # Humidity chart
                fig_humidity = px.bar(
                    latest_sensors,
                    x='field_id',
                    y='humidity',
                    title="Current Humidity per Field (%)",
                    color='humidity',
                    color_continuous_scale='Blues'
                )
                fig_humidity.update_layout(height=300)
                st.plotly_chart(fig_humidity, use_container_width=True)
                
                # Soil pH chart
                fig_ph = px.bar(
                    latest_sensors,
                    x='field_id',
                    y='soil_ph',
                    title="Current Soil pH per Field",
                    color='soil_ph',
                    color_continuous_scale='Greens'
                )
                fig_ph.update_layout(height=300)
                st.plotly_chart(fig_ph, use_container_width=True)
                
                # Current values table
                st.subheader("📊 Current Sensor Values")
                display_sensors = latest_sensors[['field_id', 'temperature', 'humidity', 'soil_ph', 'timestamp']].copy()
                display_sensors.columns = ['Field', 'Temperature (°C)', 'Humidity (%)', 'Soil pH', 'Timestamp']
                st.dataframe(display_sensors, use_container_width=True)
            else:
                st.info("No sensor data available for the selected fields")
        else:
            st.warning("⚠️ Unable to retrieve sensor data from Kafka")
            st.info("Check that the Kafka service is running and that producers are sending data")
    
    with col2:
        st.subheader("🌤️ Weather Data")
        
        if weather_data:
            # Convert to DataFrame
            weather_df = pd.DataFrame(weather_data)
            
            # Filter by selected locations
            if not show_all_locations and 'selected_locations' in locals():
                weather_df = weather_df[weather_df['location'].isin(selected_locations)]
            
            # Show the most recent data for each location
            latest_weather = weather_df.groupby('location').last().reset_index()
            
            if not latest_weather.empty:
                # Weather temperature chart
                fig_weather_temp = px.bar(
                    latest_weather,
                    x='location',
                    y='temp_c',
                    title="Current Weather Temperature (°C)",
                    color='temp_c',
                    color_continuous_scale='RdYlBu_r'
                )
                fig_weather_temp.update_layout(height=300)
                st.plotly_chart(fig_weather_temp, use_container_width=True)
                
                # Weather humidity chart
                fig_weather_humidity = px.bar(
                    latest_weather,
                    x='location',
                    y='humidity',
                    title="Current Weather Humidity (%)",
                    color='humidity',
                    color_continuous_scale='Blues'
                )
                fig_weather_humidity.update_layout(height=300)
                st.plotly_chart(fig_weather_humidity, use_container_width=True)
                
                # Wind speed chart
                fig_wind = px.bar(
                    latest_weather,
                    x='location',
                    y='wind_kph',
                    title="Current Wind Speed (km/h)",
                    color='wind_kph',
                    color_continuous_scale='Greys'
                )
                fig_wind.update_layout(height=300)
                st.plotly_chart(fig_wind, use_container_width=True)
                
                # Current weather values table
                st.subheader("📊 Current Weather Values")
                display_weather = latest_weather[['location', 'temp_c', 'humidity', 'wind_kph', 'timestamp']].copy()
                display_weather.columns = ['Location', 'Temperature (°C)', 'Humidity (%)', 'Wind (km/h)', 'Timestamp']
                st.dataframe(display_weather, use_container_width=True)
            else:
                st.info("No weather data available for the selected locations")
        else:
            st.warning("⚠️ Unable to retrieve weather data from Kafka")
            st.info("Check that the Kafka service is running and that producers are sending data")

# Footer
st.markdown("---")
st.caption(f"📊 Analytics generated on {datetime.now().strftime('%d/%m/%Y at %H:%M:%S')}")
st.caption("💡 **Tip**: Use the filters in the sidebar to customize the analysis") 