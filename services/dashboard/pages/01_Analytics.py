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
import redis
import time
import os
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="Analytics - Crop Disease Dashboard",
    page_icon=None,
    layout="wide"
)

# API Configuration
API_BASE_URL = "http://crop-disease-service:8000/api/v1"
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

st.title("Real-time Data")
st.markdown("---")

# Sidebar controls
with st.sidebar:
    st.header("Analytics Settings")
    
    # Field filters
    st.subheader("Field Selection")
    show_all_fields = st.checkbox("All fields", value=True)
    
    if not show_all_fields:
        selected_fields = st.multiselect(
            "Select specific fields",
            ["field_01", "field_02", "field_03"],
            default=["field_01", "field_02"]
        )
    
    # Weather location selection
    st.subheader("Weather Location Selection")
    show_all_locations = st.checkbox("All locations", value=True)
    
    if not show_all_locations:
        selected_locations = st.multiselect(
            "Select specific locations",
            ["Verona", "Milan", "Rome", "Naples", "Palermo"],
            default=["Verona"]
        )
    
    # Analysis type
    st.subheader("Analysis Type")
    analysis_type = st.selectbox(
        "Select analysis",
        ["Real-time Data"]
    )

# Redis connection
@st.cache_resource
def get_redis_client():
    """Get Redis client connection"""
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=0,
            decode_responses=True,
            socket_timeout=5,
            retry_on_timeout=True
        )
        # Test connection
        redis_client.ping()
        return redis_client
    except Exception as e:
        st.error(f"❌ Failed to connect to Redis: {e}")
        return None

# Cache for Redis data with shorter TTL for real-time updates
@st.cache_data(ttl=10)  # Cache for 10 seconds only for real-time feel
def get_sensor_data_from_redis(_redis_client):
    """Get all sensor data from Redis cache"""
    try:
        if not _redis_client:
            return []
        
        # Get all sensor keys
        sensor_keys = _redis_client.keys("sensors:latest:*")
        
        if not sensor_keys:
            return []
        
        # Get all sensor data in batch
        sensor_data = []
        pipe = _redis_client.pipeline()
        
        for key in sensor_keys:
            pipe.get(key)
        
        results = pipe.execute()
        
        for key, data in zip(sensor_keys, results):
            if data:
                try:
                    sensor_info = json.loads(data)
                    # Extract field_id from key
                    field_id = key.split(":")[-1]
                    sensor_info["field_id"] = field_id
                    sensor_data.append(sensor_info)
                except json.JSONDecodeError as e:
                    st.warning(f"⚠️ Error parsing sensor data for {key}: {e}")
                    continue
        
        return sensor_data
        
    except Exception as e:
        st.error(f"❌ Error retrieving sensor data from Redis: {e}")
        return []

@st.cache_data(ttl=10)  # Cache for 10 seconds
def get_weather_data_from_redis(_redis_client):
    """Get all weather data from Redis cache"""
    try:
        if not _redis_client:
            return []
        
        # Get all weather keys
        weather_keys = _redis_client.keys("weather:latest:*")
        
        if not weather_keys:
            return []
        
        # Get all weather data in batch
        weather_data = []
        pipe = _redis_client.pipeline()
        
        for key in weather_keys:
            pipe.get(key)
        
        results = pipe.execute()
        
        for key, data in zip(weather_keys, results):
            if data:
                try:
                    weather_info = json.loads(data)
                    # Extract location from key  
                    location = key.split(":")[-1]
                    weather_info["location"] = location
                    weather_data.append(weather_info)
                except json.JSONDecodeError as e:
                    st.warning(f"⚠️ Error parsing weather data for {key}: {e}")
                    continue
        
        return weather_data
        
    except Exception as e:
        st.error(f"❌ Error retrieving weather data from Redis: {e}")
        return []

# System stats functionality removed due to serialization complexity

# Load data
# Get Redis client
redis_client = get_redis_client()

# Auto-refresh every 10 seconds for real-time feel
st_autorefresh(interval=10 * 1000, key="redis_realtime_data_autorefresh")

# Main layout
if analysis_type == "Real-time Data":
    st.header("Real-time Data (Redis Cache)")
    
    # Show Redis connection status
    col_status1, col_status2, col_status3 = st.columns(3)
    
    with col_status1:
        if redis_client:
            st.success("🔴 Redis Connected")
        else:
            st.error("🔴 Redis Disconnected")
    
    with col_status2:
        # Show cache performance
        if redis_client:
            try:
                info = redis_client.info()
                hit_ratio = 0
                if info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0) > 0:
                    hit_ratio = info.get('keyspace_hits', 0) / (info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0)) * 100
                st.metric("Cache Hit Ratio", f"{hit_ratio:.1f}%")
            except:
                st.metric("Cache Hit Ratio", "N/A")
    
    with col_status3:
        # Show total cached keys
        if redis_client:
            try:
                total_keys = redis_client.dbsize()
                st.metric("Cached Keys", total_keys)
            except:
                st.metric("Cached Keys", "N/A")
    
    st.markdown("---")
    
    # Get data from Redis
    sensor_data = get_sensor_data_from_redis(redis_client)
    weather_data = get_weather_data_from_redis(redis_client)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("IoT Sensor Data (Redis Cache)")
        
        if sensor_data:
            # Convert to DataFrame
            sensor_df = pd.DataFrame(sensor_data)
            
            # Filter by selected fields
            if not show_all_fields and 'selected_fields' in locals():
                sensor_df = sensor_df[sensor_df['field_id'].isin(selected_fields)]
            
            if not sensor_df.empty:
                # Show data freshness
                if 'cached_at' in sensor_df.columns:
                    try:
                        latest_cache_time = pd.to_datetime(sensor_df['cached_at']).max()
                        cache_age = (datetime.now() - latest_cache_time.tz_localize(None)).total_seconds()
                        st.info(f"📊 Data cached {cache_age:.0f} seconds ago")
                    except:
                        pass
                
                # Temperature chart
                fig_temp = px.bar(
                    sensor_df,
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
                    sensor_df,
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
                    sensor_df,
                    x='field_id',
                    y='soil_ph',
                    title="Current Soil pH per Field",
                    color='soil_ph',
                    color_continuous_scale='Greens'
                )
                fig_ph.update_layout(height=300)
                st.plotly_chart(fig_ph, use_container_width=True)
                
                # Current values table
                st.subheader("Current Sensor Values")
                display_sensors = sensor_df[['field_id', 'temperature', 'humidity', 'soil_ph', 'timestamp']].copy()
                display_sensors.columns = ['Field', 'Temperature (°C)', 'Humidity (%)', 'Soil pH', 'Timestamp']
                st.dataframe(display_sensors, use_container_width=True)
                
            else:
                st.info("No sensor data available for the selected fields")
        else:
            st.warning("❌ Unable to retrieve sensor data from Redis cache")
            st.info("Check that Redis cache service is running and processing Kafka data")
    
    with col2:
        st.subheader("Weather Data (Redis Cache)")
        
        if weather_data:
            # Convert to DataFrame
            weather_df = pd.DataFrame(weather_data)
            
            # Filter by selected locations
            if not show_all_locations and 'selected_locations' in locals():
                weather_df = weather_df[weather_df['location'].isin(selected_locations)]
            
            if not weather_df.empty:
                # Show data freshness
                if 'cached_at' in weather_df.columns:
                    try:
                        latest_cache_time = pd.to_datetime(weather_df['cached_at']).max()
                        cache_age = (datetime.now() - latest_cache_time.tz_localize(None)).total_seconds()
                        st.info(f"🌦️ Data cached {cache_age:.0f} seconds ago")
                    except:
                        pass
                
                # Weather temperature chart
                fig_weather_temp = px.bar(
                    weather_df,
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
                    weather_df,
                    x='location',
                    y='humidity',
                    title="Current Weather Humidity (%)",
                    color='humidity',
                    color_continuous_scale='Blues'
                )
                fig_weather_humidity.update_layout(height=300)
                st.plotly_chart(fig_weather_humidity, use_container_width=True)
                
                # Wind speed chart
                if 'wind_kph' in weather_df.columns:
                    fig_wind = px.bar(
                        weather_df,
                        x='location',
                        y='wind_kph',
                        title="Current Wind Speed (km/h)",
                        color='wind_kph',
                        color_continuous_scale='Greys'
                    )
                    fig_wind.update_layout(height=300)
                    st.plotly_chart(fig_wind, use_container_width=True)
                
                # Current weather values table
                st.subheader("Current Weather Values")
                display_columns = ['location', 'temp_c', 'humidity', 'timestamp']
                if 'wind_kph' in weather_df.columns:
                    display_columns.insert(3, 'wind_kph')
                
                display_weather = weather_df[display_columns].copy()
                column_names = ['Location', 'Temperature (°C)', 'Humidity (%)']
                if 'wind_kph' in weather_df.columns:
                    column_names.insert(3, 'Wind (km/h)')
                column_names.append('Timestamp')
                
                display_weather.columns = column_names
                st.dataframe(display_weather, use_container_width=True)
                
            else:
                st.info("No weather data available for the selected locations")
        else:
            st.warning("❌ Unable to retrieve weather data from Redis cache")
            st.info("Check that Redis cache service is running and processing Kafka data")

# System Statistics Section removed due to serialization complexity
# The core Redis caching functionality works perfectly without these advanced stats

# Footer
st.markdown("---")
st.caption(f"Analytics generated on {datetime.now().strftime('%d/%m/%Y at %H:%M:%S')}")
st.caption("💾 **Data Source**: Weather data from [Weather API](https://www.weatherapi.com/) | IoT sensors from soil monitoring (simulated)")
