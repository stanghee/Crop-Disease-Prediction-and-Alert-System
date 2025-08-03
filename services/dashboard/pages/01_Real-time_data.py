#!/usr/bin/env python3
"""
Real-Time Data Page - Live sensor and weather data monitoring
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import json
import redis
import os
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="Real-Time Data - Crop Disease Dashboard",
    page_icon="📊",
    layout="wide"
)

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

st.title("Real-Time Data")
st.markdown("*Live monitoring of IoT sensor data and weather conditions across agricultural fields*")

st.markdown("<br>", unsafe_allow_html=True)

# Sidebar controls
with st.sidebar:
    st.header("Data Settings")
    
    # Field filters with expander and checkboxes
    with st.expander("🌾 Field Selection", expanded=True):
        # Checkbox for "All fields"
        show_all_fields = st.checkbox("All fields", value=True, key="all_fields")
        
        # Individual field checkboxes
        field_01 = st.checkbox("Field 01", value=True, key="field_01")
        field_02 = st.checkbox("Field 02", value=True, key="field_02")
        field_03 = st.checkbox("Field 03", value=True, key="field_03")
        
        # Create selected_fields list based on checkboxes
        if show_all_fields:
            selected_fields = ["field_01", "field_02", "field_03"]
        else:
            selected_fields = []
            if field_01:
                selected_fields.append("field_01")
            if field_02:
                selected_fields.append("field_02")
            if field_03:
                selected_fields.append("field_03")
    
    # Weather location selection with expander and radio button
    with st.expander("🌦️ Weather Location Selection", expanded=True):
        # Single location selection with radio button
        selected_location = st.radio(
            "Select a location:",
            ["Verona", "Milan", "Rome", "Naples", "Palermo"],
            index=0,  # Default to Verona
            key="weather_location"
        )
        
        # Create selected_locations list with single location
        selected_locations = [selected_location]
    


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


# Load data
# Get Redis client
redis_client = get_redis_client()

# Auto-refresh every 10 seconds for real-time feel
st_autorefresh(interval=10 * 1000, key="redis_realtime_data_autorefresh")

# Get data from Redis
sensor_data = get_sensor_data_from_redis(redis_client)
weather_data = get_weather_data_from_redis(redis_client)

# Weather Insights Section
if weather_data:
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Create a styled header
    st.markdown("""
    <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); padding: 15px; border-radius: 10px; margin-bottom: 20px;">
        <h3 style="color: white; margin: 0; text-align: center;">🌦️ Weather Insights - {}</h3>
    </div>
    """.format(selected_locations[0]), unsafe_allow_html=True)
    
    # Convert to DataFrame for insights and filter by selected location
    weather_df = pd.DataFrame(weather_data)
    weather_df = weather_df[weather_df['location'].isin(selected_locations)]
    
    if not weather_df.empty:
        # Create weather insights cards with better styling
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if 'temp_c' in weather_df.columns:
                current_temp = weather_df['temp_c'].iloc[0]
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #ff6b6b; text-align: center;">
                    <div style="font-size: 24px; color: #ff6b6b;">🌡️</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{:.1f}°C</div>
                    <div style="font-size: 12px; color: #6c757d;">Current Temperature</div>
                </div>
                """.format(current_temp), unsafe_allow_html=True)
        
        with col2:
            if 'humidity' in weather_df.columns:
                current_humidity = weather_df['humidity'].iloc[0]
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #4ecdc4; text-align: center;">
                    <div style="font-size: 24px; color: #4ecdc4;">💧</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{:.0f}%</div>
                    <div style="font-size: 12px; color: #6c757d;">Current Humidity</div>
                </div>
                """.format(current_humidity), unsafe_allow_html=True)
        
        with col3:
            if 'precip_mm' in weather_df.columns:
                current_precip = weather_df['precip_mm'].iloc[0]
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #ffc107; text-align: center;">
                    <div style="font-size: 24px; color: #ffc107;">🌧️</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{:.1f}mm</div>
                    <div style="font-size: 12px; color: #6c757d;">Current Precipitation</div>
                </div>
                """.format(current_precip), unsafe_allow_html=True)
        
        with col4:
            if 'uv' in weather_df.columns:
                current_uv = weather_df['uv'].iloc[0]
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #fd7e14; text-align: center;">
                    <div style="font-size: 24px; color: #fd7e14;">☀️</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{:.1f}</div>
                    <div style="font-size: 12px; color: #6c757d;">Current UV Index</div>
                </div>
                """.format(current_uv), unsafe_allow_html=True)
        
        # Weather conditions summary with better styling
        if 'condition' in weather_df.columns:
            
            # Add space before the Current Weather Conditions banner
            st.markdown("<br><br>", unsafe_allow_html=True)
            
            # Group conditions by type and show cities
            conditions_summary = {}
            for _, row in weather_df.iterrows():
                condition = row['condition']
                location = row['location']
                
                if condition not in conditions_summary:
                    conditions_summary[condition] = []
                conditions_summary[condition].append(location)
            
            # Display conditions with cities using Streamlit components
            st.markdown("""
            <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); padding: 15px; border-radius: 10px; margin-bottom: 20px;">
                <h3 style="color: white; margin: 0; text-align: center;">🌤️ Current Weather Conditions - {}</h3>
            </div>
            """.format(selected_locations[0]), unsafe_allow_html=True)
            
            # Display each condition with elegant styling
            for condition, cities in conditions_summary.items():
                cities_text = ", ".join(cities)
                st.markdown(f"""
                <div style="
                    background: white;
                    color: #333;
                    padding: 15px 25px;
                    border-radius: 8px;
                    margin-bottom: 10px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                    border: 1px solid #e0e0e0;
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                ">
                    <span style="
                        color: #333;
                        font-size: 20px;
                        font-weight: bold;
                    ">{condition}</span>
                    <br>
                    <span style="
                        color: #666;
                        font-size: 16px;
                    ">Location: {cities_text}</span>
                </div>
                """, unsafe_allow_html=True)
        

st.markdown("<br>", unsafe_allow_html=True)

# IoT Sensor Data visualization section

# IoT Sensor Data Section
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("""
<div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); padding: 15px; border-radius: 10px; margin-bottom: 20px;">
    <h3 style="color: white; margin: 0; text-align: center;">📡 IoT Sensor Data</h3>
</div>
""", unsafe_allow_html=True)

if sensor_data:
    # Convert to DataFrame
    sensor_df = pd.DataFrame(sensor_data)
    
    # Filter by selected fields
    if not show_all_fields and 'selected_fields' in locals():
        sensor_df = sensor_df[sensor_df['field_id'].isin(selected_fields)]
    
    if not sensor_df.empty:
            
            # Temperature chart in its own container
            st.markdown("""
            <div style="background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); 
                        padding: 15px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
                        margin-bottom: 20px;">
                <h4 style="color: white; margin: 0 0 10px 0; text-align: center; font-size: 16px;">
                    🌡️ Temperature
                </h4>
            </div>
            """, unsafe_allow_html=True)
            
            fig_temp = px.bar(
                sensor_df,
                x='field_id',
                y='temperature',
                title="Current Temperature per Field (°C)",
                color='temperature',
                color_continuous_scale='RdYlBu_r'
            )
            fig_temp.update_layout(
                height=300,
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(size=12),
                title_font_size=16,
                margin=dict(l=50, r=50, t=50, b=50)
            )
            fig_temp.update_traces(
                marker_line_width=1,
                marker_line_color='rgba(0,0,0,0.1)',
                width=0.8
            )
            st.plotly_chart(fig_temp, use_container_width=True)
            
            # Humidity chart in its own container
            st.markdown("""
            <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); 
                        padding: 15px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
                        margin-bottom: 20px;">
                <h4 style="color: white; margin: 0 0 10px 0; text-align: center; font-size: 16px;">
                    💧 Humidity
                </h4>
            </div>
            """, unsafe_allow_html=True)
            
            fig_humidity = px.bar(
                sensor_df,
                x='field_id',
                y='humidity',
                title="Current Humidity per Field (%)",
                color='humidity',
                color_continuous_scale='Blues'
            )
            fig_humidity.update_layout(
                height=300,
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(size=12),
                title_font_size=16,
                margin=dict(l=50, r=50, t=50, b=50)
            )
            fig_humidity.update_traces(
                marker_line_width=1,
                marker_line_color='rgba(0,0,0,0.1)',
                width=0.8
            )
            st.plotly_chart(fig_humidity, use_container_width=True)
            
            # Soil pH chart in its own container
            st.markdown("""
            <div style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); 
                        padding: 15px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
                        margin-bottom: 20px;">
                <h4 style="color: white; margin: 0 0 10px 0; text-align: center; font-size: 16px;">
                    🌱 Soil pH
                </h4>
            </div>
            """, unsafe_allow_html=True)
            
            fig_ph = px.bar(
                sensor_df,
                x='field_id',
                y='soil_ph',
                title="Current Soil pH per Field",
                color='soil_ph',
                color_continuous_scale='Greens'
            )
            fig_ph.update_layout(
                height=300,
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(size=12),
                title_font_size=16,
                margin=dict(l=50, r=50, t=50, b=50)
            )
            fig_ph.update_traces(
                marker_line_width=1,
                marker_line_color='rgba(0,0,0,0.1)',
                width=0.8
            )
            st.plotly_chart(fig_ph, use_container_width=True)
            
    else:
        st.info("No sensor data available for the selected fields")
else:
    st.warning("❌ Unable to retrieve sensor data from Redis cache")
    st.info("Check that Redis cache service is running and processing Kafka data")

st.markdown("<br>", unsafe_allow_html=True)

# Footer
st.caption(f"Analytics generated on {datetime.now().strftime('%d/%m/%Y at %H:%M:%S')}")
st.caption("💾 **Data Source**: Weather data from [Weather API](https://www.weatherapi.com/) | IoT sensors from soil monitoring (simulated)")
