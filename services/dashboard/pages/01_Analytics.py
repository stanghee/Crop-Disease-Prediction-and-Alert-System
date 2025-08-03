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
st.markdown("---")

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
    
    # Weather location selection with expander and checkboxes
    with st.expander("🌦️ Weather Location Selection", expanded=True):
        # Checkbox for "All locations"
        show_all_locations = st.checkbox("All locations", value=True, key="all_locations")
        
        # Individual location checkboxes
        verona = st.checkbox("Verona", value=True, key="verona")
        milan = st.checkbox("Milan", value=True, key="milan")
        rome = st.checkbox("Rome", value=True, key="rome")
        naples = st.checkbox("Naples", value=True, key="naples")
        palermo = st.checkbox("Palermo", value=True, key="palermo")
        
        # Create selected_locations list based on checkboxes
        if show_all_locations:
            selected_locations = ["Verona", "Milan", "Rome", "Naples", "Palermo"]
        else:
            selected_locations = []
            if verona:
                selected_locations.append("Verona")
            if milan:
                selected_locations.append("Milan")
            if rome:
                selected_locations.append("Rome")
            if naples:
                selected_locations.append("Naples")
            if palermo:
                selected_locations.append("Palermo")
    


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

# Main layout
st.header("Real-Time Data Monitoring")

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

# Weather Insights Section
if weather_data:
    st.markdown("---")
    
    # Create a styled header
    st.markdown("""
    <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); padding: 15px; border-radius: 10px; margin-bottom: 20px;">
        <h3 style="color: white; margin: 0; text-align: center;">🌦️ Weather Insights</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Convert to DataFrame for insights
    weather_df = pd.DataFrame(weather_data)
    
    if not weather_df.empty:
        # Create weather insights cards with better styling
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if 'temp_c' in weather_df.columns:
                avg_temp = weather_df['temp_c'].mean()
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #ff6b6b; text-align: center;">
                    <div style="font-size: 24px; color: #ff6b6b;">🌡️</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{:.1f}°C</div>
                    <div style="font-size: 12px; color: #6c757d;">Average Temperature</div>
                    <div style="font-size: 10px; color: #adb5bd;">Range: {:.1f}°C</div>
                </div>
                """.format(avg_temp, weather_df['temp_c'].max() - weather_df['temp_c'].min()), unsafe_allow_html=True)
        
        with col2:
            if 'humidity' in weather_df.columns:
                avg_humidity = weather_df['humidity'].mean()
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #4ecdc4; text-align: center;">
                    <div style="font-size: 24px; color: #4ecdc4;">💧</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{:.0f}%</div>
                    <div style="font-size: 12px; color: #6c757d;">Average Humidity</div>
                    <div style="font-size: 10px; color: #adb5bd;">Range: {:.0f}%</div>
                </div>
                """.format(avg_humidity, weather_df['humidity'].max() - weather_df['humidity'].min()), unsafe_allow_html=True)
        
        with col3:
            if 'precip_mm' in weather_df.columns:
                total_precip = weather_df['precip_mm'].sum()
                precip_level = "None" if total_precip == 0 else "Light" if total_precip < 5 else "Moderate" if total_precip < 15 else "Heavy"
                precip_color = "#28a745" if precip_level == "None" else "#ffc107" if precip_level == "Light" else "#fd7e14" if precip_level == "Moderate" else "#dc3545"
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid {}; text-align: center;">
                    <div style="font-size: 24px; color: {};">🌧️</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{}</div>
                    <div style="font-size: 12px; color: #6c757d;">Precipitation Level</div>
                    <div style="font-size: 10px; color: #adb5bd;">{:.1f}mm total</div>
                </div>
                """.format(precip_color, precip_color, precip_level, total_precip), unsafe_allow_html=True)
        
        with col4:
            if 'uv' in weather_df.columns:
                avg_uv = weather_df['uv'].mean()
                uv_level = "Low" if avg_uv < 3 else "Moderate" if avg_uv < 6 else "High" if avg_uv < 8 else "Very High"
                uv_color = "#28a745" if uv_level == "Low" else "#ffc107" if uv_level == "Moderate" else "#fd7e14" if uv_level == "High" else "#dc3545"
                st.markdown("""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid {}; text-align: center;">
                    <div style="font-size: 24px; color: {};">☀️</div>
                    <div style="font-size: 18px; font-weight: bold; color: #495057;">{}</div>
                    <div style="font-size: 12px; color: #6c757d;">UV Index Level</div>
                    <div style="font-size: 10px; color: #adb5bd;">{:.1f} average</div>
                </div>
                """.format(uv_color, uv_color, uv_level, avg_uv), unsafe_allow_html=True)
        
        # Weather conditions summary with better styling
        if 'condition' in weather_df.columns:
            st.markdown("---")
            
            # Group conditions by type and show cities
            conditions_summary = {}
            for _, row in weather_df.iterrows():
                condition = row['condition']
                location = row['location']
                
                if condition not in conditions_summary:
                    conditions_summary[condition] = []
                conditions_summary[condition].append(location)
            
            # Display conditions with cities using Streamlit components
            st.markdown("---")
            st.subheader("🌤️ Current Weather Conditions")
            
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
        
        # Current data tables section
        st.markdown("---")
        st.subheader("📊 Current Data Overview")
        
        # Two columns for tables
        tab1, tab2 = st.tabs(["🌡️ Sensor Data", "🌦️ Weather Data"])
        
        with tab1:
            if sensor_data:
                sensor_df = pd.DataFrame(sensor_data)
                if not sensor_df.empty:
                    # Create compact sensor table
                    display_sensors = sensor_df[['field_id', 'temperature', 'humidity', 'soil_ph']].copy()
                    display_sensors.columns = ['Field', 'Temp (°C)', 'Humidity (%)', 'pH']
                    display_sensors = display_sensors.reset_index(drop=True)  # Reset index to remove any existing indices
                    
                    # Pre-format the data to ensure proper decimal places
                    display_sensors['Temp (°C)'] = display_sensors['Temp (°C)'].round(1)
                    display_sensors['Humidity (%)'] = display_sensors['Humidity (%)'].round(1)
                    display_sensors['pH'] = display_sensors['pH'].round(2)
                    
                    # Add color coding for values
                    def color_temp(val):
                        if val < 10:
                            return 'background-color: #e3f2fd'  # Light blue for cold
                        elif val > 30:
                            return 'background-color: #ffebee'  # Light red for hot
                        else:
                            return 'background-color: #f1f8e9'  # Light green for normal
                    
                    def color_humidity(val):
                        if val < 30:
                            return 'background-color: #fff8e1'  # Light yellow for dry
                        elif val > 80:
                            return 'background-color: #e8f5e8'  # Light green for humid
                        else:
                            return 'background-color: #f3e5f5'  # Light purple for normal
                    
                    def color_ph(val):
                        if val < 6.0:
                            return 'background-color: #ffcdd2'  # Light red for acidic
                        elif val > 7.5:
                            return 'background-color: #c8e6c9'  # Light green for alkaline
                        else:
                            return 'background-color: #fff9c4'  # Light yellow for neutral
                    
                    # Compact legend above table with labels
                    st.markdown("""
                    <div style="background-color: #f8f9fa; padding: 12px; border-radius: 8px; border: 1px solid #e9ecef; margin-bottom: 10px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; font-size: 12px;">
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <span style="font-weight: bold; color: #495057;">🌡️ Temp:</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #e3f2fd; border: 1px solid #ccc; margin-right: 4px;"></span>Cold</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #f1f8e9; border: 1px solid #ccc; margin-right: 4px;"></span>Normal</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #ffebee; border: 1px solid #ccc; margin-right: 4px;"></span>Hot</span>
                            </div>
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <span style="font-weight: bold; color: #495057;">💧 Humidity:</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #fff8e1; border: 1px solid #ccc; margin-right: 4px;"></span>Dry</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #f3e5f5; border: 1px solid #ccc; margin-right: 4px;"></span>Normal</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #e8f5e8; border: 1px solid #ccc; margin-right: 4px;"></span>Humid</span>
                            </div>
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <span style="font-weight: bold; color: #495057;">🌱 Soil pH:</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #ffcdd2; border: 1px solid #ccc; margin-right: 4px;"></span>Acidic</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #fff9c4; border: 1px solid #ccc; margin-right: 4px;"></span>Neutral</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #c8e6c9; border: 1px solid #ccc; margin-right: 4px;"></span>Alkaline</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Apply styling
                    styled_sensors = display_sensors.style\
                        .applymap(color_temp, subset=['Temp (°C)'])\
                        .applymap(color_humidity, subset=['Humidity (%)'])\
                        .applymap(color_ph, subset=['pH'])\
                        .format({'Temp (°C)': '{:.1f}', 'Humidity (%)': '{:.1f}', 'pH': '{:.2f}'})\
                        .hide(axis='index')\
                        .set_table_styles([
                            {'selector': 'th', 'props': [('background-color', '#f8f9fa'), ('color', '#495057'), ('font-weight', 'bold'), ('text-align', 'center')]},
                            {'selector': 'td', 'props': [('text-align', 'center'), ('padding', '8px')]},
                            {'selector': '.index', 'props': [('display', 'none')]}  # Hide index column completely
                        ])
                    
                    # Calculate dynamic height based on actual number of rows
                    num_rows = len(display_sensors)
                    table_height = max(150, num_rows * 45)  # 45px per riga, minimo 150px
                    
                    st.dataframe(styled_sensors, use_container_width=True, height=table_height)
                else:
                    st.info("No sensor data available")
            else:
                st.warning("❌ Unable to retrieve sensor data")
        
        with tab2:
            if weather_data:
                weather_df = pd.DataFrame(weather_data)
                if not weather_df.empty:
                    # Create compact weather table
                    display_columns = ['location', 'temp_c', 'humidity']
                    if 'wind_kph' in weather_df.columns:
                        display_columns.append('wind_kph')
                    
                    display_weather = weather_df[display_columns].copy()
                    column_names = ['Location', 'Temp (°C)', 'Humidity (%)']
                    if 'wind_kph' in weather_df.columns:
                        column_names.append('Wind (km/h)')
                    
                    display_weather.columns = column_names
                    display_weather = display_weather.reset_index(drop=True)  # Reset index to remove any existing indices
                    
                    # Add color coding for weather values
                    def color_weather_temp(val):
                        if val < 10:
                            return 'background-color: #e3f2fd'  # Light blue for cold
                        elif val > 25:
                            return 'background-color: #ffebee'  # Light red for hot
                        else:
                            return 'background-color: #f1f8e9'  # Light green for normal
                    
                    def color_weather_humidity(val):
                        if val < 40:
                            return 'background-color: #fff8e1'  # Light yellow for dry
                        elif val > 80:
                            return 'background-color: #e8f5e8'  # Light green for humid
                        else:
                            return 'background-color: #f3e5f5'  # Light purple for normal
                    
                    def color_wind(val):
                        if val < 10:
                            return 'background-color: #e8f5e8'  # Light green for calm
                        elif val > 30:
                            return 'background-color: #ffcdd2'  # Light red for strong
                        else:
                            return 'background-color: #fff9c4'  # Light yellow for moderate
                    
                    # Compact legend above table with labels
                    st.markdown("""
                    <div style="background-color: #f8f9fa; padding: 12px; border-radius: 8px; border: 1px solid #e9ecef; margin-bottom: 10px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; font-size: 12px;">
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <span style="font-weight: bold; color: #495057;">🌡️ Temp:</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #e3f2fd; border: 1px solid #ccc; margin-right: 4px;"></span>Cold</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #f1f8e9; border: 1px solid #ccc; margin-right: 4px;"></span>Normal</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #ffebee; border: 1px solid #ccc; margin-right: 4px;"></span>Hot</span>
                            </div>
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <span style="font-weight: bold; color: #495057;">💧 Humidity:</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #fff8e1; border: 1px solid #ccc; margin-right: 4px;"></span>Dry</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #f3e5f5; border: 1px solid #ccc; margin-right: 4px;"></span>Normal</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #e8f5e8; border: 1px solid #ccc; margin-right: 4px;"></span>Humid</span>
                            </div>
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <span style="font-weight: bold; color: #495057;">💨 Wind:</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #e8f5e8; border: 1px solid #ccc; margin-right: 4px;"></span>Calm</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #fff9c4; border: 1px solid #ccc; margin-right: 4px;"></span>Moderate</span>
                                <span><span style="display: inline-block; width: 12px; height: 12px; background-color: #ffcdd2; border: 1px solid #ccc; margin-right: 4px;"></span>Strong</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Pre-format the data to ensure proper decimal places (BEFORE styling)
                    display_weather['Temp (°C)'] = display_weather['Temp (°C)'].round(1)
                    display_weather['Humidity (%)'] = display_weather['Humidity (%)'].round(1).astype(int)
                    if 'Wind (km/h)' in display_weather.columns:
                        display_weather['Wind (km/h)'] = display_weather['Wind (km/h)'].round(1)
                    
                    # Apply styling
                    styled_weather = display_weather.style\
                        .applymap(color_weather_temp, subset=['Temp (°C)'])\
                        .applymap(color_weather_humidity, subset=['Humidity (%)'])\
                        .format({'Temp (°C)': '{:.1f}', 'Humidity (%)': '{:.0f}'})\
                        .hide(axis='index')\
                        .set_table_styles([
                            {'selector': 'th', 'props': [('background-color', '#f8f9fa'), ('color', '#495057'), ('font-weight', 'bold'), ('text-align', 'center')]},
                            {'selector': 'td', 'props': [('text-align', 'center'), ('padding', '8px')]},
                            {'selector': '.index', 'props': [('display', 'none')]}  # Hide index column completely
                        ])
                    
                    if 'Wind (km/h)' in display_weather.columns:
                        styled_weather = styled_weather\
                            .format({'Wind (km/h)': '{:.1f}'})\
                            .applymap(color_wind, subset=['Wind (km/h)'])
                    
                    # Calculate dynamic height based on actual number of rows
                    num_rows = len(display_weather)
                    table_height = max(150, num_rows * 45)  # 45px per riga, minimo 150px
                    
                    st.dataframe(styled_weather, use_container_width=True, height=table_height)
                else:
                    st.info("No weather data available")
            else:
                st.warning("❌ Unable to retrieve weather data")

st.markdown("---")

# IoT Sensor Data visualization section
with st.expander("📡 IoT Sensor Data", expanded=True):
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
                    st.info(f"Data cached {cache_age:.0f} seconds ago")
                except:
                    pass
            
            # Temperature chart in its own container
            st.markdown("""
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        padding: 15px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
                        margin-bottom: 20px;">
                <h4 style="color: white; margin: 0 0 10px 0; text-align: center; font-size: 16px;">
                    🌡️ Temperature Analysis
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
            <div style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); 
                        padding: 15px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
                        margin-bottom: 20px;">
                <h4 style="color: white; margin: 0 0 10px 0; text-align: center; font-size: 16px;">
                    💧 Humidity Analysis
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
            <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                        padding: 15px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
                        margin-bottom: 20px;">
                <h4 style="color: white; margin: 0 0 10px 0; text-align: center; font-size: 16px;">
                    🌱 Soil pH Analysis
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



st.markdown("---")

# Footer
st.markdown("---")
st.caption(f"Analytics generated on {datetime.now().strftime('%d/%m/%Y at %H:%M:%S')}")
st.caption("💾 **Data Source**: Weather data from [Weather API](https://www.weatherapi.com/) | IoT sensors from soil monitoring (simulated)")
