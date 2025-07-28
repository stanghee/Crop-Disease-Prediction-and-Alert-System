#!/usr/bin/env python3
"""
Satellite Map Page - OpenStreetMap integration for field monitoring
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
import redis
import os
import json
from typing import List, Dict
from datetime import datetime
import pytz
from minio import Minio
from PIL import Image
from io import BytesIO

# Page configuration
st.set_page_config(
    page_title="Satellite Map - Crop Disease Dashboard",
    page_icon="🗺️",
    layout="wide"
)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Redis connection
@st.cache_resource(show_spinner=False)
def get_redis_client():
    """Get Redis client connection"""
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
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

# MinIO connection
@st.cache_resource(show_spinner=False)
def get_minio_client():
    """Get MinIO client connection"""
    try:
        minio_client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )
        return minio_client
    except Exception as e:
        st.error(f"❌ Failed to connect to MinIO: {e}")
        return None

# Get latest satellite image
@st.cache_data(ttl=60)  # Cache for 1 minute
def get_latest_satellite_image():
    """Get the latest satellite image from MinIO"""
    try:
        minio_client = get_minio_client()
        if not minio_client:
            return None
        
        # List objects in satellite-images bucket
        objects = list(minio_client.list_objects("satellite-images", recursive=True))
        
        if not objects:
            return None
        
        # Get the latest image by last modified time
        latest = max(objects, key=lambda x: x.last_modified)
        
        # Download the image
        image_data = minio_client.get_object("satellite-images", latest.object_name)
        image_bytes = image_data.read()
        
        return {
            'image': Image.open(BytesIO(image_bytes)),
            'timestamp': latest.last_modified,
            'filename': latest.object_name,
            'size_bytes': len(image_bytes)
        }
        
    except Exception as e:
        st.error(f"❌ Error retrieving satellite image: {e}")
        return None

# Get available field IDs from Redis
def get_available_field_ids(r: redis.Redis) -> List[str]:
    """Get all available field IDs from Redis"""
    try:
        if not r:
            return []
        
        # Get sensor keys to extract field IDs
        sensor_keys = r.keys("sensors:latest:*")
        field_ids = [key.split(":")[-1] for key in sensor_keys]
        return sorted(field_ids)
    except Exception as e:
        st.error(f"❌ Error retrieving field IDs: {e}")
        return []

# Get field data from Redis
def get_field_data(r: redis.Redis, field_id: str) -> Dict:
    """Get field data from Redis"""
    try:
        data = r.get(f"sensors:latest:{field_id}")
        if data:
            return json.loads(data)
        return None
    except Exception as e:
        st.error(f"Error getting field data: {e}")
        return None

# Get all latest alerts from Redis
def get_all_latest_alerts(r: redis.Redis) -> Dict[str, Dict]:
    """Get all latest alerts from Redis"""
    try:
        keys = r.keys("alerts:latest:*")
        alerts = {}
        for key in keys:
            zone_id = key.split(":")[-1]
            data = r.get(key)
            if data:
                alerts[zone_id] = json.loads(data)
        return alerts
    except Exception as e:
        st.error(f"Error getting alerts: {e}")
        return {}

# Get field color based on alerts
def get_field_color_from_alerts(field_id: str, alerts: Dict[str, Dict]) -> str:
    """Determine field color based on active alerts"""
    
    # Look for alert for this field_id
    field_alert = alerts.get(field_id)
    
    if not field_alert:
        return 'green'  # No alert = green (normal)
    
    # Color based on severity
    severity = field_alert.get('severity', 'MEDIUM').upper()
    
    if severity == 'HIGH':
        return 'red'      # 🔴 RED: Critical alert
    elif severity == 'MEDIUM':
        return 'orange'   # 🟠 ORANGE: Moderate alert
    else:
        return 'yellow'   # 🟡 YELLOW: Other alerts

# Create map with field markers
def create_field_map(field_data_list: List[Dict], alerts: Dict[str, Dict], center_lat: float = 45.4384, center_lon: float = 10.9917) -> folium.Map:
    """Create a Folium map with field markers based on alerts"""
    
    # Create base map centered on Italy (Verona area)
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=8,
        tiles='OpenStreetMap'
    )
    
    # Add field markers
    for field_data in field_data_list:
        if not field_data:
            continue
            
        field_id = field_data.get('field_id', 'Unknown')
        
        # Get coordinates (for now using default coordinates, will be updated when we have real GPS data)
        # These are example coordinates for different Italian cities
        field_coordinates = {
            'field_01': [45.4384, 10.9917],  # Verona
            'field_02': [45.4642, 9.1900],   # Milan
            'field_03': [41.9028, 12.4964],  # Rome
        }
        
        lat, lon = field_coordinates.get(field_id, [center_lat, center_lon])
        
        # Get field status and color based on alerts
        temperature = field_data.get('temperature', 0)
        humidity = field_data.get('humidity', 0)
        soil_ph = field_data.get('soil_ph', 7.0)
        
        # Determine marker color based on alerts (not temperature)
        color = get_field_color_from_alerts(field_id, alerts)
        
        # Get alert info for popup
        field_alert = alerts.get(field_id)
        alert_info = ""
        if field_alert:
            alert_type = field_alert.get('alert_type', 'UNKNOWN')
            severity = field_alert.get('severity', 'UNKNOWN')
            message = field_alert.get('message', 'No message')
            alert_info = f"""
            <div style="margin-top: 10px; padding: 8px; background-color: #f0f0f0; border-radius: 5px;">
                <strong>🚨 Alert:</strong> {alert_type}<br>
                <strong>Severity:</strong> {severity}<br>
                <strong>Message:</strong> {message}
            </div>
            """
        
        # Create simplified popup content without tabs
        popup_content = f"""
        <div style="width: 300px;">
            <h4 style="margin-bottom: 15px; color: #333;">Field {field_id}</h4>
            
            <div style="background-color: #f9f9f9; padding: 12px; border-radius: 6px; margin-bottom: 10px;">
                <p style="margin: 5px 0;"><strong>🌡️ Temperature:</strong> {temperature}°C</p>
                <p style="margin: 5px 0;"><strong>💧 Humidity:</strong> {humidity}%</p>
                <p style="margin: 5px 0;"><strong>🧪 Soil pH:</strong> {soil_ph}</p>
                <p style="margin: 5px 0;"><strong>⏰ Last Update:</strong> {field_data.get('timestamp', 'N/A')}</p>
            </div>
            
            {alert_info}
        </div>
        """
        
        # Add marker to map
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_content, max_width=350),
            tooltip=f"Field {field_id}",
            icon=folium.Icon(color=color, icon='leaf')
        ).add_to(m)
    
    return m

# Streamlit UI
st.title("Satellite Map - Field Monitoring")
st.markdown("---")

# Get Redis client
redis_client = get_redis_client()

# Sidebar controls
with st.sidebar:
    st.header("Map Settings")
    
    # Field selection
    st.subheader("Field Selection")
    available_fields = get_available_field_ids(redis_client)
    
    if available_fields:
        show_all_fields = st.checkbox("Show all fields", value=True)
        
        if not show_all_fields:
            selected_fields = st.multiselect(
                "Select specific fields",
                available_fields,
                default=available_fields[:2] if len(available_fields) >= 2 else available_fields
            )
        else:
            selected_fields = available_fields
    else:
        st.warning("No fields available in Redis")
        selected_fields = []
    
    # Map center selection
    st.subheader("Map Center")
    center_options = {
        "Verona (Default)": [45.4384, 10.9917],
        "Milan": [45.4642, 9.1900],
        "Rome": [41.9028, 12.4964],
        "Naples": [40.8518, 14.2681],
        "Palermo": [38.1157, 13.3615]
    }
    
    selected_center = st.selectbox(
        "Select map center",
        list(center_options.keys()),
        index=0
    )
    
    center_lat, center_lon = center_options[selected_center]
    
    # Auto-refresh settings
    st.subheader("Auto-refresh")
    auto_refresh = st.checkbox("Enable auto-refresh", value=True)
    if auto_refresh:
        refresh_interval = st.selectbox("Refresh interval", [5, 10, 30, 60], index=1)
        st.caption(f"Map will refresh every {refresh_interval} seconds")

# Main content
if not redis_client:
    st.error("❌ Cannot connect to Redis. Please check the Redis service.")
    st.stop()

if not selected_fields:
    st.info("Please select at least one field from the sidebar to display on the map.")
    st.stop()

# Get field data
field_data_list = []
for field_id in selected_fields:
    field_data = get_field_data(redis_client, field_id)
    if field_data:
        field_data['field_id'] = field_id
        field_data_list.append(field_data)

# Create and display map and satellite image
if field_data_list:
    st.subheader("Agricultural Fields Map & Satellite Image")
    
    # Get all latest alerts
    all_alerts = get_all_latest_alerts(redis_client)
    
    # Create two columns for map and satellite image (map larger, image smaller)
    col1, col2 = st.columns([3, 2])  # 60% map, 40% image
    
    with col1:
        st.markdown("**Agricultural Fields Map**")
        
        # Create map with alerts
        field_map = create_field_map(field_data_list, all_alerts, center_lat, center_lon)
        
        # Display map with larger size
        map_data = st_folium(
            field_map,
            width=500,
            height=450,
            returned_objects=["last_object_clicked"]
        )
    
    with col2:
        st.markdown("**Latest Satellite Image**")
        
        # Get and display satellite image
        satellite_data = get_latest_satellite_image()
        
        if satellite_data:
            # Display image with controlled size
            st.image(
                satellite_data['image'],
                caption=f"Satellite Image - {satellite_data['timestamp'].strftime('%d/%m/%Y %H:%M:%S')}",
                width=300  # Fixed width for smaller image
            )
            
        else:
            st.warning("⚠️ No satellite images available")
            st.info("Check that the satellite service is running and generating images")
            
            # Show placeholder
            st.markdown("""
            <div style="border: 2px dashed #ccc; padding: 20px; text-align: center; color: #666;">
                <h3>🛰️ Satellite Image</h3>
                <p>No images available</p>
                <p>Check satellite service status</p>
            </div>
            """, unsafe_allow_html=True)
    
    # Show field statistics
    st.subheader("Field Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Fields", len(field_data_list))
    
    with col2:
        avg_temp = sum(f.get('temperature', 0) for f in field_data_list) / len(field_data_list)
        st.metric("Avg Temperature", f"{avg_temp:.1f}°C")
    
    with col3:
        avg_humidity = sum(f.get('humidity', 0) for f in field_data_list) / len(field_data_list)
        st.metric("Avg Humidity", f"{avg_humidity:.1f}%")
    
    with col4:
        avg_ph = sum(f.get('soil_ph', 7.0) for f in field_data_list) / len(field_data_list)
        st.metric("Avg Soil pH", f"{avg_ph:.1f}")
    
    # Show detailed field data table
    st.subheader("Field Details")
    
    if field_data_list:
        # Prepare data for table
        table_data = []
        for field in field_data_list:
            table_data.append({
                'Field ID': field.get('field_id', 'N/A'),
                'Temperature (°C)': field.get('temperature', 'N/A'),
                'Humidity (%)': field.get('humidity', 'N/A'),
                'Soil pH': field.get('soil_ph', 'N/A'),
                'Last Update': field.get('timestamp', 'N/A')
            })
        
        st.dataframe(table_data, use_container_width=True)
    
else:
    st.warning("No field data available for the selected fields.")
    st.info("Check that the sensor service is running and sending data to Redis.")

# Footer
st.markdown("---")
st.caption(f"Map generated on {datetime.now().strftime('%d/%m/%Y at %H:%M:%S')}")
st.caption("🗺️ **Map Source**: OpenStreetMap | 📡 **Data Source**: IoT sensors via Redis cache | 🛰️ **Satellite**: Copernicus Sentinel-2 via MinIO ([Sentinel Hub](https://sh.dataspace.copernicus.eu/))")

# Auto-refresh functionality
if auto_refresh:
    import time
    time.sleep(refresh_interval)
    st.experimental_rerun() 