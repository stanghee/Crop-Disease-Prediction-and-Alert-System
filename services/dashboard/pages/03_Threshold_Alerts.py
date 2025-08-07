import streamlit as st
import streamlit.components.v1 as components
import redis
import os
import json
from typing import List, Dict
from datetime import datetime
import pytz
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="Threshold Alerts - Crop Disease Dashboard",
    page_icon="🚨",
    layout="wide"
)

# Custom CSS to ensure sidebar navigation is always visible
st.markdown("""
<style>
    /* Make sidebar wider */
    [data-testid="stSidebar"] {
        min-width: 320px !important;
        max-width: 380px !important;
    }
    
    /* Ensure navigation is always visible */
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] {
        max-height: none !important;
        overflow: visible !important;
        height: auto !important;
    }
    
    /* Remove any height restrictions on navigation list */
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] ul {
        max-height: none !important;
        overflow: visible !important;
        height: auto !important;
    }
    
    /* Ensure all navigation items are visible */
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] li {
        margin-bottom: 1px !important;
        display: block !important;
    }
</style>
""", unsafe_allow_html=True)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Redis connection
@st.cache_resource(show_spinner=False)
def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Retrieve all available zone_ids with alerts
def get_available_zone_ids(r: redis.Redis) -> List[str]:
    keys = r.keys("alerts:latest:*")
    zone_ids = [k.split(":")[-1] for k in keys]
    return sorted(zone_ids)

# Retrieve the latest alert for a zone_id
def get_latest_alert(r: redis.Redis, zone_id: str) -> Dict:
    key = f"alerts:latest:{zone_id}"
    data = r.get(key)
    if data:
        return json.loads(data)
    return None

# Get all latest alerts
def get_all_latest_alerts(r: redis.Redis) -> Dict[str, Dict]:
    keys = r.keys("alerts:latest:*")
    alerts = {}
    for key in keys:
        zone_id = key.split(":")[-1]
        data = r.get(key)
        if data:
            alerts[zone_id] = json.loads(data)
    return alerts

# Streamlit UI
st.title("Threshold Alerts - Real-time Monitoring")
st.markdown("*Monitor and manage real-time alerts for sensor anomalies and weather conditions*")

redis_client = get_redis_client()

# Auto-refresh every 10 seconds for real-time updates
st_autorefresh(interval=10 * 1000, key="threshold_alerts_autorefresh")

# Sidebar for zone selection and filters
st.sidebar.header("Filter Settings")

# Field filters with expander and checkboxes
with st.sidebar.expander("🌾 Field Selection", expanded=True):
    # Checkbox for "All fields"
    show_all_fields = st.checkbox("All fields", value=True, key="all_fields_alerts")
    
    # Individual field checkboxes
    field_01 = st.checkbox("Field 01", value=True, key="field_01_alerts")
    field_02 = st.checkbox("Field 02", value=True, key="field_02_alerts")
    field_03 = st.checkbox("Field 03", value=True, key="field_03_alerts")
    
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

# Filter by alert type with expander and checkboxes
with st.sidebar.expander("🚨 Alert Type Filter", expanded=True):
    # Checkbox for "All alert types"
    show_all_alert_types = st.checkbox("All alert types", value=True, key="all_alert_types")
    
    # Individual alert type checkboxes
    sensor_anomaly = st.checkbox("SENSOR_ANOMALY", value=True, key="sensor_anomaly")
    weather_alert = st.checkbox("WEATHER_ALERT", value=True, key="weather_alert")
    
    # Create selected_alert_types list based on checkboxes
    if show_all_alert_types:
        selected_alert_types = ["SENSOR_ANOMALY", "WEATHER_ALERT"]
    else:
        selected_alert_types = []
        if sensor_anomaly:
            selected_alert_types.append("SENSOR_ANOMALY")
        if weather_alert:
            selected_alert_types.append("WEATHER_ALERT")

# Filter by severity with expander and checkboxes
with st.sidebar.expander("⚠️ Severity Filter", expanded=True):
    # Checkbox for "All severities"
    show_all_severities = st.checkbox("All severities", value=True, key="all_severities")
    
    # Individual severity checkboxes
    high_severity = st.checkbox("HIGH", value=True, key="high_severity")
    medium_severity = st.checkbox("MEDIUM", value=True, key="medium_severity")
    
    # Create selected_severities list based on checkboxes
    if show_all_severities:
        selected_severities = ["HIGH", "MEDIUM"]
    else:
        selected_severities = []
        if high_severity:
            selected_severities.append("HIGH")
        if medium_severity:
            selected_severities.append("MEDIUM")

if not selected_fields:
    st.info("Select at least one field from the sidebar to view alerts.")
    st.stop()

# Function to get colors based on severity and alert type
SEVERITY_COLOR = {
    "high": "#ff4d4f",      # red
    "medium": "#faad14",    # orange
}

ALERT_TYPE_ICON = {
    "SENSOR_ANOMALY": "🌡️",
    "WEATHER_ALERT": "🌦️"
}

def get_severity_color(severity):
    return SEVERITY_COLOR.get(str(severity).lower(), "#d9d9d9")

def get_alert_icon(alert_type):
    return ALERT_TYPE_ICON.get(str(alert_type), "🚨")

# Function to display alert in colored card
def show_alert_card(alert):
    severity = alert.get('severity', 'UNKNOWN')
    alert_type = alert.get('alert_type', 'UNKNOWN')
    status = alert.get('status', 'UNKNOWN')
    zone_id = alert.get('zone_id', 'UNKNOWN')
    
    border_color = get_severity_color(severity)
    icon = get_alert_icon(alert_type)
    
    # Status color
    status_color = "#52c41a" if status == "ACTIVE" else "#d9d9d9"
    
    # Convert timestamp to Europe/Rome local time
    ts_utc = alert.get('alert_timestamp', '-')
    if ts_utc and ts_utc != '-':
        try:
            # Handle both formats: with and without microseconds
            if '.' in ts_utc:
                dt_utc = datetime.strptime(ts_utc, "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                dt_utc = datetime.strptime(ts_utc, "%Y-%m-%dT%H:%M:%SZ")
            dt_rome = dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Europe/Rome'))
            ts_rome = dt_rome.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            ts_rome = ts_utc
    else:
        ts_rome = "-"
    
    # Get severity color for dynamic styling
    severity_colors = {
        'HIGH': {'bg': 'linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%)', 'border': '#dc3545'},
        'MEDIUM': {'bg': 'linear-gradient(135deg, #ffa726 0%, #ff9800 100%)', 'border': '#fd7e14'},
        'LOW': {'bg': 'linear-gradient(135deg, #4ecdc4 0%, #26a69a 100%)', 'border': '#20c997'},
        'UNKNOWN': {'bg': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', 'border': '#6c757d'}
    }
    
    color_scheme = severity_colors.get(severity, severity_colors['UNKNOWN'])
    
    # Create bubble using native Streamlit components
    with st.container():

        
        # Create unified gray alert card with data and circle
        unified_html = f"""
        <style>
        @keyframes borderPulse {{
            0% {{ border-color: {color_scheme['border']}; box-shadow: 0 0 0 0 {color_scheme['border']}40; }}
            50% {{ border-color: {color_scheme['border']}; box-shadow: 0 0 0 8px {color_scheme['border']}20; }}
            100% {{ border-color: {color_scheme['border']}; box-shadow: 0 0 0 0 {color_scheme['border']}40; }}
        }}
        
        .message-circle-{severity.lower()} {{
            background: linear-gradient(145deg, #f8f9fa, #e9ecef);
            border: 3px solid {color_scheme['border']};
            border-radius: 50%;
            width: 180px;
            height: 180px;
            display: flex;
            align-items: center;
            justify-content: center;
            text-align: center;
            animation: borderPulse 2s infinite;
            transition: all 0.3s ease;
            margin: 20px auto 0 auto;
        }}
        
        .message-circle-{severity.lower()}:hover {{
            animation: none;
            box-shadow: 0 4px 12px {color_scheme['border']}40;
        }}
        
        * {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }}
        </style>
        
        <div style="
            background: linear-gradient(145deg, #ffffff, #f8f9fa);
            border: 2px solid #dee2e6;
            border-radius: 8px;
            padding: 0;
            margin-bottom: 15px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1), 0 2px 4px rgba(0,0,0,0.06);
        ">
            <!-- Header section -->
            <div style="
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border-radius: 6px 6px 0 0;
                padding: 12px 20px;
                color: white;
                font-weight: 600;
                font-size: 16px;
                text-align: center;
                border-bottom: 2px solid #dee2e6;
            ">
                <span style="font-size: 18px; margin-right: 8px;">{icon}</span>
                Alert for <strong>{zone_id}</strong>
            </div>
            
            <!-- Data section -->
            <div style="padding: 15px 20px;">
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 15px; text-align: center; margin-bottom: 15px;">
                    <div>
                        <div style="font-size: 11px; color: #6c757d; font-weight: 500; margin-bottom: 5px;">Type</div>
                        <div style="font-weight: bold; font-size: 14px; color: #212529;">{alert_type}</div>
                    </div>
                    <div>
                        <div style="font-size: 11px; color: #6c757d; font-weight: 500; margin-bottom: 5px;">Severity</div>
                        <div style="font-weight: bold; font-size: 14px; color: {color_scheme['border']};">{severity}</div>
                    </div>
                    <div>
                        <div style="font-size: 11px; color: #6c757d; font-weight: 500; margin-bottom: 5px;">Status</div>
                        <div style="font-weight: bold; font-size: 14px; color: #212529;">{status}</div>
                    </div>
                    <div>
                        <div style="font-size: 11px; color: #6c757d; font-weight: 500; margin-bottom: 5px;">Time</div>
                        <div style="font-weight: bold; font-size: 12px; color: #212529;">{ts_rome}</div>
                    </div>
                </div>
                
                <!-- Message circle inside the data container -->
                <div class="message-circle-{severity.lower()}">
                    <div style="font-weight: bold; color: #212529; font-size: 18px; line-height: 1.4; padding: 16px;">
                        Alert:<br>
                        <span style="font-size: 16px; color: #212529; margin-top: 8px; display: block;">
                            {alert.get('message', 'No message available').replace('Alert: ', '').replace('Weather Alert: ', '')}
                        </span>
                    </div>
                </div>
            </div>
        </div>
        """
        components.html(unified_html, height=350)
    st.caption(f"Cached at: {alert.get('cached_at', '-')}, TTL: {alert.get('cache_ttl', '-')}s")

# Get all alerts and apply filters
all_alerts = get_all_latest_alerts(redis_client)
filtered_alerts = {}

for zone_id, alert in all_alerts.items():
    # Filter by selected fields (zone_id corresponds to field_id)
    if zone_id not in selected_fields:
        continue
    
    # Filter by alert type
    if alert.get('alert_type') not in selected_alert_types:
        continue
    
    # Filter by severity
    if alert.get('severity') not in selected_severities:
        continue
    
    filtered_alerts[zone_id] = alert

# Display summary statistics in a raised rectangle with squares
if filtered_alerts:
    # Calculate statistics
    total_alerts = len(filtered_alerts)
    high_severity_count = sum(1 for alert in filtered_alerts.values() if alert.get('severity') == 'HIGH')
    sensor_alerts = sum(1 for alert in filtered_alerts.values() if alert.get('alert_type') == 'SENSOR_ANOMALY')
    weather_alerts = sum(1 for alert in filtered_alerts.values() if alert.get('alert_type') == 'WEATHER_ALERT')
    
    # Create a container with raised styling
    with st.container():
        # Create a slim header bar
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 8px;
            padding: 12px 20px;
            margin: 0 0 15px 0;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        ">
            <h3 style="
                color: white; 
                margin: 0; 
                text-align: center; 
                font-size: 16px; 
                font-weight: 600;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            ">
                📊 Alert Statistics Dashboard
            </h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Create 4 columns with simple metrics in containers
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div style="
                background: linear-gradient(145deg, #ffffff, #f8f9fa);
                border-radius: 8px;
                padding: 15px;
                text-align: center;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                border: 1px solid #e9ecef;
                margin-bottom: 10px;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            ">
                <div style="font-size: 24px; color: #495057; margin-bottom: 5px;">📊</div>
                <div style="font-size: 20px; font-weight: bold; color: #212529; margin-bottom: 3px;">{total_alerts}</div>
                <div style="font-size: 11px; color: #6c757d; font-weight: 500;">Total Alerts</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style="
                background: linear-gradient(145deg, #ffffff, #f8f9fa);
                border-radius: 8px;
                padding: 15px;
                text-align: center;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                border: 1px solid #e9ecef;
                margin-bottom: 10px;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            ">
                <div style="font-size: 24px; color: #dc3545; margin-bottom: 5px;">🔥</div>
                <div style="font-size: 20px; font-weight: bold; color: #212529; margin-bottom: 3px;">{high_severity_count}</div>
                <div style="font-size: 11px; color: #6c757d; font-weight: 500;">High Severity</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div style="
                background: linear-gradient(145deg, #ffffff, #f8f9fa);
                border-radius: 8px;
                padding: 15px;
                text-align: center;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                border: 1px solid #e9ecef;
                margin-bottom: 10px;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            ">
                <div style="font-size: 24px; color: #17a2b8; margin-bottom: 5px;">🌡️</div>
                <div style="font-size: 20px; font-weight: bold; color: #212529; margin-bottom: 3px;">{sensor_alerts}</div>
                <div style="font-size: 11px; color: #6c757d; font-weight: 500;">Sensor Alerts</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div style="
                background: linear-gradient(145deg, #ffffff, #f8f9fa);
                border-radius: 8px;
                padding: 15px;
                text-align: center;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                border: 1px solid #e9ecef;
                margin-bottom: 10px;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            ">
                <div style="font-size: 24px; color: #ffc107; margin-bottom: 5px;">🌦️</div>
                <div style="font-size: 20px; font-weight: bold; color: #212529; margin-bottom: 3px;">{weather_alerts}</div>
                <div style="font-size: 11px; color: #6c757d; font-weight: 500;">Weather Alerts</div>
            </div>
            """, unsafe_allow_html=True)

# Display alerts
if not filtered_alerts:
    st.info("No alerts match the selected filters.")
else:
    # Sort alerts by severity (High > Medium) and then by timestamp
    severity_order = {"HIGH": 0, "MEDIUM": 1}
    sorted_alerts = sorted(
        filtered_alerts.items(),
        key=lambda x: (
            severity_order.get(x[1].get('severity', 'MEDIUM'), 2),
            x[1].get('alert_timestamp', '')
        ),
        reverse=True
    )
    
    # Display alerts in tabs if there are many, otherwise show them all
    if len(sorted_alerts) <= 5:
        # Show all alerts in a single view
        for zone_id, alert in sorted_alerts:
            show_alert_card(alert)
    else:
        # Create tabs for better organization
        tabs = st.tabs([f"{zone_id} ({alert.get('severity', 'MEDIUM')})" for zone_id, alert in sorted_alerts[:10]])
        for tab, (zone_id, alert) in zip(tabs, sorted_alerts[:10]):
            with tab:
                show_alert_card(alert)
        
        if len(sorted_alerts) > 10:
            st.info(f"Showing first 10 alerts. Total alerts: {len(sorted_alerts)}")

st.caption("The page automatically refreshes every 10 seconds to show real-time threshold alerts.")
