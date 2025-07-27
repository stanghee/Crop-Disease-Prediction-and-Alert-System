import streamlit as st
import redis
import os
import json
from typing import List, Dict
from datetime import datetime
import pytz

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

redis_client = get_redis_client()

# Automatic refresh every 10s
st_autorefresh = st.experimental_rerun if st.experimental_get_query_params().get("refresh") else None
st.experimental_set_query_params(refresh="1")
st_autorefresh = st_autorefresh or (lambda: None)
st_autorefresh()
st.experimental_set_query_params()

# Sidebar for zone selection and filters
st.sidebar.header("Filter by Zone")
zone_ids = get_available_zone_ids(redis_client)

if not zone_ids:
    st.info("No active alerts available in Redis.")
    st.stop()

selected_zones = st.sidebar.multiselect("Select one or more zones:", zone_ids, default=zone_ids)

# Filter by alert type
st.sidebar.header("Filter by Alert Type")
alert_types = ["All", "SENSOR_ANOMALY", "WEATHER_ALERT"]
selected_alert_type = st.sidebar.selectbox("Alert Type:", alert_types)

# Filter by severity
st.sidebar.header("Filter by Severity")
severities = ["All", "HIGH", "MEDIUM"]
selected_severity = st.sidebar.selectbox("Severity:", severities)

if not selected_zones:
    st.info("Select at least one zone from the sidebar to view alerts.")
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
    
    card_style = f"border: 2px solid {border_color}; border-radius: 10px; padding: 2em 1.5em 1.5em 1.5em; margin-bottom: 1em; background-color: #fafbfc;"
    
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
    
    # Prepare card HTML content
    card_html = f'''
    <div style="{card_style}">
        <h3 style='margin-bottom:0.5em'>{icon} Alert for <span style='color:{border_color}'><b>{zone_id}</b></span></h3>
        <div style="display: flex; flex-wrap: wrap; gap: 2em; margin-bottom: 1.5em;">
            <div>
                <div style='font-size:1.1em; color:#888;'>Alert Type</div>
                <div style='font-size:1.5em; font-weight:bold'>{alert_type}</div>
            </div>
            <div>
                <div style='font-size:1.1em; color:#888;'>Severity</div>
                <div style='font-size:2em; font-weight:bold; color:{border_color}'>{severity}</div>
            </div>
            <div>
                <div style='font-size:1.1em; color:#888;'>Status</div>
                <div style='font-size:1.5em; font-weight:bold; color:{status_color}'>{status}</div>
            </div>
            <div>
                <div style='font-size:1.1em; color:#888;'>Time</div>
                <div style='font-size:1.3em; font-weight:bold'>{ts_rome}</div>
            </div>
        </div>
        <div style='margin: 1em 0 1.5em 0; padding: 1em; background: #fff; border: 1px solid #d9d9d9; border-radius: 6px; font-size: 1.1em; color: #222; word-break: break-word;'>
            <b>Message:</b><br>{alert.get('message', 'No message available')}
        </div>
    </div>
    '''
    st.markdown(card_html, unsafe_allow_html=True)
    st.caption(f"Cached at: {alert.get('cached_at', '-')}, TTL: {alert.get('cache_ttl', '-')}s")

# Get all alerts and apply filters
all_alerts = get_all_latest_alerts(redis_client)
filtered_alerts = {}

for zone_id, alert in all_alerts.items():
    if zone_id not in selected_zones:
        continue
    
    # Filter by alert type
    if selected_alert_type != "All" and alert.get('alert_type') != selected_alert_type:
        continue
    
    # Filter by severity
    if selected_severity != "All" and alert.get('severity') != selected_severity:
        continue
    
    filtered_alerts[zone_id] = alert

# Display summary statistics
if filtered_alerts:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Alerts", len(filtered_alerts))
    
    with col2:
        high_severity = sum(1 for alert in filtered_alerts.values() if alert.get('severity') == 'HIGH')
        st.metric("High Severity", high_severity)
    
    with col3:
        sensor_alerts = sum(1 for alert in filtered_alerts.values() if alert.get('alert_type') == 'SENSOR_ANOMALY')
        st.metric("Sensor Alerts", sensor_alerts)
    
    with col4:
        weather_alerts = sum(1 for alert in filtered_alerts.values() if alert.get('alert_type') == 'WEATHER_ALERT')
        st.metric("Weather Alerts", weather_alerts)

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
