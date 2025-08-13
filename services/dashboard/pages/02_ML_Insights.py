import streamlit as st
import redis
import os
import json
from typing import List, Dict
from datetime import datetime
import pytz
from streamlit_autorefresh import st_autorefresh

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

# Redis configuration (adapt if you use different variables)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Redis connection
@st.cache_resource(show_spinner=False)
def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Retrieve all available field_ids with ML predictions
def get_available_field_ids(r: redis.Redis) -> List[str]:
    keys = r.keys("predictions:latest:*")
    field_ids = [k.split(":")[-1] for k in keys]
    return sorted(field_ids)

# Retrieve the ML prediction for a field_id
def get_ml_prediction(r: redis.Redis, field_id: str) -> Dict:
    key = f"predictions:latest:{field_id}"
    data = r.get(key)
    if data:
        return json.loads(data)
    return None

# Streamlit UI
st.title("ML Insights - Real-time Anomalies")
st.markdown("*Real-time machine learning anomaly detection with severity classification and recommendations*")

redis_client = get_redis_client()

# Auto-refresh every 10 seconds for real-time updates
st_autorefresh(interval=10 * 1000, key="ml_insights_autorefresh")

# Sidebar for field selection
st.sidebar.header("Filter by agricultural field")
field_ids = get_available_field_ids(redis_client)
selected_fields = st.sidebar.multiselect("Select one or more field_id:", field_ids, default=field_ids[:1])

if not field_ids:
    st.info("No ML predictions available in Redis.")
    st.stop()
if not selected_fields:
    st.info("Select at least one field from the sidebar to view predictions.")
    st.stop()

# Function to get border color based on severity
SEVERITY_COLOR = {
    "critical": "#a61e1e",  # dark red
    "high": "#ff4d4f",      # red
    "medium": "#faad14",    # orange
}
def get_severity_color(severity):
    return SEVERITY_COLOR.get(str(severity).lower(), "#d9d9d9")  # default gray

# Function to display prediction in colored card
def show_prediction_card(pred):
    severity = pred.get('severity', '-')
    border_color = get_severity_color(severity)
    card_style = f"border: 2px solid {border_color}; border-radius: 10px; padding: 2em 1.5em 1.5em 1.5em; margin-bottom: 1em; background-color: #fafbfc;"
    # Convert timestamp to Europe/Rome local time
    ts_utc = pred.get('prediction_timestamp', '-')
    if ts_utc and ts_utc != '-':
        try:
            dt_utc = datetime.strptime(ts_utc, "%Y-%m-%dT%H:%M:%S.%fZ")
            dt_rome = dt_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Europe/Rome'))
            ts_rome = dt_rome.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            ts_rome = ts_utc
    else:
        ts_rome = "-"
    # Prepare card HTML content
    # Format anomaly score with 2 decimal places if it's a number
    anomaly_score = pred.get('anomaly_score', '-')
    if isinstance(anomaly_score, (int, float)):
        anomaly_score_display = f"{anomaly_score:.2f}"
    else:
        anomaly_score_display = str(anomaly_score)
    
    card_html = f'''
    <div style="{card_style}">
        <h3 style='margin-bottom:0.5em'>ML Prediction for <span style='color:{border_color}'><b>{pred.get('field_id', '-')}</b></span></h3>
        <div style="display: flex; flex-wrap: wrap; gap: 2em; margin-bottom: 1.5em;">
            <div>
                <div style='font-size:1.1em; color:#888;'>Anomaly Score</div>
                <div style='font-size:2em; font-weight:bold'>{anomaly_score_display}</div>
            </div>
            <div>
                <div style='font-size:1.1em; color:#888;'>Severity</div>
                <div style='font-size:2em; font-weight:bold'>{severity}</div>
            </div>
            <div>
                <div style='font-size:1.1em; color:#888;'>Time</div>
                <div style='font-size:1.3em; font-weight:bold'>{ts_rome}</div>
            </div>
        </div>
        <div style='margin: 1em 0 1.5em 0; padding: 1em; background: #fff; border: 1px solid #d9d9d9; border-radius: 6px; font-size: 1.1em; color: #222; word-break: break-word;'>
            <b>Recommendations:</b><br>{pred.get('recommendations', '-') if pred.get('recommendations', '-') else '-'}
        </div>
    </div>
    '''
    st.markdown(card_html, unsafe_allow_html=True)
    # Features outside the card, as a simple table
    features = pred.get("features", {})
    if features:
        st.markdown("**Features**")
        table_html = "<table style='border-collapse:collapse; margin-bottom:1.5em;'>"
        for k, v in features.items():
            if isinstance(v, (int, float)):
                v_disp = f"{v:.2f}"
            else:
                v_disp = v
            table_html += f"<tr><td style='padding:10px 24px; font-weight:bold; color:#333'>{k}</td><td style='padding:10px 24px; color:#222'>{v_disp}</td></tr>"
        table_html += "</table>"
        st.markdown(table_html, unsafe_allow_html=True)
    st.caption(f"Cached at: {pred.get('cached_at', '-')}, TTL: {pred.get('cache_ttl', '-')}s")

# Tabbed view
predictions = [get_ml_prediction(redis_client, fid) for fid in selected_fields]
tabs = st.tabs([f"Field {fid}" for fid in selected_fields])
for tab, pred, fid in zip(tabs, predictions, selected_fields):
    with tab:
        if not pred:
            st.warning(f"No ML prediction found for field {fid}.")
        else:
            show_prediction_card(pred)

st.caption("The page automatically refreshes every 10 seconds to show real-time ML anomalies.")
