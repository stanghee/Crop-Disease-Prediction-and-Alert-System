import streamlit as st
import requests
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Threshold alerts", layout="wide")
st.title("Threshold alerts")

# Funzione per recuperare gli alert dall'API
@st.cache_data(ttl=60)
def get_alerts():
    try:
        response = requests.get("http://crop-disease-service:8000/api/v1/alerts?active_only=false&limit=200")
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data["data"]["alerts"])
        else:
            st.error(f"API error: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Connection error: {e}")
        return pd.DataFrame()

df = get_alerts()

if df.empty:
    st.info("No alerts found.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")
fields = sorted(df["zone_id"].dropna().unique())
locations = sorted(df[df["alert_type"] == "WEATHER_ALERT"]["zone_id"].dropna().unique())
selected_fields = st.sidebar.multiselect("Select field(s)", fields, default=fields)
selected_locations = st.sidebar.multiselect("Select weather location(s)", locations, default=locations)

# Applica filtri
mask_field = df["zone_id"].isin(selected_fields)
mask_location = (df["alert_type"] != "WEATHER_ALERT") | df["zone_id"].isin(selected_locations)
df_filtered = df[mask_field & mask_location]

# Ordina per gravità e data
severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
df_filtered["severity_rank"] = df_filtered["severity"].map(severity_order)
df_filtered = df_filtered.sort_values(["zone_id", "severity_rank", "alert_timestamp"], ascending=[True, True, False])

# Funzione per colore box
def get_box_color(severity):
    return {
        "CRITICAL": "#ff4d4d",
        "HIGH": "#ffa500",
        "MEDIUM": "#fff700",
        "LOW": "#90ee90"
    }.get(severity, "#f0f0f0")

# Sezione Latest Alert
st.subheader("Latest alert for each field/location")
latest_alerts = (
    df_filtered.sort_values(["zone_id", "alert_timestamp"], ascending=[True, False])
    .groupby("zone_id", as_index=False).first()
)

if latest_alerts.empty:
    st.info("No latest alerts found for the selected filters.")
else:
    for _, row in latest_alerts.iterrows():
        with st.container():
            st.markdown(f"""
                <div style='background-color:{get_box_color(row['severity'])};padding:1em;border-radius:8px;margin-bottom:0.5em;'>
                    <b>Field/Location:</b> {row['zone_id']}<br>
                    <b>Type:</b> {row['alert_type']}<br>
                    <b>Severity:</b> {row['severity']}<br>
                    <b>Timestamp:</b> {row['alert_timestamp']}<br>
                    <b>Message:</b> {row['message']}<br>
                    <b>Status:</b> {row['status']}
                </div>
            """, unsafe_allow_html=True)

# Sezione storico
st.subheader("Alert history")
show_cols = ["zone_id", "alert_type", "severity", "alert_timestamp", "message", "status"]

def highlight_severity(val):
    color = {
        "CRITICAL": "background-color: #ff4d4d; color: white;",
        "HIGH": "background-color: #ffa500; color: black;",
        "MEDIUM": "background-color: #fff700; color: black;",
        "LOW": "background-color: #90ee90; color: black;"
    }.get(val, "")
    return color

# Ordina la tabella storica per timestamp decrescente
history_df = df_filtered[show_cols].sort_values("alert_timestamp", ascending=False)

st.dataframe(
    history_df.style.applymap(highlight_severity, subset=["severity"]),
    use_container_width=True,
    hide_index=True
) 