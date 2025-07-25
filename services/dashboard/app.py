#!/usr/bin/env python3
"""
Crop Disease Dashboard - Streamlit Application
Main dashboard for monitoring the agricultural system
"""

import streamlit as st

st.set_page_config(
    page_title="🌾 Dashboard Overview",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🌾 Crop Disease Monitoring - Overview")
st.markdown("""
Questa è la pagina principale della dashboard.

Utilizza la sidebar a sinistra per navigare tra le diverse sezioni della dashboard (Analytics, ML Insights, Threshold Alerts).
""") 