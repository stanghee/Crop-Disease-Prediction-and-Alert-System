#!/usr/bin/env python3
"""
Crop Disease Dashboard - Streamlit Application
Main dashboard for monitoring the agricultural system
"""

import streamlit as st
import redis
import os

st.set_page_config(
    page_title="🌾 Dashboard Overview",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🌾 Crop Disease Monitoring - Overview")

# Logo principale al centro
st.markdown("<br><br>", unsafe_allow_html=True)
col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    st.image("Logo/Logo Crop.png", width=250)
st.markdown("<br><br>", unsafe_allow_html=True)

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

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

# Get Redis client
redis_client = get_redis_client()

# Real-Time Data Monitoring Section
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

st.markdown("""
Questa è la pagina principale della dashboard.

Utilizza la sidebar a sinistra per navigare tra le diverse sezioni della dashboard (Analytics, ML Insights, Threshold Alerts).
""") 