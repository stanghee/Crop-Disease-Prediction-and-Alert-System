#!/usr/bin/env python3
"""
Data Insights Page - Advanced Analytics and Performance Monitoring
Provides comprehensive insights into alerts, ML predictions, and system performance
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import psycopg2
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json

# Page configuration
st.set_page_config(
    page_title="Data Insights - Crop Disease Dashboard",
    page_icon="📈",
    layout="wide"
)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'crop_disease_ml'),
    'user': os.getenv('POSTGRES_USER', 'ml_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'ml_password')
}

# API endpoints
CROP_DISEASE_API = os.getenv('CROP_DISEASE_API_URL', 'http://crop-disease-service:8000/api/v1')
ML_API = 'http://ml-anomaly-service:8002'

# Title and description
st.title("📈 Data Insights Dashboard")
st.markdown("---")
st.markdown("""
Advanced analytics and performance monitoring for agricultural data. 
This dashboard provides deep insights into alert patterns, ML prediction accuracy, 
and system operational metrics.
""")

# Sidebar filters
with st.sidebar:
    st.header("🔍 Filters & Controls")
    
    # Time range selection
    st.subheader("Time Range")
    time_range = st.selectbox(
        "Select time range",
        ["Today", "Last 24 hours", "Last 7 days", "Last 30 days", "Custom"],
        index=0  # Default to "Today"
    )
    
    if time_range == "Custom":
        start_date = st.date_input("Start date", value=datetime.now().date() - timedelta(days=7))
        end_date = st.date_input("End date", value=datetime.now().date())
    else:
        # Calculate dates based on selection
        if time_range == "Today":
            start_date = datetime.now().date()
            end_date = datetime.now().date()
        elif time_range == "Last 24 hours":
            start_date = datetime.now().date() - timedelta(days=1)
            end_date = datetime.now().date()
        elif time_range == "Last 7 days":
            start_date = datetime.now().date() - timedelta(days=7)
            end_date = datetime.now().date()
        else:  # Last 30 days
            start_date = datetime.now().date() - timedelta(days=30)
            end_date = datetime.now().date()
    
    # Field selection
    st.subheader("Field Selection")
    show_all_fields = st.checkbox("Show all fields", value=True)
    
    if not show_all_fields:
        selected_fields = st.multiselect(
            "Select specific fields",
            ["field_01", "field_02", "field_03"],
            default=["field_01", "field_02"]
        )
    else:
        selected_fields = ["field_01", "field_02", "field_03"]
    
    # Alert type filter
    st.subheader("Alert Types")
    alert_types = st.multiselect(
        "Filter by alert type",
        ["SENSOR_ANOMALY", "WEATHER_ALERT", "DISEASE_DETECTED", "HIGH_RISK", "DATA_QUALITY"],
        default=["SENSOR_ANOMALY", "WEATHER_ALERT"]
    )
    
    # Refresh button
    st.subheader("Data Refresh")
    if st.button("🔄 Refresh Data", type="primary"):
        st.rerun()



# Data retrieval functions
@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_all_alerts_data():
    """Get all alerts data from PostgreSQL (for debugging)"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        query = """
        SELECT 
            id, zone_id, alert_timestamp, alert_type, severity, 
            message, status, created_at
        FROM alerts 
        ORDER BY alert_timestamp DESC
        LIMIT 100
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            df['alert_timestamp'] = pd.to_datetime(df['alert_timestamp'])
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        return df
        
    except Exception as e:
        st.error(f"❌ Error retrieving all alerts data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_alerts_data(start_date, end_date, alert_types=None):
    """Get alerts data from PostgreSQL"""
    try:
        conn = get_db_connection()
        if not conn:
            return pd.DataFrame()
        
        query = """
        SELECT 
            id, zone_id, alert_timestamp, alert_type, severity, 
            message, status, created_at
        FROM alerts 
        WHERE DATE(alert_timestamp) >= %s AND DATE(alert_timestamp) <= %s
        """
        
        params = [start_date, end_date]
        
        if alert_types:
            placeholders = ','.join(['%s'] * len(alert_types))
            query += f" AND alert_type IN ({placeholders})"
            params.extend(alert_types)
        
        query += " ORDER BY alert_timestamp DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if not df.empty:
            df['alert_timestamp'] = pd.to_datetime(df['alert_timestamp'])
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        return df
        
    except Exception as e:
        st.error(f"❌ Error retrieving alerts data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_ml_predictions_data(start_date, end_date, field_ids=None):
    """Get ML predictions data from PostgreSQL"""
    try:
        # Create fresh connection for each query
        conn = psycopg2.connect(**DB_CONFIG)
        
        query = """
        SELECT 
            id, field_id, location, prediction_timestamp, anomaly_score,
            is_anomaly, severity, recommendations, model_version, features
        FROM ml_predictions 
        WHERE DATE(prediction_timestamp) >= %s AND DATE(prediction_timestamp) <= %s
        """
        
        params = [start_date, end_date]
        
        if field_ids:
            placeholders = ','.join(['%s'] * len(field_ids))
            query += f" AND field_id IN ({placeholders})"
            params.extend(field_ids)
        
        query += " ORDER BY prediction_timestamp DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if not df.empty:
            df['prediction_timestamp'] = pd.to_datetime(df['prediction_timestamp'])
        
        return df
        
    except Exception as e:
        st.error(f"❌ Error retrieving ML predictions data: {e}")
        return pd.DataFrame()

# Main content
st.subheader("📊 Data Overview")

# Get data
alerts_df = get_alerts_data(start_date, end_date, alert_types)
ml_df = get_ml_predictions_data(start_date, end_date, selected_fields)

# Debug section - Show all available data
with st.expander("🔍 Debug - All Available Data"):
    st.markdown("**All Alerts (Last 100 records):**")
    all_alerts_df = get_all_alerts_data()
    if not all_alerts_df.empty:
        st.dataframe(all_alerts_df.head(10), use_container_width=True)
        st.info(f"Total alerts in database: {len(all_alerts_df)}")
    else:
        st.warning("No alerts found in database")
    
    st.markdown("**Today's Data:**")
    today_alerts = get_alerts_data(datetime.now().date(), datetime.now().date())
    st.info(f"Alerts today: {len(today_alerts)}")
    
    st.markdown("**Selected Date Range:**")
    st.info(f"From: {start_date} To: {end_date}")
    
    st.markdown("**Filtered Data:**")
    st.info(f"Alerts in selected range: {len(alerts_df)}")
    st.info(f"ML predictions in selected range: {len(ml_df)}")

# Key metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Alerts", len(alerts_df))
    
with col2:
    active_alerts = len(alerts_df[alerts_df['status'] == 'ACTIVE']) if not alerts_df.empty else 0
    st.metric("Active Alerts", active_alerts)
    
with col3:
    st.metric("ML Predictions", len(ml_df))
    
with col4:
    anomalies = len(ml_df[ml_df['is_anomaly'] == True]) if not ml_df.empty else 0
    st.metric("Anomalies Detected", anomalies)

st.markdown("---")

# Alert Analytics Section
st.subheader("🚨 Alert Analytics")

if not alerts_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Alert timeline
        st.markdown("**Alert Timeline**")
        alerts_timeline = alerts_df.groupby(alerts_df['alert_timestamp'].dt.date).size().reset_index()
        alerts_timeline.columns = ['Date', 'Alert Count']
        
        fig_timeline = px.line(
            alerts_timeline, 
            x='Date', 
            y='Alert Count',
            title="Daily Alert Count"
        )
        fig_timeline.update_layout(height=300)
        st.plotly_chart(fig_timeline, use_container_width=True)
    
    with col2:
        # Severity distribution
        st.markdown("**Alert Severity Distribution**")
        severity_counts = alerts_df['severity'].value_counts()
        
        fig_severity = px.pie(
            values=severity_counts.values,
            names=severity_counts.index,
            title="Alert Severity Distribution"
        )
        fig_severity.update_layout(height=300)
        st.plotly_chart(fig_severity, use_container_width=True)
    
    # Alert type analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Alert Types**")
        alert_type_counts = alerts_df['alert_type'].value_counts()
        
        fig_types = px.bar(
            x=alert_type_counts.index,
            y=alert_type_counts.values,
            title="Alert Types Distribution"
        )
        fig_types.update_layout(height=300)
        st.plotly_chart(fig_types, use_container_width=True)
    
    with col2:
        st.markdown("**Alert Status**")
        status_counts = alerts_df['status'].value_counts()
        
        fig_status = px.bar(
            x=status_counts.index,
            y=status_counts.values,
            title="Alert Status Distribution"
        )
        fig_status.update_layout(height=300)
        st.plotly_chart(fig_status, use_container_width=True)

else:
    st.info("No alert data available for the selected time range and filters.")

st.markdown("---")

# ML Predictions Analytics Section
st.subheader("🤖 ML Predictions Analytics")

if not ml_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Anomaly score distribution
        st.markdown("**Anomaly Score Distribution**")
        fig_anomaly = px.histogram(
            ml_df,
            x='anomaly_score',
            nbins=20,
            title="Anomaly Score Distribution"
        )
        fig_anomaly.update_layout(height=300)
        st.plotly_chart(fig_anomaly, use_container_width=True)
    
    with col2:
        # Anomaly vs Normal predictions
        st.markdown("**Anomaly Detection Results**")
        anomaly_counts = ml_df['is_anomaly'].value_counts()
        
        fig_anomaly_pie = px.pie(
            values=anomaly_counts.values,
            names=['Normal', 'Anomaly'],
            title="Anomaly Detection Results"
        )
        fig_anomaly_pie.update_layout(height=300)
        st.plotly_chart(fig_anomaly_pie, use_container_width=True)
    
    # Field performance
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Field Performance - Anomaly Rate**")
        field_anomaly_rate = ml_df.groupby('field_id')['is_anomaly'].mean().reset_index()
        field_anomaly_rate.columns = ['Field', 'Anomaly Rate']
        
        fig_field = px.bar(
            field_anomaly_rate,
            x='Field',
            y='Anomaly Rate',
            title="Anomaly Rate by Field"
        )
        fig_field.update_layout(height=300)
        st.plotly_chart(fig_field, use_container_width=True)
    
    with col2:
        st.markdown("**ML Severity Distribution**")
        ml_severity_counts = ml_df['severity'].value_counts()
        
        fig_ml_severity = px.pie(
            values=ml_severity_counts.values,
            names=ml_severity_counts.index,
            title="ML Prediction Severity"
        )
        fig_ml_severity.update_layout(height=300)
        st.plotly_chart(fig_ml_severity, use_container_width=True)

else:
    st.info("No ML prediction data available for the selected time range and filters.")

st.markdown("---")

# Data Tables Section
st.subheader("📋 Detailed Data Tables")

tab1, tab2 = st.tabs(["Alerts Data", "ML Predictions"])

with tab1:
    if not alerts_df.empty:
        st.dataframe(alerts_df, use_container_width=True)
    else:
        st.info("No alerts data to display")

with tab2:
    if not ml_df.empty:
        st.dataframe(ml_df, use_container_width=True)
    else:
        st.info("No ML predictions data to display")

# Footer
st.markdown("---")
st.caption(f"Data Insights dashboard generated on {datetime.now().strftime('%d/%m/%Y at %H:%M:%S')}")
st.caption("📊 **Data Sources**: PostgreSQL (alerts, ml_predictions) | 🔗 **APIs**: Crop Disease Service, ML Anomaly Service") 