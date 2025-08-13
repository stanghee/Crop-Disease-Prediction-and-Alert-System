#!/usr/bin/env python3
"""
Data Insights Page - Historical Analysis of ML Predictions and Alerts
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Page configuration
st.set_page_config(
    page_title="Data Insights - Historical Analysis",
    page_icon="📊",
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

# API Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://crop-disease-service:8000/api/v1")

def fetch_ml_predictions(days_back: int = 7, field_id: str = None) -> List[Dict]:
    """Fetch ML predictions from API"""
    try:
        params = {"days_back": days_back}
        if field_id:
            params["field_id"] = field_id
        
        response = requests.get(f"{API_BASE_URL}/predictions", params=params, timeout=10)
        if response.status_code == 200:
            return response.json()["data"]["predictions"]
        else:
            st.error(f"Error fetching ML predictions: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Error connecting to API: {e}")
        return []

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_ml_statistics() -> Dict[str, Any]:
    """Fetch ML statistics from API"""
    try:
        response = requests.get(f"{API_BASE_URL}/predictions/statistics", timeout=10)
        if response.status_code == 200:
            return response.json()["data"]
        else:
            st.error(f"Error fetching ML statistics: {response.status_code}")
            return {}
    except Exception as e:
        st.error(f"Error connecting to API: {e}")
        return {}

def fetch_historical_alerts(days_back: int = 7, zone_id: str = None) -> List[Dict]:
    """Fetch historical alerts from API"""
    try:
        params = {"days_back": days_back}
        if zone_id:
            params["zone_id"] = zone_id
        
        response = requests.get(f"{API_BASE_URL}/alerts/historical", params=params, timeout=10)
        if response.status_code == 200:
            return response.json()["data"]["alerts"]
        else:
            st.error(f"Error fetching historical alerts: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Error connecting to API: {e}")
        return []

def create_ml_anomaly_barplot(predictions_df: pd.DataFrame) -> go.Figure:
    """Create bar plot of anomaly counts by field and severity"""
    if predictions_df.empty:
        return go.Figure().add_annotation(text="No data available", showarrow=False)
    
    # Filter only anomalies
    anomalies_df = predictions_df[predictions_df['is_anomaly'] == True]
    
    if anomalies_df.empty:
        return go.Figure().add_annotation(text="No anomalies detected", showarrow=False)
    
    # Count anomalies by field and severity
    anomaly_counts = anomalies_df.groupby(['field_id', 'severity']).size().reset_index(name='count')
    
    # Color mapping for severity
    severity_colors = {
        'LOW': '#52c41a',
        'MEDIUM': '#faad14', 
        'HIGH': '#ff4d4f',
        'CRITICAL': '#a61e1e'
    }
    
    fig = px.bar(
        anomaly_counts,
        x='field_id',
        y='count',
        color='severity',
        color_discrete_map=severity_colors,
        title='Anomaly Count by Field and Severity',
        labels={'field_id': 'Field ID', 'count': 'Number of Anomalies', 'severity': 'Severity'}
    )
    
    fig.update_layout(
        xaxis_title="Field ID",
        yaxis_title="Number of Anomalies",
        showlegend=True
    )
    
    return fig

def create_ml_anomaly_score_chart(predictions_df: pd.DataFrame) -> go.Figure:
    """Create chart showing average anomaly score by field"""
    if predictions_df.empty:
        return go.Figure().add_annotation(text="No data available", showarrow=False)
    
    # Calculate average anomaly score by field
    avg_scores = predictions_df.groupby('field_id')['anomaly_score'].mean().reset_index()
    
    fig = px.bar(
        avg_scores,
        x='field_id',
        y='anomaly_score',
        title='Average Anomaly Score by Field',
        labels={'field_id': 'Field ID', 'anomaly_score': 'Average Anomaly Score'},
        color='anomaly_score',
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        xaxis_title="Field ID",
        yaxis_title="Average Anomaly Score",
        showlegend=False
    )
    
    return fig

def create_ml_temporal_chart(predictions_df: pd.DataFrame) -> go.Figure:
    """Create temporal chart showing anomalies over time by field"""
    if predictions_df.empty:
        return go.Figure().add_annotation(text="No data available", showarrow=False)
    
    # Convert timestamp to datetime
    predictions_df['prediction_timestamp'] = pd.to_datetime(predictions_df['prediction_timestamp'])
    
    fig = go.Figure()
    
    # Add line for each field
    for field_id in predictions_df['field_id'].unique():
        field_data = predictions_df[predictions_df['field_id'] == field_id]
        
        fig.add_trace(go.Scatter(
            x=field_data['prediction_timestamp'],
            y=field_data['is_anomaly'].astype(int),
            mode='lines+markers',
            name=field_id,
            line=dict(width=2),
            marker=dict(size=6)
        ))
    
    fig.update_layout(
        title='Anomaly Detection Over Time by Field',
        xaxis_title='Timestamp',
        yaxis_title='Anomaly Detected (1=Yes, 0=No)',
        yaxis=dict(tickmode='array', tickvals=[0, 1], ticktext=['No', 'Yes']),
        showlegend=True
    )
    
    return fig

def create_alerts_temporal_chart(alerts_df: pd.DataFrame) -> go.Figure:
    """Create temporal chart showing alerts over time by field"""
    if alerts_df.empty:
        return go.Figure().add_annotation(text="No data available", showarrow=False)
    
    # Convert timestamp to datetime
    alerts_df['alert_timestamp'] = pd.to_datetime(alerts_df['alert_timestamp'])
    
    # Create binary indicator for alerts
    alerts_df['alert_occurred'] = 1
    
    fig = go.Figure()
    
    # Add line for each zone_id (field)
    for zone_id in alerts_df['zone_id'].unique():
        zone_data = alerts_df[alerts_df['zone_id'] == zone_id]
        
        fig.add_trace(go.Scatter(
            x=zone_data['alert_timestamp'],
            y=zone_data['alert_occurred'],
            mode='lines+markers',
            name=zone_id,
            line=dict(width=2),
            marker=dict(size=8)
        ))
    
    fig.update_layout(
        title='Alert Occurrences Over Time by Field',
        xaxis_title='Timestamp',
        yaxis_title='Alert Occurred (1=Yes)',
        yaxis=dict(tickmode='array', tickvals=[0, 1], ticktext=['No Alert', 'Alert']),
        showlegend=True
    )
    
    return fig

def create_alerts_barplot(alerts_df: pd.DataFrame) -> go.Figure:
    """Create bar plot of alert counts by field and severity"""
    if alerts_df.empty:
        return go.Figure().add_annotation(text="No data available", showarrow=False)
    
    # Count alerts by zone_id and severity
    alert_counts = alerts_df.groupby(['zone_id', 'severity']).size().reset_index(name='count')
    
    # Color mapping for severity
    severity_colors = {
        'LOW': '#52c41a',
        'MEDIUM': '#faad14',
        'HIGH': '#ff4d4f',
        'CRITICAL': '#a61e1e'
    }
    
    fig = px.bar(
        alert_counts,
        x='zone_id',
        y='count',
        color='severity',
        color_discrete_map=severity_colors,
        title='Alert Count by Field and Severity',
        labels={'zone_id': 'Field/Zone ID', 'count': 'Number of Alerts', 'severity': 'Severity'}
    )
    
    fig.update_layout(
        xaxis_title="Field/Zone ID",
        yaxis_title="Number of Alerts",
        showlegend=True
    )
    
    return fig

def create_feature_statistics_table(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """Create feature statistics table by field"""
    if predictions_df.empty:
        return pd.DataFrame()
    
    # Extract features from JSON if available
    feature_stats = []
    
    for field_id in predictions_df['field_id'].unique():
        field_data = predictions_df[predictions_df['field_id'] == field_id]
        
        stats = {
            'Field ID': field_id,
            'Total Predictions': len(field_data),
            'Anomalies': field_data['is_anomaly'].sum(),
            'Anomaly Rate (%)': round(field_data['is_anomaly'].mean() * 100, 2),
            'Avg Anomaly Score': round(field_data['anomaly_score'].mean(), 3),
            'Max Anomaly Score': round(field_data['anomaly_score'].max(), 3),
            'Min Anomaly Score': round(field_data['anomaly_score'].min(), 3)
        }
        
        feature_stats.append(stats)
    
    return pd.DataFrame(feature_stats)

# Main UI
st.title("Data Insights - Historical Analysis")
st.markdown("*Historical analysis of ML predictions and alerts with statistical insights and data visualization*")
st.markdown("---")

# Sidebar controls
with st.sidebar:
    st.header("Analysis Settings")
    
    # Time range selection
    st.subheader("Time Range")
    days_back = st.selectbox(
        "Select time range",
        [1, 3, 7, 14, 30],
        index=2,  # Default to 7 days
        format_func=lambda x: f"Last {x} day{'s' if x > 1 else ''}"
    )
    
    # Field filter for ML
    st.subheader("ML Analysis Filters")
    ml_field_filter = st.text_input("Filter by Field ID (ML)", placeholder="e.g., field_01")
    
    # Zone filter for Alerts
    st.subheader("Alert Analysis Filters")
    alert_zone_filter = st.text_input("Filter by Zone/Field ID (Alerts)", placeholder="e.g., field_01")

# Fetch data
with st.spinner("Loading historical data..."):
    ml_predictions = fetch_ml_predictions(days_back, ml_field_filter if ml_field_filter else None)
    ml_statistics = fetch_ml_statistics()
    historical_alerts = fetch_historical_alerts(days_back, alert_zone_filter if alert_zone_filter else None)

# Debug info
st.sidebar.markdown("---")
st.sidebar.subheader("Debug Info")
st.sidebar.caption(f"Days back: {days_back}")
st.sidebar.caption(f"ML field filter: {ml_field_filter or 'None'}")
st.sidebar.caption(f"Alert zone filter: {alert_zone_filter or 'None'}")
st.sidebar.caption(f"Total alerts fetched: {len(historical_alerts) if historical_alerts else 0}")

# Convert to DataFrames
predictions_df = pd.DataFrame(ml_predictions) if ml_predictions else pd.DataFrame()
alerts_df = pd.DataFrame(historical_alerts) if historical_alerts else pd.DataFrame()

# ML PREDICTIONS SECTION
st.header("Machine Learning Predictions Analysis")

if not predictions_df.empty:
    # Create tabs for ML analysis
    ml_tab1, ml_tab2, ml_tab3, ml_tab4 = st.tabs(["📊 Anomaly Counts", "📈 Anomaly Scores", "⏰ Temporal Analysis", "📋 Statistics"])
    
    with ml_tab1:
        st.subheader("Anomaly Count by Field and Severity")
        fig_ml_bar = create_ml_anomaly_barplot(predictions_df)
        st.plotly_chart(fig_ml_bar, use_container_width=True)
    
    with ml_tab2:
        st.subheader("Average Anomaly Score by Field")
        fig_ml_score = create_ml_anomaly_score_chart(predictions_df)
        st.plotly_chart(fig_ml_score, use_container_width=True)
    
    with ml_tab3:
        st.subheader("Anomaly Detection Over Time")
        fig_ml_temporal = create_ml_temporal_chart(predictions_df)
        st.plotly_chart(fig_ml_temporal, use_container_width=True)
    
    with ml_tab4:
        st.subheader("Feature Statistics by Field")
        feature_stats_df = create_feature_statistics_table(predictions_df)
        if not feature_stats_df.empty:
            st.dataframe(feature_stats_df, use_container_width=True)
        else:
            st.info("No feature statistics available")
else:
    st.info("No ML prediction data available for the selected time range and filters.")

st.markdown("---")

# ALERTS SECTION
st.header("Alert Analysis")

if not alerts_df.empty:
    # Create tabs for Alert analysis
    alert_tab1, alert_tab2 = st.tabs(["⏰ Temporal Analysis", "📊 Alert Counts"])
    
    with alert_tab1:
        st.subheader("Alert Occurrences Over Time")
        fig_alert_temporal = create_alerts_temporal_chart(alerts_df)
        st.plotly_chart(fig_alert_temporal, use_container_width=True)
    
    with alert_tab2:
        st.subheader("Alert Count by Field and Severity")
        fig_alert_bar = create_alerts_barplot(alerts_df)
        st.plotly_chart(fig_alert_bar, use_container_width=True)
    
    # Summary statistics
    st.subheader("Alert Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Alerts", len(alerts_df))
    
    with col2:
        high_severity = len(alerts_df[alerts_df['severity'] == 'HIGH'])
        st.metric("High Severity", high_severity)
    
    with col3:
        unique_zones = alerts_df['zone_id'].nunique()
        st.metric("Affected Fields", unique_zones)
    
    with col4:
        sensor_alerts = len(alerts_df[alerts_df['alert_type'] == 'SENSOR_ANOMALY'])
        st.metric("Sensor Alerts", sensor_alerts)

else:
    st.info("No alert data available for the selected time range and filters.")

# Footer
st.markdown("---")
st.caption(f"Data analysis generated on {datetime.now().strftime('%d/%m/%Y at %H:%M:%S')}")
st.caption("📊 **Data Source**: PostgreSQL Database via API | 🤖 **ML Data**: ml_predictions table | 🚨 **Alert Data**: alerts table")
