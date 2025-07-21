#!/usr/bin/env python3
"""
ML Insights Page - Machine Learning Predictions and Anomaly Detection
Shows real-time ML predictions from ml-anomalies Kafka topic
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer
import os
from streamlit_autorefresh import st_autorefresh
import numpy as np

# Page configuration
st.set_page_config(
    page_title="ML Insights - Crop Disease Dashboard",
    page_icon=None,
    layout="wide"
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

st.title("ML Insights - Machine Learning Predictions")
st.markdown("---")

# Sidebar controls
with st.sidebar:
    st.header("ML Insights Settings")
    

    
    # Severity filters
    st.subheader("Severity Filter")
    severity_filter = st.multiselect(
        "Select severity levels",
        ["CRITICAL", "HIGH", "NORMAL"],
        default=["CRITICAL", "HIGH", "NORMAL"]
    )
    
    # Time range
    st.subheader("Time Range")
    time_range = st.selectbox(
        "Select time range",
        ["Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days"],
        index=1
    )
    
    # Model version filter
    st.subheader("Model Version")
    show_model_version = st.checkbox("Show model version info", value=True)
    
    # Auto-refresh
    st.subheader("Refresh Settings")
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=True)
    if auto_refresh:
        st_autorefresh(interval=30 * 1000, key="ml_insights_autorefresh")

# Cache for ML anomalies data
@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_ml_anomalies_data():
    """Get ML anomalies data from Kafka topic"""
    try:
        # ML anomalies consumer
        ml_consumer = KafkaConsumer(
            'ml-anomalies',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='dashboard_ml_consumer',
            consumer_timeout_ms=5000  # 5 seconds timeout
        )
        
        if not ml_consumer.bootstrap_connected():
            st.error("Kafka connection failed for ml-anomalies topic!")
            return []
        
        # Collect ML anomalies data
        ml_data = []
        try:
            for message in ml_consumer:
                ml_data.append(message.value)
                if len(ml_data) >= 1000:  # Limit to 1000 messages
                    break
        except Exception as e:
            st.error(f"Error reading ML anomalies data: {e}")
        finally:
            ml_consumer.close()
        
        return ml_data
    except Exception as e:
        st.error(f"Error retrieving ML anomalies data: {e}")
        return []

def process_ml_data(ml_data):
    """Process and filter ML anomalies data"""
    if not ml_data:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(ml_data)
    
    # Extract features from nested structure
    if 'features' in df.columns:
        # Extract features into separate columns
        features_df = pd.json_normalize(df['features'])
        df = pd.concat([df.drop('features', axis=1), features_df], axis=1)
    
    # Convert timestamp columns
    if 'prediction_timestamp' in df.columns:
        df['prediction_timestamp'] = pd.to_datetime(df['prediction_timestamp'])
        # Convert to timezone-naive datetime for comparison
        df['prediction_timestamp'] = df['prediction_timestamp'].dt.tz_localize(None)
    
    # Filter by time range
    if time_range == "Last 1 hour":
        cutoff_time = datetime.now() - timedelta(hours=1)
    elif time_range == "Last 6 hours":
        cutoff_time = datetime.now() - timedelta(hours=6)
    elif time_range == "Last 24 hours":
        cutoff_time = datetime.now() - timedelta(hours=24)
    elif time_range == "Last 7 days":
        cutoff_time = datetime.now() - timedelta(days=7)
    else:
        cutoff_time = datetime.now() - timedelta(hours=6)
    
    if 'prediction_timestamp' in df.columns:
        df = df[df['prediction_timestamp'] >= cutoff_time]
    

    
    # Filter by severity
    if severity_filter:
        df = df[df['severity'].isin(severity_filter)]
    
    return df

def create_anomaly_score_chart(df):
    """Create anomaly score visualization"""
    if df.empty:
        return None
    
    fig = px.scatter(
        df,
        x='field_id',
        y='anomaly_score',
        color='severity',
        size='anomaly_score',
        hover_data=['location', 'recommendations', 'model_version'],
        title="Anomaly Scores by Field and Severity",
        color_discrete_map={
            'CRITICAL': '#d62728',
            'HIGH': '#ff7f0e',
            'NORMAL': '#2ca02c'
        }
    )
    fig.update_layout(height=400)
    return fig

def create_severity_distribution_chart(df):
    """Create severity distribution chart"""
    if df.empty:
        return None
    
    severity_counts = df['severity'].value_counts()
    
    fig = px.pie(
        values=severity_counts.values,
        names=severity_counts.index,
        title="Distribution of Anomaly Severity",
        color_discrete_map={
            'CRITICAL': '#d62728',
            'HIGH': '#ff7f0e',
            'NORMAL': '#2ca02c'
        }
    )
    fig.update_layout(height=400)
    return fig

def create_temporal_analysis_chart(df):
    """Create temporal analysis of anomalies"""
    if df.empty or 'processing_timestamp' not in df.columns:
        return None
    
    # Group by hour and severity
    df_hourly = df.copy()
    df_hourly['hour'] = df_hourly['processing_timestamp'].dt.floor('H')
    hourly_counts = df_hourly.groupby(['hour', 'severity']).size().reset_index(name='count')
    
    fig = px.line(
        hourly_counts,
        x='hour',
        y='count',
        color='severity',
        title="Anomaly Trends Over Time",
        color_discrete_map={
            'CRITICAL': '#d62728',
            'HIGH': '#ff7f0e',
            'NORMAL': '#2ca02c'
        }
    )
    fig.update_layout(height=400)
    return fig

def create_environmental_correlation_chart(df):
    """Create environmental correlation analysis"""
    if df.empty:
        return None
    
    # Check which environmental columns are available (from features)
    available_env_cols = []
    for col in ['temperature', 'humidity', 'soil_ph']:
        if col in df.columns:
            available_env_cols.append(col)
    
    # If no environmental data available, return None
    if not available_env_cols or 'anomaly_score' not in df.columns:
        return None
    
    # Create correlation matrix for available environmental factors
    env_cols = available_env_cols + ['anomaly_score']
    env_df = df[env_cols].dropna()
    
    if env_df.empty:
        return None
    
    corr_matrix = env_df.corr()
    
    fig = px.imshow(
        corr_matrix,
        title="Environmental Factors Correlation with Anomaly Score",
        color_continuous_scale='RdBu',
        aspect='auto'
    )
    fig.update_layout(height=400)
    return fig

def create_recommendations_summary(df):
    """Create recommendations summary"""
    if df.empty or 'recommendations' not in df.columns:
        return None
    
    # Extract unique recommendations
    all_recommendations = []
    for recs in df['recommendations'].dropna():
        if isinstance(recs, str):
            all_recommendations.extend([r.strip() for r in recs.split('|')])
    
    if not all_recommendations:
        return None
    
    # Count recommendations
    rec_counts = pd.Series(all_recommendations).value_counts().head(10)
    
    fig = px.bar(
        x=rec_counts.values,
        y=rec_counts.index,
        orientation='h',
        title="Top 10 Most Common Recommendations",
        labels={'x': 'Count', 'y': 'Recommendation'}
    )
    fig.update_layout(height=400)
    return fig

# Main layout
st.header("Real-time ML Anomaly Detection")

# Get ML anomalies data
ml_data = get_ml_anomalies_data()
df = process_ml_data(ml_data)

# KPI Overview
st.subheader("ML Insights Overview")

if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_anomalies = len(df)
        st.metric(
            label="Total Anomalies",
            value=total_anomalies,
            delta=f"Last {time_range.lower()}"
        )
    
    with col2:
        critical_count = len(df[df['severity'] == 'CRITICAL'])
        st.metric(
            label="Critical Anomalies",
            value=critical_count,
            delta="Requires immediate attention" if critical_count > 0 else "No critical issues"
        )
    
    with col3:
        avg_score = df['anomaly_score'].mean() if 'anomaly_score' in df.columns else 0
        st.metric(
            label="Average Anomaly Score",
            value=f"{avg_score:.2f}",
            delta="Higher = more anomalous"
        )
    
    with col4:
        unique_fields = df['field_id'].nunique()
        st.metric(
            label="Fields with Anomalies",
            value=unique_fields,
            delta="Affected fields"
        )
    
    st.markdown("---")
    
    # Main visualizations
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("Anomaly Analysis")
        
        # Anomaly score chart
        anomaly_chart = create_anomaly_score_chart(df)
        if anomaly_chart:
            st.plotly_chart(anomaly_chart, use_container_width=True)
        
        # Temporal analysis
        temporal_chart = create_temporal_analysis_chart(df)
        if temporal_chart:
            st.plotly_chart(temporal_chart, use_container_width=True)
    
    with col_right:
        st.subheader("Severity Distribution")
        
        # Severity distribution
        severity_chart = create_severity_distribution_chart(df)
        if severity_chart:
            st.plotly_chart(severity_chart, use_container_width=True)
    
        # Recommendations section
    st.markdown("---")
    st.subheader("Recommendations")
    
    # Show recommendations in a more readable format
    if 'recommendations' in df.columns and not df.empty:
        st.write("**Latest Recommendations by Field:**")
        
        for field_id in df['field_id'].unique():
            field_data = df[df['field_id'] == field_id].iloc[-1]  # Get latest recommendation
            if pd.notna(field_data['recommendations']):
                with st.expander(f"Field {field_id} - {field_data['location']} (Severity: {field_data['severity']})"):
                    recommendations = field_data['recommendations'].split('|')
                    for i, rec in enumerate(recommendations, 1):
                        rec = rec.strip()
                        if rec:
                            st.write(f"**{i}.** {rec}")
    
    # Detailed anomalies table
    st.markdown("---")
    st.subheader("Detailed Anomalies")
    
    # Prepare table data
    table_cols = ['field_id', 'location', 'severity', 'anomaly_score']
    if show_model_version and 'model_version' in df.columns:
        table_cols.append('model_version')
    if 'prediction_timestamp' in df.columns:
        table_cols.append('prediction_timestamp')
    
    # Add environmental data if available
    env_cols = ['temperature', 'humidity', 'soil_ph']
    for col in env_cols:
        if col in df.columns:
            table_cols.append(col)
    
    display_df = df[table_cols].copy()
    
    # Format timestamp
    if 'prediction_timestamp' in display_df.columns:
        display_df['prediction_timestamp'] = display_df['prediction_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Rename columns for display
    display_df.columns = [col.replace('_', ' ').title() for col in display_df.columns]
    
    st.dataframe(display_df, use_container_width=True)
    

    
    # Download button
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download ML Anomalies Data (CSV)",
        data=csv,
        file_name=f"ml_anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

else:
    st.warning("No ML anomalies data available")
    st.info("""
    This could be due to:
    - No anomalies detected in the selected time range
    - Kafka connection issues
    - ML service not running
    - No data being produced to ml-anomalies topic
    """)

# Footer with system info
st.markdown("---")
st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*") 