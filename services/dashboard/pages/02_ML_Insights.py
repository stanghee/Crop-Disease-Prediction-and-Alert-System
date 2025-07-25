import streamlit as st
import redis
import os
import json
from typing import List, Dict

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

redis_client = get_redis_client()

# Automatic refresh every 10s
st_autorefresh = st.experimental_rerun if st.experimental_get_query_params().get("refresh") else None
st.experimental_set_query_params(refresh="1")
st_autorefresh = st_autorefresh or (lambda: None)
st_autorefresh()
st.experimental_set_query_params()

field_ids = get_available_field_ids(redis_client)

if not field_ids:
    st.info("No ML predictions available in Redis.")
    st.stop()

selected_field = st.selectbox("Select agricultural field (field_id):", field_ids)

prediction = get_ml_prediction(redis_client, selected_field)

if not prediction:
    st.warning(f"No ML prediction found for field {selected_field}.")
    st.stop()

# Detailed view
def show_prediction(pred):
    st.subheader(f"ML Prediction for {pred.get('field_id', '-')}")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Anomaly Score", f"{pred.get('anomaly_score', '-')}")
        st.metric("Severity", pred.get('severity', '-'))
        st.metric("Timestamp", pred.get('prediction_timestamp', '-'))
    with col2:
        st.metric("Recommendations", pred.get('recommendations', '-'))
        st.metric("Model Version", pred.get('model_version', '-'))
    st.markdown("---")
    st.markdown("**Features**")
    features = pred.get("features", {})
    st.json(features)
    st.caption(f"Cached at: {pred.get('cached_at', '-')}, TTL: {pred.get('cache_ttl', '-')}s")

show_prediction(prediction)

st.caption("The page automatically refreshes every 10 seconds to show real-time ML anomalies.")

#TODO: make this page more user friendly 