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

st.title("Crop Disease Monitoring - Overview")

# Logo principale al centro
st.markdown("<br><br>", unsafe_allow_html=True)
col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    st.image("Logo/Logo Crop.png", width=350)
st.markdown("<br><br>", unsafe_allow_html=True)

# Team section
st.header("Development Team")
st.markdown("This project was developed by:")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    **Filippo Stanghellini**  
    [@filippostanghellini](https://github.com/stanghee)
    """)

with col2:
    st.markdown("""
    **Paolo Fabbri**  
    [@paolofabbri](https://github.com/PaoloFabbri8)
    """)

with col3:
    st.markdown("""
    **Francesco Molteni**  
    [@MolteniF](https://github.com/MolteniF)
    """)

st.markdown("<br>", unsafe_allow_html=True)
