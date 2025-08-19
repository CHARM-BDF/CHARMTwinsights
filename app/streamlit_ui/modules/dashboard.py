"""
Dashboard page for CHARMTwinsights
"""

import streamlit as st
from api_client import check_service_health, get_available_models
from utils import get_system_stats


def show_dashboard():
    """Main dashboard with system overview"""
    st.header("System Overview")
    
    # Get system metrics
    health_status = check_service_health()
    models = get_available_models()
    services_healthy = sum(health_status.values())
    total_services = len(health_status)
    
    stats = get_system_stats(len(models), services_healthy, total_services)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Models Available",
            value=stats["models_available"]
        )
    
    with col2:
        st.metric(
            label="Services Online",
            value=f"{stats['services_healthy']}/{stats['total_services']}"
        )
    
    with col3:
        st.metric(
            label="System Health",
            value=stats["system_health"]
        )
    
    with col4:
        st.metric(
            label="Platform Status",
            value="Ready"
        )
    
    # Quick actions
    st.markdown("---")
    
    # Instructions for navigation
    st.markdown("**Tip:** Use the Navigation dropdown in the sidebar to generate data, view cohorts, or test models.")
    
    # Create a half-width column for the API button
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("View API Documentation", use_container_width=True):
            st.info("Access full REST API at http://localhost:8000/docs")
