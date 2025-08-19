"""
Sidebar components for CHARMTwinsights
"""

import streamlit as st
from api_client import check_service_health


def show_navigation_sidebar():
    """Display navigation sidebar"""
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Select Page",
        ["Dashboard", "Synthetic Data", "Patient Browser", "Models"]
    )
    return page


def show_system_status_sidebar():
    """Display system status in sidebar"""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### System Status")
    
    health_status = check_service_health()
    for service, is_healthy in health_status.items():
        status_class = "status-healthy" if is_healthy else "status-unhealthy"
        status_text = "🟢 Online" if is_healthy else "🔴 Offline"
        st.sidebar.markdown(f'<span class="{status_class}">{service}: {status_text}</span>', unsafe_allow_html=True)


def show_debug_options():
    """Show debug options in sidebar"""
    if st.sidebar.checkbox("Show Debug Info", key="debug"):
        return True
    return False
