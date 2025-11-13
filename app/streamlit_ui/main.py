"""
CHARMTwinsights Clinical AI Research Studio

A professional interface for researchers to explore synthetic data generation,
AI model testing, and clinical prediction workflows.
"""

import streamlit as st
from config import PAGE_CONFIG, CUSTOM_CSS
from components.sidebar import show_navigation_sidebar, show_system_status_sidebar
from pages.dashboard import show_dashboard
from pages.synthetic_data import show_synthetic_data_lab
from pages.patient_browser import show_patient_browser
from pages.models import show_model_marketplace


def main():
    # Page configuration
    st.set_page_config(**PAGE_CONFIG)
    
    # Apply custom CSS
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    
    # Header
    st.markdown('<h1 class="main-header">🏥 CHARMTwinsights</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Synthetic Data Generation & Predictive Analytics</p>', unsafe_allow_html=True)
    
    # Sidebar navigation
    page = show_navigation_sidebar()
    
    # System status in sidebar
    show_system_status_sidebar()
    
    # Route to appropriate page
    if page == "Dashboard":
        show_dashboard()
    elif page == "Synthetic Data":
        show_synthetic_data_lab()
    elif page == "Patient Browser":
        show_patient_browser()
    elif page == "Models":
        show_model_marketplace()


if __name__ == "__main__":
    main()
