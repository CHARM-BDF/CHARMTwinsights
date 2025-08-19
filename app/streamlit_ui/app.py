"""
CHARMTwinsights Clinical AI Research Studio

A professional interface for researchers to explore synthetic data generation,
AI model testing, and clinical prediction workflows.
"""

import streamlit as st
import pandas as pd
import requests
import json
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import time
from typing import Dict, List, Any, Optional

# Page configuration
st.set_page_config(
    page_title="CHARMTwinsights Research Studio",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
import os
API_BASE = os.getenv("API_BASE", "http://localhost:8000")
SERVICES = {
    "Router": f"{API_BASE}/healthz",
    "Model Server": f"{API_BASE.replace(':8000', ':8004')}/health" if "localhost" in API_BASE else "http://model_server:8000/health",
    # These services don't have /health endpoints, so we'll check their docs endpoints instead
    "Stats Server": f"{API_BASE.replace(':8000', ':8001')}/docs" if "localhost" in API_BASE else "http://stat_server_py:8000/docs", 
    "Synthea Server": f"{API_BASE.replace(':8000', ':8003')}/docs" if "localhost" in API_BASE else "http://synthea_server:8000/docs"
}

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f4e79;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 0.5rem;
        color: white;
        text-align: center;
    }
    .status-healthy {
        color: #28a745;
        font-weight: bold;
    }
    .status-unhealthy {
        color: #dc3545;
        font-weight: bold;
    }
    .model-card {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        background: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .prediction-result {
        background: #f8f9fa;
        border-left: 4px solid #007bff;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 8px 8px 0;
    }
</style>
""", unsafe_allow_html=True)

def check_service_health() -> Dict[str, bool]:
    """Check health status of all services"""
    health_status = {}
    for service_name, url in SERVICES.items():
        try:
            response = requests.get(url, timeout=5)
            health_status[service_name] = response.status_code == 200
        except Exception as e:
            health_status[service_name] = False
            # For debugging (can be removed in production)
            if st.sidebar.checkbox("Show Debug Info", key="debug"):
                st.sidebar.text(f"{service_name} error: {str(e)[:50]}...")
    return health_status

def get_system_stats() -> Dict[str, Any]:
    """Get system statistics"""
    stats = {
        "models_available": 0,
        "services_healthy": 0,
        "total_services": len(SERVICES)
    }
    
    try:
        # Get model count
        response = requests.get(f"{API_BASE}/modeling/models", timeout=10)
        if response.status_code == 200:
            stats["models_available"] = len(response.json())
    except:
        pass
    
    # Count healthy services
    health_status = check_service_health()
    stats["services_healthy"] = sum(health_status.values())
    
    return stats

def main():
    # Header
    st.markdown('<h1 class="main-header">🏥 CHARMTwinsights</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Synthetic Data Generation & Predictive Analytics</p>', unsafe_allow_html=True)
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Select Page",
        ["Dashboard", "Synthetic Data", "Models"]
    )
    
    # System status in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### System Status")
    
    health_status = check_service_health()
    for service, is_healthy in health_status.items():
        status_class = "status-healthy" if is_healthy else "status-unhealthy"
        status_text = "🟢 Online" if is_healthy else "🔴 Offline"
        st.sidebar.markdown(f'<span class="{status_class}">{service}: {status_text}</span>', unsafe_allow_html=True)
    
    # Route to appropriate page
    if page == "Dashboard":
        show_dashboard()
    elif page == "Synthetic Data":
        show_synthetic_data_lab()
    elif page == "Models":
        show_model_marketplace()

def show_dashboard():
    """Main dashboard with system overview"""
    st.header("System Overview")
    
    # System metrics
    stats = get_system_stats()
    
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
            value="Operational" if stats['services_healthy'] >= 3 else "Degraded"
        )
    
    with col4:
        st.metric(
            label="Platform Status",
            value="Ready"
        )
    
    # Quick actions
    st.markdown("---")
    st.subheader("Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Generate Data", use_container_width=True):
            st.info("Navigate to Synthetic Data page to generate patient cohorts")
    
    with col2:
        if st.button("Test Models", use_container_width=True):
            st.info("Navigate to the Models page to test available models")
    
    with col3:
        if st.button("View API", use_container_width=True):
            st.info("Access full REST API at http://localhost:8000/docs")
    

def show_synthetic_data_lab():
    """Synthetic data generation interface"""
    st.header("Synthetic Data Generation")
    st.markdown("Generate synthetic patient cohorts")
    
    # Cohort configuration
    st.subheader("Cohort Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        num_patients = st.slider("Number of Patients", min_value=1, max_value=10, value=5)
        num_years = st.slider("Years of Medical History", min_value=1, max_value=20, value=5)
        cohort_id = st.text_input("Cohort ID", value="research-cohort", placeholder="Enter unique cohort identifier")
    
    with col2:
        min_age = st.slider("Minimum Age", min_value=0, max_value=100, value=0)
        max_age = st.slider("Maximum Age", min_value=0, max_value=100, value=90)
        gender = st.selectbox("Gender Distribution", ["both", "male", "female"])
        export_format = st.selectbox("Export Format", ["fhir", "csv"])
    
    # Generation controls
    if st.button("Generate", type="primary", use_container_width=True):
        with st.spinner("Generating synthetic patients... This may take a few minutes."):
            try:
                url = f"{API_BASE}/synthetic/synthea/generate-synthetic-patients"
                params = {
                    "num_patients": num_patients,
                    "num_years": num_years,
                    "cohort_id": cohort_id,
                    "exporter": export_format,
                    "min_age": min_age,
                    "max_age": max_age,
                    "gender": gender
                }
                
                response = requests.post(url, params=params, timeout=300)
                
                if response.status_code == 200:
                    result = response.json()
                    st.success(f"Successfully generated {num_patients} synthetic patients.")
                    
                    # Display results
                    st.subheader("Generation Results")
                    st.json(result)
                    
                    # Show download option
                    if "cohort_id" in result:
                        st.info(f"Cohort saved as: `{result['cohort_id']}`")
                
                else:
                    st.error(f"❌ Generation failed: {response.text}")
                    
            except requests.exceptions.Timeout:
                st.error("⏱️ Generation timed out. Try with fewer patients or shorter history.")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # Display existing patients
    st.markdown("---")
    st.subheader("Existing Synthetic Patients")
    
    if st.button("List Patients"):
        try:
            response = requests.get(f"{API_BASE}/synthetic/synthea/list-all-patients", timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                if data and "patients" in data and len(data["patients"]) > 0:
                    patients_list = data["patients"]
                    st.success(f"Found {len(patients_list)} patients")
                    
                    # Create a summary table
                    patient_data = []
                    all_cohorts = []
                    
                    for patient in patients_list:
                        patient_id = patient.get("id", "N/A")
                        gender = patient.get("gender", "N/A").title()
                        ethnicity = patient.get("ethnicity", "N/A")
                        birth_date = patient.get("birth_date", "N/A")
                        
                        # Handle cohort_ids (it's a list)
                        cohort_ids = patient.get("cohort_ids", [])
                        cohort_display = ", ".join(cohort_ids) if cohort_ids else "N/A"
                        all_cohorts.extend(cohort_ids)
                        
                        # Calculate age if birth_date is available
                        age = "N/A"
                        if birth_date != "N/A":
                            try:
                                from datetime import datetime
                                birth_dt = datetime.strptime(birth_date, "%Y-%m-%d")
                                today = datetime.now()
                                age = today.year - birth_dt.year - ((today.month, today.day) < (birth_dt.month, birth_dt.day))
                            except:
                                age = "N/A"
                        
                        patient_data.append({
                            "ID": patient_id,
                            "Gender": gender,
                            "Age": age,
                            "Birth Date": birth_date,
                            "Ethnicity": ethnicity,
                            "Cohorts": cohort_display
                        })
                    
                    # Display the table
                    df = pd.DataFrame(patient_data)
                    st.dataframe(df, use_container_width=True)
                    
                    # Show statistics
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Patients", len(patients_list))
                    
                    with col2:
                        gender_counts = df["Gender"].value_counts()
                        if len(gender_counts) > 0:
                            most_common_gender = gender_counts.index[0]
                            st.metric("Most Common Gender", f"{most_common_gender} ({gender_counts[most_common_gender]})")
                    
                    with col3:
                        unique_cohorts = len(set(all_cohorts))
                        st.metric("Number of Cohorts", unique_cohorts)
                    
                    # Show cohort distribution
                    if all_cohorts:
                        st.subheader("📊 Cohort Distribution")
                        cohort_counts = pd.Series(all_cohorts).value_counts()
                        st.bar_chart(cohort_counts)
                    
                    # Show gender distribution
                    st.subheader("👥 Gender Distribution")
                    gender_counts = df["Gender"].value_counts()
                    st.bar_chart(gender_counts)
                    
                    # Show raw data for debugging if enabled
                    if st.sidebar.checkbox("Show Raw Patient Data", key="show_raw_patients"):
                        with st.expander("🔍 Raw Response Data"):
                            st.json(data)
                            
                elif data and "patients" in data:
                    st.info("No patients found. Generate some synthetic data first!")
                else:
                    st.warning("Unexpected response format from server")
                    with st.expander("🔍 Raw Response"):
                        st.json(data)
            else:
                st.error(f"Failed to fetch patients: {response.text}")
        except Exception as e:
            st.error(f"Error fetching patients: {str(e)}")
            # Add debug info if enabled
            if st.sidebar.checkbox("Show Debug Info", key="debug_patients"):
                st.exception(e)

def show_model_marketplace():
    """Model marketplace and testing interface"""
    st.header("Models")
    st.markdown("Explore and test available models")
    
    # Fetch available models
    try:
        response = requests.get(f"{API_BASE}/modeling/models", timeout=10)
        if response.status_code == 200:
            models = response.json()
            
            if models:
                st.success(f"📊 Found {len(models)} available models")
                
                # Model cards
                for model in models:
                    with st.container():
                        st.markdown(f"""
                        <div class="model-card">
                            <h3>{model.get('title', 'Unknown Model')}</h3>
                            <p><strong>Image:</strong> {model.get('image', 'N/A')}</p>
                            <p><strong>Description:</strong> {model.get('short_description', 'No description available')}</p>
                            <p><strong>Authors:</strong> {model.get('authors', 'Unknown')}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Model testing section
                        with st.expander(f"Test {model.get('title', 'Model')}"):
                            if model.get("examples"):
                                st.subheader("Example Inputs")
                                
                                # Show first example
                                example = model["examples"][0]
                                
                                # Create input form based on example
                                st.markdown("**Modify input parameters:**")
                                
                                # Dynamic input creation
                                test_input = {}
                                for key, value in example.items():
                                    if isinstance(value, bool):
                                        test_input[key] = st.checkbox(
                                            f"{key}", 
                                            value=value, 
                                            key=f"{model['image']}_{key}"
                                        )
                                    elif isinstance(value, int):
                                        test_input[key] = st.number_input(
                                            f"{key}", 
                                            value=value, 
                                            step=1,
                                            key=f"{model['image']}_{key}"
                                        )
                                    elif isinstance(value, float):
                                        test_input[key] = st.number_input(
                                            f"{key}", 
                                            value=value, 
                                            step=0.1,
                                            key=f"{model['image']}_{key}"
                                        )
                                    elif isinstance(value, str):
                                        test_input[key] = st.text_input(
                                            f"{key}", 
                                            value=value, 
                                            key=f"{model['image']}_{key}"
                                        )
                                    else:
                                        test_input[key] = value
                                
                                if st.button(f"🚀 Run Prediction", key=f"predict_{model['image']}"):
                                    with st.spinner("Running prediction..."):
                                        try:
                                            predict_data = {
                                                "image": model["image"],
                                                "input": [test_input]
                                            }
                                            
                                            pred_response = requests.post(
                                                f"{API_BASE}/modeling/predict",
                                                json=predict_data,
                                                timeout=30
                                            )
                                            
                                            if pred_response.status_code == 200:
                                                result = pred_response.json()
                                                
                                                st.markdown("""
                                                <div class="prediction-result">
                                                    <h4>Prediction Results</h4>
                                                </div>
                                                """, unsafe_allow_html=True)
                                                
                                                # Display predictions
                                                if "predictions" in result:
                                                    st.json(result["predictions"])
                                                
                                                # Display logs if available
                                                if result.get("stderr"):
                                                    with st.expander("📝 Model Logs"):
                                                        st.code(result["stderr"])
                                            else:
                                                st.error(f"Prediction failed: {pred_response.text}")
                                        except Exception as e:
                                            st.error(f"Error running prediction: {str(e)}")
                            else:
                                st.info("No examples available for this model")
            else:
                st.warning("No models are currently registered")
        else:
            st.error(f"Failed to fetch models: {response.text}")
    except Exception as e:
        st.error(f"Error connecting to model server: {str(e)}")



if __name__ == "__main__":
    main()
