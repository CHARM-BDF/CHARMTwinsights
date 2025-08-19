"""
Synthetic data generation page for CHARMTwinsights
"""

import streamlit as st
import pandas as pd
from api_client import generate_synthetic_patients, list_all_synthetic_patients
from utils import process_synthetic_patients


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
            result = generate_synthetic_patients(
                num_patients=num_patients,
                num_years=num_years,
                cohort_id=cohort_id,
                export_format=export_format,
                min_age=min_age,
                max_age=max_age,
                gender=gender
            )
            
            if result["success"]:
                st.success(f"Successfully generated {num_patients} synthetic patients.")
                
                # Display results
                st.subheader("Generation Results")
                st.json(result["data"])
                
                # Show download option
                if "cohort_id" in result["data"]:
                    st.info(f"Cohort saved as: `{result['data']['cohort_id']}`")
            else:
                st.error(f"❌ Generation failed: {result['error']}")
    
    # Display existing patients
    st.markdown("---")
    st.subheader("Existing Synthetic Patients")
    
    if st.button("List Patients"):
        result = list_all_synthetic_patients()
        
        if result["success"]:
            data = result["data"]
            
            if data and "patients" in data and len(data["patients"]) > 0:
                patients_list = data["patients"]
                st.success(f"Found {len(patients_list)} patients")
                
                # Create a summary table
                df, all_cohorts = process_synthetic_patients(patients_list)
                
                # Display the table
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
            st.error(f"Failed to fetch patients: {result['error']}")
            # Add debug info if enabled
            if st.sidebar.checkbox("Show Debug Info", key="debug_patients"):
                st.text(result["error"])
