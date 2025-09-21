"""
Synthetic data generation page for CHARMTwinsights
Enhanced with async job management, state/city selection, and real-time monitoring
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from api_client import (
    generate_synthetic_patients, list_all_synthetic_patients, 
    get_job_status, list_all_jobs, cancel_job,
    get_available_states, get_cities_for_state
)
from utils import process_synthetic_patients


@st.cache_data(ttl=3600)  # Cache for 1 hour - states rarely change
def load_available_states():
    """Load available states with long-term caching"""
    try:
        states_result = get_available_states()
        if states_result["success"]:
            return states_result["data"].get("states", []), None
        else:
            return [], f"Could not load states from server: {states_result['error']}"
    except Exception as e:
        return [], f"Connection error loading states: {str(e)}"




def show_synthetic_data_lab():
    """Enhanced synthetic data generation interface"""
    st.header("🧬 Synthetic Data Generation")
    st.markdown("Generate synthetic patient cohorts with geographic and demographic controls")
    
    # Create tabs for different sections
    tab1, tab2, tab3 = st.tabs(["🚀 Generate Data", "📊 Job Monitor", "👥 Existing Patients"])
    
    with tab1:
        show_generation_interface()
    
    with tab2:
        show_job_monitor()
    
    with tab3:
        show_existing_patients()


def show_generation_interface():
    """Show the patient generation interface"""
    st.subheader("Cohort Configuration")
    
    # Basic configuration
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📈 Generation Parameters**")
        num_patients = st.slider("Number of Patients", min_value=1, max_value=1000, value=10)
        num_years = st.slider("Years of Medical History", min_value=1, max_value=20, value=2)
        cohort_id = st.text_input(
            "Cohort ID", 
            value="research-cohort", 
            placeholder="Enter unique cohort identifier",
            help="Must follow FHIR ID rules: letters, numbers, hyphens, periods only (1-64 chars)"
        )
    
    with col2:
        st.markdown("**👤 Patient Demographics**")
        min_age = st.slider("Minimum Age", min_value=0, max_value=100, value=0)
        max_age = st.slider("Maximum Age", min_value=0, max_value=100, value=90)
        gender = st.selectbox("Gender Distribution", ["both", "male", "female"])
        export_format = st.selectbox("Export Format", ["fhir", "csv"])
    
    # Geographic configuration
    st.markdown("---")
    st.subheader("🌍 Geographic Distribution")
    
    # Load available states with caching
    available_states, states_error = load_available_states()
    if states_error:
        st.warning(states_error)
        st.info("💡 Using fallback state list. Some features may be limited.")
        # Add some common states as fallback
        available_states = ["California", "Texas", "Florida", "New York", "Pennsylvania", "Illinois", "Ohio", "Georgia", "North Carolina", "Michigan"]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # State selection
        state_options = ["All States (Population Sampling)"] + available_states
        selected_state_option = st.selectbox("State", state_options)
        
        if selected_state_option == "All States (Population Sampling)":
            selected_state = None
            use_population_sampling = True
        else:
            selected_state = selected_state_option
            use_population_sampling = False
    
    with col2:
        # City selection (only if state is selected)
        city = None
        if selected_state:
            city = st.text_input(
                "City (Optional)", 
                placeholder="Enter city name",
                help="Leave empty to use entire state"
            )
            if city and city.strip() == "":
                city = None
        else:
            st.text_input(
                "City (Optional)", 
                disabled=True, 
                placeholder="Select a state first",
                help="City selection requires a specific state"
            )
    
    with col3:
        # Show sampling info
        if use_population_sampling:
            st.metric("Population Sampling", "Enabled")
            st.caption("Weighted by census data")
        elif selected_state:
            st.metric("Selected State", selected_state)
            if city:
                st.caption(f"City: {city}")
            else:
                st.caption("Entire state")
    
    # Generation button and status
    st.markdown("---")
    
    if st.button("🚀 Generate Patients", type="primary", use_container_width=True):
        with st.spinner("Starting patient generation job..."):
            result = generate_synthetic_patients(
                num_patients=num_patients,
                num_years=num_years,
                cohort_id=cohort_id,
                export_format=export_format,
                min_age=min_age,
                max_age=max_age,
                gender=gender,
                state=selected_state,
                city=city,
                use_population_sampling=use_population_sampling
            )
            
            if result["success"]:
                job_data = result["data"]
                st.success("✅ Generation job started successfully!")
                
                # Display job information
                st.subheader("Job Details")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Job ID", job_data["job_id"][:8] + "...")
                with col2:
                    st.metric("Status", job_data["status"].title())
                with col3:
                    created_time = datetime.fromisoformat(job_data["created_at"].replace('Z', '+00:00'))
                    st.metric("Created", created_time.strftime("%H:%M:%S"))
                
                # Store job ID in session state for monitoring
                if "active_jobs" not in st.session_state:
                    st.session_state.active_jobs = []
                st.session_state.active_jobs.append(job_data["job_id"])
                
                st.info(f"📊 Monitor progress in the **Job Monitor** tab")
                
            else:
                st.error(f"❌ Job creation failed: {result['error']}")


def show_job_monitor():
    """Show job monitoring interface"""
    st.subheader("📊 Active Jobs")
    
    # Auto-refresh controls
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        auto_refresh = st.checkbox("🔄 Auto-refresh (5s)", value=False)
    with col2:
        if st.button("🔄 Refresh Now"):
            st.rerun()
    with col3:
        if st.button("📜 Show All Jobs"):
            st.session_state.show_all_jobs = True
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(5)
        st.rerun()
    
    # Get jobs to display with error handling
    jobs = []
    error_message = None
    
    try:
        if st.session_state.get("show_all_jobs", False):
            jobs_result = list_all_jobs()
            if jobs_result["success"]:
                jobs = jobs_result["data"]
            else:
                error_message = f"Failed to fetch all jobs: {jobs_result['error']}"
        else:
            # Show only active jobs from session state
            active_job_ids = st.session_state.get("active_jobs", [])
            if not active_job_ids:
                st.info("No active jobs. Generate some patients to see jobs here!")
                return
            
            for job_id in active_job_ids:
                try:
                    job_result = get_job_status(job_id)
                    if job_result["success"]:
                        jobs.append(job_result["data"])
                    else:
                        st.warning(f"Could not fetch status for job {job_id[:8]}...")
                except Exception as e:
                    st.warning(f"Error fetching job {job_id[:8]}...: {str(e)}")
    
    except Exception as e:
        error_message = f"Connection error: {str(e)}"
    
    if error_message:
        st.error(error_message)
        st.info("💡 Try refreshing the page or check if the services are running")
        return
    
    if not jobs:
        st.info("No jobs found.")
        return
    
    # Display jobs with error handling
    for job in jobs:
        try:
            show_job_card(job)
        except Exception as e:
            st.error(f"Error displaying job {job.get('job_id', 'unknown')[:8]}...: {str(e)}")


def show_job_card(job):
    """Display a job status card"""
    job_id = job["job_id"]
    status = job["status"]
    
    # Status color mapping
    status_colors = {
        "queued": "🟡",
        "running": "🔵", 
        "completed": "🟢",
        "failed": "🔴",
        "cancelled": "⚫"
    }
    
    status_emoji = status_colors.get(status, "⚪")
    
    with st.container():
        # Job header
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
        
        with col1:
            st.markdown(f"**{status_emoji} Job: {job_id[:8]}...**")
        with col2:
            st.markdown(f"**Status:** {status.title()}")
        with col3:
            created = datetime.fromisoformat(job["created_at"].replace('Z', '+00:00'))
            st.markdown(f"**Created:** {created.strftime('%H:%M:%S')}")
        with col4:
            if status in ["queued", "running"]:
                if st.button("❌", key=f"cancel_{job_id}", help="Cancel job"):
                    cancel_result = cancel_job(job_id)
                    if cancel_result["success"]:
                        st.success("Job cancelled")
                        st.rerun()
                    else:
                        st.error(f"Cancel failed: {cancel_result['error']}")
        
        # Job details
        if status == "running":
            # Progress information
            progress = job.get("progress", 0)
            current_phase = job.get("current_phase", "Processing...")
            
            st.progress(progress)
            st.caption(f"Progress: {progress:.1%} - {current_phase}")
            
            # Additional running details
            col1, col2 = st.columns(2)
            with col1:
                if "completed_chunks" in job and "total_chunks" in job:
                    st.metric("Chunks", f"{job['completed_chunks']}/{job['total_chunks']}")
            with col2:
                if "estimated_remaining_seconds" in job:
                    remaining = job["estimated_remaining_seconds"]
                    if remaining is not None and remaining > 0:
                        st.metric("ETA", f"{remaining//60}m {remaining%60}s")
        
        elif status == "completed":
            # Success details
            col1, col2, col3 = st.columns(3)
            with col1:
                if "total_patients" in job:
                    st.metric("Patients Generated", job["total_patients"])
            with col2:
                if "completed_at" in job:
                    completed = datetime.fromisoformat(job["completed_at"].replace('Z', '+00:00'))
                    duration = completed - datetime.fromisoformat(job["created_at"].replace('Z', '+00:00'))
                    st.metric("Duration", f"{duration.total_seconds():.0f}s")
            with col3:
                cohort_id = job.get("request_data", {}).get("cohort_id", "N/A")
                st.metric("Cohort ID", cohort_id)
        
        elif status == "failed":
            # Error details
            if "error" in job:
                st.error(f"Error: {job['error']}")
        
        # Request parameters (collapsible)
        with st.expander(f"📋 Request Details - {job_id[:8]}"):
            request_data = job.get("request_data", {})
            if request_data:
                col1, col2 = st.columns(2)
                with col1:
                    st.json({k: v for k, v in request_data.items() if k in 
                            ["num_patients", "num_years", "cohort_id", "exporter"]})
                with col2:
                    st.json({k: v for k, v in request_data.items() if k in 
                            ["min_age", "max_age", "gender", "state", "city"]})
        
        st.markdown("---")


def show_existing_patients():
    """Show existing synthetic patients"""
    st.subheader("👥 Existing Synthetic Patients")
    
    if st.button("🔍 List Patients"):
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
                col1, col2, col3, col4 = st.columns(4)
                
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
                
                with col4:
                    # Calculate average age
                    ages = [int(age) for age in df["Age"] if age != "N/A" and age.isdigit()]
                    if ages:
                        avg_age = sum(ages) / len(ages)
                        st.metric("Average Age", f"{avg_age:.1f}")
                
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