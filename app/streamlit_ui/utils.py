"""
Utility functions for CHARMTwinsights Streamlit app
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional


def calculate_age(birth_date_str: str) -> str:
    """Calculate age from birth date string"""
    try:
        birth_dt = datetime.strptime(birth_date_str, "%Y-%m-%d")
        today = datetime.now()
        age = today.year - birth_dt.year - ((today.month, today.day) < (birth_dt.month, birth_dt.day))
        return str(age)
    except Exception:
        return "N/A"


def extract_patient_name(patient: Dict[str, Any]) -> str:
    """Extract patient name from FHIR structure"""
    name = "N/A"
    resource_name = patient.get("resource.name")
    if resource_name and isinstance(resource_name, list) and len(resource_name) > 0:
        name_obj = resource_name[0]
        given = name_obj.get("given", [])
        family = name_obj.get("family", "")
        if given and family:
            given_name = given[0] if isinstance(given, list) else given
            name = f"{given_name} {family}"
        elif family:
            name = family
        elif given:
            name = given[0] if isinstance(given, list) else given
    return name


def extract_cohort_id(patient: Dict[str, Any]) -> str:
    """Extract cohort ID from FHIR patient meta tags"""
    try:
        # In the flattened DataFrame structure, meta tags are directly accessible
        tags = patient.get("resource.meta.tag")
        if not tags or not isinstance(tags, list):
            return "N/A"
        
        # Find the tag with system "urn:charm:cohort"
        for tag in tags:
            if isinstance(tag, dict) and tag.get("system") == "urn:charm:cohort":
                cohort_id = tag.get("code")
                return cohort_id if cohort_id else "N/A"
        
        return "N/A"
    except Exception:
        return "N/A"


def process_patient_search_results(patients_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process patient search results into a DataFrame"""
    patient_data = []
    for patient in patients_list:
        # Extract patient ID
        patient_id = patient.get("id") or patient.get("patientId", "N/A")
        
        # Extract name from FHIR structure
        name = extract_patient_name(patient)
        
        # Extract gender and birth date
        gender = patient.get("resource.gender", "N/A")
        birth_date = patient.get("resource.birthDate", "N/A")
        
        # Calculate age
        age = calculate_age(birth_date) if birth_date != "N/A" else "N/A"
        
        # Extract marital status
        marital_status = patient.get("resource.maritalStatus.text", "N/A")
        
        # Extract cohort ID from meta tags
        cohort_id = extract_cohort_id(patient)
        
        patient_data.append({
            "ID": patient_id,
            "Name": name,
            "Gender": gender.title() if gender != "N/A" else "N/A",
            "Age": age,
            "Birth Date": birth_date,
            "Marital Status": marital_status,
            "Cohort ID": cohort_id
        })
    
    return pd.DataFrame(patient_data)


def process_synthetic_patients(patients_list: List[Dict[str, Any]]) -> tuple[pd.DataFrame, List[str]]:
    """Process synthetic patients data into DataFrame and cohort list"""
    patient_data = []
    all_cohorts = []
    
    for patient in patients_list:
        patient_id = patient.get("id", "N/A")
        gender = patient.get("gender", "N/A").title()
        ethnicity = patient.get("ethnicity", "N/A")
        birth_date = patient.get("birth_date", "N/A")
        
        # Handle cohort_ids
        cohort_ids = patient.get("cohort_ids", [])
        cohort_display = ", ".join(cohort_ids) if cohort_ids else "N/A"
        all_cohorts.extend(cohort_ids)
        
        # Calculate age
        age = calculate_age(birth_date) if birth_date != "N/A" else "N/A"
        
        patient_data.append({
            "ID": patient_id,
            "Gender": gender,
            "Age": age,
            "Birth Date": birth_date,
            "Ethnicity": ethnicity,
            "Cohorts": cohort_display
        })
    
    return pd.DataFrame(patient_data), all_cohorts


def process_cohorts_data(cohorts: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process cohorts data into a DataFrame"""
    cohort_data = []
    for cohort in cohorts:
        cohort_data.append({
            "Cohort ID": cohort.get("cohort_id", "N/A"),
            "Patient Count": cohort.get("patient_count", 0),
            "Source": cohort.get("source", "N/A"),
            "Created": cohort.get("created_at", "N/A")[:19] if cohort.get("created_at") else "N/A"
        })
    return pd.DataFrame(cohort_data)


def create_model_input_form(example: Dict[str, Any], model_image: str) -> Dict[str, Any]:
    """Create a dynamic input form for model testing based on example data"""
    import streamlit as st
    
    test_input = {}
    for key, value in example.items():
        if isinstance(value, bool):
            test_input[key] = st.checkbox(
                f"{key}", 
                value=value, 
                key=f"{model_image}_{key}"
            )
        elif isinstance(value, int):
            test_input[key] = st.number_input(
                f"{key}", 
                value=value, 
                step=1,
                key=f"{model_image}_{key}"
            )
        elif isinstance(value, float):
            test_input[key] = st.number_input(
                f"{key}", 
                value=value, 
                step=0.1,
                key=f"{model_image}_{key}"
            )
        elif isinstance(value, str):
            test_input[key] = st.text_input(
                f"{key}", 
                value=value, 
                key=f"{model_image}_{key}"
            )
        else:
            test_input[key] = value
    
    return test_input


def get_system_stats(models_count: int, services_healthy: int, total_services: int) -> Dict[str, Any]:
    """Calculate system statistics"""
    return {
        "models_available": models_count,
        "services_healthy": services_healthy,
        "total_services": total_services,
        "system_health": "Operational" if services_healthy >= 3 else "Degraded"
    }
