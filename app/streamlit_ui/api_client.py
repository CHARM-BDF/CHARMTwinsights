"""
API client functions for interacting with CHARMTwinsights backend services
"""

import requests
from typing import Dict, List, Any, Optional
from config import API_BASE, SERVICES, DEFAULT_SETTINGS


def check_service_health() -> Dict[str, bool]:
    """Check health status of all services"""
    health_status = {}
    for service_name, url in SERVICES.items():
        try:
            response = requests.get(url, timeout=5)
            health_status[service_name] = response.status_code == 200
        except Exception:
            health_status[service_name] = False
    return health_status


def get_available_models() -> List[Dict[str, Any]]:
    """Get list of available models"""
    try:
        response = requests.get(f"{API_BASE}/modeling/models", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return response.json()
        return []
    except Exception:
        return []


def get_available_cohorts() -> List[str]:
    """Get list of available cohorts for dropdown selection"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/list-all-cohorts", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            data = response.json()
            if data and "cohorts" in data:
                cohorts = data["cohorts"]
                return [cohort.get("cohort_id") for cohort in cohorts if cohort.get("cohort_id")]
        return []
    except Exception:
        return []


def search_patients(name: Optional[str] = None, gender: Optional[str] = None, 
                   birth_date: Optional[str] = None, count: int = 20) -> Dict[str, Any]:
    """Search patients using the stats API"""
    try:
        params = {"_count": count}
        if name:
            params["name"] = name
        if gender and gender != "All":
            params["gender"] = gender
        if birth_date:
            params["birthdate"] = birth_date
        
        response = requests.get(f"{API_BASE}/stats/patients", params=params, timeout=DEFAULT_SETTINGS["timeout"])
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_patient_details(patient_id: str) -> Dict[str, Any]:
    """Get detailed information for a specific patient"""
    try:
        response = requests.get(f"{API_BASE}/stats/patients/{patient_id}/$everything", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def list_all_synthetic_patients() -> Dict[str, Any]:
    """List all synthetic patients from Synthea"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/list-all-patients", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def list_all_cohorts() -> Dict[str, Any]:
    """List all available cohorts"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/list-all-cohorts", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_cohort_metadata(cohort_id: str) -> Dict[str, Any]:
    """Get metadata for a specific cohort"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/cohort-metadata/{cohort_id}", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def delete_cohort(cohort_id: str) -> Dict[str, Any]:
    """Delete a cohort"""
    try:
        response = requests.delete(f"{API_BASE}/synthetic/synthea/cohort/{cohort_id}", timeout=30)
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def generate_synthetic_patients(num_patients: int, num_years: int, cohort_id: str,
                              export_format: str = "fhir", min_age: int = 0, 
                              max_age: int = 90, gender: str = "both",
                              state: Optional[str] = None, city: Optional[str] = None,
                              use_population_sampling: bool = True) -> Dict[str, Any]:
    """Generate synthetic patients using async job system via router"""
    try:
        url = f"{API_BASE}/synthetic/synthea/synthetic-patients"
        
        data = {
            "num_patients": num_patients,
            "num_years": num_years,
            "cohort_id": cohort_id,
            "exporter": export_format,
            "min_age": min_age,
            "max_age": max_age,
            "gender": gender,
            "use_population_sampling": use_population_sampling
        }
        
        if state:
            data["state"] = state
        if city:
            data["city"] = city
        
        response = requests.post(url, json=data, timeout=30)
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_visualization_image(endpoint: str, limit: int, cohort_filter: Optional[str] = None, 
                          bracket_size: Optional[int] = None) -> Dict[str, Any]:
    """Get visualization image from stats API"""
    try:
        params = {"limit": limit}
        if cohort_filter:
            params["cohort_id"] = cohort_filter
        if bracket_size is not None:
            params["bracket_size"] = bracket_size
        
        response = requests.get(f"{API_BASE}{endpoint}", params=params, timeout=DEFAULT_SETTINGS["visualization_timeout"])
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            return {
                "success": True, 
                "content": response.content,
                "content_type": content_type
            }
        else:
            return {"success": False, "error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def predict_with_model(model_image: str, input_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Run prediction with a model"""
    try:
        predict_data = {
            "image": model_image,
            "input": input_data
        }
        
        response = requests.post(
            f"{API_BASE}/modeling/predict",
            json=predict_data,
            timeout=30
        )
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def load_resource_data(resource_type: str) -> Dict[str, Any]:
    """Load data for a specific resource type (conditions, observations, procedures)"""
    try:
        endpoint_map = {
            "conditions": "/stats/all-patient-conditions",
            "observations": "/stats/all-patient-observations", 
            "procedures": "/stats/all-patient-procedures"
        }
        
        if resource_type not in endpoint_map:
            return {"success": False, "error": f"Unknown resource type: {resource_type}"}
        
        response = requests.get(f"{API_BASE}{endpoint_map[resource_type]}", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


# New functions for async job management and demographics

def get_job_status(job_id: str) -> Dict[str, Any]:
    """Get the status of a synthetic patient generation job"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/synthetic-patients/jobs/{job_id}", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def list_all_jobs() -> Dict[str, Any]:
    """List all synthetic patient generation jobs"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/synthetic-patients/jobs", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def cancel_job(job_id: str) -> Dict[str, Any]:
    """Cancel a running generation job"""
    try:
        response = requests.delete(f"{API_BASE}/synthetic/synthea/synthetic-patients/jobs/{job_id}", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_available_states() -> Dict[str, Any]:
    """Get list of available US states for patient generation"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/demographics/states", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_cities_for_state(state: str) -> Dict[str, Any]:
    """Get list of available cities for a specific state"""
    try:
        response = requests.get(f"{API_BASE}/synthetic/synthea/demographics/cities/{state}", timeout=DEFAULT_SETTINGS["timeout"])
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


