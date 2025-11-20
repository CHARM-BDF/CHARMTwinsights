"""
CHARMTwinsight MCP Server

This MCP server provides tools for:
- Synthetic patient data generation via Synthea
- Patient data access from HAPI FHIR
- Predictive model execution

All tools communicate with internal microservices in the CHARMTwinsight stack.
"""

import os
import requests
from typing import Optional, List, Dict, Any
from fastmcp import FastMCP

# Create the MCP server
mcp = FastMCP("CHARMTwinsight")

# Service URLs (internal Docker network)
SYNTHEA_SERVER_URL = os.getenv("SYNTHEA_SERVER_URL", "http://synthea_server:8000")
STAT_SERVER_URL = os.getenv("STAT_SERVER_URL", "http://stat_server_py:8000")
MODEL_SERVER_URL = os.getenv("MODEL_SERVER_URL", "http://model_server:8000")

# ============================================================================
# SYNTHETIC DATA GENERATION TOOLS
# ============================================================================

@mcp.tool()
def create_synthetic_patients_job(
    num_patients: int = 10,
    num_years: int = 1,
    cohort_id: str = "default",
    min_age: int = 0,
    max_age: int = 140,
    gender: str = "both",
    state: Optional[str] = None,
    city: Optional[str] = None,
    use_population_sampling: bool = True
) -> Dict[str, Any]:
    """
    Create a job to generate synthetic patient data using Synthea.
    
    This creates an asynchronous job that generates synthetic patient FHIR data
    and stores it in the HAPI FHIR server with the specified cohort_id.
    
    Args:
        num_patients: Number of patients to generate (1-100000)
        num_years: Years of medical history per patient (1-100)
        cohort_id: Identifier for the patient cohort (valid FHIR ID: alphanumeric, hyphens, periods)
        min_age: Minimum patient age (0-140)
        max_age: Maximum patient age (0-140)
        gender: Patient gender - "both", "male", or "female"
        state: US state for patient generation (optional)
        city: US city for patient generation (optional, requires state)
        use_population_sampling: If no state specified, sample states by population
        
    Returns:
        Job information including job_id, status, and status_url for tracking
    """
    url = f"{SYNTHEA_SERVER_URL}/synthetic-patients"
    payload = {
        "num_patients": num_patients,
        "num_years": num_years,
        "cohort_id": cohort_id,
        "min_age": min_age,
        "max_age": max_age,
        "gender": gender,
        "exporter": "fhir",
        "use_population_sampling": use_population_sampling
    }
    
    if state:
        payload["state"] = state
    if city:
        payload["city"] = city
        
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def get_synthetic_job_status(job_id: str) -> Dict[str, Any]:
    """
    Get the status of a synthetic patient generation job.
    
    Args:
        job_id: The UUID of the job to check
        
    Returns:
        Job status information including:
        - status: queued, running, completed, failed, or cancelled
        - progress: Percentage complete (0.0-1.0)
        - current_phase: Current operation being performed
        - total_chunks: Total number of generation chunks
        - completed_chunks: Number of completed chunks
        - estimated_remaining_seconds: Estimated time remaining
        - result: Complete results if status is "completed"
        - error: Error message if status is "failed"
    """
    url = f"{SYNTHEA_SERVER_URL}/synthetic-patients/jobs/{job_id}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def list_synthetic_jobs(limit: int = 50) -> Dict[str, Any]:
    """
    List recent synthetic patient generation jobs.
    
    Args:
        limit: Maximum number of recent jobs to return (default 50)
        
    Returns:
        List of recent jobs with their status information
    """
    url = f"{SYNTHEA_SERVER_URL}/synthetic-patients/jobs?limit={limit}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def cancel_synthetic_job(job_id: str) -> Dict[str, Any]:
    """
    Cancel a queued or running synthetic patient generation job.
    
    Args:
        job_id: The UUID of the job to cancel
        
    Returns:
        Confirmation message with the cancelled job status
    """
    url = f"{SYNTHEA_SERVER_URL}/synthetic-patients/jobs/{job_id}"
    response = requests.delete(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def get_available_states() -> Dict[str, Any]:
    """
    Get list of available US states for synthetic patient generation.
    
    Returns:
        List of state names and total count
    """
    url = f"{SYNTHEA_SERVER_URL}/demographics/states"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def get_cities_for_state(state: str) -> Dict[str, Any]:
    """
    Get list of available cities for a specific US state.
    
    Args:
        state: Name of the US state
        
    Returns:
        List of city names and total count for the specified state
    """
    url = f"{SYNTHEA_SERVER_URL}/demographics/cities/{state}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def list_all_cohorts() -> Dict[str, Any]:
    """
    List all patient cohorts stored in the HAPI FHIR server.
    
    Returns:
        List of cohorts with their IDs, patient counts, source, and creation time
    """
    url = f"{SYNTHEA_SERVER_URL}/list-all-cohorts"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def delete_cohort(cohort_id: str) -> Dict[str, Any]:
    """
    Delete a cohort and all its patients from the HAPI FHIR server.
    
    WARNING: This permanently deletes all patient data in the cohort!
    
    Args:
        cohort_id: The ID of the cohort to delete
        
    Returns:
        Confirmation with number of patients deleted
    """
    url = f"{SYNTHEA_SERVER_URL}/delete-cohort/{cohort_id}"
    response = requests.delete(url, timeout=30)
    response.raise_for_status()
    return response.json()


# ============================================================================
# PATIENT DATA ACCESS TOOLS
# ============================================================================

@mcp.tool()
def get_patient_by_id(patient_id: str) -> Dict[str, Any]:
    """
    Get detailed demographic information for a specific patient.
    
    Args:
        patient_id: The FHIR Patient resource ID
        
    Returns:
        Patient demographic data including name, gender, birth date, etc.
    """
    url = f"{STAT_SERVER_URL}/patients/{patient_id}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def get_patient_everything(patient_id: str) -> Dict[str, Any]:
    """
    Get complete clinical record for a patient including all related resources.
    
    This is equivalent to the FHIR $everything operation, returning the patient's
    complete medical history including conditions, observations, procedures,
    medications, encounters, and more.
    
    Args:
        patient_id: The FHIR Patient resource ID
        
    Returns:
        Complete patient data including:
        - demographics: Basic patient information
        - resources: All clinical resources organized by type (Condition, Observation, 
          Procedure, MedicationRequest, Encounter, etc.)
    """
    # The stat_server_py doesn't have a direct $everything endpoint,
    # but we can query the HAPI server directly
    hapi_url = os.getenv("HAPI_URL", "http://hapi:8080/fhir")
    url = f"{hapi_url}/Patient/{patient_id}/$everything"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def search_patients(
    name: Optional[str] = None,
    gender: Optional[str] = None,
    birthdate: Optional[str] = None,
    count: int = 10
) -> Dict[str, Any]:
    """
    Search for patients matching specified criteria.
    
    Args:
        name: Patient name to search for (partial match)
        gender: Patient gender ("male", "female", "other", "unknown")
        birthdate: Patient birth date in YYYY-MM-DD format
        count: Maximum number of results to return (default 10)
        
    Returns:
        List of matching patients with their demographic information
    """
    url = f"{STAT_SERVER_URL}/patients"
    params = {"_count": count}
    
    if name:
        params["name"] = name
    if gender:
        params["gender"] = gender
    if birthdate:
        params["birthdate"] = birthdate
        
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def get_patient_conditions(patient_id: str) -> Dict[str, Any]:
    """
    Get all medical conditions for a specific patient.
    
    Args:
        patient_id: The FHIR Patient resource ID
        
    Returns:
        List of conditions with codes, descriptions, and dates
    """
    url = f"{STAT_SERVER_URL}/conditions"
    params = {"patient": patient_id}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


# ============================================================================
# MODEL SERVER TOOLS
# ============================================================================

@mcp.tool()
def list_available_models() -> List[Dict[str, Any]]:
    """
    List all registered predictive models available for execution.
    
    Returns:
        List of models with metadata including:
        - image: Docker image tag for the model
        - title: Human-readable model name
        - short_description: Brief description of what the model does
        - authors: Model authors
        - examples: Example input data for the model
    """
    url = f"{MODEL_SERVER_URL}/models"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def get_model_metadata(image_tag: str) -> Dict[str, Any]:
    """
    Get complete metadata for a specific model including its README documentation.
    
    The README typically contains detailed information about:
    - What the model predicts
    - Input data format and requirements
    - Output format and interpretation
    - Model performance metrics
    - Citation information
    - Usage examples
    
    Args:
        image_tag: The Docker image tag for the model (e.g., "coxcopdmodel:latest")
        
    Returns:
        Complete model metadata including:
        - image: Docker image tag
        - title: Model name
        - short_description: Brief description
        - authors: Model authors
        - examples: Example input data
        - readme: Full README documentation (markdown format)
    """
    url = f"{MODEL_SERVER_URL}/models/{image_tag}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def execute_model(image_tag: str, input_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Execute a predictive model with provided input data.
    
    The input data format depends on the specific model. Use get_model_metadata()
    to see the expected format and example inputs for a model.
    
    Args:
        image_tag: The Docker image tag for the model to execute
        input_data: List of input records matching the model's expected format
        
    Returns:
        Model predictions and execution logs:
        - predictions: The model's output predictions
        - stdout: Standard output from model execution
        - stderr: Standard error/logging from model execution
    """
    url = f"{MODEL_SERVER_URL}/predict"
    payload = {
        "image": image_tag,
        "input": input_data
    }
    response = requests.post(url, json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


# ============================================================================
# RESOURCES - Provide access to documentation
# ============================================================================

@mcp.resource("readme://synthea-server")
def get_synthea_readme() -> str:
    """
    Get documentation about the Synthea synthetic data generation service.
    """
    return """
# Synthea Server

The Synthea server provides synthetic patient data generation using the Synthea™ Patient Generator.

## Features

- Generate synthetic FHIR patient data
- Asynchronous job-based generation for large cohorts
- Geographic sampling (by state/city or population-weighted)
- Age and gender filtering
- Cohort management and tagging

## Job-Based Generation

For reliability and progress tracking, patient generation uses an asynchronous job system:

1. Create a job with `create_synthetic_patients_job()`
2. Poll status with `get_synthetic_job_status(job_id)`
3. Access results when status is "completed"

Large generations are automatically chunked (100 patients per chunk) and patients are 
incrementally added to the cohort, so partial results are available if a job fails.

## Cohorts

All generated patients are tagged with a cohort_id and added to a FHIR Group resource.
This allows for organized management of different patient populations.
"""


@mcp.resource("readme://stat-server")
def get_stat_server_readme() -> str:
    """
    Get documentation about the statistics/patient data server.
    """
    return """
# Statistics Server (stat_server_py)

Python-based REST API for accessing and analyzing FHIR patient data from the HAPI server.

## Features

- Patient search and retrieval
- Access to specific patient resources (conditions, observations, etc.)
- FHIR $everything operation support
- Data visualization endpoints

## Patient Data Access

The server provides structured access to patient data:

- **Demographics**: Basic patient information (name, gender, birth date)
- **Conditions**: Medical diagnoses and health problems
- **Observations**: Measurements and test results
- **Procedures**: Medical procedures performed
- **Medications**: Prescribed and administered medications
- **Encounters**: Healthcare visits and episodes

Use `get_patient_everything()` to retrieve all related resources for a patient in one call.
"""


@mcp.resource("readme://model-server")
def get_model_server_readme() -> str:
    """
    Get documentation about the model server for predictive analytics.
    """
    return """
# Model Server

REST API for hosting and executing machine learning models packaged as Docker containers.

## Features

- Model registration and metadata management
- Containerized model execution with isolated environments
- Support for Python and R models
- Automatic example extraction from containers

## Available Models

Use `list_available_models()` to see all registered models. Each model includes:

- **README**: Detailed documentation about the model
- **Examples**: Sample input data showing expected format
- **Metadata**: Title, description, and author information

## Executing Models

1. Get model metadata with `get_model_metadata(image_tag)` to understand input format
2. Prepare your input data matching the model's expected schema
3. Execute with `execute_model(image_tag, input_data)`
4. Review predictions and any model logging output

Models run in isolated Docker containers and communicate via file-based I/O for reliability.
"""


# ============================================================================
# Server entry point
# ============================================================================

if __name__ == "__main__":
    # Run the MCP server in HTTP mode (streamable HTTP transport)
    # This will start an HTTP server on the specified host and port
    import os
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    # Use streamable HTTP transport (recommended over SSE)
    mcp.run(transport="streamable-http", host=host, port=port)

