"""
CHARMTwinsight MCP Server
"""

import os
import requests
from typing import Optional, List, Dict, Any
from fastmcp import FastMCP
import sys

# Create the MCP server
mcp = FastMCP("CHARMTwinsight-Modeling")

# Service URLs (internal Docker network)
STAT_SERVER_URL = os.getenv("STAT_SERVER_URL", "http://stat_server_py:8000")
MODEL_SERVER_URL = os.getenv("MODEL_SERVER_URL", "http://model_server:8000")

# ============================================================================
# PATIENT SEARCH & DEMOGRAPHICS
# ============================================================================

@mcp.tool()
def search_patients(
    name: Optional[str] = None,
    gender: Optional[str] = None,
    birthdate: Optional[str] = None,
    count: int = 10
) -> str:
    """
    Search for patients matching specified criteria.
    
    Use this to find patient IDs that can then be used with other tools
    to retrieve detailed clinical data.
    
    Args:
        name: Patient name to search for (partial match)
        gender: Patient gender ("male", "female", "other", "unknown")
        birthdate: Patient birth date in YYYY-MM-DD format
        count: Maximum number of results to return (default 10)
        
    Returns:
        Markdown table matching patients with ID, name, gender, birth date, etc.
    """
    url = f"{STAT_SERVER_URL}/patients"
    params = {"_count": count}
    
    if name:
        params["name"] = name
    if gender:
        params["gender"] = gender
    if birthdate:
        params["birthdate"] = birthdate
    
    params["as_markdown"] = "true"
        
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    
    sys.stderr.write(response.text)
    return response.text


@mcp.tool()
def get_patient_demographics(
    patient_id: str
) -> str:
    """
    Get demographic information for a specific patient.
    
    This retrieves parsed patient demographics including:
    - Name (family name, given name)
    - Gender
    - Birth date and calculated age
    - Address (city, state, country, postal code)
    - Ethnicity and race
    - Language
    - Marital status
    
    Args:
        patient_id: The FHIR Patient resource ID
        
    Returns:
        Patient demographic data in human-readable markdown format
    """
    url = f"{STAT_SERVER_URL}/Patient/{patient_id}"
    params = {"as_markdown": "true"}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    
    return response.text


# ============================================================================
# COMPREHENSIVE PATIENT DATA ACCESS
# ============================================================================

@mcp.tool()
def get_patient_all_structured_data(
    patient_id: str
) -> str:
    """
    Get ALL structured clinical data for a patient in one call.
    
    This retrieves all structured resource types including:
    - Patient demographics
    - Conditions (diagnoses)
    - Observations (lab values, vital signs, BMI, etc.)
    - Procedures
    - MedicationRequests
    - MedicationAdministration
    - Immunizations
    - CarePlans
    
    This is the most comprehensive way to get patient data for model input preparation.
    
    Args:
        patient_id: The FHIR Patient resource ID
        
    Returns:
        All structured clinical resources organized by type in human-readable markdown tables.
    """
    url = f"{STAT_SERVER_URL}/Patient/{patient_id}/all-structured"
    params = {"as_markdown_df": "true"}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    return response.text


@mcp.tool()
def get_patient_narrative_data(
    patient_id: str
) -> str:
    """
    Get narrative/free-text clinical data for a patient.
    
    This retrieves:
    - DiagnosticReports (lab reports, imaging reports with text descriptions)
    - DocumentReferences (clinical notes, discharge summaries, etc.)
    
    Narrative data contains free-text descriptions that may provide additional
    context not available in structured data.

    WARNING: This tool may return a lot of data. Use with caution.
    If you need to get specific resource types, use the get_patient_resource_type() tool.
    If you need to get all resource types other than narrative, use the get_patient_all_structured_data() tool.
    
    Args:
        patient_id: The FHIR Patient resource ID
        
    Returns:
        Narrative clinical resources with text content in formatted markdown
    """
    url = f"{STAT_SERVER_URL}/Patient/{patient_id}/narratives"
    params = {"as_markdown_df": "true"}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    return response.text


@mcp.tool()
def get_patient_resource_type(
    patient_id: str,
    resource_types: list[str]
) -> str:
    """
    Get specific resource types for a patient.
    
    Use this when you need one or more types of clinical data (e.g., only Observations
    or only Conditions, or both) rather than all data at once.
    
    Available resource types:
    - Observation: Lab values, vital signs, social history (smoking status, etc.)
    - Condition: Diagnoses and medical conditions
    - Procedure: Procedures performed
    - MedicationRequest: Prescribed medications
    - MedicationAdministration: Medications actually administered
    - Immunization: Vaccines administered
    - CarePlan: Care plans

    Observations and Conditions should generally be used together for evaluating a patient's health.
    MedicationRequests, MedicationAdministrations, and Immunizations may be used to evaluate a patient's treatment history.
    
    Args:
        patient_id: The FHIR Patient resource ID
        resource_types: List of FHIR resource types (e.g., ["Observation", "Condition"])
        
    Returns:
        Combined markdown tables for the specified resource types
    """
    results = []
    for resource_type in resource_types:
        url = f"{STAT_SERVER_URL}/Patient/{patient_id}/{resource_type}"
        params = {"as_markdown_df": "true"}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        sys.stderr.write(f"Response for {resource_type}: {response.text}\n")
        
        if response.text.strip():
            results.append(f"## {resource_type}\n\n{response.text}")
    
    return "\n\n".join(results) if results else "No data found for the specified resource types."


# ============================================================================
# PREDICTIVE MODEL TOOLS
# ============================================================================

@mcp.tool()
def list_available_models() -> List[Dict[str, Any]]:
    """
    List all registered predictive models available for execution.
    
    Each model has specific input requirements (features, data types, units).
    Use get_model_metadata() to see detailed requirements for a specific model.
    
    Returns:
        List of models with basic metadata:
        - image: Docker image tag (used to reference the model)
        - title: Human-readable model name
        - short_description: What the model predicts
        - authors: Model authors
        - examples: Example input records showing expected format
    """
    url = f"{MODEL_SERVER_URL}/models"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def get_model_metadata(image_tag: str) -> Dict[str, Any]:
    """
    Get complete metadata and documentation for a specific model.
    
    This is ESSENTIAL for understanding how to prepare patient data for model input.
    The README contains critical information about:
    - Required input features (e.g., "age_at_time_0", "bmi", "smoking_status")
    - Expected data types and units
    - How to encode categorical variables
    - What the model predicts
    - Output format and interpretation
    
    Compare the model's required features with available patient data to determine
    how to map FHIR resources (Observations, Conditions, etc.) to model inputs.
    
    Args:
        image_tag: Model identifier from list_available_models() (e.g., "coxcopdmodel:latest")
        
    Returns:
        Complete model metadata:
        - image: Docker image tag
        - title: Model name
        - short_description: Brief description
        - authors: Model authors
        - examples: Example input records (shows exact format expected)
        - readme: Full README documentation with feature descriptions
    """
    url = f"{MODEL_SERVER_URL}/models/{image_tag}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@mcp.tool()
def execute_model(image_tag: str, input_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Execute a predictive model on prepared input data.
    
    IMPORTANT: Input data must exactly match the model's expected format.
    Use get_model_metadata() first to understand:
    1. What features are required
    2. What data types and units are expected
    3. How to encode categorical variables
    
    The input_data should be a list of records (one per patient/observation),
    where each record is a dictionary with keys matching the model's required features.
    
    Args:
        image_tag: Model identifier (e.g., "coxcopdmodel:latest")
        input_data: List of input records matching the model's expected format.
                    Each record should be a dict with all required features.
        
    Returns:
        Model execution results:
        - predictions: List of prediction records (one per input record)
        - stdout: Standard output from model execution (may contain warnings)
        - stderr: Standard error/logging from model execution
        
    Example:
        execute_model(
            "coxcopdmodel:latest",
            [
                {
                    "ethnicity": "Not Hispanic or Latino",
                    "sex_at_birth": "Female",
                    "obesity": 0.0,
                    "diabetes": 0.0,
                    "bmi": 25.0,
                    "age_at_time_0": 50.0
                }
            ]
        )
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
# RESOURCES - Documentation and Workflow Guidance
# ============================================================================

@mcp.resource("readme://workflow")
def get_workflow_readme() -> str:
    """
    Get documentation about the recommended workflow for LLM-assisted predictive modeling.
    """
    return """
# CHARMTwinsight Agent-Assisted Predictive Modeling Workflow

## Overview for Agents

You have access to:
1. **Patient data tools** - Retrieve structured clinical data for specific patients, already parsed and cleaned for easier consumption.
2. **Model execution tools** - Get model requirements and execute predictions

Your job is to bridge the gap between FHIR patient data and model input requirements.

## Key Concept: Data Mapping

**The Challenge**: Models expect specific feature names (e.g., "bmi", "age_at_time_0", "smoking_status")
but raw patient data is stored in FHIR format with:
- Coded observations (LOINC codes for BMI, smoking status, labs)
- Coded conditions (SNOMED codes for diabetes, COPD, etc.)

Fortunatley, the stat_server_py provides structured access to FHIR patient data with 
resource-specific parsers that clean and flatten the data for easier consumption.

**Your Solution**: 
1. Understand model requirements from metadata
2. Retrieve patient data for a specific patient
3. Analyze the markdown tables to extract relevant values
4. Map data to model features based on LOINC/SNOMED codes and field names
5. Execute model with prepared input data

## Recommended Workflow

### 1. Find Patients
Use `search_patients()` to find patients by name, gender, or birthdate.
Returns demographic info including patient_id, name, gender, birth_date, race, ethnicity.

### 2. Get Patient Demographics
Use `get_patient_demographics(patient_id)` for detailed demographics including:
- cohort, given_name, family_name, prefix, gender
- birth_date, deceased_date, race, ethnicity, birth_sex
- address, city, state, postal_code, phone, language

Returns markdown-formatted key-value pairs.

### 3. Retrieve Patient Clinical Data
Use `get_patient_resource_type(patient_id, resource_types_list)` to get one or more resource types:

Common combinations:
- `["Observation", "Condition"]` - For clinical assessments and diagnoses
- `["MedicationRequest", "MedicationAdministration"]` - For medication history
- `["Procedure"]` - For procedures performed

Or use `get_patient_all_structured_data(patient_id)` to retrieve everything at once
(Observation, Condition, Procedure, MedicationRequest, MedicationAdministration, Immunization, CarePlan).

All return markdown-formatted tables.

### 4. Explore and Understand Models
- `list_available_models()` - See what models are available
- `get_model_metadata(image_tag)` - Get model details including README with feature requirements and examples

### 5. Map Data to Model Features
Analyze the markdown tables returned from patient data tools to extract values needed by the model.
Use the code columns (like code_text for observations, condition_text for conditions) and LOINC/SNOMED
codes to identify relevant data points. Extract numeric values from value_with_unit columns.

### 6. Execute Model
Call `execute_model(image_tag, input_data)` where input_data is a list of dicts with model features.

Result contains:
- predictions: List of prediction dicts
- stdout: Model execution logs
- stderr: Any warnings/errors

### 7. Interpret Results
Interpret the predictions for the user, citing relevant patient data that informed the model.

## Tips for Agents

1. **Always check model metadata first** - Use `get_model_metadata()` to understand exact requirements
2. **Use markdown table structure** - Patient data comes as markdown tables; parse them to extract values
3. **Look for specific codes** - LOINC codes generally identify observations, SNOMED codes generally identify conditions
4. **Handle missing data** - Not all patients have all observations; decide how to handle nulls per model requirements
5. **Get multiple resource types at once** - `get_patient_resource_type()` accepts a list of resource types
6. **Check actual data** - Use `get_patient_resource_type()` to see what data a patient actually has
"""


@mcp.resource("readme://patient-data")
def get_patient_data_readme() -> str:
    """
    Get documentation about patient data access tools.
    """
    return """
# Patient Data Access

The stat_server_py provides structured access to FHIR patient data with resource-specific
parsers that clean and flatten the data for easier consumption.

## Resource Types Available

### Structured Resources
- **Observation**: Lab values, vital signs, social history (smoking, alcohol), BMI, etc.
- **Condition**: Diagnoses and medical conditions
- **Procedure**: Procedures performed
- **MedicationRequest**: Prescribed medications
- **MedicationAdministration**: Medications actually administered
- **Immunization**: Vaccines administered
- **CarePlan**: Care plans and goals

### Narrative Resources
- **DiagnosticReport**: Lab reports, imaging reports with text descriptions
- **DocumentReference**: Clinical notes, discharge summaries, consultation notes

NOTE: Narrative resources are available via `get_patient_narratives()`. They are not included
in the structured data tools because they are typically very large and may contain extensive
free-text content.

## Using the Tools

### For Patient Demographics
Use `get_patient_demographics(patient_id)` to get a patient's demographics:
- cohort, given_name, family_name, prefix
- gender, birth_date, deceased_date
- race, ethnicity, birth_sex, birth_place
- address_line, city, state, postal_code
- marital_status, phone, language

Returns markdown-formatted key-value pairs.

### For Comprehensive Clinical Data
Use `get_patient_all_structured_data(patient_id)` to retrieve all structured resource types at once:
- Observation, Condition, Procedure, MedicationRequest, MedicationAdministration, Immunization, CarePlan

Returns markdown-formatted tables organized by resource type.

### For Specific Resource Types
Use `get_patient_resource_type(patient_id, resource_types_list)` to get one or more specific resource types.
Note: Takes a LIST of resource types (even if you only want one).

Example:
- Single type: `get_patient_resource_type("123", ["Observation"])`
- Multiple types: `get_patient_resource_type("123", ["Observation", "Condition"])`

Returns combined markdown-formatted tables with a header for each resource type.

### For Narrative Resources
Use `get_patient_narratives(patient_id)` to get DiagnosticReport and DocumentReference resources.

Returns markdown-formatted content with narrative text.

## Output Format
All patient data tools return **markdown-formatted content** - either tables or key-value pairs.

## Resource-Specific Parser Output

Each resource type has a parser that extracts the most clinically relevant fields.
Here's what you'll actually see in the markdown tables:

### Observation
Columns:
- **cohort**: Patient cohort identifier
- **status**: Observation status (e.g., "final")
- **category**: Category (e.g., "Vital signs", "Social history", "Survey", "Lab")
- **code_text**: Human-readable description (e.g., "Body Mass Index", "Tobacco smoking status")
- **encounter_id**: Associated encounter ID
- **effective_date**: Date of observation (YYYY-MM-DD)
- **issued**: Date issued (YYYY-MM-DD)
- **value_with_unit**: Combined value and unit (e.g., "29.31 kg/m2", "Ex-smoker (finding)")

### Condition
Columns:
- **cohort**: Patient cohort identifier
- **clinical_status**: Status code (e.g., "active", "resolved")
- **verification_status**: Verification status code
- **category**: Condition category
- **condition_text**: Human-readable diagnosis (e.g., "Chronic intractable migraine without aura")
- **condition_code**: SNOMED code
- **condition_system**: Code system URL
- **encounter_id**: Associated encounter ID
- **onset_date**: Date condition started (YYYY-MM-DD)
- **recorded_date**: Date recorded (YYYY-MM-DD)

### MedicationRequest
Columns:
- **cohort**: Patient cohort identifier
- **status**: Status (e.g., "active", "stopped")
- **intent**: Intent (e.g., "order")
- **category**: Category (e.g., "Community")
- **medication**: Medication name (e.g., "Naproxen sodium 220 MG Oral Tablet")
- **encounter_id**: Associated encounter ID
- **authored_on**: Date prescribed (YYYY-MM-DD)
- **dosage_text**: Dosage instructions text
- **as_needed**: Whether taken as needed (True/False)
- **dose**: Dose amount with unit (e.g., "50 mg")
- **timing**: Frequency (e.g., "2x per 1 day(s)")

### Procedure
Columns:
- **cohort**: Patient cohort identifier
- **status**: Status (e.g., "completed")
- **procedure_text**: Human-readable procedure name
- **procedure_code**: Procedure code
- **encounter_id**: Associated encounter ID
- **performed_start**: Start date (YYYY-MM-DD)
- **performed_end**: End date (YYYY-MM-DD)
- **location**: Location name

## Common LOINC Codes for Observations

- **BMI**: 39156-5
- **Body Weight**: 29463-7
- **Body Height**: 8302-2
- **Smoking Status**: 72166-2
- **Blood Pressure**: 85354-9 (systolic 8480-6, diastolic 8462-4)
- **Heart Rate**: 8867-4
- **Respiratory Rate**: 9279-1
- **Body Temperature**: 8310-5
- **Oxygen Saturation**: 2708-6

## Common SNOMED Codes for Conditions

- **Diabetes**: 44054006
- **Hypertension**: 38341003
- **Obesity**: 414916001
- **COPD**: 13645005
- **Asthma**: 195967001
- **Coronary Artery Disease**: 53741008

## Tips

- Match observations using **code_text** (human-readable) rather than trying to extract LOINC codes
- Extract numeric values from **value_with_unit** column (parse out the number)
- Match conditions using **condition_text** or **condition_code** (SNOMED)
- Check the **category** column to filter observations (e.g., only "Vital signs")
"""


@mcp.resource("readme://models")
def get_models_readme() -> str:
    """
    Get documentation about the model execution system.
    """
    return """
# Predictive Model Execution

Models are available for execution via the model_server, which
executes a specific model on prepared input data. Useful tools:
- `list_available_models()` - See what models are available
- `get_model_metadata(image_tag)` - Get model details including README with feature requirements and examples
- `execute_model(image_tag, input_data)` - Execute a model with prepared input data

## Feature Requirements

Each model has specific requirements. Common patterns:

### Demographic Features
- Age (usually as `age` or `age_at_time_0`)
- Sex/Gender (usually as `sex_at_birth`, "Male"/"Female")
- Ethnicity/Race (text labels)

### Clinical Measurements
- BMI (usually float in kg/m2)
- Blood pressure (systolic, diastolic)
- Lab values (glucose, cholesterol, etc.)

### Condition Flags
Often binary indicators (0.0/1.0):
- `diabetes`: 1.0 if patient has diabetes
- `obesity`: 1.0 if patient has obesity
- `cardiovascular_disease`: 1.0 if CVD present
- `smoking_status`: 0.0 = never, 1.0 = current/former

### Dates and Time
- May need age at specific time point
- Time since diagnosis
- Duration of condition

## Handling Missing Data

Models may:
1. Accept null/None values and impute internally
2. Require all features (fail if missing)
3. Have default values documented in README

Check the model's README and examples for guidance.

## Model Output Interpretation

Potential output types:
- **Classification**: Predicted class label and/or probability
- **Regression**: Predicted continuous value (risk score, survival time, etc.)
- **Survival**: Hazard ratios, survival probabilities at time points
- **Clustering**: Cluster assignments

Read the model's README for specifics on interpreting output.
"""


# ============================================================================
# Server entry point
# ============================================================================

if __name__ == "__main__":
    # Run the MCP server in HTTP mode
    # Using streamable-http transport
    import os
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    # Use streamable-http transport
    mcp.run(transport="streamable-http", host=host, port=port)

