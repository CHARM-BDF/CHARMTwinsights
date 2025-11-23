"""
CHARMTwinsight MCP Server - Patient Data & Predictive Modeling

This MCP server provides tools for LLM-assisted predictive modeling on patient data:
- Comprehensive patient data access from HAPI FHIR
- Data exploration (understanding codes, units, and available observations)
- Predictive model execution

The primary use case is enabling an LLM to:
1. Understand what data is available in the system
2. Retrieve patient clinical data in structured formats
3. Map FHIR data to model input requirements
4. Execute predictive models on patient data

All tools communicate with internal microservices in the CHARMTwinsight stack.
"""

import os
import requests
from typing import Optional, List, Dict, Any
from fastmcp import FastMCP

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
) -> Dict[str, Any]:
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
        List of matching patients with ID, name, gender, birth date, etc.
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
def get_patient_demographics(
    patient_id: str,
    as_markdown: bool = False
) -> Dict[str, Any]:
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
        as_markdown: If True, returns human-readable markdown format.
                     If False (default), returns structured JSON.
        
    Returns:
        Patient demographic data in requested format
    """
    url = f"{STAT_SERVER_URL}/Patient/{patient_id}"
    params = {"as_markdown": str(as_markdown).lower()}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    
    if as_markdown:
        return {"markdown": response.text}
    return response.json()


# ============================================================================
# COMPREHENSIVE PATIENT DATA ACCESS
# ============================================================================

@mcp.tool()
def get_patient_all_structured_data(
    patient_id: str,
    as_markdown: bool = False
) -> Dict[str, Any]:
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
        as_markdown: If True, returns human-readable markdown tables (useful for quick review).
                     If False (default), returns structured JSON for programmatic processing.
        
    Returns:
        All structured clinical resources organized by type with parsed/cleaned data
    """
    url = f"{STAT_SERVER_URL}/Patient/{patient_id}/all-structured"
    params = {"as_markdown_df": str(as_markdown).lower()}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    if as_markdown:
        return {"markdown": response.text}
    return response.json()


@mcp.tool()
def get_patient_narrative_data(
    patient_id: str,
    as_markdown: bool = False
) -> Dict[str, Any]:
    """
    Get narrative/free-text clinical data for a patient.
    
    This retrieves:
    - DiagnosticReports (lab reports, imaging reports with text descriptions)
    - DocumentReferences (clinical notes, discharge summaries, etc.)
    
    Narrative data contains free-text descriptions that may provide additional
    context not available in structured data.
    
    Args:
        patient_id: The FHIR Patient resource ID
        as_markdown: If True, returns formatted markdown. If False, returns JSON.
        
    Returns:
        Narrative clinical resources with text content
    """
    url = f"{STAT_SERVER_URL}/Patient/{patient_id}/narratives"
    params = {"as_markdown_df": str(as_markdown).lower()}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    if as_markdown:
        return {"markdown": response.text}
    return response.json()


@mcp.tool()
def get_patient_resource_type(
    patient_id: str,
    resource_type: str,
    as_markdown: bool = False
) -> Dict[str, Any]:
    """
    Get a specific resource type for a patient.
    
    Use this when you need only one type of clinical data (e.g., only Observations
    or only Conditions) rather than all data at once.
    
    Common resource types:
    - Observation: Lab values, vital signs, social history (smoking status, etc.)
    - Condition: Diagnoses and medical conditions
    - Procedure: Procedures performed
    - MedicationRequest: Prescribed medications
    - MedicationAdministration: Medications actually administered
    - DiagnosticReport: Lab and imaging reports
    - Immunization: Vaccines administered
    - CarePlan: Care plans
    
    Args:
        patient_id: The FHIR Patient resource ID
        resource_type: FHIR resource type (e.g., "Observation", "Condition")
        as_markdown: If True, returns formatted markdown table. If False, returns JSON.
        
    Returns:
        List of resources of the specified type for the patient
    """
    url = f"{STAT_SERVER_URL}/Patient/{patient_id}/{resource_type}"
    params = {"as_markdown_df": str(as_markdown).lower()}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    if as_markdown:
        return {"markdown": response.text}
    return response.json()


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
1. **Patient data tools** - Retrieve structured clinical data for specific patients
2. **Model execution tools** - Get model requirements and execute predictions
3. **Resource parsers** - Patient data is already cleaned/parsed for easier consumption

Your job is to bridge the gap between FHIR patient data and model input requirements.

## Key Concept: Data Mapping

**The Challenge**: Models expect specific feature names (e.g., "bmi", "age_at_time_0", "smoking_status")
but patient data comes as FHIR resources with:
- Coded observations (LOINC codes for BMI, smoking status, labs)
- Coded conditions (SNOMED codes for diabetes, COPD, etc.)
- Nested structures (already flattened by parsers)

**Your Solution**: 
1. Read model requirements from metadata
2. Retrieve patient data for a specific patient
3. Map FHIR observations/conditions to model features
4. Execute model with mapped data

**Important**: All patient data is already parsed and cleaned by resource-specific parsers.
You'll get flat dictionaries with fields like `code`, `code_display`, `value`, `value_unit` 
for Observations, making mapping straightforward.

## What Parsed Patient Data Looks Like

When you retrieve patient data, it's already parsed into clean dictionaries:

**Patient Demographics** (from `get_patient_demographics()`):
```json
{
  "id": "123",
  "family_name": "Smith",
  "given_name": "John",
  "gender": "male",
  "birth_date": "1970-05-15",
  "age": 54,
  "city": "Portland",
  "state": "Oregon",
  "ethnicity": "Not Hispanic or Latino",
  "race": "White"
}
```

**Observations** (from `get_patient_resource_type(patient_id, "Observation")`):
```json
[
  {
    "id": "obs-1",
    "code": "39156-5",
    "code_display": "Body Mass Index",
    "value": 28.5,
    "value_unit": "kg/m2",
    "date": "2024-01-15",
    "category": "vital-signs"
  },
  {
    "id": "obs-2",
    "code": "72166-2",
    "code_display": "Tobacco smoking status",
    "value_display": "Former smoker",
    "date": "2024-01-15",
    "category": "social-history"
  }
]
```

**Conditions** (from `get_patient_resource_type(patient_id, "Condition")`):
```json
[
  {
    "id": "cond-1",
    "code": "44054006",
    "code_display": "Diabetes mellitus type 2",
    "clinical_status": "active",
    "onset_date": "2020-03-10"
  }
]
```

These clean structures make mapping to model features straightforward!

## Recommended Workflow

### 1. Explore Available Models
```
models = list_available_models()
# Review what models are available and what they predict
```

### 2. Understand Model Requirements
```
metadata = get_model_metadata("coxcopdmodel:latest")
# Read the README to understand:
# - Required input features
# - Data types and units expected
# - How to encode categorical variables
# - Example inputs
```

### 3. Find Patients
```
patients = search_patients(count=10)
# Or work with a specific patient ID
```

### 4. Retrieve Patient Clinical Data
```
# Get all structured data for comprehensive view
data = get_patient_all_structured_data(patient_id)

# Or get specific resource types if you know what you need
observations = get_patient_resource_type(patient_id, "Observation")
conditions = get_patient_resource_type(patient_id, "Condition")
```

### 5. Map FHIR Data to Model Inputs

This is where your intelligence is crucial. Since data is already parsed, mapping is straightforward:

**Example: Mapping for a COPD Risk Model**

Model requires:
- `age_at_time_0` (float)
- `sex_at_birth` (string: "Male" or "Female")
- `bmi` (float)
- `diabetes` (float: 0.0 or 1.0)
- `smoking_status` (float: 0.0=never, 1.0=current/former)

Mapping code:
```python
# 1. Get patient demographics
demographics = get_patient_demographics(patient_id)
age_at_time_0 = demographics['age']
sex_at_birth = demographics['gender'].capitalize()  # "male" -> "Male"

# 2. Get observations
observations = get_patient_resource_type(patient_id, "Observation")['observation']

# 3. Extract BMI (LOINC 39156-5)
bmi_obs = [o for o in observations if o['code'] == '39156-5']
bmi = bmi_obs[0]['value'] if bmi_obs else None

# 4. Extract smoking status (LOINC 72166-2)
smoking_obs = [o for o in observations if o['code'] == '72166-2']
if smoking_obs:
    status = smoking_obs[0]['value_display'].lower()
    smoking_status = 0.0 if 'never' in status else 1.0
else:
    smoking_status = None

# 5. Check for diabetes condition (SNOMED 44054006)
conditions = get_patient_resource_type(patient_id, "Condition")['condition']
has_diabetes = any(c['code'] == '44054006' for c in conditions)
diabetes = 1.0 if has_diabetes else 0.0

# 6. Build model input
model_input = {
    "age_at_time_0": age_at_time_0,
    "sex_at_birth": sex_at_birth,
    "bmi": bmi,
    "diabetes": diabetes,
    "smoking_status": smoking_status
}
```

### 6. Execute Model
```python
result = execute_model(
    image_tag="coxcopdmodel:latest",
    input_data=[model_input]  # List of input records
)

# Result contains:
# - predictions: List of prediction dicts
# - stdout: Model execution logs
# - stderr: Any warnings/errors
```

### 7. Interpret Results

```python
prediction = result['predictions'][0]
# e.g., {"partial_hazard": 0.866, "survival_probability_5_years": 0.963}

# Check stdout/stderr for any warnings
if result['stderr']:
    print(f"Model warnings: {result['stderr']}")
```

## Tips for Agents

1. **Always check model metadata first** - Use `get_model_metadata()` to understand exact requirements
2. **Start with one patient** - Test your mapping logic on a single patient before batch processing
3. **Use markdown output for exploration** - Set `as_markdown=True` to see human-readable tables when exploring data
4. **Use JSON output for processing** - Set `as_markdown=False` to get structured data for writing mapping code
5. **Handle missing data gracefully** - Not all patients will have all observations; decide how to handle nulls
6. **Document your mapping logic** - Explain which FHIR codes/fields map to which model features
7. **Check the parsed data structure** - Patient data is already parsed/cleaned by resource-specific parsers
8. **Look at actual data** - Use `get_patient_resource_type()` to see what observations/conditions a patient has

## Quick Reference: Common Code Mappings

When mapping patient data to model inputs, here are common FHIR codes:

**Observations (LOINC codes)**:
- BMI: `39156-5` → use `value` field (float in kg/m2)
- Smoking Status: `72166-2` → use `value_display` field (text like "Never smoker")
- Body Weight: `29463-7` → use `value` field (usually kg)
- Body Height: `8302-2` → use `value` field (usually cm)

**Conditions (SNOMED codes)**:
- Diabetes: `44054006`
- Obesity: `414916001`
- Hypertension: `38341003`
- COPD: `13645005`

**Mapping Tips**:
- For binary flags (0/1): Check if condition exists in patient's condition list
- For measurements: Extract `value` field from observations with matching code
- For categories: Use `value_display` or `code_display` fields
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
- **Patient**: Demographics (name, gender, birth date, address, ethnicity, race)
- **Observation**: Lab values, vital signs, social history (smoking, alcohol), BMI, etc.
- **Condition**: Diagnoses and medical conditions (coded with SNOMED, ICD-10, etc.)
- **Procedure**: Procedures performed (coded with SNOMED, CPT, etc.)
- **MedicationRequest**: Prescribed medications
- **MedicationAdministration**: Medications actually administered
- **Immunization**: Vaccines administered
- **CarePlan**: Care plans and goals

### Narrative Resources
- **DiagnosticReport**: Lab reports, imaging reports with text descriptions
- **DocumentReference**: Clinical notes, discharge summaries, consultation notes

## Using the Tools

### For Comprehensive Data
Use `get_patient_all_structured_data()` to retrieve everything at once:
```python
data = get_patient_all_structured_data(patient_id="123")
# Returns: {"patient_id": "123", "resources": {"Patient": {...}, "Observation": {...}, ...}}
```

### For Specific Resource Types
Use `get_patient_resource_type()` when you only need one type:
```python
observations = get_patient_resource_type(patient_id="123", resource_type="Observation")
conditions = get_patient_resource_type(patient_id="123", resource_type="Condition")
```

### Markdown vs JSON Output
- **JSON (default)**: Structured data for programmatic processing
- **Markdown**: Human-readable tables for quick review and exploration

```python
# For processing
data = get_patient_all_structured_data(patient_id="123", as_markdown=False)

# For review
data = get_patient_all_structured_data(patient_id="123", as_markdown=True)
print(data['markdown'])  # Formatted tables
```

## Resource-Specific Parsers

Each resource type has a parser that extracts the most relevant fields:

### Observation Parser
Extracts:
- code, code_display (what was measured)
- value, value_unit (the measurement)
- date (when measured)
- category (lab, vital-signs, social-history, etc.)

### Condition Parser
Extracts:
- code, code_display (diagnosis)
- clinical_status (active, resolved, etc.)
- onset_date, abatement_date
- severity

### Medication Parser
Extracts:
- medication_code, medication_display
- dosage, route, frequency
- authored_date (when prescribed)
- status (active, completed, stopped)

## Common Observation Codes

- **BMI**: LOINC 39156-5
- **Body Weight**: LOINC 29463-7
- **Body Height**: LOINC 8302-2
- **Smoking Status**: LOINC 72166-2
- **Blood Pressure**: LOINC 85354-9 (systolic 8480-6, diastolic 8462-4)
- **Heart Rate**: LOINC 8867-4
- **Respiratory Rate**: LOINC 9279-1
- **Body Temperature**: LOINC 8310-5
- **Oxygen Saturation**: LOINC 2708-6

## Common Condition Codes (SNOMED)

- **Diabetes**: 44054006
- **Hypertension**: 38341003
- **Obesity**: 414916001
- **COPD**: 13645005
- **Asthma**: 195967001
- **Coronary Artery Disease**: 53741008

Use `get_patient_resource_type(patient_id, "Observation")` to see what observations
exist for a specific patient. Common observations include BMI, smoking status, vital signs, and lab values.
"""


@mcp.resource("readme://models")
def get_models_readme() -> str:
    """
    Get documentation about the model execution system.
    """
    return """
# Predictive Model Execution

Models are packaged as Docker containers and executed in isolated environments.

## Model Structure

Each model includes:
- **model_metadata.json**: Title, description, authors
- **examples.json**: Example input records showing exact format
- **README.md**: Detailed documentation about features, output, and usage
- **predict script**: Executable that reads input.json and writes output.json

## Input/Output Format

Models communicate via JSON files:

### Input (what you provide)
```json
[
  {
    "feature1": value1,
    "feature2": value2,
    ...
  },
  {
    "feature1": value3,
    "feature2": value4,
    ...
  }
]
```

### Output (what model returns)
```json
{
  "predictions": [
    {
      "predicted_field1": value1,
      "predicted_field2": value2,
      ...
    },
    ...
  ],
  "stdout": "Model execution logs...",
  "stderr": "Warnings or errors..."
}
```

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

Common output types:
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
    # Run the MCP server in HTTP mode (streamable HTTP transport)
    # This will start an HTTP server on the specified host and port
    import os
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    # Use streamable HTTP transport (recommended over SSE)
    mcp.run(transport="streamable-http", host=host, port=port)

