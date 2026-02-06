import os
import requests
import json
import logging
import io
import base64
import numpy as np
import pandas as pd

# FastAPI imports
from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

# FHIR imports
from fhiry import Fhirsearch

# Resource parsers
from pyserver.parsers import (
    ObservationParser, 
    MedicationRequestParser, 
    MedicationParser, 
    MedicationAdministrationParser,
    DiagnosticReportParser, 
    DocumentReferenceParser, 
    ProcedureParser,
    CarePlanParser,
    ImmunizationParser,
    ConditionParser,
    PatientParser
)

# Matplotlib for visualization
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FHIR API Server",
    description="A REST API for accessing FHIR resources from HAPI FHIR server",
    version="0.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from .config import settings

fs = Fhirsearch(fhir_base_url=settings.hapi_url)
SYNTHEA_SERVER_URL = settings.synthea_server_url


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test HAPI FHIR server connection
        hapi_connected = False
        hapi_error = None
        try:
            test_response = requests.get(f"{settings.hapi_url}/$meta", timeout=5)
            hapi_connected = test_response.status_code == 200
        except Exception as e:
            hapi_error = str(e)
        
        # Service is healthy if it can start and respond (regardless of HAPI)
        # This allows for better debugging of individual service issues
        service_status = "healthy"
        
        return {
            "status": service_status,
            "service": "stat_server_py",
            "dependencies": {
                "hapi_fhir": {
                    "connected": hapi_connected,
                    "url": settings.hapi_url,
                    "error": hapi_error if not hapi_connected else None
                }
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "service": "stat_server_py",
            "error": str(e),
            "dependencies": {
                "hapi_fhir": {
                    "connected": False,
                    "url": settings.hapi_url,
                    "error": "Health check failed"
                }
            }
        }


@app.get("/patients")
async def get_patients(
    name: str = Query(None, description="Patient name to search for"),
    gender: str = Query(None, description="Patient gender"),
    birthdate: str = Query(None, description="Patient birthdate (YYYY-MM-DD)"),
    count: int = Query(10, description="Number of results to return"),
    as_markdown: bool = Query(False, description="Return results as a markdown-formatted table")
):
    """
    Search for patients based on various parameters.
    
    Parameters:
    - name: Patient name to search for
    - gender: Patient gender
    - birthdate: Patient birthdate (YYYY-MM-DD)
    - count: Number of results to return
    - as_markdown: If True, returns formatted markdown table. If False (default), returns JSON.
    """
    try:
        search_params = {}
        if name:
            search_params["name"] = name
        if gender:
            search_params["gender"] = gender
        if birthdate:
            search_params["birthdate"] = birthdate
            
        logger.info(f"Searching for patients with params: {search_params}")
        
        df = fs.search(resource_type="Patient", search_parameters=search_params)
        
        if df is not None and not df.empty:
            logger.info(f"Found {len(df)} patients")
            logger.info(f"DataFrame columns: {df.columns.tolist()}")
            
            # Limit results to _count (fhiry library may not respect _count parameter)
            if count and len(df) > count:
                logger.info(f"Limiting results from {len(df)} to {count}")
                df = df.head(count)
        else:
            logger.warning("No patients found or empty dataframe returned")
            if as_markdown:
                return Response(content="No patients found", media_type="text/plain")
            return {"patients": []}
        
        # replace NaN values with None
        df = df.astype(object).where(pd.notna(df), None)
        
        if as_markdown:
            # Use PatientParser to parse the data for cleaner output
            parsed_df = PatientParser.parse(df)
            
            # Convert to markdown table
            markdown_table = parsed_df.to_markdown(index=False)
            return Response(content=markdown_table, media_type="text/plain")
        else:
            # Return as JSON - use PatientParser for clean, consistent output with patient_id
            parsed_df = PatientParser.parse(df)
            patients_dict = parsed_df.to_dict(orient='records')
            return {"patients": patients_dict, "count": len(patients_dict)}
        
    except Exception as e:
        logger.error(f"Error retrieving patients: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving patients: {str(e)}")

@app.get("/patients/{patient_id}", response_class=JSONResponse)
async def get_patient_by_id(patient_id: str):
    try:
        search_params = {"_id": patient_id}
        df = fs.search(resource_type="Patient", search_parameters=search_params)
        
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail=f"Patient with ID {patient_id} not found")
        
        # replace NaN values with None
        df = df.astype(object).where(pd.notna(df), None)
        
        # Use PatientParser for clean, consistent output with patient_id
        parsed_df = PatientParser.parse(df)
        patient_dict = parsed_df.to_dict(orient='records')[0]
        
        return patient_dict
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving patient {patient_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving patient: {str(e)}")


@app.get("/Patient/{patient_id}")
async def get_patient_parsed(
    patient_id: str = Path(..., description="Patient ID"),
    as_markdown: bool = Query(False, description="Return results as markdown-formatted key-value pairs")
):
    """
    Get parsed demographic information for a specific patient.
    
    This endpoint uses the PatientParser to extract and clean patient demographics including:
    - Name (family name, given name)
    - Gender
    - Birth date and calculated age
    - Address (city, state, country, postal code)
    - Ethnicity and race
    - Language
    - Marital status
    
    Parameters:
    - patient_id: The ID of the patient
    - as_markdown: If True, returns formatted markdown with key-value pairs. If False (default), returns JSON.
    """
    try:
        search_params = {"_id": patient_id}
        df = fs.search(resource_type="Patient", search_parameters=search_params)
        
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail=f"Patient with ID {patient_id} not found")
        
        # Replace NaN values with None
        df = df.astype(object).where(pd.notna(df), None)
        
        # Use PatientParser to parse the data
        parsed_df = PatientParser.parse(df)
        
        if as_markdown:
            # Format as markdown with key-value pairs
            patient_data = parsed_df.to_dict(orient='records')[0]
            
            markdown_lines = [f"# Patient {patient_id}\n"]
            
            for key, value in patient_data.items():
                # Format the key to be more readable (convert snake_case to Title Case)
                readable_key = key.replace('_', ' ').title()
                markdown_lines.append(f"**{readable_key}**: {value}")
            
            markdown_document = "\n".join(markdown_lines)
            return Response(content=markdown_document, media_type="text/plain")
        else:
            # Return as JSON
            patient_dict = parsed_df.to_dict(orient='records')[0]
            return patient_dict
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving patient {patient_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving patient: {str(e)}")

@app.get("/Patient/{patient_id}/all-structured")
async def get_all_structured_patient_resources(
    patient_id: str = Path(..., description="Patient ID"),
    as_markdown_df: bool = Query(False, description="Return results as a formatted markdown document")
):
    """
    Get all structured resource types for a given patient (excludes free-text resources like DiagnosticReport and DocumentReference).
    
    Parameters:
    - patient_id: The ID of the patient
    - as_markdown_df: If True, returns a formatted markdown document with sections and tables. If False (default), returns JSON.
    """
    try:
        # Define structured resource types (excluding narrative/free-text types)
        resource_types = [
            'Patient',
            'Condition',
            'Observation',
            'Procedure',
            'MedicationRequest',
            'MedicationAdministration',
            'Immunization',
            'CarePlan',
        ]
        
        # Resource parsers mapping
        resource_parsers = {
            'Observation': ObservationParser,
            'MedicationRequest': MedicationRequestParser,
            'Medication': MedicationParser,
            'MedicationAdministration': MedicationAdministrationParser,
            'Procedure': ProcedureParser,
            'CarePlan': CarePlanParser,
            'Immunization': ImmunizationParser,
            'Condition': ConditionParser,
            'Patient': PatientParser,
        }
        
        # Map resource types to their patient search parameter name
        patient_search_params = {
            'Claim': 'patient',
            'ExplanationOfBenefit': 'patient',
            'Coverage': 'patient',
            'Account': 'patient',
        }
        
        results = {}
        
        for resource_type in resource_types:
            try:
                # Get the appropriate search parameter name (default to 'subject')
                param_name = patient_search_params.get(resource_type, 'subject')
                
                # Build search parameters
                if resource_type == 'Patient':
                    # For Patient resource, search by ID directly
                    df = fs.search(resource_type='Patient', search_parameters={'_id': patient_id})
                else:
                    # For other resources, search by patient reference
                    search_params = {param_name: f"Patient/{patient_id}"}
                    df = fs.search(resource_type=resource_type, search_parameters=search_params)
                
                if df is not None and not df.empty:
                    # Replace NaN values with None
                    df = df.astype(object).where(pd.notna(df), None)
                    
                    # Use parser if available
                    if resource_type in resource_parsers:
                        parser = resource_parsers[resource_type]
                        parsed_df = parser.parse(df)
                        results[resource_type] = {
                            'data': parsed_df.to_dict(orient='records'),
                            'count': len(parsed_df)
                        }
                    else:
                        results[resource_type] = {
                            'data': df.to_dict(orient='records'),
                            'count': len(df)
                        }
                else:
                    results[resource_type] = {
                        'data': [],
                        'count': 0
                    }
            except Exception as e:
                logger.warning(f"Error retrieving {resource_type} for patient {patient_id}: {str(e)}")
                results[resource_type] = {
                    'data': [],
                    'count': 0,
                    'error': str(e)
                }
        
        if as_markdown_df:
            # Build a formatted markdown document
            markdown_sections = []
            markdown_sections.append(f"# Patient {patient_id} - Structured Data\n")
            
            # Resources that should use key-value format (not tables)
            resources_as_key_value = {
                'Patient',
            }
            
            for resource_type in resource_types:
                resource_data = results.get(resource_type, {})
                count = resource_data.get('count', 0)
                data = resource_data.get('data', [])
                
                markdown_sections.append(f"## {resource_type} ({count} records)\n")
                
                if count > 0 and data:
                    # Check if this resource type should use key-value format
                    if resource_type in resources_as_key_value:
                        # Format as key-value pairs
                        for idx, record in enumerate(data, 1):
                            if len(data) > 1:
                                markdown_sections.append(f"### {resource_type} {idx}\n")
                            
                            for key, value in record.items():
                                # Format regular fields as key-value
                                markdown_sections.append(f"{key}: {value}")
                            
                            markdown_sections.append("")  # Blank line between records
                    else:
                        # Format as table for resources without key-value format
                        df_section = pd.DataFrame(data)
                        markdown_table = df_section.to_markdown(index=False)
                        markdown_sections.append(markdown_table)
                        markdown_sections.append("")  # Add blank line after table
                else:
                    markdown_sections.append("*No records found*\n")
            
            markdown_document = "\n".join(markdown_sections)
            return Response(content=markdown_document, media_type="text/plain")
        else:
            # Return as JSON
            return {
                "patient_id": patient_id,
                "resources": results
            }
        
    except Exception as e:
        logger.error(f"Error retrieving structured resources for patient {patient_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving patient resources: {str(e)}")


@app.get("/Patient/{patient_id}/narratives")
async def get_narrative_patient_resources(
    patient_id: str = Path(..., description="Patient ID"),
    as_markdown_df: bool = Query(False, description="Return results as a formatted markdown document")
):
    """
    Get narrative/free-text resource types for a given patient (DiagnosticReport and DocumentReference).
    
    Parameters:
    - patient_id: The ID of the patient
    - as_markdown_df: If True, returns a formatted markdown document with sections and tables. If False (default), returns JSON.
    """
    try:
        # Define narrative resource types (free-text types)
        resource_types = [
            'DiagnosticReport',
            'DocumentReference',
        ]
        
        # Resource parsers mapping
        resource_parsers = {
            'DiagnosticReport': DiagnosticReportParser,
            'DocumentReference': DocumentReferenceParser,
        }
        
        # Map resource types to their patient search parameter name
        patient_search_params = {
            'Claim': 'patient',
            'ExplanationOfBenefit': 'patient',
            'Coverage': 'patient',
            'Account': 'patient',
        }
        
        results = {}
        
        for resource_type in resource_types:
            try:
                # Get the appropriate search parameter name (default to 'subject')
                param_name = patient_search_params.get(resource_type, 'subject')
                
                # Build search parameters
                search_params = {param_name: f"Patient/{patient_id}"}
                df = fs.search(resource_type=resource_type, search_parameters=search_params)
                
                if df is not None and not df.empty:
                    # Replace NaN values with None
                    df = df.astype(object).where(pd.notna(df), None)
                    
                    # Use parser if available
                    if resource_type in resource_parsers:
                        parser = resource_parsers[resource_type]
                        parsed_df = parser.parse(df)
                        results[resource_type] = {
                            'data': parsed_df.to_dict(orient='records'),
                            'count': len(parsed_df)
                        }
                    else:
                        results[resource_type] = {
                            'data': df.to_dict(orient='records'),
                            'count': len(df)
                        }
                else:
                    results[resource_type] = {
                        'data': [],
                        'count': 0
                    }
            except Exception as e:
                logger.warning(f"Error retrieving {resource_type} for patient {patient_id}: {str(e)}")
                results[resource_type] = {
                    'data': [],
                    'count': 0,
                    'error': str(e)
                }
        
        if as_markdown_df:
            # Build a formatted markdown document
            markdown_sections = []
            markdown_sections.append(f"# Patient {patient_id} - Narrative/Free-Text Data\n")
            
            # Define which resources have large text fields that should be formatted differently
            resources_with_large_text = {
                'DiagnosticReport': ['report_text'],
                'DocumentReference': ['document_text'],
            }
            
            # Resources that should use key-value format (not tables)
            resources_as_key_value = {
                'DiagnosticReport',
                'DocumentReference',
            }
            
            for resource_type in resource_types:
                resource_data = results.get(resource_type, {})
                count = resource_data.get('count', 0)
                data = resource_data.get('data', [])
                
                markdown_sections.append(f"## {resource_type} ({count} records)\n")
                
                if count > 0 and data:
                    # Format as key-value pairs
                    large_text_fields = resources_with_large_text.get(resource_type, [])
                    
                    for idx, record in enumerate(data, 1):
                        if len(data) > 1:
                            markdown_sections.append(f"### {resource_type} {idx}\n")
                        
                        for key, value in record.items():
                            if key in large_text_fields and value:
                                # Format large text fields in code blocks
                                markdown_sections.append(f"{key}:\n")
                                markdown_sections.append("```")
                                markdown_sections.append(str(value))
                                markdown_sections.append("```\n")
                            else:
                                # Format regular fields as key-value
                                markdown_sections.append(f"{key}: {value}")
                        
                        markdown_sections.append("")  # Blank line between records
                else:
                    markdown_sections.append("*No records found*\n")
            
            markdown_document = "\n".join(markdown_sections)
            return Response(content=markdown_document, media_type="text/plain")
        else:
            # Return as JSON
            return {
                "patient_id": patient_id,
                "resources": results
            }
        
    except Exception as e:
        logger.error(f"Error retrieving narrative resources for patient {patient_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving patient resources: {str(e)}")



@app.get("/Patient/{patient_id}/{resource_type}")
async def get_patient_resources(
    patient_id: str = Path(..., description="Patient ID"),
    resource_type: str = Path(..., description="FHIR resource type (e.g., Condition, Observation, Procedure)"),
    as_markdown_df: bool = Query(False, description="Return results as markdown-formatted table (automatically uses compact format)")
):
    """
    Get resources of a specific type for a given patient.
    
    Parameters:
    - patient_id: The ID of the patient
    - resource_type: The FHIR resource type to retrieve (e.g., Condition, Observation, Procedure, MedicationRequest)
    - as_markdown_df: If True, returns the result as a markdown-formatted table with compact format. If False (default), returns full JSON.
    """
    try:
        # Map resource types to their patient search parameter name
        # Most resources use 'subject', but some use 'patient'
        patient_search_params = {
            'Claim': 'patient',
            'ExplanationOfBenefit': 'patient',
            'Coverage': 'patient',
            'Account': 'patient',
        }
        
        # Get the appropriate search parameter name (default to 'subject')
        param_name = patient_search_params.get(resource_type, 'subject')
        
        # Build search parameters to filter by patient
        search_params = {param_name: f"Patient/{patient_id}"}
        
        logger.info(f"Searching for {resource_type} resources for patient {patient_id} using {param_name} parameter")
        
        # Search for the specified resource type
        df = fs.search(resource_type=resource_type, search_parameters=search_params)
        
        if df is None or df.empty:
            if as_markdown_df:
                return Response(
                    content=f"No {resource_type} resources found for patient {patient_id}",
                    media_type="text/plain"
                )
            return {resource_type.lower(): [], "count": 0}
        
        logger.info(f"Found {len(df)} {resource_type} resources for patient {patient_id}")
        
        # Replace NaN values with None
        df = df.astype(object).where(pd.notna(df), None)
        
        # Use resource-specific parser if available and markdown_df is requested
        resource_parsers = {
            'Observation': ObservationParser,
            'MedicationRequest': MedicationRequestParser,
            'Medication': MedicationParser,
            'MedicationAdministration': MedicationAdministrationParser,
            'DiagnosticReport': DiagnosticReportParser,
            'DocumentReference': DocumentReferenceParser,
            'Procedure': ProcedureParser,
            'CarePlan': CarePlanParser,
            'Immunization': ImmunizationParser,
            'Condition': ConditionParser,
            'Patient': PatientParser,
        }
        
        if as_markdown_df and resource_type in resource_parsers:
            # Use the resource-specific parser for compact output
            parser = resource_parsers[resource_type]
            compact_df = parser.parse(df)
            
            # Convert to markdown table
            markdown_table = compact_df.to_markdown(index=False)
            return Response(content=markdown_table, media_type="text/plain")
        
        elif as_markdown_df:
            # Fall back to generic compact formatting for resources without specific parsers
            # Extract cohort from meta.tag before filtering
            def extract_cohort(tags):
                if isinstance(tags, list):
                    for tag in tags:
                        if isinstance(tag, dict) and tag.get('system') == 'urn:charm:cohort':
                            return tag.get('code', 'Default')
                return 'Default'
            
            if 'resource.meta.tag' in df.columns:
                df['cohort'] = df['resource.meta.tag'].apply(extract_cohort)
            else:
                df['cohort'] = 'Default'
            
            # Generic smart filtering - exclude metadata, keep clinical data
            exclude_patterns = [
                'fullUrl', 'resource.resourceType', 'resource.meta', 
                'search.', 'resource.subject', 'resource.encounter',
                'resource.category', 'resource.profile'
            ]
            
            # Keep columns that don't match exclude patterns, plus our new cohort column
            keep_cols = ['cohort'] + [col for col in df.columns 
                        if col != 'cohort' and not any(pattern in col for pattern in exclude_patterns)]
            
            compact_df = df[keep_cols].copy()
            
            # Shorten date columns to just date (not datetime)
            for col in compact_df.columns:
                if 'Date' in col or 'date' in col or 'DateTime' in col:
                    compact_df[col] = compact_df[col].apply(
                        lambda x: x[:10] if isinstance(x, str) and len(x) > 10 else x
                    )
            
            # Intelligent extraction of human-readable values from FHIR structures
            def extract_readable_value(x):
                """Extract human-readable value from FHIR structures"""
                if x is None:
                    return None
                
                # Handle lists - process first element
                if isinstance(x, list):
                    if len(x) == 0:
                        return None
                    x = x[0]
                
                # Handle dicts - extract meaningful values
                if isinstance(x, dict):
                    # Common FHIR patterns in order of preference
                    if 'display' in x:
                        return x['display']
                    if 'text' in x:
                        return x['text']
                    if 'value' in x:
                        value = x['value']
                        # Recursively extract if value is complex
                        return extract_readable_value(value) if isinstance(value, (dict, list)) else value
                    if 'reference' in x:
                        return x['reference']
                    if 'code' in x and isinstance(x['code'], str):
                        return x['code']
                    # If dict has coding array, try to extract from there
                    if 'coding' in x and isinstance(x['coding'], list) and len(x['coding']) > 0:
                        coding = x['coding'][0]
                        if isinstance(coding, dict):
                            return coding.get('display') or coding.get('code')
                    # If nothing else, convert to string but keep it compact
                    return str(x)
                
                # Return simple values as-is
                return x
            
            for col in compact_df.columns:
                compact_df[col] = compact_df[col].apply(extract_readable_value)
            
            # Remove columns where all values are the same (low information), except cohort
            cols_to_drop = []
            for col in compact_df.columns:
                if col != 'cohort':
                    try:
                        if compact_df[col].nunique() == 1:
                            cols_to_drop.append(col)
                    except (TypeError, AttributeError):
                        # Skip columns that still have unhashable types
                        pass
            
            if cols_to_drop:
                compact_df = compact_df.drop(columns=cols_to_drop)
            
            # Simplify column names (remove 'resource.' prefix)
            compact_df.columns = [col.replace('resource.', '').replace('codingcodes', 'code') 
                                 for col in compact_df.columns]
            
            # Return as markdown table
            markdown_table = compact_df.to_markdown(index=False)
            return Response(content=markdown_table, media_type="text/plain")
        
        # Return full resource JSON (as_markdown_df is False)
        resources_dict = df.to_dict(orient='records')
        return {resource_type.lower(): resources_dict, "count": len(resources_dict)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving {resource_type} for patient {patient_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving {resource_type}: {str(e)}")



@app.get("/conditions", response_class=JSONResponse)
async def get_conditions(
    patient: str = Query(None, description="Patient reference (Patient/id)"),
    code: str = Query(None, description="Condition code (system|code format)")
):
    try:
        search_params = {}
        if patient:
            search_params["subject"] = f"Patient/{patient}"
        if code:
            search_params["code"] = code
            
        df = fs.search(resource_type="Condition", search_parameters=search_params)
        
        if df is None or df.empty:
            return {"conditions": []}
        
        # replace NaN values with None
        df = df.astype(object).where(pd.notna(df), None)
        
        conditions_dict = df.to_dict(orient='records')
        
        return {"conditions": conditions_dict, "count": len(conditions_dict)}
        
    except Exception as e:
        logger.error(f"Error retrieving conditions: {str(e)}", exc_info=True)
        raise HTTPException


# example queries:
# get specific a subset of properties from Patient resources in the default cohort
# {settings.hapi_url}/Patient?_tag=urn:charm:cohort|default&_elements=id,name,gender
# get all Patient resources in cohort named cohort1
# {settings.hapi_url}/Patient?_tag=urn:charm:cohort|cohort1
# same works for Conditions, Observations, etc.
# given a list of Patient IDs (from above), we can also get all Conditions for those patients
# {settings.hapi_url}/Condition?subject=Patient/123,Patient/456



# Import the FHIR utilities
from .fhir_utils import FHIRResourceProcessor

# Create a FHIR resource processor instance
fhir_processor = None

@app.on_event("startup")
async def startup_event():
    global fhir_processor
    fhir_processor = FHIRResourceProcessor(settings.hapi_url)

@app.get("/list-all-patient-conditions", response_class=JSONResponse)
async def list_all_patient_conditions():
    """
    Lists all conditions from all patients in the HAPI FHIR server.
    Returns a summary of conditions with their counts and associated patient details.
    """
    return await fhir_processor.process_fhir_resources('Condition', include_patients=True, include_patient_details=True)

@app.get("/list-all-patient-procedures", response_class=JSONResponse)
async def list_all_patient_procedures():
    """
    Lists all procedures from all patients in the HAPI FHIR server.
    Returns a summary of procedures with their counts and associated patient details.
    """
    return await fhir_processor.process_fhir_resources('Procedure', include_patients=True, include_patient_details=True)

@app.get("/list-all-patient-observations", response_class=JSONResponse)
async def list_all_patient_observations():
    """
    Lists all observations from all patients in the HAPI FHIR server.
    Returns a summary of observations with their counts and associated patient details.
    """
    return await fhir_processor.process_fhir_resources('Observation', include_patients=True, include_patient_details=True)

@app.get("/visualize-observations", response_class=Response)
async def visualize_observations(
    limit: int = Query(20, description="Limit the number of observation types to show"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common observation types.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of observation types to show
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource('Observation', limit, cohort_id)


@app.get("/visualize-observations-by-gender", response_class=Response)
async def visualize_observations_by_gender(
    limit: int = Query(10, description="Limit the number of observation types to show per gender"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common observation types broken down by gender.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of observation types to show per gender
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_gender('Observation', limit, cohort_id)


@app.get("/visualize-observations-by-age", response_class=Response)
async def visualize_observations_by_age(
    limit: int = Query(10, description="Limit the number of observation types to show per age bracket"),
    bracket_size: int = Query(5, description="Size of each age bracket in years"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common observation types broken down by age brackets.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of observation types to show per age bracket
    - bracket_size: Size of each age bracket in years
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_age_bracket('Observation', limit, bracket_size, cohort_id)


@app.get("/visualize-conditions", response_class=Response)
async def visualize_conditions(
    limit: int = Query(20, description="Limit the number of condition types to show"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common condition types.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of condition types to show
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource('Condition', limit, cohort_id)


@app.get("/visualize-conditions-by-gender", response_class=Response)
async def visualize_conditions_by_gender(
    limit: int = Query(10, description="Limit the number of condition types to show per gender"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common condition types broken down by gender.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of condition types to show per gender
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_gender('Condition', limit, cohort_id)


@app.get("/visualize-conditions-by-age", response_class=Response)
async def visualize_conditions_by_age(
    limit: int = Query(10, description="Limit the number of condition types to show per age bracket"),
    bracket_size: int = Query(5, description="Size of each age bracket in years"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common condition types broken down by age brackets.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of condition types to show per age bracket
    - bracket_size: Size of each age bracket in years
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_age_bracket('Condition', limit, bracket_size, cohort_id)


@app.get("/visualize-procedures", response_class=Response)
async def visualize_procedures(
    limit: int = Query(20, description="Limit the number of procedure types to show"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common procedure types.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of procedure types to show
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource('Procedure', limit, cohort_id)


@app.get("/visualize-procedures-by-gender", response_class=Response)
async def visualize_procedures_by_gender(
    limit: int = Query(10, description="Limit the number of procedure types to show per gender"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common procedure types broken down by gender.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of procedure types to show per gender
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_gender('Procedure', limit, cohort_id)


@app.get("/visualize-procedures-by-age", response_class=Response)
async def visualize_procedures_by_age(
    limit: int = Query(10, description="Limit the number of procedure types to show per age bracket"),
    bracket_size: int = Query(5, description="Size of each age bracket in years"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common procedure types broken down by age brackets.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of procedure types to show per age bracket
    - bracket_size: Size of each age bracket in years
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_age_bracket('Procedure', limit, bracket_size, cohort_id)


@app.get("/visualize-medications", response_class=Response)
async def visualize_medications(
    limit: int = Query(10, description="Limit the number of medications to show"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common medications.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of medications to show
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource('MedicationRequest', limit, cohort_id)


@app.get("/visualize-medications-by-gender", response_class=Response)
async def visualize_medications_by_gender(
    limit: int = Query(10, description="Limit the number of medications to show per gender"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common medications broken down by gender.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of medications to show per gender
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_gender('MedicationRequest', limit, cohort_id)


@app.get("/visualize-medications-by-age", response_class=Response)
async def visualize_medications_by_age(
    limit: int = Query(10, description="Limit the number of medications to show per age bracket"),
    bracket_size: int = Query(5, description="Size of each age bracket in years"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common medications broken down by age brackets.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of medications to show per age bracket
    - bracket_size: Size of each age bracket in years
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_age_bracket('MedicationRequest', limit, bracket_size, cohort_id)


@app.get("/visualize-diagnostics", response_class=Response)
async def visualize_diagnostics(
    limit: int = Query(10, description="Limit the number of diagnostic reports to show"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common diagnostic report types.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of diagnostic report types to show
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource('DiagnosticReport', limit, cohort_id)


@app.get("/visualize-diagnostics-by-gender", response_class=Response)
async def visualize_diagnostics_by_gender(
    limit: int = Query(10, description="Limit the number of diagnostic report types to show per gender"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common diagnostic report types broken down by gender.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of diagnostic report types to show per gender
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_gender('DiagnosticReport', limit, cohort_id)


@app.get("/visualize-diagnostics-by-age", response_class=Response)
async def visualize_diagnostics_by_age(
    limit: int = Query(10, description="Limit the number of diagnostic report types to show per age bracket"),
    bracket_size: int = Query(5, description="Size of each age bracket in years"),
    cohort_id: str = Query(None, description="Optional cohort ID to filter resources by cohort tag")
):
    """
    Generates a bar chart visualization of the most common diagnostic report types broken down by age brackets.
    Returns a PNG image of the visualization.
    
    Parameters:
    - limit: Maximum number of diagnostic report types to show per age bracket
    - bracket_size: Size of each age bracket in years
    - cohort_id: Optional cohort ID to filter resources by cohort tag
    """
    return await fhir_processor.visualize_resource_by_age_bracket('DiagnosticReport', limit, bracket_size, cohort_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)