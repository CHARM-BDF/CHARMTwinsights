from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import httpx
import logging

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ingest",
    tags=["Data Ingestion"],
)


class ExternalFHIRIngestRequest(BaseModel):
    """Request model for external FHIR data ingestion"""
    bundle: dict = Field(
        ..., 
        description="FHIR Bundle containing patient data",
        json_schema_extra={
            "example": {
                "resourceType": "Bundle",
                "type": "collection",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Patient",
                            "id": "patient-001",
                            "gender": "female",
                            "birthDate": "1985-03-15",
                            "name": [{
                                "family": "Smith",
                                "given": ["Jane"]
                            }],
                            "address": [{
                                "city": "Boston",
                                "state": "MA",
                                "postalCode": "02101"
                            }]
                        }
                    },
                    {
                        "resource": {
                            "resourceType": "Observation",
                            "id": "obs-vitals-001",
                            "status": "final",
                            "category": [{
                                "coding": [{
                                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                    "code": "vital-signs",
                                    "display": "Vital Signs"
                                }]
                            }],
                            "code": {
                                "coding": [{
                                    "system": "http://loinc.org",
                                    "code": "8867-4",
                                    "display": "Heart rate"
                                }],
                                "text": "Heart rate"
                            },
                            "subject": {
                                "reference": "Patient/patient-001"
                            },
                            "effectiveDateTime": "2026-01-02T10:30:00Z",
                            "valueQuantity": {
                                "value": 72,
                                "unit": "beats/minute",
                                "system": "http://unitsofmeasure.org",
                                "code": "/min"
                            }
                        }
                    },
                    {
                        "resource": {
                            "resourceType": "Condition",
                            "id": "condition-001",
                            "clinicalStatus": {
                                "coding": [{
                                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                    "code": "active",
                                    "display": "Active"
                                }]
                            },
                            "verificationStatus": {
                                "coding": [{
                                    "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                                    "code": "confirmed",
                                    "display": "Confirmed"
                                }]
                            },
                            "category": [{
                                "coding": [{
                                    "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                                    "code": "encounter-diagnosis",
                                    "display": "Encounter Diagnosis"
                                }]
                            }],
                            "code": {
                                "coding": [{
                                    "system": "http://snomed.info/sct",
                                    "code": "38341003",
                                    "display": "Hypertension"
                                }],
                                "text": "Hypertension"
                            },
                            "subject": {
                                "reference": "Patient/patient-001"
                            },
                            "onsetDateTime": "2020-05-10"
                        }
                    }
                ]
            }
        }
    )
    cohort_id: str = Field(
        "external", 
        description="Cohort identifier for organizing data (FHIR ID format: letters, numbers, hyphens, periods only)",
        json_schema_extra={
            "example": "mobile-app-cohort"
        }
    )
    datatype: str = Field(
        "external", 
        description="Data type classification - must be 'external' or 'synthetic'",
        json_schema_extra={
            "example": "external"
        }
    )


@router.post("/fhir", response_class=JSONResponse)
async def ingest_external_fhir(request: ExternalFHIRIngestRequest):
    """
    Ingest FHIR bundle from external sources (e.g., mobile apps, wearables, EHRs).
    
    This endpoint accepts a FHIR Bundle containing patient data from external sources,
    automatically prefixes patient IDs with 'ext-' to prevent conflicts with synthetic data,
    applies appropriate CHARM tags for cohort organization, and stores the data in HAPI FHIR.
    
    ## Features
    
    - **ID Prefixing**: All Patient IDs are automatically prefixed with 'ext-' to prevent conflicts
    - **Reference Updates**: All references to patients throughout the bundle are updated accordingly
    - **Transaction Bundles**: Bundles are converted to transaction type for atomic operations
    - **Update Support**: Re-submitting data for the same patient updates existing records
    - **Automatic Tagging**: Data is tagged with CHARM metadata for cohort organization
    
    ## Tags Applied
    
    All resources receive the following tags:
    - `urn:charm:source`: "external" (always set, not user-configurable)
    - `urn:charm:datatype`: Value from `datatype` parameter ("external" or "synthetic")
    - `urn:charm:cohort`: Value from `cohort_id` parameter
    - `urn:charm:created`: ISO timestamp of ingestion
    
    ## Example Usage
    
    ```bash
    curl -X POST "http://localhost:8000/ingest/fhir" \\
      -H "Content-Type: application/json" \\
      -d '{
        "bundle": {
          "resourceType": "Bundle",
          "type": "collection",
          "entry": [...]
        },
        "cohort_id": "mobile-app-users",
        "datatype": "external"
      }'
    ```
    
    ## Returns
    
    Success response includes:
    - `success`: Boolean indicating success
    - `message`: Descriptive message
    - `cohort_id`: The cohort ID used
    - `datatype`: The datatype classification
    - `patient_count`: Number of patients in the bundle
    - `patient_ids`: Array of prefixed patient IDs
    - `tags_applied`: Dictionary of tags applied to resources
    
    ## Error Handling
    
    - 400: Invalid bundle structure, datatype, or cohort_id
    - 422: Validation error (e.g., cohort_id contains underscore)
    - 500: HAPI FHIR error or server error
    
    HAPI FHIR validation errors are passed through to help debug invalid FHIR resources.
    """
    url = f"{settings.synthea_server_url}/ingest-external-fhir"
    
    request_data = {
        "bundle": request.bundle,
        "cohort_id": request.cohort_id,
        "datatype": request.datatype
    }
    
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(url, json=request_data)
            
            if response.status_code == 200:
                return response.json()
            else:
                # Pass through error from backend
                try:
                    error_detail = response.json()
                except:
                    error_detail = response.text
                
                logger.error(f"Synthea backend error (ingest-external-fhir): Status {response.status_code}, Detail: {error_detail}")
                raise HTTPException(status_code=response.status_code, detail=error_detail)
                
    except httpx.RequestError as e:
        logger.error(f"Error contacting Synthea backend (ingest-external-fhir): {e}")
        raise HTTPException(status_code=500, detail="Synthea server unreachable")

