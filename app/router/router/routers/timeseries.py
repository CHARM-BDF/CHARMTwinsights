from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional
import httpx
import logging

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/synthetic/timeseries",
    tags=["Synthetic Timeseries Generation"],
)


class SinglePatientGenerateRequest(BaseModel):
    """Request model for generating synthetic timeseries for a single patient."""
    ethnicity: Optional[int] = Field(None, ge=0, le=3, description="0=White, 1=Black, 2=Asian, 3=Other")
    gender: Optional[int] = Field(None, ge=0, le=1, description="0=Female, 1=Male")
    age_group: Optional[int] = Field(None, ge=0, le=3, description="0=0-30, 1=30-50, 2=50-70, 3=70-100")
    mortality_label: Optional[int] = Field(None, ge=0, le=1, description="0=Survived, 1=Died")


class NPatientsGenerateRequest(BaseModel):
    """Request model for generating synthetic timeseries for multiple patients."""
    n_patients: int = Field(10, ge=1, le=10000, description="Number of synthetic patients to generate")


@router.get("/model-information", response_class=JSONResponse)
async def get_timeseries_model_information():
    """Get detailed information about the TimeAutoDiff synthetic timeseries model."""
    url = f"{settings.synthea_server_url}/synthetic-vitals-timeseries/model-information"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Synthea backend error (timeseries model info): {e.response.text}")
        detail = e.response.text or "Error getting timeseries model information"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting Synthea backend (timeseries model info): {e}")
        raise HTTPException(status_code=500, detail="Synthea server unreachable")


@router.post("/generate-raw-1-patient", response_class=JSONResponse)
async def generate_timeseries_single_patient(request: SinglePatientGenerateRequest):
    """
    Generate synthetic ICU timeseries data for a single patient using TimeAutoDiff.

    ## Output Features (10 vital signs over 25 hourly timesteps)

    Based on MIMIC-III/IV ICU data definitions:

    | Feature | Full Name | Unit | Description |
    |---------|-----------|------|-------------|
    | fio2 | Fraction of Inspired Oxygen | % (0-100) | Oxygen concentration delivered to patient |
    | map | Mean Arterial Pressure | mmHg | Average arterial pressure during cardiac cycle |
    | dbp | Diastolic Blood Pressure | mmHg | Arterial pressure during heart relaxation |
    | o2sat | Oxygen Saturation (SpO2) | % (0-100) | Peripheral oxygen saturation |
    | hr | Heart Rate | bpm | Beats per minute |
    | temp | Temperature | °C | Body temperature |
    | resp | Respiratory Rate | breaths/min | Breathing rate |
    | sbp | Systolic Blood Pressure | mmHg | Arterial pressure during heart contraction |
    | ph | Blood pH | pH units | Arterial blood acidity/alkalinity (normal: 7.35-7.45) |
    | lymph | Lymphocyte Count | % or K/uL | White blood cell differential |

    ## Conditioning Parameters

    Control the characteristics of the generated patient:

    **ethnicity** (integer 0-3):
    - 0 = White
    - 1 = Black/African American
    - 2 = Asian
    - 3 = Other/Unknown

    **gender** (integer 0-1):
    - 0 = Female
    - 1 = Male

    **age_group** (integer 0-3):
    - 0 = 0-30 years
    - 1 = 30-50 years
    - 2 = 50-70 years
    - 3 = 70-100 years

    **mortality_label** (integer 0-1):
    - 0 = Survived ICU stay
    - 1 = Died during ICU stay

    If conditioning values are not provided, random values are sampled.
    """
    url = f"{settings.synthea_server_url}/synthetic-vitals-timeseries/generate-raw-1-patient"

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(url, json=request.model_dump(exclude_none=True))
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Synthea backend error (timeseries 1 patient): {e.response.text}")
        detail = e.response.text or "Error generating single-patient timeseries"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting Synthea backend (timeseries 1 patient): {e}")
        raise HTTPException(status_code=500, detail="Synthea server unreachable")


@router.get("/generate-visualization-1-patient", response_class=HTMLResponse)
async def generate_timeseries_visualization_single_patient(
    request: Request,
    ethnicity: int = Query(0, ge=0, le=3, description="0=White, 1=Black, 2=Asian, 3=Other"),
    gender: int = Query(0, ge=0, le=1, description="0=Female, 1=Male"),
    age_group: int = Query(0, ge=0, le=3, description="0=0-30, 1=30-50, 2=50-70, 3=70-100"),
    mortality_label: int = Query(0, ge=0, le=1, description="0=Survived, 1=Died"),
):
    """
    Generate and visualize synthetic ICU timeseries as an interactive HTML page.

    Returns an interactive Plotly visualization with 10 stacked subplots showing
    all vital signs over 25 hourly timesteps. Hover over any point to see details.

    ## Conditioning Parameters (all optional, random if not specified)

    - **ethnicity**: 0=White, 1=Black, 2=Asian, 3=Other
    - **gender**: 0=Female, 1=Male
    - **age_group**: 0=0-30, 1=30-50, 2=50-70, 3=70-100
    - **mortality_label**: 0=Survived, 1=Died
    """
    url = f"{settings.synthea_server_url}/synthetic-vitals-timeseries/generate-visualization-1-patient"
    # Preserve backend behavior: if a parameter is omitted from the query string,
    # don't forward it so the backend can sample randomly.
    present = request.query_params
    params = {}
    if "ethnicity" in present:
        params["ethnicity"] = ethnicity
    if "gender" in present:
        params["gender"] = gender
    if "age_group" in present:
        params["age_group"] = age_group
    if "mortality_label" in present:
        params["mortality_label"] = mortality_label

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return HTMLResponse(
                content=resp.content,
                media_type=resp.headers.get("content-type", "text/html"),
            )
    except httpx.HTTPStatusError as e:
        logger.error(f"Synthea backend error (timeseries visualization): {e.response.text}")
        detail = e.response.text or "Error generating timeseries visualization"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting Synthea backend (timeseries visualization): {e}")
        raise HTTPException(status_code=500, detail="Synthea server unreachable")


@router.post("/generate-raw-n-patients", response_class=JSONResponse)
async def generate_timeseries_n_patients(
    body: Optional[NPatientsGenerateRequest] = Body(
        default=None,
        description="Body form (recommended in Swagger UI).",
        examples={
            "default": {
                "summary": "Generate 10 patients",
                "value": {"n_patients": 10},
            },
            "larger_batch": {
                "summary": "Generate 50 patients",
                "value": {"n_patients": 50},
            },
        },
    ),
    n_patients: Optional[int] = Query(
        default=None,
        ge=1,
        le=10000,
        description="Query form (backward compatible). If both body and query are provided, body is used.",
        example=10,
    ),
):
    """
    Generate synthetic ICU timeseries data for multiple patients with random conditioning.

    All conditioning features (ethnicity, gender, age_group, mortality_label) are
    randomly sampled for each patient.

    ## Parameters

    - **n_patients**: Number of synthetic patients to generate (default: 10)

    ## Output

    Returns an array of patient objects, each containing:
    - Patient demographics (randomly assigned)
    - 25-hour timeseries of 10 vital signs
    """
    resolved_n_patients = body.n_patients if body is not None else (n_patients if n_patients is not None else 10)
    url = f"{settings.synthea_server_url}/synthetic-vitals-timeseries/generate-raw-n-patients"

    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.post(url, params={"n_patients": resolved_n_patients})
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Synthea backend error (timeseries n patients): {e.response.text}")
        detail = e.response.text or "Error generating multi-patient timeseries"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting Synthea backend (timeseries n patients): {e}")
        raise HTTPException(status_code=500, detail="Synthea server unreachable")
