from fastapi import APIRouter, HTTPException, Body, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Any, Optional, Dict
import httpx
import logging
from ..config import settings  # expects settings.model_server_url

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/modeling",
    tags=["Modeling"],
)

# --- Pydantic Models ---

class RegisterRequest(BaseModel):
    """
    Request model for registering a new model.

    Models require LinkML schemas for input/output validation. Schemas can be:
    - Baked into the Docker image as /app/input_schema.yaml or /app/input_schema.json
    - Provided via the input_schema and output_schema fields as JSON objects (overrides container schemas)

    Container schemas can be YAML or JSON files. API schemas are JSON objects.
    Examples must conform to the input_schema - validation will fail if they don't match.
    Categorical fields (like ethnicity, sex_at_birth) use enum validation with specific allowed values.
    """
    image: str = Field(..., example="coxcopdmodel:latest")
    title: str = Field(..., example="Cox PH Model for COPD Prediction (Demo)")
    short_description: str = Field(..., example="A survival model to predict risk and survival probability for COPD based on demographics and comorbidities.")
    authors: str = Field(..., example="Lakshmi Anandan, Shawn O'Neil")
    examples: Optional[List[Any]] = Field(None, example=[{"ethnicity": "Not Hispanic or Latino", "sex_at_birth": "Female", "obesity": 0.0, "diabetes": 0.0, "cardiovascular_disease": 0.0, "smoking_status": 0.0, "alcohol_use": 0.0, "bmi": 25.0, "age_at_time_0": 50.0}, {"ethnicity": "Hispanic or Latino", "sex_at_birth": "Male", "obesity": 1.0, "diabetes": 1.0, "cardiovascular_disease": 1.0, "smoking_status": 1.0, "alcohol_use": 1.0, "bmi": 32.0, "age_at_time_0": 65.0}])
    readme: Optional[str] = Field(None, example="## Cox PH Model for COPD Prediction\nThis model implements a Cox Proportional Hazards model using lifelines for survival analysis of COPD. It predicts partial hazard scores and survival probabilities at 5 years based on a set of demographic and comorbidity features.")
    input_schema: Optional[Dict[str, Any]] = Field(
        None,
        description="LinkML input schema as JSON object (overrides container schema)",
        example={
            "id": "https://w3id.org/charmtwinsights/coxcopdmodel/input",
            "name": "coxcopd_input",
            "description": "Input schema for Cox COPD survival model",
            "prefixes": {"linkml": "https://w3id.org/linkml/"},
            "imports": ["linkml:types"],
            "default_range": "string",
            "enums": {
                "EthnicityEnum": {
                    "description": "Patient ethnicity categories",
                    "permissible_values": {
                        "Not Hispanic or Latino": {"description": "Not Hispanic or Latino"},
                        "Hispanic or Latino": {"description": "Hispanic or Latino"}
                    }
                },
                "SexAtBirthEnum": {
                    "description": "Biological sex at birth",
                    "permissible_values": {
                        "Female": {"description": "Female"},
                        "Male": {"description": "Male"}
                    }
                }
            },
            "classes": {
                "CoxCOPDInputItem": {
                    "description": "Patient data for COPD risk prediction",
                    "attributes": {
                        "ethnicity": {"description": "Patient ethnicity", "range": "EthnicityEnum", "required": True},
                        "sex_at_birth": {"description": "Biological sex at birth", "range": "SexAtBirthEnum", "required": True},
                        "obesity": {"description": "Obesity status (0.0=no, 1.0=yes)", "range": "float", "required": True},
                        "diabetes": {"description": "Diabetes status (0.0=no, 1.0=yes)", "range": "float", "required": True},
                        "cardiovascular_disease": {"description": "Cardiovascular disease status", "range": "float", "required": True},
                        "smoking_status": {"description": "Smoking status (0.0=no, 1.0=yes)", "range": "float", "required": True},
                        "alcohol_use": {"description": "Alcohol use (0.0=no, 1.0=yes)", "range": "float", "required": True},
                        "bmi": {"description": "Body Mass Index", "range": "float", "required": True},
                        "age_at_time_0": {"description": "Patient age at baseline", "range": "float", "required": True}
                    }
                }
            }
        }
    )
    output_schema: Optional[Dict[str, Any]] = Field(
        None,
        description="LinkML output schema as JSON object (overrides container schema)",
        example={
            "id": "https://w3id.org/charmtwinsights/coxcopdmodel/output",
            "name": "coxcopd_output",
            "description": "Output schema for Cox COPD survival model",
            "prefixes": {"linkml": "https://w3id.org/linkml/"},
            "imports": ["linkml:types"],
            "default_range": "string",
            "classes": {
                "CoxCOPDOutputItem": {
                    "description": "COPD risk prediction result",
                    "attributes": {
                        "partial_hazard": {"description": "Partial hazard score from Cox model", "range": "float", "required": True},
                        "survival_probability_5_years": {"description": "Predicted 5-year survival probability", "range": "float", "required": True}
                    }
                }
            }
        }
    )

class PredictRequest(BaseModel):
    image: str = Field(..., example="coxcopdmodel:latest")
    input: List[Any] = Field(..., example=[{"ethnicity": "Not Hispanic or Latino", "sex_at_birth": "Female", "obesity": 0.0, "diabetes": 0.0, "cardiovascular_disease": 0.0, "smoking_status": 0.0, "alcohol_use": 0.0, "bmi": 25.0, "age_at_time_0": 50.0},{"ethnicity": "Hispanic or Latino", "sex_at_birth": "Male", "obesity": 1.0, "diabetes": 1.0, "cardiovascular_disease": 1.0, "smoking_status": 1.0, "alcohol_use": 1.0, "bmi": 32.0, "age_at_time_0": 65.0}])

# --- Endpoints ---

@router.get("/models", response_class=JSONResponse)
async def list_models():
    """
    List all registered models with core metadata.
    """
    url = f"{settings.model_server_url}/models"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Model server error: {e.response.text}")
        detail = e.response.text or "Error listing models"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error listing models: {e}")
        raise HTTPException(status_code=500, detail="Model server unreachable")


@router.get("/models/{image_tag}", response_class=JSONResponse)
async def model_info(image_tag: str = Path(..., example="coxcopdmodel:latest")):
    """
    Get detailed information about a specific model.
    """
    url = f"{settings.model_server_url}/models/{image_tag}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Model server error: {e.response.text}")
        detail = e.response.text or f"Error fetching model info for {image_tag}"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error fetching model info for {image_tag}: {e}")
        raise HTTPException(status_code=500, detail="Model server unreachable")


@router.post("/models", response_class=JSONResponse)
async def register_model(req: RegisterRequest):
    """
    Register a new model with the model server.
    """
    url = f"{settings.model_server_url}/models"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json=req.dict())
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Model server error: {e.response.text}")
        detail = e.response.text or "Error registering model"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error registering model: {e}")
        raise HTTPException(status_code=500, detail="Model server unreachable")


@router.post("/predict", response_class=JSONResponse)
async def predict(request: PredictRequest):
    """
    Make a prediction using a registered model.
    """
    url = f"{settings.model_server_url}/predict"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json=request.dict())
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Model server error: {e.response.text}")
        detail = e.response.text or "Error making prediction"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error making prediction: {e}")
        raise HTTPException(status_code=500, detail="Model server unreachable")