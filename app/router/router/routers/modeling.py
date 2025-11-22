from fastapi import APIRouter, HTTPException, Body, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Any, Optional
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
    image: str = Field(..., example="coxcopdmodel:latest")
    title: str = Field(..., example="Cox PH Model for COPD Prediction (Demo)")
    short_description: str = Field(..., example="A survival model to predict risk and survival probability for COPD based on demographics and comorbidities.")
    authors: str = Field(..., example="Lakshmi Anandan, Shawn O'Neil")
    examples: Optional[List[Any]] = Field(..., example=[{"ethnicity": "Not Hispanic or Latino", "sex_at_birth": "Female", "obesity": 0.0, "diabetes": 0.0, "cardiovascular_disease": 0.0, "smoking_status": 0.0, "alcohol_use": 0.0, "bmi": 25.0, "age_at_time_0": 50.0}, {"ethnicity": "Hispanic or Latino", "sex_at_birth": "Male", "obesity": 1.0, "diabetes": 1.0, "cardiovascular_disease": 1.0, "smoking_status": 1.0, "alcohol_use": 1.0, "bmi": 32.0, "age_at_time_0": 65.0}])
    readme: Optional[str] = Field(..., example="## Cox PH Model for COPD Prediction\nThis model implements a Cox Proportional Hazards model using lifelines for survival analysis of COPD. It predicts partial hazard scores and survival probabilities at 5 years based on a set of demographic and comorbidity features.")

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