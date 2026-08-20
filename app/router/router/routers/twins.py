from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import JSONResponse
import httpx
import logging
from typing import Any, Dict

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/twins",
    tags=["Digital Twins"],
)

BACKEND_URL = settings.stat_server_py_url.rstrip("/")


@router.get("/profile/{patient_id}", response_class=JSONResponse)
async def proxy_twin_subject_profile(patient_id: str):
    """Attribute profile of one patient (proxied to stat_server_py)."""
    url = f"{BACKEND_URL}/twins/profile/{patient_id}"
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Backend error (twin profile): {e.response.text}")
        detail = e.response.text or "Error fetching subject profile"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting backend (twin profile): {e}")
        raise HTTPException(status_code=500, detail="stat_server_py unreachable")


@router.post("/attribute-counts", response_class=JSONResponse)
async def proxy_twin_attribute_counts(body: Dict[str, Any] = Body(...)):
    """Store-wide attribute prevalence counts (proxied to stat_server_py)."""
    url = f"{BACKEND_URL}/twins/attribute-counts"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, json=body)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Backend error (attribute counts): {e.response.text}")
        detail = e.response.text or "Error computing attribute counts"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting backend (attribute counts): {e}")
        raise HTTPException(status_code=500, detail="stat_server_py unreachable")


@router.post("/find", response_class=JSONResponse)
async def proxy_find_digital_twins(request: Dict[str, Any] = Body(...)):
    """Similarity-ranked digital twin search (proxied to stat_server_py).

    Body: subject_id plus the selected attributes to match on. See the
    stat server's TwinFindRequest model. Scoring can scan a whole cohort,
    so the timeout is generous.
    """
    url = f"{BACKEND_URL}/twins/find"
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.post(url, json=request)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Backend error (twin search): {e.response.text}")
        detail = e.response.text or "Error running twin search"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting backend (twin search): {e}")
        raise HTTPException(status_code=500, detail="stat_server_py unreachable")
