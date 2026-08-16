"""Proxy for FHIR data export (stat_server_py builds the zip; we stream it)."""

import logging
from typing import List

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/export",
    tags=["Data Export"],
)

BACKEND_URL = settings.stat_server_py_url.rstrip("/")


@router.get("/fhir")
async def proxy_export_fhir(cohort_id: List[str] = Query(default=[]),
                            format: List[str] = Query(default=[])):
    """Zip export: format=ndjson (Bulk Data layout), format=bundles (one
    Bundle file per patient, Synthea-style layout), format=flat (one CSV row
    per patient) — repeat ?format= to get several in one archive, each under
    its own directory. Repeat ?cohort_id= to export specific cohorts; omit for
    the whole store. Streamed through so large exports never buffer here."""
    url = f"{BACKEND_URL}/export/fhir"
    params = ([("cohort_id", c) for c in cohort_id]
              + [("format", f) for f in (format or ["ndjson"])])
    client = httpx.AsyncClient(timeout=httpx.Timeout(1800.0, connect=10.0))
    try:
        req = client.build_request("GET", url, params=params)
        resp = await client.send(req, stream=True)
    except httpx.RequestError as e:
        await client.aclose()
        logger.error(f"Error contacting backend (export): {e}")
        raise HTTPException(status_code=500, detail="stat_server_py unreachable")

    if resp.status_code != 200:
        detail = (await resp.aread()).decode(errors="replace")
        await resp.aclose()
        await client.aclose()
        logger.error(f"Backend error (export): {detail}")
        raise HTTPException(status_code=resp.status_code, detail=detail or "Export failed")

    async def cleanup():
        await resp.aclose()
        await client.aclose()

    passthrough = {
        k: v for k, v in resp.headers.items()
        if k.lower() in ("content-disposition", "content-length", "x-export-total-resources")
    }
    return StreamingResponse(
        resp.aiter_bytes(),
        media_type="application/zip",
        headers=passthrough,
        background=BackgroundTask(cleanup),
    )
