from fastapi import APIRouter, HTTPException, Path
from fastapi.responses import Response
import httpx
import logging

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/synthetic/synthea",
    tags=["Synthetic Data PDF Reports"],
)

PDF_RESPONSE = {
    200: {
        "description": "PDF file",
        "content": {
            "application/pdf": {
                "schema": {"type": "string", "format": "binary"}
            }
        },
    }
}


@router.get(
    "/patient/{patient_id}/pdf",
    response_class=Response,
    responses=PDF_RESPONSE,
)
async def get_patient_pdf(
    patient_id: str = Path(..., description="FHIR Patient resource ID")
):
    """
    Generate a PDF health summary for a specific patient.

    The PDF includes:
    - Personal Information (name, gender, DOB, address, phone, marital status)
    - Demographics (race, ethnicity, birth place, language)
    - Diagnoses/Conditions (with status and onset date)
    - Medications (with status and date)
    - Procedures (with status and date)
    - Healthcare Visits/Encounters (type, class, date, provider)
    - Immunizations
    - Recent Vital Signs

    Technical IDs, FHIR codes, and non-human-readable data are filtered out.
    """
    url = f"{settings.synthea_server_url}/patient/{patient_id}/pdf"

    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return Response(
                content=resp.content,
                media_type=resp.headers.get("content-type", "application/pdf"),
                headers={
                    "Content-Disposition": resp.headers.get(
                        "content-disposition",
                        f"attachment; filename=synthetic-patient-report-ID-{patient_id}.pdf",
                    )
                },
            )
    except httpx.HTTPStatusError as e:
        logger.error(f"Synthea backend error (patient PDF): {e.response.text}")
        detail = e.response.text or f"Error generating PDF for patient {patient_id}"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting Synthea backend (patient PDF): {e}")
        raise HTTPException(status_code=500, detail="Synthea server unreachable")


@router.get(
    "/random-patient/pdf",
    response_class=Response,
    responses=PDF_RESPONSE,
)
async def get_random_patient_pdf():
    """
    Generate a PDF health summary for a random patient from the database.

    This is useful for testing the PDF generation feature.
    """
    url = f"{settings.synthea_server_url}/random-patient/pdf"

    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return Response(
                content=resp.content,
                media_type=resp.headers.get("content-type", "application/pdf"),
                headers={
                    "Content-Disposition": resp.headers.get(
                        "content-disposition",
                        "attachment; filename=synthetic-random-patient-report.pdf",
                    )
                },
            )
    except httpx.HTTPStatusError as e:
        logger.error(f"Synthea backend error (random patient PDF): {e.response.text}")
        detail = e.response.text or "Error generating random patient PDF"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error contacting Synthea backend (random patient PDF): {e}")
        raise HTTPException(status_code=500, detail="Synthea server unreachable")
