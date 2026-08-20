"""External FHIR import: version detection, stub patients, isolation transaction.

Kept separate from main.py so these pure functions are unit-testable without
importing the FastAPI application.
"""
from __future__ import annotations

# Resource types that were renamed/removed after DSTU2 (strong DSTU2 signal).
_DSTU2_ONLY_TYPES = {"MedicationOrder", "DeviceUseRequest", "DiagnosticOrder", "BodySite"}


def detect_fhir_version(bundle: dict, hint: str | None = None) -> str:
    """Return "DSTU2" or "R4". Explicit hint wins; otherwise use structural heuristics."""
    if hint:
        h = hint.strip().upper()
        if h in ("DSTU2", "R4"):
            return h
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") in _DSTU2_ONLY_TYPES:
            return "DSTU2"
        # DSTU2 Observation.category is a single object; R4 makes it an array.
        if res.get("resourceType") == "Observation" and isinstance(res.get("category"), dict):
            return "DSTU2"
    return "R4"
