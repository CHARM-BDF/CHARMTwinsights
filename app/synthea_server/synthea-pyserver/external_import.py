"""External FHIR import: version detection, stub patients, isolation transaction.

Kept separate from main.py so these pure functions are unit-testable without
importing the FastAPI application.
"""
from __future__ import annotations

import copy

# Resource types that were renamed/removed after DSTU2 (strong DSTU2 signal).
_DSTU2_ONLY_TYPES = {"MedicationOrder", "DeviceUseRequest", "DiagnosticOrder", "BodySite"}

SRC_ID_SYSTEM = "urn:charm:apple-healthkit-src-id"


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


def _iter_references(obj):
    """Yield every reference string found anywhere in a nested structure."""
    if isinstance(obj, dict):
        ref = obj.get("reference")
        if isinstance(ref, str):
            yield ref
        for v in obj.values():
            yield from _iter_references(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_references(item)


def synthesize_stub_patients(bundle: dict) -> dict:
    """Add a minimal R4 Patient for each Patient/<id> referenced but not contained."""
    bundle = copy.deepcopy(bundle)
    entries = bundle.setdefault("entry", [])

    present = {e["resource"]["id"] for e in entries
               if e.get("resource", {}).get("resourceType") == "Patient" and e["resource"].get("id")}

    referenced = set()
    for e in entries:
        for ref in _iter_references(e.get("resource", {})):
            if ref.startswith("Patient/"):
                referenced.add(ref[len("Patient/"):])

    for pid in sorted(referenced - present):
        entries.append({"resource": {
            "resourceType": "Patient",
            "id": pid,
            "identifier": [{"system": SRC_ID_SYSTEM, "value": pid}],
        }})
    return bundle
