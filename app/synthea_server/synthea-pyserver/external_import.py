"""External FHIR import: version detection, stub patients, isolation transaction.

Kept separate from main.py so these pure functions are unit-testable without
importing the FastAPI application.
"""
from __future__ import annotations

import copy
import uuid
from urllib.parse import quote as _urlquote

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


def _primary_identifier(resource: dict) -> tuple[str, str] | None:
    """Return (system, value) of the resource's first identifier, or None."""
    ids = resource.get("identifier")
    if isinstance(ids, list) and ids:
        first = ids[0]
        if isinstance(first, dict) and first.get("system") and first.get("value") is not None:
            return first["system"], str(first["value"])
    return None


def build_isolation_transaction(bundle: dict) -> tuple[dict, list[dict]]:
    bundle = copy.deepcopy(bundle)
    entries = bundle.get("entry", [])

    # 1. Assign a urn:uuid to every entry and index (Type/id) -> urn:uuid.
    ref_index: dict[str, str] = {}
    for e in entries:
        res = e.get("resource", {})
        e["fullUrl"] = f"urn:uuid:{uuid.uuid4()}"
        rtype, rid = res.get("resourceType"), res.get("id")
        if rtype and rid:
            ref_index[f"{rtype}/{rid}"] = e["fullUrl"]

    # 2. Rewrite in-bundle refs to urn:uuid; collect dangling ones.
    unresolved: list[dict] = []

    def rewrite(obj, source_label):
        if isinstance(obj, dict):
            ref = obj.get("reference")
            if isinstance(ref, str) and "/" in ref and not ref.startswith("urn:"):
                if ref in ref_index:
                    obj["reference"] = ref_index[ref]
                else:
                    unresolved.append({"source": source_label, "reference": ref})
            for v in obj.values():
                rewrite(v, source_label)
        elif isinstance(obj, list):
            for item in obj:
                rewrite(item, source_label)

    for e in entries:
        res = e.get("resource", {})
        source_label = f"{res.get('resourceType')}/{res.get('id')}" if res.get("id") else res.get("resourceType", "?")
        rewrite(res, source_label)

    # 3. Build POST + ifNoneExist request per entry (idempotent conditional-create).
    for e in entries:
        res = e.get("resource", {})
        rtype = res.get("resourceType")
        ident = _primary_identifier(res)
        if ident is None and res.get("id"):
            ident = (SRC_ID_SYSTEM, str(res["id"]))
            res.setdefault("identifier", []).append({"system": ident[0], "value": ident[1]})
        # Server assigns the id; drop the source id so it is never asserted.
        res.pop("id", None)
        req = {"method": "POST", "url": rtype}
        if ident is not None:
            token = f"{ident[0]}|{ident[1]}"
            req["ifNoneExist"] = f"identifier={_urlquote(token, safe=':')}"
        e["request"] = req

    bundle["type"] = "transaction"
    return bundle, unresolved
