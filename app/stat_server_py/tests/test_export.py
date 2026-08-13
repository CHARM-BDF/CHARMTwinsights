"""Offline tests for the FHIR export zip builder (no HAPI needed).

Run from anywhere: python3 app/stat_server_py/tests/test_export.py
"""
import io
import json
import os
import sys
import zipfile
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyserver.export import build_export_zip, PATIENT_TYPES, REFERENCE_TYPES

fails = []
def check(name, cond, extra=""):
    status = "PASS" if cond else "FAIL"
    if not cond:
        fails.append(name)
    print(f"[{status}] {name} {extra}")

TAGGED = {
    "c1": {
        "Patient": [{"resourceType": "Patient", "id": "p1"}],
        "Condition": [{"resourceType": "Condition", "id": "c-1", "code": {"text": "HTN"}}],
    },
    "c2": {
        "Patient": [{"resourceType": "Patient", "id": "p2"},
                    {"resourceType": "Patient", "id": "p1"}],  # p1 tagged into both cohorts
        "Condition": [],
    },
}
ALL = {
    "Patient": [{"resourceType": "Patient", "id": "p1"},
                {"resourceType": "Patient", "id": "p2"},
                {"resourceType": "Patient", "id": "p3"}],
    "Condition": [{"resourceType": "Condition", "id": "c-1"}],
    "Organization": [{"resourceType": "Organization", "id": "org1"}],
}

def fake_search(hapi_url, resource_type, extra_params, max_items=10**6):
    tag = next((p.split("|", 1)[1] for p in extra_params if p.startswith("_tag=")), None)
    src = TAGGED.get(tag, ALL) if tag else ALL
    yield from src.get(resource_type, [])

with patch("pyserver.export._iter_search", fake_search):
    # ── whole store ──
    path, manifest = build_export_zip("http://fake")
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
            check("manifest + card present", {"manifest.json", "README.md"} <= names)
            check("patient file present", "Patient.ndjson" in names)
            check("reference types in full export", "Organization.ndjson" in names)
            lines = zf.read("Patient.ndjson").decode().strip().splitlines()
            check("all patients exported", len(lines) == 3, f"(n={len(lines)})")
            check("lines are FHIR resources",
                  json.loads(lines[0])["resourceType"] == "Patient")
            m = json.loads(zf.read("manifest.json"))
            check("manifest counts", m["resource_counts"]["Patient"] == 3 and m["scope"] == "all")
            check("manifest totals", m["total_resources"] == sum(m["resource_counts"].values()))
            card = zf.read("README.md").decode()
            check("card has HF snippet", "load_dataset" in card)
    finally:
        os.unlink(path)

    # ── cohort-scoped, with a patient tagged into both requested cohorts ──
    path2, manifest2 = build_export_zip("http://fake", ["c1", "c2"])
    try:
        with zipfile.ZipFile(path2) as zf:
            lines = zf.read("Patient.ndjson").decode().strip().splitlines()
            ids = {json.loads(l)["id"] for l in lines}
            check("cohort scope deduped", ids == {"p1", "p2"}, f"(ids={ids})")
            names = set(zf.namelist())
            check("no reference types in cohort export", "Organization.ndjson" not in names)
            check("empty types omitted", "Observation.ndjson" not in names)
            m2 = json.loads(zf.read("manifest.json"))
            check("cohort manifest", m2["scope"] == "cohorts" and m2["cohorts"] == ["c1", "c2"])
    finally:
        os.unlink(path2)

check("type lists sane", "Patient" in PATIENT_TYPES and "Organization" in REFERENCE_TYPES)

print()
print("FAILURES:", fails if fails else "none — all tests passed")
sys.exit(1 if fails else 0)
