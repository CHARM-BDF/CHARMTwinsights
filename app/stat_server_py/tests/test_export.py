"""Offline tests for the FHIR export builders (no HAPI needed).

Run from anywhere: python3 app/stat_server_py/tests/test_export.py
"""
import csv
import io
import json
import os
import sys
import zipfile
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyserver.export import (
    build_export_zip, build_flat_export_zip, PATIENT_TYPES, REFERENCE_TYPES,
)

fails = []
def check(name, cond, extra=""):
    status = "PASS" if cond else "FAIL"
    if not cond:
        fails.append(name)
    print(f"[{status}] {name} {extra}")

P1 = {"resourceType": "Patient", "id": "p1", "gender": "female", "birthDate": "1970-01-01",
      "meta": {"tag": [{"system": "urn:charm:cohort", "code": "c1"},
                       {"system": "urn:charm:datatype", "code": "synthetic"}]}}
P2 = {"resourceType": "Patient", "id": "p2", "gender": "male", "birthDate": "1980-06-01",
      "meta": {"tag": [{"system": "urn:charm:cohort", "code": "c2"}]}}
P3 = {"resourceType": "Patient", "id": "p3", "gender": "male", "birthDate": "1990-01-01"}

COND = lambda pid, text, code: {
    "resourceType": "Condition", "id": f"cond-{pid}-{code}",
    "subject": {"reference": f"Patient/{pid}"},
    "code": {"text": text, "coding": [{"code": code}]},
}

TAGGED = {
    "c1": {
        "Patient": [P1],
        "Condition": [COND("p1", "Hypertension (disorder)", "38341003")],
        "Organization": [{"resourceType": "Organization", "id": "org-c1"}],
    },
    "c2": {
        "Patient": [P2, P1],  # p1 tagged into both cohorts
        "Condition": [],
    },
}
ALL = {
    "Patient": [P1, P2, P3],
    "Condition": [COND("p1", "Hypertension (disorder)", "38341003"),
                  COND("p2", "Hypertension (disorder)", "38341003"),
                  COND("p2", "Asthma", "195967001")],
    "MedicationRequest": [{
        "resourceType": "MedicationRequest", "id": "m1",
        "subject": {"reference": "Patient/p1"},
        "medicationCodeableConcept": {"text": "Metformin", "coding": [{"code": "860975"}]},
    }],
    "Organization": [{"resourceType": "Organization", "id": "org1"},
                     {"resourceType": "Organization", "id": "org-c1"}],
}

def _src(extra_params):
    tag = next((p.split("|", 1)[1] for p in extra_params if p.startswith("_tag=")), None)
    return TAGGED.get(tag, ALL) if tag else ALL

def fake_iter(hapi_url, resource_type, extra_params, max_items=10**6):
    yield from _src(extra_params).get(resource_type, [])

def fake_count(hapi_url, resource_type, extra_params):
    return len(_src(extra_params).get(resource_type, []))

with patch("pyserver.export._iter_search", fake_iter), \
     patch("pyserver.export._count_type", fake_count):

    # ── NDJSON: whole store ──
    path, manifest = build_export_zip("http://fake")
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
            check("manifest + card present", {"manifest.json", "README.md"} <= names)
            check("reference types in full export", "Organization.ndjson" in names)
            lines = zf.read("Patient.ndjson").decode().strip().splitlines()
            check("all patients exported", len(lines) == 3, f"(n={len(lines)})")
            m = json.loads(zf.read("manifest.json"))
            check("count verification clean",
                  m["count_verification"]["performed"] and not m["count_verification"]["mismatches"])
    finally:
        os.unlink(path)

    # ── NDJSON: cohort-scoped, dedupe + providers included ──
    path2, _m2 = build_export_zip("http://fake", ["c1", "c2"])
    try:
        with zipfile.ZipFile(path2) as zf:
            ids = {json.loads(l)["id"] for l in zf.read("Patient.ndjson").decode().strip().splitlines()}
            check("cohort scope deduped", ids == {"p1", "p2"}, f"(ids={ids})")
            check("providers included in cohort export",
                  "Organization.ndjson" in zf.namelist(),
                  f"(names={sorted(zf.namelist())[:6]})")
            m2 = json.loads(zf.read("manifest.json"))
            check("multi-cohort skips exact verification",
                  m2["count_verification"]["performed"] is False)
    finally:
        os.unlink(path2)

    # ── flat table ──
    path3, m3 = build_flat_export_zip("http://fake")
    try:
        with zipfile.ZipFile(path3) as zf:
            rows = list(csv.reader(io.TextIOWrapper(zf.open("patients_flat.csv"), encoding="utf-8")))
            header, body = rows[0], rows[1:]
            check("one row per patient", len(body) == 3, f"(rows={len(body)})")
            check("demographic columns first",
                  header[:4] == ["patient_id", "gender", "birth_date", "age"])
            check("indicator columns present",
                  any(c.startswith("cond_hypertension") for c in header)
                  and any(c.startswith("med_metformin") for c in header),
                  f"(cols={header[7:]})")
            hyp_idx = next(i for i, c in enumerate(header) if c.startswith("cond_hypertension"))
            hyp_sum = sum(int(r[hyp_idx]) for r in body)
            check("indicator sums match prevalence", hyp_sum == 2, f"(sum={hyp_sum})")
            byid = {r[0]: r for r in body}
            check("p3 has no conditions", all(v == "0" for v in byid["p3"][7:]))
            check("cohorts column populated", byid["p1"][5] == "c1" and byid["p1"][6] == "synthetic",
                  f"(p1={byid['p1'][:7]})")
            d = json.loads(zf.read("data_dictionary.json"))
            check("dictionary maps labels",
                  any(v["label"] == "Hypertension (disorder)" and v["patients"] == 2 for v in d.values()))
            mf = json.loads(zf.read("manifest.json"))
            check("flat manifest", mf["patients"] == 3 and mf["indicator_columns"] == len(header) - 7)
            card = zf.read("README.md").decode()
            check("flat card has csv snippet", 'load_dataset("csv"' in card)
    finally:
        os.unlink(path3)

check("type lists sane", "Patient" in PATIENT_TYPES and "Organization" in REFERENCE_TYPES)

print()
print("FAILURES:", fails if fails else "none — all tests passed")
sys.exit(1 if fails else 0)
