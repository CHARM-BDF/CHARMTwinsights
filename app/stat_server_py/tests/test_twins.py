"""Offline tests for the twin-finder scoring logic (no HAPI needed).

Run from anywhere: python3 app/stat_server_py/tests/test_twins.py
(needs pydantic + requests, both already stat_server_py dependencies).
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from unittest.mock import patch
from pyserver.twins import (
    TwinFinder, TwinFindRequest, TwinCriteriaItem, TwinDemographics,
    normalize_label, item_keys, resource_keys, age_from_birth_date,
    extract_ethnicity, patient_tags,
)

fails = []
def check(name, cond, extra=""):
    status = "PASS" if cond else "FAIL"
    if not cond:
        fails.append(name)
    print(f"[{status}] {name} {extra}")

# ── helpers ──────────────────────────────────────────────────────────────────
check("normalize strips qualifier", normalize_label("Hypertension  (disorder)") == "hypertension")
check("normalize collapses ws", normalize_label("  Type 2   Diabetes ") == "type 2 diabetes")

item = TwinCriteriaItem(label="Hypertension (disorder)", codes=["38341003"])
check("item_keys has code+text", item_keys(item) == {"38341003", "hypertension"})

res = {"code": {"text": "Hypertension (disorder)",
                "coding": [{"code": "38341003", "display": "Hypertension"}]}}
check("resource_keys union", resource_keys(res) == {"38341003", "hypertension"})

med = {"medicationCodeableConcept": {"text": "Metformin 500mg", "coding": [{"code": "860975"}]}}
check("med code_field", resource_keys(med, "medicationCodeableConcept") == {"860975", "metformin 500mg"})

check("age None", age_from_birth_date(None) is None)
check("age year-only parses", isinstance(age_from_birth_date("1970"), int))

pat_ext = {"extension": [{"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                          "extension": [{"url": "ombCategory", "valueCoding": {"display": "Not Hispanic or Latino"}},
                                        {"url": "text", "valueString": "Not Hispanic or Latino"}]}]}
check("ethnicity extracted", extract_ethnicity(pat_ext) == "Not Hispanic or Latino")

pat_tagged = {"meta": {"tag": [{"system": "urn:charm:cohort", "code": "cohort-a"},
                               {"system": "urn:charm:datatype", "code": "synthetic"}]}}
t = patient_tags(pat_tagged)
check("tags parsed", t == {"cohort_ids": ["cohort-a"], "datatype": "synthetic"})

# ── category scoring ─────────────────────────────────────────────────────────
items = [TwinCriteriaItem(label="Hypertension", codes=["38341003"]),
         TwinCriteriaItem(label="Type 2 diabetes", codes=["44054006"])]
s, m, mi = TwinFinder.score_category(items, {"38341003", "something-else"})
check("category half match", abs(s - 0.5) < 1e-9 and m == ["Hypertension"] and mi == ["Type 2 diabetes"])

s2, _, _ = TwinFinder.score_category(items, {"hypertension", "type 2 diabetes"})
check("category text match", s2 == 1.0)

# ── demographic scoring ──────────────────────────────────────────────────────
demo = TwinDemographics(gender="female", age=58, age_tolerance=10)
from datetime import date
by = date.today().year - 61
s3, d3 = TwinFinder.score_demographics(demo, {"gender": "female", "birthDate": f"{by}-01-01"})
check("demo gender+age", 0.8 <= s3 <= 1.0, f"(score={s3}, detail={d3})")

s4, d4 = TwinFinder.score_demographics(demo, {"gender": "male", "birthDate": None})
check("demo mismatch floors", s4 == 0.0, f"(score={s4})")

# ── end-to-end find() with a mocked HAPI ─────────────────────────────────────
SUBJECT = "subj-1"
PATIENTS = [
    {"resourceType": "Patient", "id": "subj-1", "gender": "female", "birthDate": "1968-05-01"},
    {"resourceType": "Patient", "id": "twin-good", "gender": "female", "birthDate": "1966-03-02",
     "meta": {"tag": [{"system": "urn:charm:cohort", "code": "c1"}]}},
    {"resourceType": "Patient", "id": "twin-meh", "gender": "male", "birthDate": "1950-01-01"},
]
CONDITIONS = [
    {"resourceType": "Condition", "subject": {"reference": "Patient/subj-1"},
     "code": {"text": "Hypertension (disorder)", "coding": [{"code": "38341003"}]}},
    {"resourceType": "Condition", "subject": {"reference": "Patient/twin-good"},
     "code": {"text": "Hypertension (disorder)", "coding": [{"code": "38341003"}]}},
    {"resourceType": "Condition", "subject": {"reference": "Patient/twin-meh"},
     "code": {"text": "Fracture of arm", "coding": [{"code": "123"}]}},
]
MEDS = [
    {"resourceType": "MedicationRequest", "subject": {"reference": "Patient/subj-1"},
     "medicationCodeableConcept": {"text": "Metformin", "coding": [{"code": "860975"}]}},
    {"resourceType": "MedicationRequest", "subject": {"reference": "Patient/twin-good"},
     "medicationCodeableConcept": {"text": "Metformin", "coding": [{"code": "860975"}]}},
]

def fake_fetch(self, resource_type, cohort_id, max_items, elements=None):
    data = {"Patient": PATIENTS, "Condition": CONDITIONS,
            "MedicationRequest": MEDS, "MedicationStatement": [], "Procedure": []}
    return data[resource_type][:max_items], True

def fake_search(self, resource_type, extra_params, max_items):
    # Honors subject=Patient/a,Patient/b filters like real FHIR search, so
    # candidate-scoped fetches exclude the subject exactly as HAPI would.
    data = {"Patient": PATIENTS, "Condition": CONDITIONS,
            "MedicationRequest": MEDS, "MedicationStatement": [], "Procedure": []}
    rows = data[resource_type]
    for p in extra_params:
        if p.startswith("subject="):
            allowed = set(p[len("subject="):].split(","))
            rows = [r for r in rows if (r.get("subject") or {}).get("reference") in allowed]
    return rows[:max_items], True

req = TwinFindRequest(
    subject_id=SUBJECT,
    demographics=TwinDemographics(gender="female", age=58, age_tolerance=10),
    conditions=[TwinCriteriaItem(label="Hypertension", codes=["38341003"])],
    medications=[TwinCriteriaItem(label="Metformin", codes=["860975"])],
    top_k=10, weighting="balanced",
)

with patch.object(TwinFinder, "_fetch_paged", fake_fetch), patch.object(TwinFinder, "_fetch_search", fake_search):
    out = TwinFinder("http://fake").find(req)

check("subject excluded", all(m["patient_id"] != SUBJECT for m in out["matches"]))
check("two candidates", out["total_candidates"] == 2)
top = out["matches"][0]
check("best is twin-good", top["patient_id"] == "twin-good", f"(top={top['patient_id']} score={top['score']})")
check("top score high", top["score"] > 0.9, f"(score={top['score']})")
check("subscores present", set(top["subscores"]) == {"demographics", "conditions", "medications"})
check("matched lists", top["matched"]["conditions"] == ["Hypertension"] and top["matched"]["medications"] == ["Metformin"])
check("cohort tag surfaced", top["cohort_ids"] == ["c1"])
worst = out["matches"][-1]
check("worst is twin-meh w/ low score", worst["patient_id"] == "twin-meh" and worst["score"] < 0.3, f"(score={worst['score']})")
check("coverage lists categories", set(out["coverage"]["categories_scored"]) == {"demographics", "conditions", "medications"})

# ── weighting resolution ─────────────────────────────────────────────────────
from pyserver.twins import resolve_weights
check("equal weights all 1.0", set(resolve_weights("equal").values()) == {1.0})
check("category emphasis doubles",
      resolve_weights("medications")["medications"] == 2.0
      and resolve_weights("medications")["conditions"] == 1.0)
check("legacy balanced = equal", resolve_weights("balanced") == resolve_weights("equal"))
check("legacy pharma emphasizes meds", resolve_weights("pharma")["medications"] == 2.0)
try:
    resolve_weights("bogus")
    check("bogus weighting rejected", False)
except ValueError:
    check("bogus weighting rejected", True)

# emphasis end-to-end: legacy alias and category name both run and rank twin-good first
for w in ("pharma", "conditions"):
    with patch.object(TwinFinder, "_fetch_paged", fake_fetch), patch.object(TwinFinder, "_fetch_search", fake_search):
        out2 = TwinFinder("http://fake").find(req.model_copy(update={"weighting": w}))
    check(f"weighting '{w}' runs", out2["matches"][0]["patient_id"] == "twin-good")

# ── prevalence block ─────────────────────────────────────────────────────────
def fake_profile(self, pid):
    return {"id": pid, "gender": "female", "age": 58, "ethnicity": None,
            "cohort_ids": [], "datatype": None,
            "conditions": [{"label": "Hypertension", "codes": ["38341003"]}],
            "medications": [{"label": "Metformin", "codes": ["860975"]}],
            "procedures": []}

with patch.object(TwinFinder, "_fetch_paged", fake_fetch), \
     patch.object(TwinFinder, "_fetch_search", fake_search), \
     patch.object(TwinFinder, "subject_profile", fake_profile):
    out3 = TwinFinder("http://fake").find(req)
pv = out3["prevalence"]
check("prevalence present", pv is not None and pv["of"] == 2, f"(pv={pv and pv['of']})")
check("prevalence condition count",
      pv["conditions"][0]["label"] == "Hypertension" and pv["conditions"][0]["count"] == 1,
      f"(rows={pv['conditions']})")
check("prevalence med count", pv["medications"][0]["count"] == 1)
check("prevalence gender row", any(r["key"] == "gender" and r["count"] == 1 for r in pv["demographics"]))
check("prevalence age row", any(r["key"] == "age" and r["count"] == 1 for r in pv["demographics"]))

# ── store-wide attribute-count cache ─────────────────────────────────────────
from pyserver.twins import AttributeCountsRequest

with patch.object(TwinFinder, "_fetch_paged", fake_fetch), \
     patch.object(TwinFinder, "_fetch_search", fake_search):
    fc = TwinFinder("http://fake")
    fc._build_count_cache()  # synchronous build (no background thread in tests)
    outc = fc.attribute_counts(AttributeCountsRequest(
        subject_id=SUBJECT,
        demographics=TwinDemographics(gender="female", age=58, age_tolerance=10),
        conditions=[TwinCriteriaItem(label="Hypertension", codes=["38341003"])],
        medications=[TwinCriteriaItem(label="Metformin", codes=["860975"])],
    ))
check("counts ready", outc["status"] == "ready", f"(status={outc.get('status')})")
check("counts total others", outc["total_others"] == 2)
check("counts exclude the subject",
      outc["conditions"][0]["count"] == 1 and outc["medications"][0]["count"] == 1,
      f"(cond={outc['conditions']}, med={outc['medications']})")
check("counts gender excludes subject",
      any(r["key"] == "gender" and r["count"] == 1 for r in outc["demographics"]))
check("counts age band", any(r["key"] == "age" and r["count"] == 1 for r in outc["demographics"]))

# error: nothing selected
try:
    bad = TwinFindRequest(subject_id="x")
    with patch.object(TwinFinder, "_fetch_paged", fake_fetch), patch.object(TwinFinder, "_fetch_search", fake_search):
        TwinFinder("http://fake").find(bad)
    check("empty selection rejected", False)
except ValueError:
    check("empty selection rejected", True)

print()
print("FAILURES:", fails if fails else "none — all tests passed")
sys.exit(1 if fails else 0)
