"""
Digital Twin Finder Module

Scores patients in the FHIR store against a subject profile and returns
similarity-ranked matches. The caller selects which attributes of the subject
to match on (demographics, conditions, medications, procedures); each selected
category contributes a subscore, combined with a weighting preset.

Matching keys per clinical item are the union of code values and the
normalized display text, so synthetic data (uniform code.text) and external
data (same codes, possibly different text) both match.
"""

import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set

import requests
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

COHORT_TAG_SYSTEM = "urn:charm:cohort"
DATATYPE_TAG_SYSTEM = "urn:charm:datatype"

# Weighting presets: relative weight per category. Only categories the caller
# actually selected participate; weights are renormalized over those.
WEIGHT_PRESETS = {
    "balanced": {"demographics": 1.0, "conditions": 1.0, "medications": 1.0, "procedures": 1.0},
    "clinical": {"demographics": 1.0, "conditions": 2.0, "medications": 1.0, "procedures": 2.0},
    "pharma":   {"demographics": 1.0, "conditions": 1.0, "medications": 2.0, "procedures": 1.0},
}


# ─── request / response models ────────────────────────────────────────────────

class TwinCriteriaItem(BaseModel):
    """One selected clinical attribute of the subject (a condition, med, ...)."""
    label: str = Field(..., description="Display text as shown to the user")
    codes: List[str] = Field(default_factory=list, description="Code values from the resource's codings")


class TwinDemographics(BaseModel):
    """Selected demographic attributes. Omitted fields are not matched on."""
    gender: Optional[str] = Field(None, description="Subject gender to match (exact)")
    age: Optional[int] = Field(None, ge=0, le=140, description="Subject age in years")
    age_tolerance: int = Field(10, gt=0, le=100, description="Years of age difference at which the age score reaches 0")
    ethnicity: Optional[str] = Field(None, description="Subject ethnicity to match (exact, case-insensitive)")


class TwinFindRequest(BaseModel):
    subject_id: str = Field(..., description="Patient ID of the subject; excluded from results by default")
    demographics: Optional[TwinDemographics] = None
    conditions: List[TwinCriteriaItem] = Field(default_factory=list)
    medications: List[TwinCriteriaItem] = Field(default_factory=list)
    procedures: List[TwinCriteriaItem] = Field(default_factory=list)
    cohort_id: Optional[str] = Field(None, description="Restrict candidates to this cohort (tag filter); None = all patients")
    exclude_subject: bool = Field(True, description="Drop the subject itself from the results")
    top_k: int = Field(20, gt=0, le=500)
    weighting: str = Field("balanced", description="One of: " + ", ".join(WEIGHT_PRESETS))
    max_candidates: int = Field(2000, gt=0, le=10000, description="Safety cap on patients scanned")


# ─── helpers ──────────────────────────────────────────────────────────────────

def normalize_label(text: str) -> str:
    """Normalize a display label for text matching: lowercase, collapse
    whitespace, and drop trailing SNOMED qualifiers like '(disorder)'."""
    t = re.sub(r"\s+", " ", text.strip().lower())
    t = re.sub(r"\s*\((disorder|finding|procedure|situation|product)\)$", "", t)
    return t


def item_keys(item: TwinCriteriaItem) -> Set[str]:
    """Match keys for one selected attribute: its codes plus normalized label."""
    keys = {c.strip() for c in item.codes if c and c.strip()}
    if item.label and item.label.strip():
        keys.add(normalize_label(item.label))
    return keys


def resource_keys(resource: Dict, code_field: str = "code") -> Set[str]:
    """Match keys for a candidate's resource: coding codes + normalized texts."""
    keys: Set[str] = set()
    concept = resource.get(code_field) or {}
    for coding in concept.get("coding", []) or []:
        if coding.get("code"):
            keys.add(str(coding["code"]).strip())
        if coding.get("display"):
            keys.add(normalize_label(coding["display"]))
    if concept.get("text"):
        keys.add(normalize_label(concept["text"]))
    return keys


def age_from_birth_date(birth_date: Optional[str]) -> Optional[int]:
    """Age in whole years from a YYYY[-MM[-DD]] birthDate string."""
    if not birth_date:
        return None
    try:
        parts = [int(p) for p in birth_date.split("-")]
        born = date(parts[0], parts[1] if len(parts) > 1 else 7, parts[2] if len(parts) > 2 else 1)
        today = date.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    except (ValueError, IndexError):
        return None


def extract_ethnicity(patient: Dict) -> Optional[str]:
    """Best-effort ethnicity from US-Core-style extensions."""
    for ext in patient.get("extension", []) or []:
        url = ext.get("url", "")
        if "us-core-ethnicity" not in url and "us-core-race" not in url:
            continue
        for sub in ext.get("extension", []) or []:
            if sub.get("url") in ("ombCategory", "detailed"):
                coding = sub.get("valueCoding") or {}
                if coding.get("display"):
                    return coding["display"]
            if sub.get("url") == "text" and sub.get("valueString"):
                return sub["valueString"]
        if ext.get("valueString"):
            return ext["valueString"]
    return None


def patient_tags(patient: Dict) -> Dict[str, Any]:
    """Cohort membership and datatype from a Patient's meta tags."""
    cohorts, datatype = [], None
    for tag in (patient.get("meta") or {}).get("tag", []) or []:
        if tag.get("system") == COHORT_TAG_SYSTEM and tag.get("code"):
            cohorts.append(tag["code"])
        elif tag.get("system") == DATATYPE_TAG_SYSTEM and tag.get("code"):
            datatype = tag["code"]
    return {"cohort_ids": cohorts, "datatype": datatype}


# ─── finder ───────────────────────────────────────────────────────────────────

class TwinFinder:
    """Fetches candidates from HAPI and scores them against the request."""

    PAGE_SIZE = 500

    def __init__(self, hapi_url: str):
        self.hapi_url = hapi_url.rstrip("/")

    # -- fetching ------------------------------------------------------------

    def _fetch_search(self, resource_type: str, extra_params: List[str], max_items: int):
        """Run a FHIR search with stateless offset paging, up to max_items.

        Returns (resources, complete). Each page is an independent
        _count/_offset query, so there is no server-side paging cursor to
        expire (HAPI 410 Gone under load, which link-following suffers from).
        If a later page still fails, we keep what we have and report
        complete=False; the first page failing is a hard error.
        """
        resources: List[Dict] = []
        offset = 0
        while len(resources) < max_items:
            # _sort pins the row order: without it, offset slices of an
            # unsorted query can skip/duplicate rows between pages.
            params = [f"_count={self.PAGE_SIZE}", f"_offset={offset}", "_sort=_id"] + extra_params
            url = f"{self.hapi_url}/{resource_type}?{'&'.join(params)}"
            try:
                r = requests.get(url, headers={"Accept": "application/fhir+json"}, timeout=60)
                r.raise_for_status()
            except requests.RequestException as e:
                if offset == 0:
                    raise
                logger.warning(
                    f"Paging aborted for {resource_type} after {len(resources)} resources: {e}")
                return resources, False
            bundle = r.json()
            for entry in bundle.get("entry", []) or []:
                res = entry.get("resource")
                if res and res.get("resourceType") == resource_type:
                    resources.append(res)
            has_next = any(l.get("relation") == "next" for l in bundle.get("link", []) or [])
            if not has_next:
                return resources[:max_items], len(resources) <= max_items
            offset += self.PAGE_SIZE
        return resources[:max_items], False

    def _fetch_paged(self, resource_type: str, cohort_id: Optional[str], max_items: int,
                     elements: Optional[str] = None):
        """Fetch resources of one type, optionally cohort-tag-filtered and slimmed
        via _elements (fewer bytes per page keeps deep paging fast and reduces
        cursor-expiry risk). Returns (resources, complete)."""
        extra = []
        if cohort_id:
            extra.append(f"_tag={COHORT_TAG_SYSTEM}|{cohort_id}")
        if elements:
            extra.append(f"_elements={elements}")
        return self._fetch_search(resource_type, extra, max_items)

    # Candidates per subject=... OR-list query. Bounds each search to a few
    # hundred–few thousand resources (1-10 shallow pages), instead of deep-paging
    # the whole store, where HAPI expires paging cursors nondeterministically.
    FEATURE_CHUNK = 50

    def _features_by_patient(self, resource_type: str, candidate_ids: List[str],
                             code_field: str = "code"):
        """Map patient id -> union of match keys over their resources of one type,
        fetched in bounded chunks of candidates. Returns (features, complete)."""
        features: Dict[str, Set[str]] = {}
        complete = True
        for i in range(0, len(candidate_ids), self.FEATURE_CHUNK):
            chunk = candidate_ids[i:i + self.FEATURE_CHUNK]
            refs = ",".join(f"Patient/{pid}" for pid in chunk)
            resources, ok = self._fetch_search(
                resource_type,
                [f"subject={refs}", f"_elements=subject,{code_field}"],
                self.FEATURE_CHUNK * 400,
            )
            if not ok:
                complete = False
            for res in resources:
                ref = (res.get("subject") or {}).get("reference", "")
                if not ref.startswith("Patient/"):
                    continue
                pid = ref.split("/", 1)[1]
                features.setdefault(pid, set()).update(resource_keys(res, code_field))
        return features, complete

    # -- scoring -------------------------------------------------------------

    @staticmethod
    def score_category(selected: List[TwinCriteriaItem], candidate_keys: Set[str]):
        """Fraction of selected items the candidate has; returns (score, matched, missing)."""
        matched, missing = [], []
        for item in selected:
            if item_keys(item) & candidate_keys:
                matched.append(item.label)
            else:
                missing.append(item.label)
        score = len(matched) / len(selected) if selected else 0.0
        return score, matched, missing

    @staticmethod
    def score_demographics(demo: TwinDemographics, patient: Dict):
        """Mean over the requested demographic components; returns (score, detail)."""
        parts: List[float] = []
        detail: Dict[str, Any] = {}
        if demo.gender is not None:
            same = (patient.get("gender") or "").lower() == demo.gender.lower()
            parts.append(1.0 if same else 0.0)
            detail["gender_match"] = same
        if demo.age is not None:
            cand_age = age_from_birth_date(patient.get("birthDate"))
            if cand_age is None:
                parts.append(0.0)
                detail["age_diff"] = None
            else:
                diff = abs(cand_age - demo.age)
                parts.append(max(0.0, 1.0 - diff / demo.age_tolerance))
                detail["age_diff"] = diff
                detail["age"] = cand_age
        if demo.ethnicity is not None:
            cand_eth = extract_ethnicity(patient)
            same = bool(cand_eth) and cand_eth.strip().lower() == demo.ethnicity.strip().lower()
            parts.append(1.0 if same else 0.0)
            detail["ethnicity_match"] = same
            detail["ethnicity"] = cand_eth
        score = sum(parts) / len(parts) if parts else 0.0
        return score, detail

    # -- subject profile -----------------------------------------------------

    def subject_profile(self, patient_id: str) -> Dict[str, Any]:
        """The attribute profile of one patient: demographics plus deduped
        {label, codes} lists per clinical category.

        Queries each resource type directly (paged, complete) rather than
        $everything, which HAPI pages at sizes that silently truncate
        long-history patients.
        """
        r = requests.get(f"{self.hapi_url}/Patient/{patient_id}",
                         headers={"Accept": "application/fhir+json"}, timeout=30)
        if r.status_code == 404:
            raise ValueError(f"Patient '{patient_id}' not found")
        r.raise_for_status()
        patient = r.json()

        def collect(resource_types: List[str], code_field: str = "code") -> List[Dict]:
            by_key: Dict[str, Dict[str, Any]] = {}
            for rt in resource_types:
                resources, _complete = self._fetch_search(rt, [f"subject=Patient/{patient_id}"], 10000)
                for res in resources:
                    concept = res.get(code_field) or {}
                    label = concept.get("text") or next(
                        (c.get("display") for c in concept.get("coding", []) or [] if c.get("display")), None)
                    if not label:
                        continue
                    entry = by_key.setdefault(normalize_label(label), {"label": label, "codes": set()})
                    for c in concept.get("coding", []) or []:
                        if c.get("code"):
                            entry["codes"].add(str(c["code"]))
            return sorted(
                ({"label": v["label"], "codes": sorted(v["codes"])} for v in by_key.values()),
                key=lambda x: x["label"].lower(),
            )

        return {
            "id": patient.get("id"),
            "gender": patient.get("gender"),
            "birth_date": patient.get("birthDate"),
            "age": age_from_birth_date(patient.get("birthDate")),
            "ethnicity": extract_ethnicity(patient),
            **patient_tags(patient),
            "conditions": collect(["Condition"]),
            "medications": collect(["MedicationRequest", "MedicationStatement"], "medicationCodeableConcept"),
            "procedures": collect(["Procedure"]),
        }

    # -- entry point ---------------------------------------------------------

    def find(self, req: TwinFindRequest) -> Dict[str, Any]:
        weights = WEIGHT_PRESETS.get(req.weighting)
        if weights is None:
            raise ValueError(f"Unknown weighting '{req.weighting}'. Valid: {', '.join(WEIGHT_PRESETS)}")

        # Which categories participate in scoring
        use_demo = req.demographics is not None and (
            req.demographics.gender is not None
            or req.demographics.age is not None
            or req.demographics.ethnicity is not None
        )
        clinical = {
            "conditions": (req.conditions, ["Condition"]),
            "medications": (req.medications, ["MedicationRequest", "MedicationStatement"]),
            "procedures": (req.procedures, ["Procedure"]),
        }
        selected_clinical = {k: v for k, v in clinical.items() if v[0]}
        if not use_demo and not selected_clinical:
            raise ValueError("No attributes selected — pick at least one demographic or clinical attribute.")

        # Candidates
        patients, patients_complete = self._fetch_paged("Patient", req.cohort_id, req.max_candidates)
        patients_truncated = not patients_complete or len(patients) >= req.max_candidates
        candidates = {
            p["id"]: p for p in patients
            if p.get("id") and not (req.exclude_subject and p["id"] == req.subject_id)
        }

        # Clinical features, fetched per resource type in bounded chunks of
        # candidate ids — deterministic and complete, unlike deep-paging the
        # whole store (see _features_by_patient).
        candidate_ids = list(candidates.keys())
        feature_sets: Dict[str, Dict[str, Set[str]]] = {}
        incomplete_features: List[str] = []
        for category, (_items, resource_types) in selected_clinical.items():
            merged: Dict[str, Set[str]] = {}
            for rt in resource_types:
                code_field = "medicationCodeableConcept" if rt.startswith("Medication") else "code"
                by_patient, complete = self._features_by_patient(rt, candidate_ids, code_field)
                if not complete and category not in incomplete_features:
                    incomplete_features.append(category)
                for pid, keys in by_patient.items():
                    merged.setdefault(pid, set()).update(keys)
            feature_sets[category] = merged

        # Score every candidate
        matches = []
        for pid, patient in candidates.items():
            subscores: Dict[str, float] = {}
            matched: Dict[str, List[str]] = {}
            missing: Dict[str, List[str]] = {}
            demographic_detail: Dict[str, Any] = {}

            if use_demo:
                subscores["demographics"], demographic_detail = self.score_demographics(req.demographics, patient)
            for category, (items, _rts) in selected_clinical.items():
                cand_keys = feature_sets[category].get(pid, set())
                subscores[category], matched[category], missing[category] = self.score_category(items, cand_keys)

            total_weight = sum(weights[c] for c in subscores)
            score = sum(weights[c] * s for c, s in subscores.items()) / total_weight if total_weight else 0.0

            matches.append({
                "patient_id": pid,
                "gender": patient.get("gender"),
                "birth_date": patient.get("birthDate"),
                "age": age_from_birth_date(patient.get("birthDate")),
                **patient_tags(patient),
                "score": round(score, 4),
                "subscores": {k: round(v, 4) for k, v in subscores.items()},
                "matched": matched,
                "missing": missing,
                "demographic_detail": demographic_detail,
            })

        matches.sort(key=lambda m: m["score"], reverse=True)
        return {
            "subject_id": req.subject_id,
            "weighting": req.weighting,
            "total_candidates": len(candidates),
            "matches": matches[: req.top_k],
            "coverage": {
                "patients_scanned": len(patients),
                "patients_truncated": patients_truncated,
                "incomplete_feature_categories": incomplete_features,
                "cohort_id": req.cohort_id,
                "categories_scored": list(
                    (["demographics"] if use_demo else []) + list(selected_clinical)
                ),
            },
        }
