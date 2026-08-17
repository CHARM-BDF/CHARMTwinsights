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
import threading
import time
from collections import Counter
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set

import requests
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

COHORT_TAG_SYSTEM = "urn:charm:cohort"
DATATYPE_TAG_SYSTEM = "urn:charm:datatype"

# Clinical attribute categories and the resource types backing each.
CLINICAL_CATEGORIES = {
    "conditions": ["Condition"],
    "medications": ["MedicationRequest", "MedicationStatement"],
    "procedures": ["Procedure"],
}

# Weighting: 'equal' weighs every selected category the same; naming one
# category ('demographics', 'conditions', 'medications', 'procedures')
# emphasizes it by this factor. Only categories the caller actually selected
# participate; weights are renormalized over those.
WEIGHT_EMPHASIS_FACTOR = 2.0
WEIGHT_CATEGORIES = ("demographics", "conditions", "medications", "procedures")


def resolve_weights(weighting: str) -> Dict[str, float]:
    """Weights per category for a weighting name. Accepts 'equal' (default),
    one category name to emphasize, or the legacy preset aliases
    'balanced' (= equal), 'clinical', and 'pharma'."""
    weights = {c: 1.0 for c in WEIGHT_CATEGORIES}
    key = (weighting or "equal").strip().lower()
    if key in ("equal", "balanced"):
        return weights
    if key in weights:
        weights[key] = WEIGHT_EMPHASIS_FACTOR
        return weights
    if key == "clinical":  # legacy preset
        weights["conditions"] = weights["procedures"] = WEIGHT_EMPHASIS_FACTOR
        return weights
    if key == "pharma":  # legacy preset
        weights["medications"] = WEIGHT_EMPHASIS_FACTOR
        return weights
    raise ValueError(
        f"Unknown weighting '{weighting}'. Valid: equal, {', '.join(WEIGHT_CATEGORIES)}")


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
    weighting: str = Field(
        "equal",
        description="'equal', or a category name to emphasize it "
                    f"×{WEIGHT_EMPHASIS_FACTOR:g}: " + ", ".join(WEIGHT_CATEGORIES))
    max_candidates: int = Field(2000, gt=0, le=10000, description="Safety cap on patients scanned")


class AttributeCountsRequest(BaseModel):
    """Ask how many patients in the store share each of the subject's
    attributes. Served from the store-wide count cache."""
    subject_id: str
    demographics: Optional[TwinDemographics] = None
    conditions: List[TwinCriteriaItem] = Field(default_factory=list)
    medications: List[TwinCriteriaItem] = Field(default_factory=list)
    procedures: List[TwinCriteriaItem] = Field(default_factory=list)


# ─── store-wide attribute-count cache ─────────────────────────────────────────
# One full chunked sweep of the store (~seconds to ~a minute, same fetch path
# the twin search uses) yields patient-level counts for EVERY attribute key at
# once. Cached per HAPI url with stale-while-revalidate: requests are served
# from the last build instantly, and a rebuild is kicked off in the background
# when the cache ages out or the store's patient count changes.
_COUNT_CACHE_LOCK = threading.Lock()
_COUNT_CACHES: Dict[str, Dict[str, Any]] = {}  # hapi_url -> {"building": bool, "data": {...}}
COUNT_CACHE_TTL_SECONDS = 600


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
        weights = resolve_weights(req.weighting)

        # Which categories participate in scoring
        use_demo = req.demographics is not None and (
            req.demographics.gender is not None
            or req.demographics.age is not None
            or req.demographics.ethnicity is not None
        )
        clinical = {
            "conditions": (req.conditions, CLINICAL_CATEGORIES["conditions"]),
            "medications": (req.medications, CLINICAL_CATEGORIES["medications"]),
            "procedures": (req.procedures, CLINICAL_CATEGORIES["procedures"]),
        }
        selected_clinical = {k: v for k, v in clinical.items() if v[0]}
        if not use_demo and not selected_clinical:
            raise ValueError("No attributes selected. Pick at least one demographic or clinical attribute.")

        # Candidates
        patients, patients_complete = self._fetch_paged("Patient", req.cohort_id, req.max_candidates)
        patients_truncated = not patients_complete or len(patients) >= req.max_candidates
        candidates = {
            p["id"]: p for p in patients
            if p.get("id") and not (req.exclude_subject and p["id"] == req.subject_id)
        }

        # Clinical features, fetched per resource type in bounded chunks of
        # candidate ids. Deterministic and complete, unlike deep-paging the
        # whole store (see _features_by_patient). All categories are fetched
        # (not just the selected ones): scoring uses the selected subset, and
        # the attribute-prevalence block uses all of them.
        candidate_ids = list(candidates.keys())
        feature_sets: Dict[str, Dict[str, Set[str]]] = {}
        incomplete_features: List[str] = []
        for category, resource_types in CLINICAL_CATEGORIES.items():
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
            "prevalence": self._attribute_prevalence(req, candidates, feature_sets),
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

    def _attribute_prevalence(self, req: TwinFindRequest, candidates: Dict[str, Dict],
                              feature_sets: Dict[str, Dict[str, Set[str]]]):
        """For every attribute of the subject, count how many candidates share
        it. Uses the already-fetched candidate feature sets; returns None if
        the subject's own profile cannot be loaded (prevalence is decoration,
        not worth failing the search over)."""
        try:
            sp = self.subject_profile(req.subject_id)
        except Exception as e:
            logger.warning(f"Prevalence skipped, could not load subject profile: {e}")
            return None

        out: Dict[str, Any] = {"of": len(candidates)}

        demo_rows = []
        if sp.get("gender"):
            n = sum(1 for p in candidates.values()
                    if (p.get("gender") or "").lower() == sp["gender"].lower())
            demo_rows.append({"label": f"gender = {sp['gender']}", "key": "gender", "count": n})
        tol = req.demographics.age_tolerance if req.demographics else 10
        if sp.get("age") is not None:
            n = 0
            for p in candidates.values():
                a = age_from_birth_date(p.get("birthDate"))
                if a is not None and abs(a - sp["age"]) <= tol:
                    n += 1
            demo_rows.append({"label": f"age within ±{tol} y of {sp['age']}", "key": "age", "count": n})
        if sp.get("ethnicity"):
            n = sum(1 for p in candidates.values()
                    if (extract_ethnicity(p) or "").lower() == sp["ethnicity"].lower())
            demo_rows.append({"label": f"ethnicity = {sp['ethnicity']}", "key": "ethnicity", "count": n})
        out["demographics"] = demo_rows

        for category in CLINICAL_CATEGORIES:
            feats = feature_sets.get(category, {})
            rows = []
            for item in sp.get(category, []):
                keys = {str(c) for c in item.get("codes", [])} | {normalize_label(item["label"])}
                n = sum(1 for pid_keys in feats.values() if pid_keys & keys)
                rows.append({"label": item["label"], "count": n})
            rows.sort(key=lambda r: (-r["count"], r["label"].lower()))
            out[category] = rows
        return out

    # -- store-wide attribute counts (cached) ---------------------------------

    def attribute_counts(self, req: AttributeCountsRequest) -> Dict[str, Any]:
        """Counts of patients sharing each requested attribute, served from the
        store-wide cache. Returns {"status": "building"} until the first build
        finishes; after that, requests are answered instantly from the last
        build while any refresh happens in the background."""
        data, stale = self._ensure_count_cache()
        if data is None:
            return {"status": "building"}

        total = data["patient_total"]
        # The requested attributes come from the subject's own profile, so the
        # subject is among the counted patients, so report "other subjects".
        out: Dict[str, Any] = {
            "status": "ready",
            "stale": stale,
            "built_at": data["built_at"],
            "total_others": max(0, total - 1),
            "incomplete_categories": data["incomplete"],
        }

        demo_rows = []
        d = req.demographics
        if d and d.gender:
            n = data["gender"].get(d.gender.lower(), 0)
            demo_rows.append({"key": "gender", "label": f"gender = {d.gender}", "count": max(0, n - 1)})
        if d and d.age is not None:
            tol = d.age_tolerance or 10
            cy = date.today().year
            n = sum(c for y, c in data["birth_years"].items() if abs((cy - y) - d.age) <= tol)
            demo_rows.append({"key": "age", "label": f"age within ±{tol} y of {d.age}", "count": max(0, n - 1)})
        if d and d.ethnicity:
            n = data["ethnicity"].get(d.ethnicity.lower(), 0)
            demo_rows.append({"key": "ethnicity", "label": f"ethnicity = {d.ethnicity}", "count": max(0, n - 1)})
        out["demographics"] = demo_rows

        for category in CLINICAL_CATEGORIES:
            key_counts = data["key_counts"].get(category, {})
            rows = []
            for item in getattr(req, category) or []:
                keys = item_keys(item)
                # Aliases (codes, normalized label) of one attribute count the
                # same patients; max over aliases avoids double counting.
                n = max((key_counts.get(k, 0) for k in keys), default=0)
                rows.append({"label": item.label, "count": max(0, n - 1)})
            out[category] = rows
        return out

    def _ensure_count_cache(self):
        """Return (data, stale). Kicks off a background build when the cache is
        missing, older than the TTL, or the store's patient count changed."""
        with _COUNT_CACHE_LOCK:
            entry = _COUNT_CACHES.setdefault(self.hapi_url, {"building": False, "data": None})
            data = entry["data"]

        needs_build = data is None
        stale = False
        if data is not None:
            if time.time() - data["built_ts"] > COUNT_CACHE_TTL_SECONDS:
                needs_build, stale = True, True
            else:
                try:
                    r = requests.get(
                        f"{self.hapi_url}/Patient?_summary=count",
                        headers={"Accept": "application/fhir+json"}, timeout=10)
                    r.raise_for_status()
                    if r.json().get("total") != data["patient_total"]:
                        needs_build, stale = True, True
                except requests.RequestException as e:
                    logger.warning(f"Count-cache freshness check failed: {e}")

        if needs_build:
            self._schedule_count_build()
        return data, stale

    def _schedule_count_build(self):
        with _COUNT_CACHE_LOCK:
            entry = _COUNT_CACHES.setdefault(self.hapi_url, {"building": False, "data": None})
            if entry["building"]:
                return
            entry["building"] = True

        def run():
            try:
                self._build_count_cache()
            except Exception as e:
                logger.error(f"Attribute-count cache build failed: {e}")
            finally:
                with _COUNT_CACHE_LOCK:
                    _COUNT_CACHES[self.hapi_url]["building"] = False

        threading.Thread(target=run, name="twin-count-cache-build", daemon=True).start()

    def _build_count_cache(self):
        """One sweep of the whole store: per-key patient counts for every
        clinical attribute plus demographic tallies."""
        t0 = time.time()
        patients, _complete = self._fetch_paged("Patient", None, 100000)
        ids = [p["id"] for p in patients if p.get("id")]

        key_counts: Dict[str, Dict[str, int]] = {}
        incomplete: List[str] = []
        for category, resource_types in CLINICAL_CATEGORIES.items():
            merged: Dict[str, Set[str]] = {}
            for rt in resource_types:
                code_field = "medicationCodeableConcept" if rt.startswith("Medication") else "code"
                by_patient, complete = self._features_by_patient(rt, ids, code_field)
                if not complete and category not in incomplete:
                    incomplete.append(category)
                for pid, keys in by_patient.items():
                    merged.setdefault(pid, set()).update(keys)
            counts: Dict[str, int] = {}
            for keys in merged.values():
                for k in keys:
                    counts[k] = counts.get(k, 0) + 1
            key_counts[category] = counts

        gender = Counter((p.get("gender") or "unknown").lower() for p in patients)
        birth_years: Counter = Counter()
        ethnicity: Counter = Counter()
        for p in patients:
            bd = p.get("birthDate") or ""
            if len(bd) >= 4 and bd[:4].isdigit():
                birth_years[int(bd[:4])] += 1
            eth = extract_ethnicity(p)
            if eth:
                ethnicity[eth.lower()] += 1

        data = {
            "built_ts": time.time(),
            "built_at": datetime.now().isoformat(timespec="seconds"),
            "patient_total": len(patients),
            "key_counts": key_counts,
            "gender": dict(gender),
            "birth_years": dict(birth_years),
            "ethnicity": dict(ethnicity),
            "incomplete": incomplete,
        }
        with _COUNT_CACHE_LOCK:
            entry = _COUNT_CACHES.setdefault(self.hapi_url, {"building": False, "data": None})
            entry["data"] = data
        logger.info(
            f"Attribute-count cache built: {len(patients)} patients, "
            f"{sum(len(c) for c in key_counts.values())} keys, {time.time() - t0:.1f}s")
