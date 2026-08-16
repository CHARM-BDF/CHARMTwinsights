"""
FHIR Data Export Module

Three export formats, all zipped:

- "ndjson" (default): the FHIR Bulk Data layout — one newline-delimited JSON
  file per resource type, full fidelity, plus manifest.json and a README.
- "bundles": the layout Synthea generates — one FHIR Bundle file per patient
  (all of their resources), plus practitionerInformation.json and
  hospitalInformation.json holding the provider/reference resources.
- "flat": one CSV row per patient — demographics plus 0/1 indicator columns
  for every condition / medication / procedure present in the scope, with a
  data_dictionary.json mapping columns back to display labels.

Scoping: cohort exports rely on the urn:charm:cohort meta tag, which the
generation and ingestion pipelines apply to every resource in a bundle —
including the Synthea provider bundles (hospitalInformation*/
practitionerInformation*), so Organization/Practitioner/Location references
inside patient records resolve within the export.

Paging is _count/_offset/_sort=_id: the explicit sort costs ~2x per page but
is required for correctness — unsorted offset scans returned unstable
orderings under load and silently dropped rows. Each type's exported count is
cross-checked against a _summary=count query and mismatches are reported in
the manifest.
"""

import csv
import io
import json
import logging
import os
import re
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .twins import (
    age_from_birth_date,
    extract_ethnicity,
    normalize_label,
    patient_tags,
)

logger = logging.getLogger(__name__)

COHORT_TAG_SYSTEM = "urn:charm:cohort"
PAGE_SIZE = 1000
MAX_RESOURCES_PER_TYPE = 500_000  # runaway-store safety cap, reported in manifest
# Concurrent per-type fetch lanes for the NDJSON export. Sorted scans are what
# make the export correct but slow; four lanes bring a ~400k-resource store
# from ~30 min sequential down to roughly the longest single type.
EXPORT_FETCH_WORKERS = 4

# Patient-linked types, then provider/reference data. Both participate in
# cohort exports (provider bundles are cohort-tagged by the pipelines).
PATIENT_TYPES = [
    "Patient", "Condition", "MedicationRequest", "MedicationStatement",
    "Procedure", "Observation", "Encounter", "Immunization",
    "AllergyIntolerance", "DiagnosticReport", "DocumentReference",
    "CarePlan", "CareTeam", "Claim", "ExplanationOfBenefit",
    "SupplyDelivery", "Device", "ImagingStudy", "Medication",
]
REFERENCE_TYPES = ["Organization", "Practitioner", "PractitionerRole", "Location"]

# Flat-format categories: (resource types, code element, column prefix)
FLAT_CATEGORIES = {
    "conditions": (["Condition"], "code", "cond"),
    "medications": (["MedicationRequest", "MedicationStatement"], "medicationCodeableConcept", "med"),
    "procedures": (["Procedure"], "code", "proc"),
}


# ─── paging ───────────────────────────────────────────────────────────────────

def _iter_search(hapi_url: str, resource_type: str, extra_params: List[str],
                 max_items: int = MAX_RESOURCES_PER_TYPE):
    """Yield resources of one type page by page. _sort=_id pins the row order
    so offset paging is deterministic and complete (unsorted scans lost rows
    under load); there is no server-side cursor to expire."""
    offset = 0
    fetched = 0
    while fetched < max_items:
        params = [f"_count={PAGE_SIZE}", f"_offset={offset}", "_sort=_id"] + extra_params
        url = f"{hapi_url}/{resource_type}?{'&'.join(params)}"
        r = requests.get(url, headers={"Accept": "application/fhir+json"}, timeout=120)
        r.raise_for_status()
        bundle = r.json()
        for entry in bundle.get("entry", []) or []:
            res = entry.get("resource")
            if res and res.get("resourceType") == resource_type:
                fetched += 1
                yield res
                if fetched >= max_items:
                    return
        if not any(l.get("relation") == "next" for l in bundle.get("link", []) or []):
            return
        offset += PAGE_SIZE


def _count_type(hapi_url: str, resource_type: str, extra_params: List[str]) -> Optional[int]:
    """Server-side count for one type/scope; None if the query fails."""
    try:
        params = ["_summary=count"] + extra_params
        url = f"{hapi_url}/{resource_type}?{'&'.join(params)}"
        r = requests.get(url, headers={"Accept": "application/fhir+json"}, timeout=60)
        r.raise_for_status()
        return r.json().get("total")
    except requests.RequestException as e:
        logger.warning(f"Count check failed for {resource_type}: {e}")
        return None


def _tag_param(cohort: Optional[str]) -> List[str]:
    return [f"_tag={COHORT_TAG_SYSTEM}|{cohort}"] if cohort else []


# ─── dataset cards ────────────────────────────────────────────────────────────

def _scope_line(manifest: Dict) -> str:
    return (
        "the entire FHIR store" if manifest["scope"] == "all"
        else "cohorts: " + ", ".join(f"`{c}`" for c in manifest["cohorts"])
    )


def _ndjson_card(manifest: Dict) -> str:
    counts = "\n".join(f"| {rt} | {n} |" for rt, n in manifest["resource_counts"].items())
    return f"""# CHARMTwinsights FHIR export

Exported {manifest['exported_at']} from {_scope_line(manifest)}.

One newline-delimited JSON file per FHIR resource type (the FHIR Bulk Data
layout). Each line is one complete FHIR R4 resource. Provider/reference
resources (Organization, Practitioner, ...) are included so references inside
patient records resolve.

| Resource type | Count |
|---|---|
{counts}

See `manifest.json` for scope, counts, and count verification.
"""


def _flat_card(manifest: Dict) -> str:
    return f"""# CHARMTwinsights flat patient table

Exported {manifest['exported_at']} from {_scope_line(manifest)}.

`patients_flat.csv` — one row per patient: demographics plus 0/1 indicator
columns for every condition (`cond_*`), medication (`med_*`), and procedure
(`proc_*`) observed in the scope. `data_dictionary.json` maps each column to
its display label and patient count.

- Patients: {manifest['patients']}
- Indicator columns: {manifest['indicator_columns']}
"""


# ─── NDJSON (Bulk Data) export ────────────────────────────────────────────────

def _fetch_type_to_file(hapi_url: str, rt: str, scopes: List[Optional[str]],
                        tmpdir: str):
    """One export lane: stream a resource type (sorted pages, deduped by id
    across cohorts) into a temp NDJSON file — memory stays one page deep.
    Returns (rt, path or None if empty, count, expected server-side count)."""
    fd, path = tempfile.mkstemp(suffix=".ndjson", prefix=f"{rt}-", dir=tmpdir)
    count = 0
    seen = set()
    try:
        with os.fdopen(fd, "wb") as fh:
            for cohort in scopes:
                for res in _iter_search(hapi_url, rt, _tag_param(cohort)):
                    rid = res.get("id")
                    if rid and rid in seen:
                        continue
                    if rid:
                        seen.add(rid)
                    fh.write(json.dumps(res, separators=(",", ":")).encode() + b"\n")
                    count += 1
    except Exception:
        os.unlink(path)
        raise
    if count == 0:
        os.unlink(path)
        path = None
    # Multi-cohort unions have no single count query to verify against.
    expected = _count_type(hapi_url, rt, _tag_param(scopes[0])) if len(scopes) == 1 else None
    return rt, path, count, expected


def _write_ndjson(zf: zipfile.ZipFile, prefix: str, hapi_url: str,
                  cohort_ids: Optional[List[str]]) -> Dict:
    """Write the Bulk-Data NDJSON layout into an open zip under `prefix`;
    returns this format's manifest.

    Types are fetched on EXPORT_FETCH_WORKERS parallel lanes into temp files
    (disk, not memory) and zipped as each lane finishes, in stable order."""
    cohort_ids = [c for c in (cohort_ids or []) if c]
    scope_all = not cohort_ids
    scopes = [None] if scope_all else cohort_ids

    resource_types = PATIENT_TYPES + REFERENCE_TYPES
    counts: Dict[str, int] = {}
    truncated: List[str] = []
    mismatches: List[Dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="charm-export-lanes-") as tmpdir, \
            ThreadPoolExecutor(max_workers=EXPORT_FETCH_WORKERS) as pool:
        futures = [
            pool.submit(_fetch_type_to_file, hapi_url, rt, scopes, tmpdir)
            for rt in resource_types
        ]
        for fut in futures:
            rt, path, count, expected = fut.result()
            if path is None:
                continue
            zf.write(path, arcname=f"{prefix}{rt}.ndjson")
            os.unlink(path)
            counts[rt] = count
            if count >= MAX_RESOURCES_PER_TYPE:
                truncated.append(rt)
            if expected is not None and expected != count:
                mismatches.append({"type": rt, "exported": count, "expected": expected})
                logger.warning(f"Export count mismatch for {rt}: {count} != {expected}")
            logger.info(f"Export: {rt} — {count} resources")

    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "CHARMTwinsights FHIR store",
        "fhir_version": "R4",
        "format": "FHIR Bulk Data NDJSON (one file per resource type)",
        "scope": "all" if scope_all else "cohorts",
        "cohorts": cohort_ids,
        "resource_counts": counts,
        "total_resources": sum(counts.values()),
        "truncated_types": truncated,
        "count_verification": {"performed": len(scopes) == 1, "mismatches": mismatches},
    }
    zf.writestr(f"{prefix}manifest.json", json.dumps(manifest, indent=2))
    zf.writestr(f"{prefix}README.md", _ndjson_card(manifest))
    return manifest


def build_export_zip(hapi_url: str, cohort_ids: Optional[List[str]] = None) -> Tuple[str, Dict]:
    """NDJSON-only export zip; returns (path, manifest)."""
    return build_combined_export_zip(hapi_url, cohort_ids, ["ndjson"])


# ─── flat (ML-ready) export ───────────────────────────────────────────────────

def _slug(normalized_label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", normalized_label).strip("_")[:60] or "unnamed"


def _write_flat(zf: zipfile.ZipFile, prefix: str, hapi_url: str,
                cohort_ids: Optional[List[str]]) -> Dict:
    """One CSV row per patient: demographics + 0/1 indicator columns for every
    clinical attribute in scope. Writes into an open zip under `prefix`;
    returns this format's manifest."""
    cohort_ids = [c for c in (cohort_ids or []) if c]
    scope_all = not cohort_ids
    scopes = [None] if scope_all else cohort_ids

    # Patients, deduped across cohorts.
    patients: Dict[str, Dict] = {}
    for cohort in scopes:
        for res in _iter_search(hapi_url, "Patient", _tag_param(cohort)):
            pid = res.get("id")
            if pid and pid not in patients:
                patients[pid] = res

    # Per-patient normalized-label sets per category (slim _elements fetch).
    label_sets: Dict[str, Dict[str, set]] = {cat: {} for cat in FLAT_CATEGORIES}
    display: Dict[str, Dict[str, str]] = {cat: {} for cat in FLAT_CATEGORIES}
    for cat, (rtypes, code_field, _prefix) in FLAT_CATEGORIES.items():
        for rt in rtypes:
            for cohort in scopes:
                extra = _tag_param(cohort) + [f"_elements=subject,{code_field}"]
                for res in _iter_search(hapi_url, rt, extra):
                    ref = (res.get("subject") or {}).get("reference", "")
                    if not ref.startswith("Patient/"):
                        continue
                    pid = ref.split("/", 1)[1]
                    if pid not in patients:
                        continue
                    concept = res.get(code_field) or {}
                    label = concept.get("text") or next(
                        (c.get("display") for c in concept.get("coding", []) or [] if c.get("display")), None)
                    if not label:
                        continue
                    norm = normalize_label(label)
                    label_sets[cat].setdefault(pid, set()).add(norm)
                    display[cat].setdefault(norm, label)

    # Columns: per category, sorted by prevalence (desc) then name.
    columns: List[Tuple[str, str, str]] = []  # (column, category, normalized label)
    dictionary: Dict[str, Dict[str, Any]] = {}
    # NB: col_prefix, not `prefix` — the latter is this writer's zip-path prefix.
    for cat, (_rt, _cf, col_prefix) in FLAT_CATEGORIES.items():
        prevalence = {
            norm: sum(1 for s in label_sets[cat].values() if norm in s)
            for norm in display[cat]
        }
        for norm in sorted(display[cat], key=lambda n: (-prevalence[n], n)):
            col = base = f"{col_prefix}_{_slug(norm)}"
            i = 2
            while col in dictionary:  # slug collision
                col = f"{base}_{i}"
                i += 1
            dictionary[col] = {"category": cat, "label": display[cat][norm], "patients": prevalence[norm]}
            columns.append((col, cat, norm))

    demo_cols = ["patient_id", "gender", "birth_date", "age", "ethnicity", "cohorts", "datatype"]

    with zf.open(f"{prefix}patients_flat.csv", "w", force_zip64=True) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
        writer = csv.writer(text)
        writer.writerow(demo_cols + [c for c, _cat, _n in columns])
        for pid, p in patients.items():
            tags = patient_tags(p)
            row = [
                pid,
                p.get("gender") or "",
                p.get("birthDate") or "",
                age_from_birth_date(p.get("birthDate")),
                extract_ethnicity(p) or "",
                ";".join(tags.get("cohort_ids", [])),
                tags.get("datatype") or "",
            ]
            row += [
                1 if norm in label_sets[cat].get(pid, set()) else 0
                for (_col, cat, norm) in columns
            ]
            writer.writerow(row)
        text.flush()
        text.detach()  # keep the underlying zip stream open for zipfile to close

    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "CHARMTwinsights FHIR store",
        "format": "flat patient table (CSV, 0/1 indicators)",
        "scope": "all" if scope_all else "cohorts",
        "cohorts": cohort_ids,
        "patients": len(patients),
        "indicator_columns": len(columns),
        "demographic_columns": demo_cols,
    }
    zf.writestr(f"{prefix}data_dictionary.json", json.dumps(dictionary, indent=2))
    zf.writestr(f"{prefix}manifest.json", json.dumps(manifest, indent=2))
    zf.writestr(f"{prefix}README.md", _flat_card(manifest))
    return manifest


def build_flat_export_zip(hapi_url: str, cohort_ids: Optional[List[str]] = None) -> Tuple[str, Dict]:
    """Flat-CSV-only export zip; returns (path, manifest)."""
    return build_combined_export_zip(hapi_url, cohort_ids, ["flat"])


# ─── per-patient bundles (Synthea-style layout) export ───────────────────────

def _bundles_card(manifest: Dict) -> str:
    return f"""# CHARMTwinsights FHIR export — per-patient bundles

Exported {manifest['exported_at']} from {_scope_line(manifest)}.

The layout Synthea generates: one FHIR R4 Bundle (type `collection`) per
patient holding every resource of that patient, named `Given_Family_id.json`,
plus the provider/reference resources the records point at:

- `practitionerInformation.json` — Practitioner and PractitionerRole
- `hospitalInformation.json` — Organization and Location

- Patients: {manifest['patients']}
- Total resources: {manifest['total_resources']}

See `manifest.json` for details.
"""


def _patient_filename(patient: Dict) -> str:
    """Mirror Synthea's Given_Family_id.json naming where a name exists."""
    name = (patient.get("name") or [{}])[0]
    given = (name.get("given") or [""])[0] or ""
    family = name.get("family") or ""
    slug = re.sub(r"[^A-Za-z0-9_-]+", "", f"{given}_{family}").strip("_")
    pid = patient.get("id") or "unknown"
    return f"{slug}_{pid}.json" if slug else f"{pid}.json"


def _fetch_everything(hapi_url: str, patient_id: str) -> List[Dict]:
    """Every resource of one patient via $everything, following next links.
    Runs server-side, so HAPI's paging links resolve; one retry covers an
    expired paging cursor."""
    for attempt in (1, 2):
        try:
            url = f"{hapi_url}/Patient/{patient_id}/$everything?_count=1000"
            out: List[Dict] = []
            while url:
                r = requests.get(url, headers={"Accept": "application/fhir+json"}, timeout=120)
                r.raise_for_status()
                bundle = r.json()
                for entry in bundle.get("entry", []) or []:
                    res = entry.get("resource")
                    if res:
                        out.append(res)
                url = next((l.get("url") for l in bundle.get("link", []) or []
                            if l.get("relation") == "next"), None)
            return out
        except requests.RequestException:
            if attempt == 2:
                raise
            logger.warning(f"$everything retry for patient {patient_id}")
    return []


def _write_bundle_file(path: str, resources: List[Dict]) -> None:
    """Stream a collection Bundle to disk without holding its JSON in memory."""
    with open(path, "wb") as fh:
        fh.write(b'{"resourceType":"Bundle","type":"collection","total":')
        fh.write(str(len(resources)).encode())
        fh.write(b',"entry":[')
        for i, res in enumerate(resources):
            if i:
                fh.write(b",")
            fh.write(b'{"resource":')
            fh.write(json.dumps(res, separators=(",", ":")).encode())
            fh.write(b"}")
        fh.write(b"]}")


def _fetch_patient_bundle_to_file(hapi_url: str, patient: Dict, tmpdir: str):
    """One bundles-export lane. Returns (filename, path, resource_count)."""
    resources = _fetch_everything(hapi_url, patient["id"])
    fd, path = tempfile.mkstemp(suffix=".json", prefix="bundle-", dir=tmpdir)
    os.close(fd)
    _write_bundle_file(path, resources)
    return _patient_filename(patient), path, len(resources)


def _write_bundles(zf: zipfile.ZipFile, prefix: str, hapi_url: str,
                   cohort_ids: Optional[List[str]]) -> Dict:
    """Synthea-style layout: one Bundle file per patient plus provider files.
    Writes into an open zip under `prefix`; returns this format's manifest."""
    cohort_ids = [c for c in (cohort_ids or []) if c]
    scope_all = not cohort_ids
    scopes = [None] if scope_all else cohort_ids

    # Patients in scope, deduped across cohorts.
    patients: Dict[str, Dict] = {}
    for cohort in scopes:
        for res in _iter_search(hapi_url, "Patient", _tag_param(cohort)):
            pid = res.get("id")
            if pid and pid not in patients:
                patients[pid] = res

    total_resources = 0
    provider_counts: Dict[str, int] = {}

    with tempfile.TemporaryDirectory(prefix="charm-export-bundles-") as tmpdir, \
            ThreadPoolExecutor(max_workers=EXPORT_FETCH_WORKERS) as pool:
        futures = [
            pool.submit(_fetch_patient_bundle_to_file, hapi_url, p, tmpdir)
            for p in patients.values()
        ]
        for fut in futures:
            filename, path, n = fut.result()
            zf.write(path, arcname=f"{prefix}{filename}")
            os.unlink(path)
            total_resources += n

        # Provider/reference bundles, mirroring Synthea's special files.
        for arcname, rtypes in (
            ("practitionerInformation.json", ["Practitioner", "PractitionerRole"]),
            ("hospitalInformation.json", ["Organization", "Location"]),
        ):
            resources: List[Dict] = []
            seen: set = set()
            for rt in rtypes:
                for cohort in scopes:
                    for res in _iter_search(hapi_url, rt, _tag_param(cohort)):
                        rid = res.get("id")
                        if rid and rid in seen:
                            continue
                        if rid:
                            seen.add(rid)
                        resources.append(res)
                provider_counts[rt] = sum(
                    1 for r in resources if r.get("resourceType") == rt)
            if resources:
                fd, ppath = tempfile.mkstemp(suffix=".json", dir=tmpdir)
                os.close(fd)
                _write_bundle_file(ppath, resources)
                zf.write(ppath, arcname=f"{prefix}{arcname}")
                os.unlink(ppath)
                total_resources += len(resources)
            logger.info(f"Export: {arcname} — {len(resources)} resources")

    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "CHARMTwinsights FHIR store",
        "fhir_version": "R4",
        "format": "per-patient FHIR bundles (Synthea-style layout)",
        "scope": "all" if scope_all else "cohorts",
        "cohorts": cohort_ids,
        "patients": len(patients),
        "provider_resources": provider_counts,
        "total_resources": total_resources,
    }
    zf.writestr(f"{prefix}manifest.json", json.dumps(manifest, indent=2))
    zf.writestr(f"{prefix}README.md", _bundles_card(manifest))
    return manifest


def build_bundles_export_zip(hapi_url: str, cohort_ids: Optional[List[str]] = None) -> Tuple[str, Dict]:
    """Bundles-only export zip; returns (path, manifest)."""
    return build_combined_export_zip(hapi_url, cohort_ids, ["bundles"])


# ─── combined (multi-format) export ───────────────────────────────────────────

# Writer per format. Order here fixes the order formats are built and listed.
FORMAT_WRITERS = {
    "ndjson": _write_ndjson,
    "bundles": _write_bundles,
    "flat": _write_flat,
}


def _combined_card(manifest: Dict) -> str:
    lines = []
    for name, m in manifest["formats"].items():
        lines.append(f"- `{name}/` — {m['format']}")
    return f"""# CHARMTwinsights export

Exported {manifest['exported_at']} from {_scope_line(manifest)}.

This archive contains {len(manifest['formats'])} views of the same data, each
in its own directory with its own manifest and README:

{chr(10).join(lines)}
"""


def build_combined_export_zip(hapi_url: str, cohort_ids: Optional[List[str]] = None,
                              formats: Optional[List[str]] = None) -> Tuple[str, Dict]:
    """Build one zip containing every requested format; returns (path, manifest).

    A single format keeps the flat layout it has always had (files at the zip
    root). Several formats are placed under <format>/ directories — they each
    emit manifest.json and README.md, so they would otherwise collide — plus a
    top-level manifest describing the whole archive.
    """
    hapi_url = hapi_url.rstrip("/")
    cohort_ids = [c for c in (cohort_ids or []) if c]
    requested = [f for f in (formats or ["ndjson"]) if f in FORMAT_WRITERS]
    # Preserve FORMAT_WRITERS order and drop duplicates.
    ordered = [f for f in FORMAT_WRITERS if f in requested]
    if not ordered:
        raise ValueError(
            f"No valid export format requested. Valid: {', '.join(FORMAT_WRITERS)}")
    multi = len(ordered) > 1

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip", prefix="charm-export-")
    manifests: Dict[str, Dict] = {}
    try:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in ordered:
                logger.info(f"Export: building '{name}' format")
                manifests[name] = FORMAT_WRITERS[name](
                    zf, f"{name}/" if multi else "", hapi_url, cohort_ids)

            if multi:
                combined = {
                    "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "source": "CHARMTwinsights FHIR store",
                    "scope": "all" if not cohort_ids else "cohorts",
                    "cohorts": cohort_ids,
                    "formats": manifests,
                    "total_resources": max(
                        (m.get("total_resources", 0) for m in manifests.values()), default=0),
                }
                zf.writestr("manifest.json", json.dumps(combined, indent=2))
                zf.writestr("README.md", _combined_card(combined))
    except Exception:
        tmp.close()
        os.unlink(tmp.name)
        raise
    tmp.close()
    return tmp.name, (combined if multi else manifests[ordered[0]])
