"""
FHIR Data Export Module

Builds a zip of the FHIR store — everything, or selected cohorts — in the
FHIR Bulk Data layout: one newline-delimited JSON file per resource type
(Patient.ndjson, Condition.ndjson, ...) plus a manifest and a dataset-card
README. NDJSON is the official FHIR bulk-export format and loads directly
into Hugging Face datasets:

    load_dataset("json", data_files={"patients": "Patient.ndjson"})

Cohort scoping relies on the urn:charm:cohort meta tag, which the ingestion
and synthea pipelines apply to every resource in a bundle (not just the
Patient), so a per-type _tag search captures the whole cohort.
"""

import json
import logging
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

COHORT_TAG_SYSTEM = "urn:charm:cohort"
PAGE_SIZE = 500
MAX_RESOURCES_PER_TYPE = 500_000  # runaway-store safety cap, reported in manifest

# Patient-linked types first, then reference data that only a full export
# includes (they carry no cohort tag).
PATIENT_TYPES = [
    "Patient", "Condition", "MedicationRequest", "MedicationStatement",
    "Procedure", "Observation", "Encounter", "Immunization",
    "AllergyIntolerance", "DiagnosticReport", "DocumentReference",
    "CarePlan", "CareTeam", "Claim", "ExplanationOfBenefit",
    "SupplyDelivery", "Device", "ImagingStudy", "Medication",
]
REFERENCE_TYPES = ["Organization", "Practitioner", "PractitionerRole", "Location"]


def _iter_search(hapi_url: str, resource_type: str, extra_params: List[str],
                 max_items: int = MAX_RESOURCES_PER_TYPE):
    """Yield resources of one type page by page (stateless _count/_offset
    paging — no server-side cursor to expire).

    Deliberately unsorted: _sort=_id doubled page latency on the large types,
    and the caller dedupes by resource id anyway, which absorbs the page-shear
    duplicates an unsorted offset scan can produce. (Rows written concurrently
    with the export can still be missed — exports are point-in-time-ish.)"""
    offset = 0
    fetched = 0
    while fetched < max_items:
        params = [f"_count={PAGE_SIZE}", f"_offset={offset}"] + extra_params
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


def _chain_first(first: Dict, rest) -> "iter":
    """Re-attach a peeked first element to its iterator."""
    yield first
    yield from rest


def _dataset_card(manifest: Dict) -> str:
    """README.md for the zip — doubles as a Hugging Face dataset card stub."""
    scope = manifest["scope"]
    scope_line = (
        "the entire FHIR store" if scope == "all"
        else "cohorts: " + ", ".join(f"`{c}`" for c in manifest["cohorts"])
    )
    counts = "\n".join(
        f"| {rt} | {n} |" for rt, n in manifest["resource_counts"].items()
    )
    return f"""# CHARMTwinsights FHIR export

Exported {manifest['exported_at']} from {scope_line}.

One newline-delimited JSON file per FHIR resource type (the FHIR Bulk Data
layout). Each line is one complete FHIR R4 resource.

| Resource type | Count |
|---|---|
{counts}

## Loading with Hugging Face datasets

```python
from datasets import load_dataset

ds = load_dataset("json", data_files={{
    "patients": "Patient.ndjson",
    "conditions": "Condition.ndjson",
}})
```

`push_to_hub()` converts to Parquet automatically, so this layout uploads
to the Hub as-is.

Synthetic data generated with Synthea and/or tagged external data —
see `manifest.json` for scope, counts, and truncation flags.
"""


def build_export_zip(hapi_url: str, cohort_ids: Optional[List[str]] = None) -> Tuple[str, Dict]:
    """Write the export zip to a temp file; returns (path, manifest).
    Empty/None cohort_ids means the whole store, including reference types."""
    hapi_url = hapi_url.rstrip("/")
    cohort_ids = [c for c in (cohort_ids or []) if c]
    scope_all = not cohort_ids

    resource_types = PATIENT_TYPES + (REFERENCE_TYPES if scope_all else [])
    counts: Dict[str, int] = {}
    truncated: List[str] = []

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip", prefix="charm-export-")
    try:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
            for rt in resource_types:
                scopes = [None] if scope_all else cohort_ids

                def resources():
                    # Dedupe by id across cohorts (a resource tagged into
                    # several requested cohorts) and within a scan (unsorted
                    # offset paging can repeat rows across page boundaries).
                    seen = set()
                    for cohort in scopes:
                        extra = [f"_tag={COHORT_TAG_SYSTEM}|{cohort}"] if cohort else []
                        for res in _iter_search(hapi_url, rt, extra):
                            rid = res.get("id")
                            if rid and rid in seen:
                                continue
                            if rid:
                                seen.add(rid)
                            yield res

                # Peek before creating the zip entry so empty types are
                # omitted entirely; then stream page-sized chunks straight
                # into the (deflated) entry — the previous version buffered
                # whole types in memory and OOM-killed the container on a
                # 400k-resource store.
                it = resources()
                first = next(it, None)
                if first is None:
                    continue
                count = 0
                with zf.open(f"{rt}.ndjson", "w", force_zip64=True) as fh:
                    for res in _chain_first(first, it):
                        fh.write(json.dumps(res, separators=(",", ":")).encode() + b"\n")
                        count += 1
                if count >= MAX_RESOURCES_PER_TYPE:
                    truncated.append(rt)
                counts[rt] = count
                logger.info(f"Export: {rt} — {count} resources")

            manifest = {
                "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source": "CHARMTwinsights FHIR store",
                "fhir_version": "R4",
                "scope": "all" if scope_all else "cohorts",
                "cohorts": cohort_ids,
                "resource_counts": counts,
                "total_resources": sum(counts.values()),
                "truncated_types": truncated,
                "format": "FHIR Bulk Data NDJSON (one file per resource type)",
            }
            zf.writestr("manifest.json", json.dumps(manifest, indent=2))
            zf.writestr("README.md", _dataset_card(manifest))
    except Exception:
        tmp.close()
        os.unlink(tmp.name)
        raise
    tmp.close()
    return tmp.name, manifest
