# DSTU2 → R4 External FHIR Import — Design

**Date:** 2026-08-20
**Branch:** `dstu2-import`
**Status:** Approved design, pending implementation plan

## Problem

The repo can ingest external FHIR bundles via `POST /ingest/fhir` (router) →
`POST /ingest-external-fhir` (synthea_server) → HAPI. But:

1. **Version mismatch.** We have Apple HealthKit clinical sample bundles in FHIR
   **DSTU2** (confirmed: `MedicationOrder` resource, single-object
   `Observation.category`, `dateWritten`). HAPI is configured `fhir_version: R4`
   ([app/hapi/hapi.application.yaml:70](../../../app/hapi/hapi.application.yaml))
   and rejects DSTU2.

2. **Pre-existing import-isolation gaps that DSTU2/Apple bundles expose.** The
   current external path (`ingest_external_fhir` → `prefix_patient_ids` →
   `convert_to_transaction_bundle`) only namespaces **Patient** IDs (`ext-`
   prefix) and emits `PUT ResourceType/<rawId>` for everything else. Against
   HAPI's default `SEQUENTIAL_NUMERIC` server IDs + permissive client-ID
   strategy, a bare Apple id like `Observation/1` **overwrites** a Synthea
   resource already at server id `1`. Apple bundles also (a) contain **no
   Patient resource** despite referencing `Patient/1`, and (b) reference
   `Practitioner/20`, `Encounter/355`, etc. that they don't contain.

These are genuinely separate layers; the version conversion is the easy one.

## Goals

Make external DSTU2 (initially Apple HealthKit) bundles ingest reliably as an
**ongoing production capability**, general across resource types, without
corrupting existing (Synthea or other-import) data.

## Non-goals

- Stubbing external Practitioner/Encounter/etc. resources (they are reported,
  not created — see Layer 3).
- Enabling HAPI referential integrity globally (would break Synthea's
  forward-reference ingestion, which relies on it being off).
- Terminology / value-set translation beyond what the HAPI converter performs.
- Touching the **Synthea generation** ingest path (`post_bundle()`), which
  already POSTs `urn:uuid` transaction bundles with rich business identifiers
  and needs none of this treatment.

## Decisions (locked during brainstorming)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Conversion engine | **Java sidecar** wrapping HAPI `VersionConvertorFactory_10_40` |
| 2 | Scope | **Full isolation** — convert + ID isolation + dangling-ref handling + stub Patient/cohort |
| 3 | ID isolation | **Identifier + server-assigned IDs** — source id → `identifier`, `urn:uuid` transaction refs, `ifNoneExist` conditional-create (idempotent re-import) |
| 4 | Dangling non-Patient refs | **Report + allow** — leave literal, return an `unresolved_references` report |
| 5 | Missing Patient | **Synthesize stub Patient**, wire into cohort membership |
| 6 | Mobile (FHIR-HOSE) path | **Migrate too** — R4 external bundles use the same isolation logic (skip convert step) — see Compatibility Assumption |

## Architecture & data flow

```
router  POST /ingest/fhir   (+ optional source_fhir_version)
  └─> synthea_server  POST /ingest-external-fhir
        1. detect/receive FHIR version (explicit hint wins; heuristic fallback)
        2. if DSTU2 → POST to fhir-converter sidecar → R4 bundle
        3. synthesize stub Patient(s) for referenced-but-absent patient ids
        4. build isolation transaction:
             - assign urn:uuid fullUrls
             - prefer resource's existing identifier; else synthesize
               identifier {system: urn:charm:apple-healthkit-src-id, value: <srcId>}
             - rewrite in-bundle references → urn:uuid
             - request: POST <Type>, ifNoneExist=<identifier query>
             - collect unresolved external references
        5. apply CHARM tags (source/datatype/cohort/created)
        6. POST transaction to HAPI
        7. upsert cohort Group from patient ids (now includes stub Patients)
  └─> response: { success, patient_ids, cohort, unresolved_references[] }

NEW service:  fhir-converter  (Java, containerized alongside hapi)
        POST /convert  { sourceVersion:"DSTU2", bundle:{…} } → R4 bundle
```

### Why the `urn:uuid` + `ifNoneExist` idiom

It does three jobs at once: (1) intra-bundle references resolve atomically
without knowing server IDs in advance, (2) collisions with Synthea become
impossible because we never assert a server ID, (3) re-importing the same
export is a no-op — HAPI matches the `identifier` and skips the create. This
replaces the fragile `PUT ResourceType/<rawId>` upsert.

## Components

### 1. `fhir-converter` sidecar (new Java service)

- Minimal HTTP service (lightweight embedded server) depending on
  `org.hl7.fhir.convertors` / `hapi-fhir-converter`.
- Single endpoint `POST /convert`:
  - Input: `{ sourceVersion: "DSTU2", bundle: {…} }`.
  - Parse with the DSTU2 parser → `VersionConvertorFactory_10_40.convertResource`
    → serialize R4 → return.
  - Errors (parse failure, unconvertible resource) → 4xx with an
    `OperationOutcome`.
- Stateless, no DB. Handles all resource types generically.
- Added to `docker-compose.yml` on the internal network; reachable at
  `http://fhir-converter:8080/convert`. JRE base image.

### 2. Importer rework in `synthea_server`

Replace `prefix_patient_ids` + `convert_to_transaction_bundle` **for the
external path** with focused, independently testable units:

- `detect_fhir_version(bundle, hint) -> "R4" | "DSTU2"` — explicit
  `source_fhir_version` wins; heuristic fallback (e.g. presence of
  `MedicationOrder`, single-object `Observation.category`).
- `synthesize_stub_patients(bundle) -> bundle` — scan all `Patient/<x>`
  references; for any `<x>` with no Patient resource, add a minimal R4 Patient
  carrying the source id as an identifier.
- `build_isolation_transaction(bundle) -> (transaction, unresolved_refs)` — the
  `urn:uuid` / identifier / `ifNoneExist` builder; identifier preference logic
  (existing identifier first, synthesize only when absent); collects references
  whose target is neither in-bundle nor a synthesized stub.
- Cohort membership derives patient ids from the (now-present) Patients.

### 3. API / contract changes

- Router `ExternalFHIRIngestRequest` + synthea `ExternalFHIRRequest`: add
  optional `source_fhir_version` (default `"R4"`).
- Ingest response gains `unresolved_references: [{ source, reference }]`.

## Compatibility assumption (must verify before release)

The FHIR-HOSE mobile app is known only from a docstring
([main.py:2960](../../../app/synthea_server/synthea-pyserver/main.py)); no client
code or contract exists in this repo. Migrating its R4 bundles to the new
isolation logic is safe **iff** the client interacts via cohort/tag queries
rather than hardcoded server resource IDs, and does not depend on
`PUT`-by-raw-id merge semantics. **Action:** confirm with the FHIR-HOSE client
owner before shipping. Behavior preserved regardless: data still lands, carries
the same CHARM tags, and is queryable by cohort/tag.

## Testing

- **Sidecar unit:** convert each of the 3 Apple samples DSTU2→R4; assert
  `MedicationOrder`→`MedicationRequest`, `Observation.category` becomes an
  array, `dateWritten`→`authoredOn`.
- **Importer:** golden test that a sample ingests; stub Patient created;
  re-import is idempotent (no duplicates); unresolved refs (`Practitioner/20`,
  `Encounter/355`) appear in the report; no collision with a pre-seeded Synthea
  resource sharing a bare id.
- Extend `ci/external_fhir_ingestion_validation.sh` with a DSTU2 case.

## Open risks

- HAPI converter has known per-resource gaps (e.g. `Procedure.status` drop,
  some Bundle-level quirks). Acceptable for import; surfaced via tests on real
  samples.
- FHIR-HOSE compatibility assumption above.

## Sample data

`app/hapi/Sample A.json`, `Sample B.json`, `Sample C.json` — DSTU2 `collection`
bundles, no Patient resource, external Practitioner/Encounter references, bare
sequential resource ids. (Relocate to a fixtures directory during
implementation.)
