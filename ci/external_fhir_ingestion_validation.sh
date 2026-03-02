#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYNTHEA_PORT="${SYNTHEA_PORT:-8003}"
HAPI_PORT="${HAPI_PORT:-8080}"
SYNTHEA_BASE_URL="${SYNTHEA_BASE_URL:-http://localhost:${SYNTHEA_PORT}}"
HAPI_BASE_URL="${HAPI_BASE_URL:-http://localhost:${HAPI_PORT}/fhir}"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

require_cmd curl
require_cmd jq

wait_for_http "${SYNTHEA_BASE_URL}/health" 180 3
wait_for_http "${HAPI_BASE_URL}/\$meta" 180 3

REQUEST_STATUS=""
REQUEST_BODY=""

request_json() {
  local method="$1"
  local url="$2"
  local payload="$3"
  local raw_response

  raw_response="$(
    curl -sS -w '\n%{http_code}' -X "$method" "$url" \
      -H "Content-Type: application/json" \
      -d "$payload"
  )"

  REQUEST_STATUS="$(echo "$raw_response" | tail -n 1)"
  REQUEST_BODY="$(echo "$raw_response" | sed '$d')"
}

timestamp="$(date +%s)"
cohort_id="ci-external-ingest-${timestamp}"
patient_id="ci-patient-${timestamp}"
obs1_id="ci-obs-1-${timestamp}"
obs2_id="ci-obs-2-${timestamp}"

bundle_one="$(jq -nc \
  --arg patient_id "$patient_id" \
  --arg obs1_id "$obs1_id" \
  '{
    resourceType: "Bundle",
    type: "collection",
    entry: [
      {
        resource: {
          resourceType: "Patient",
          id: $patient_id,
          gender: "male",
          birthDate: "1990-05-15",
          name: [{ family: "CiPatient", given: ["John"] }]
        }
      },
      {
        resource: {
          resourceType: "Observation",
          id: $obs1_id,
          status: "final",
          code: {
            coding: [{
              system: "http://loinc.org",
              code: "29463-7",
              display: "Body Weight"
            }]
          },
          subject: { reference: ("Patient/" + $patient_id) },
          valueQuantity: {
            value: 75,
            unit: "kg",
            system: "http://unitsofmeasure.org",
            code: "kg"
          }
        }
      }
    ]
  }'
)"

bundle_two="$(jq -nc \
  --arg patient_id "$patient_id" \
  --arg obs1_id "$obs1_id" \
  --arg obs2_id "$obs2_id" \
  '{
    resourceType: "Bundle",
    type: "collection",
    entry: [
      {
        resource: {
          resourceType: "Patient",
          id: $patient_id,
          gender: "male",
          birthDate: "1990-05-15",
          name: [{ family: "CiPatient", given: ["John", "Updated"] }],
          address: [{ city: "Portland", state: "OR", postalCode: "97201" }]
        }
      },
      {
        resource: {
          resourceType: "Observation",
          id: $obs1_id,
          status: "final",
          code: {
            coding: [{
              system: "http://loinc.org",
              code: "29463-7",
              display: "Body Weight"
            }]
          },
          subject: { reference: ("Patient/" + $patient_id) },
          valueQuantity: {
            value: 76,
            unit: "kg",
            system: "http://unitsofmeasure.org",
            code: "kg"
          }
        }
      },
      {
        resource: {
          resourceType: "Observation",
          id: $obs2_id,
          status: "final",
          code: {
            coding: [{
              system: "http://loinc.org",
              code: "8867-4",
              display: "Heart rate"
            }]
          },
          subject: { reference: ("Patient/" + $patient_id) },
          valueQuantity: {
            value: 72,
            unit: "beats/minute",
            system: "http://unitsofmeasure.org",
            code: "/min"
          }
        }
      }
    ]
  }'
)"

log "Ingesting initial external FHIR bundle for cohort ${cohort_id}"
request_json "POST" "${SYNTHEA_BASE_URL}/ingest-external-fhir" "$(jq -nc \
  --argjson bundle "$bundle_one" \
  --arg cohort_id "$cohort_id" \
  '{bundle: $bundle, cohort_id: $cohort_id, datatype: "external"}'
)"

if [ "$REQUEST_STATUS" != "200" ]; then
  error "Initial ingestion failed with HTTP ${REQUEST_STATUS}: ${REQUEST_BODY}"
  exit 1
fi

if ! echo "$REQUEST_BODY" | jq -e --arg cohort_id "$cohort_id" '
  .success == true
  and .cohort_id == $cohort_id
  and .datatype == "external"
  and (.patient_count | tonumber) >= 1
  and (.patient_ids | type == "array" and length >= 1)
' >/dev/null; then
  error "Initial ingestion response validation failed: ${REQUEST_BODY}"
  exit 1
fi

prefixed_patient_id="$(echo "$REQUEST_BODY" | jq -r '.patient_ids[0] // empty')"
if [ -z "$prefixed_patient_id" ] || [[ "$prefixed_patient_id" != ext-* ]]; then
  error "Expected prefixed patient id starting with ext-, got '${prefixed_patient_id}'"
  exit 1
fi

log "Validating patient and cohort resources in HAPI for ${prefixed_patient_id}"
patient_json="$(curl -fsS "${HAPI_BASE_URL}/Patient/${prefixed_patient_id}")"
if ! echo "$patient_json" | jq -e '
  .resourceType == "Patient"
  and (.id | type == "string" and length > 0)
' >/dev/null; then
  error "HAPI patient validation failed for ${prefixed_patient_id}: ${patient_json}"
  exit 1
fi

if ! echo "$patient_json" | jq -e --arg cohort_id "$cohort_id" '
  any(.meta.tag[]?; .system == "urn:charm:source" and .code == "external")
  and any(.meta.tag[]?; .system == "urn:charm:cohort" and .code == $cohort_id)
' >/dev/null; then
  error "Expected CHARM tags not found on patient ${prefixed_patient_id}"
  exit 1
fi

group_json="$(curl -fsS "${HAPI_BASE_URL}/Group/${cohort_id}")"
if ! echo "$group_json" | jq -e --arg patient_ref "Patient/${prefixed_patient_id}" '
  .resourceType == "Group"
  and any(.member[]?; .entity.reference == $patient_ref)
' >/dev/null; then
  error "Group/${cohort_id} missing expected patient member ${prefixed_patient_id}"
  exit 1
fi

obs_count_before="$(curl -fsS "${HAPI_BASE_URL}/Observation?subject=Patient/${prefixed_patient_id}&_summary=count" | jq -r '.total // 0')"
if ! [[ "$obs_count_before" =~ ^[0-9]+$ ]] || [ "$obs_count_before" -lt 1 ]; then
  error "Invalid initial observation count for ${prefixed_patient_id}: ${obs_count_before}"
  exit 1
fi

log "Re-ingesting updated bundle for ${prefixed_patient_id}"
request_json "POST" "${SYNTHEA_BASE_URL}/ingest-external-fhir" "$(jq -nc \
  --argjson bundle "$bundle_two" \
  --arg cohort_id "$cohort_id" \
  '{bundle: $bundle, cohort_id: $cohort_id, datatype: "external"}'
)"

if [ "$REQUEST_STATUS" != "200" ]; then
  error "Updated ingestion failed with HTTP ${REQUEST_STATUS}: ${REQUEST_BODY}"
  exit 1
fi

patient_duplicate_count="$(curl -fsS "${HAPI_BASE_URL}/Patient?_id=${prefixed_patient_id}&_summary=count" | jq -r '.total // 0')"
if ! [[ "$patient_duplicate_count" =~ ^[0-9]+$ ]] || [ "$patient_duplicate_count" -ne 1 ]; then
  error "Expected exactly one patient after re-ingest, got ${patient_duplicate_count}"
  exit 1
fi

obs_count_after="$(curl -fsS "${HAPI_BASE_URL}/Observation?subject=Patient/${prefixed_patient_id}&_summary=count" | jq -r '.total // 0')"
if ! [[ "$obs_count_after" =~ ^[0-9]+$ ]] || [ "$obs_count_after" -lt 2 ]; then
  error "Expected at least two observations after re-ingest, got ${obs_count_after}"
  exit 1
fi

log "Checking ingestion validation failures"
request_json "POST" "${SYNTHEA_BASE_URL}/ingest-external-fhir" "$(jq -nc \
  --argjson bundle "$bundle_one" \
  --arg cohort_id "$cohort_id" \
  '{bundle: $bundle, cohort_id: $cohort_id, datatype: "invalid-type"}'
)"
if [ "$REQUEST_STATUS" -lt 400 ]; then
  error "Expected invalid datatype request to fail, got HTTP ${REQUEST_STATUS}"
  exit 1
fi

request_json "POST" "${SYNTHEA_BASE_URL}/ingest-external-fhir" "$(jq -nc \
  --argjson bundle "$bundle_one" \
  '{bundle: $bundle, cohort_id: "invalid_cohort_id", datatype: "external"}'
)"
if [ "$REQUEST_STATUS" -lt 400 ]; then
  error "Expected invalid cohort_id request to fail, got HTTP ${REQUEST_STATUS}"
  exit 1
fi

request_json "POST" "${SYNTHEA_BASE_URL}/ingest-external-fhir" '{
  "bundle": {
    "resourceType": "Bundle",
    "type": "collection",
    "entry": []
  },
  "cohort_id": "ci-empty-bundle",
  "datatype": "external"
}'
if [ "$REQUEST_STATUS" -lt 400 ]; then
  error "Expected empty bundle request to fail, got HTTP ${REQUEST_STATUS}"
  exit 1
fi

log "External FHIR ingestion validation passed for cohort ${cohort_id}"
