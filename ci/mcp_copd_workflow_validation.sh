#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$ROOT_DIR/app"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

SYNTHEA_PORT="${SYNTHEA_PORT:-8003}"
HAPI_PORT="${HAPI_PORT:-8080}"
STAT_PORT="${STAT_PORT:-8001}"
MODEL_PORT="${MODEL_PORT:-8004}"
SYNTHEA_BASE_URL="http://localhost:${SYNTHEA_PORT}"
HAPI_BASE_URL="http://localhost:${HAPI_PORT}/fhir"
STAT_BASE_URL="http://localhost:${STAT_PORT}"
MODEL_BASE_URL="http://localhost:${MODEL_PORT}"

cleanup() {
  if [ "${CI_KEEP_SERVICES:-0}" = "1" ]; then
    log "CI_KEEP_SERVICES=1 set; leaving services running"
    return
  fi

  log "Stopping compose services"
  (
    cd "$APP_DIR"
    docker compose down --remove-orphans -v
  ) || true
}

trap cleanup EXIT

require_cmd docker
require_cmd curl
require_cmd jq

log "Building built-in model images"
(
  cd "$APP_DIR/model_server/models"
  ./build_model_images.sh
)

log "Starting MCP COPD workflow stack"
(
  cd "$APP_DIR"
  docker compose up -d --build \
    hapi_db \
    hapi \
    synthea_server \
    stat_server_py \
    model_server_db \
    model_server \
    mcp_server
)

log "Waiting for dependency health checks"
wait_for_http "${SYNTHEA_BASE_URL}/health" 300 3
wait_for_http "${HAPI_BASE_URL}/\$meta" 300 3
wait_for_http "${STAT_BASE_URL}/health" 300 3
wait_for_http "${MODEL_BASE_URL}/health" 300 3

timestamp="$(date +%s)"
synthetic_cohort_id="ci-mcp-precheck-${timestamp}"
synthetic_payload="$(jq -nc --arg cohort "$synthetic_cohort_id" '{
  num_patients: 1,
  num_years: 1,
  cohort_id: $cohort,
  exporter: "fhir",
  use_population_sampling: false,
  state: "Massachusetts"
}')"

log "Running synthetic precondition job for cohort ${synthetic_cohort_id}"
synthetic_create_response="$(
  curl -fsS -X POST "${SYNTHEA_BASE_URL}/synthetic-patients" \
    -H "Content-Type: application/json" \
    -d "$synthetic_payload"
)"

synthetic_job_id="$(echo "$synthetic_create_response" | jq -r '.job_id // empty')"
if [ -z "$synthetic_job_id" ]; then
  error "Missing job_id in synthetic create response: $synthetic_create_response"
  exit 1
fi

log "Polling synthetic precondition job ${synthetic_job_id}"
max_attempts=180
sleep_seconds=2
attempt=1
synthetic_job_json=""

while [ "$attempt" -le "$max_attempts" ]; do
  synthetic_job_json="$(curl -fsS "${SYNTHEA_BASE_URL}/synthetic-patients/jobs/${synthetic_job_id}")"
  synthetic_status="$(echo "$synthetic_job_json" | jq -r '.status')"

  if [ "$synthetic_status" = "completed" ]; then
    break
  fi

  if [ "$synthetic_status" = "failed" ] || [ "$synthetic_status" = "cancelled" ]; then
    error "Synthetic precondition job ended with status=${synthetic_status}: $(echo "$synthetic_job_json" | jq -c '.')"
    exit 1
  fi

  sleep "$sleep_seconds"
  attempt=$((attempt + 1))
done

if [ "$attempt" -gt "$max_attempts" ]; then
  error "Timed out waiting for synthetic precondition job ${synthetic_job_id}"
  exit 1
fi

synthetic_total="$(echo "$synthetic_job_json" | jq -r '.result.total_patients // 0')"
if [ "$synthetic_total" -lt 1 ]; then
  error "Synthetic precondition job completed but total_patients=${synthetic_total}"
  exit 1
fi

log "Synthetic precondition completed with ${synthetic_total} generated patient(s)"

fixture_suffix="$timestamp"
fixture_cohort="ci-mcp-copd-${fixture_suffix}"
fixture_given="CiMcpGiven${fixture_suffix}"
fixture_family="CiMcpFamily${fixture_suffix}"
fixture_birthdate="1974-06-24"
fixture_local_patient_id="ci-patient-${fixture_suffix}"
fixture_bmi_obs_id="ci-obs-bmi-${fixture_suffix}"
fixture_smoking_obs_id="ci-obs-smoking-${fixture_suffix}"
fixture_cvd_condition_id="ci-cond-cvd-${fixture_suffix}"
fixture_obesity_condition_id="ci-cond-obesity-${fixture_suffix}"

bundle_payload="$(jq -nc \
  --arg fixture_local_patient_id "$fixture_local_patient_id" \
  --arg fixture_given "$fixture_given" \
  --arg fixture_family "$fixture_family" \
  --arg fixture_birthdate "$fixture_birthdate" \
  --arg fixture_bmi_obs_id "$fixture_bmi_obs_id" \
  --arg fixture_smoking_obs_id "$fixture_smoking_obs_id" \
  --arg fixture_cvd_condition_id "$fixture_cvd_condition_id" \
  --arg fixture_obesity_condition_id "$fixture_obesity_condition_id" \
  '{
    resourceType: "Bundle",
    type: "collection",
    entry: [
      {
        resource: {
          resourceType: "Patient",
          id: $fixture_local_patient_id,
          name: [{ use: "official", family: $fixture_family, given: [$fixture_given] }],
          gender: "male",
          birthDate: $fixture_birthdate,
          extension: [
            {
              url: "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
              extension: [{ url: "text", valueString: "Not Hispanic or Latino" }]
            },
            {
              url: "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
              valueCode: "M"
            }
          ]
        }
      },
      {
        resource: {
          resourceType: "Observation",
          id: $fixture_bmi_obs_id,
          status: "final",
          code: {
            coding: [{ system: "http://loinc.org", code: "39156-5", display: "Body mass index (BMI) [Ratio]" }],
            text: "Body Mass Index"
          },
          subject: { reference: ("Patient/" + $fixture_local_patient_id) },
          effectiveDateTime: "2025-06-23T00:00:00Z",
          valueQuantity: {
            value: 30.54,
            unit: "kg/m2",
            system: "http://unitsofmeasure.org",
            code: "kg/m2"
          }
        }
      },
      {
        resource: {
          resourceType: "Observation",
          id: $fixture_smoking_obs_id,
          status: "final",
          code: {
            coding: [{ system: "http://loinc.org", code: "72166-2", display: "Tobacco smoking status" }],
            text: "Tobacco smoking status"
          },
          subject: { reference: ("Patient/" + $fixture_local_patient_id) },
          effectiveDateTime: "2025-06-23T00:00:00Z",
          valueCodeableConcept: {
            text: "Never smoked tobacco (finding)"
          }
        }
      },
      {
        resource: {
          resourceType: "Condition",
          id: $fixture_cvd_condition_id,
          clinicalStatus: {
            coding: [{ system: "http://terminology.hl7.org/CodeSystem/condition-clinical", code: "active" }]
          },
          verificationStatus: {
            coding: [{ system: "http://terminology.hl7.org/CodeSystem/condition-ver-status", code: "confirmed" }]
          },
          code: {
            coding: [{ system: "http://snomed.info/sct", code: "414545008", display: "Ischemic heart disease (disorder)" }],
            text: "Ischemic heart disease"
          },
          subject: { reference: ("Patient/" + $fixture_local_patient_id) }
        }
      },
      {
        resource: {
          resourceType: "Condition",
          id: $fixture_obesity_condition_id,
          clinicalStatus: {
            coding: [{ system: "http://terminology.hl7.org/CodeSystem/condition-clinical", code: "active" }]
          },
          verificationStatus: {
            coding: [{ system: "http://terminology.hl7.org/CodeSystem/condition-ver-status", code: "confirmed" }]
          },
          code: {
            coding: [{ system: "http://snomed.info/sct", code: "414916001", display: "Obesity (disorder)" }],
            text: "Obesity"
          },
          subject: { reference: ("Patient/" + $fixture_local_patient_id) }
        }
      }
    ]
  }'
)"

ingest_payload="$(jq -nc \
  --arg cohort_id "$fixture_cohort" \
  --argjson bundle "$bundle_payload" \
  '{cohort_id: $cohort_id, datatype: "external", bundle: $bundle}'
)"

log "Ingesting deterministic MCP COPD fixture bundle into cohort ${fixture_cohort}"
ingest_response="$(
  curl -fsS -X POST "${SYNTHEA_BASE_URL}/ingest-external-fhir" \
    -H "Content-Type: application/json" \
    -d "$ingest_payload"
)"

if ! echo "$ingest_response" | jq -e --arg cohort "$fixture_cohort" '
  .success == true
  and .cohort_id == $cohort
  and (.patient_ids | type == "array" and length == 1)
' >/dev/null; then
  error "Invalid ingestion response: ${ingest_response}"
  exit 1
fi

prefixed_patient_id="$(echo "$ingest_response" | jq -r '.patient_ids[0] // empty')"
if [ -z "$prefixed_patient_id" ] || [[ "$prefixed_patient_id" != ext-* ]]; then
  error "Expected prefixed patient id, got '${prefixed_patient_id}'"
  exit 1
fi

log "Fixture ingested for patient ${prefixed_patient_id}; executing MCP workflow validation"
(
  cd "$APP_DIR"
  docker compose exec -T \
    -e CI_MCP_PATIENT_ID="$prefixed_patient_id" \
    -e CI_MCP_PATIENT_GIVEN="$fixture_given" \
    -e CI_MCP_PATIENT_FAMILY="$fixture_family" \
    -e CI_MCP_PATIENT_BIRTHDATE="$fixture_birthdate" \
    mcp_server python - < "$ROOT_DIR/ci/mcp_copd_workflow_client.py"
)

log "MCP COPD workflow validation passed"
