#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$ROOT_DIR/app"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

SYNTHEA_PORT="${SYNTHEA_PORT:-8003}"
HAPI_PORT="${HAPI_PORT:-8080}"
SYNTHEA_BASE_URL="http://localhost:${SYNTHEA_PORT}"
HAPI_BASE_URL="http://localhost:${HAPI_PORT}/fhir"
COHORT_ID="ci-synthetic-$(date +%s)"

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

log "Starting minimal Synthea + HAPI stack"
(
  cd "$APP_DIR"
  docker compose up -d --build hapi_db hapi synthea_server
)

log "Waiting for Synthea and HAPI health checks"
wait_for_http "$SYNTHEA_BASE_URL/health" 240 3
wait_for_http "${HAPI_BASE_URL}/\$meta" 240 3

payload="$(jq -nc --arg cohort "$COHORT_ID" '{
  num_patients: 2,
  num_years: 1,
  cohort_id: $cohort,
  exporter: "fhir",
  use_population_sampling: false,
  state: "Massachusetts"
}')"

log "Submitting synthetic generation job for cohort ${COHORT_ID}"
create_response="$(
  curl -fsS -X POST "${SYNTHEA_BASE_URL}/synthetic-patients" \
    -H "Content-Type: application/json" \
    -d "$payload"
)"

job_id="$(echo "$create_response" | jq -r '.job_id // empty')"
if [ -z "$job_id" ]; then
  error "Missing job_id in create response: $create_response"
  exit 1
fi

log "Created job ${job_id}; polling for completion"
max_attempts=180
sleep_seconds=2
attempt=1
job_json=""

while [ "$attempt" -le "$max_attempts" ]; do
  job_json="$(curl -fsS "${SYNTHEA_BASE_URL}/synthetic-patients/jobs/${job_id}")"
  status="$(echo "$job_json" | jq -r '.status')"

  if [ "$status" = "completed" ]; then
    break
  fi

  if [ "$status" = "failed" ] || [ "$status" = "cancelled" ]; then
    error "Job ${job_id} ended with status=${status}: $(echo "$job_json" | jq -c '.')"
    exit 1
  fi

  sleep "$sleep_seconds"
  attempt=$((attempt + 1))
done

if [ "$attempt" -gt "$max_attempts" ]; then
  error "Timed out waiting for job ${job_id} completion"
  exit 1
fi

generated_total="$(echo "$job_json" | jq -r '.result.total_patients // 0')"
if [ "$generated_total" -lt 1 ]; then
  error "Job completed but total_patients is ${generated_total}"
  exit 1
fi

log "Job completed with ${generated_total} generated patients"

cohorts_json="$(curl -fsS "${SYNTHEA_BASE_URL}/list-all-cohorts")"
cohort_matches="$(echo "$cohorts_json" | jq --arg id "$COHORT_ID" '[.cohorts[] | select(.cohort_id == $id)] | length')"
if [ "$cohort_matches" -lt 1 ]; then
  error "Cohort ${COHORT_ID} not found in /list-all-cohorts"
  exit 1
fi

cohort_patient_count="$(echo "$cohorts_json" | jq --arg id "$COHORT_ID" '[.cohorts[] | select(.cohort_id == $id) | .patient_count][0] // 0')"
if [ "$cohort_patient_count" -lt 1 ]; then
  error "Cohort ${COHORT_ID} has invalid patient_count=${cohort_patient_count}"
  exit 1
fi

patients_json="$(curl -fsS "${SYNTHEA_BASE_URL}/list-all-patients")"
patients_in_cohort="$(echo "$patients_json" | jq --arg id "$COHORT_ID" '[.patients[] | select((.cohort_ids // []) | index($id))] | length')"
if [ "$patients_in_cohort" -lt 1 ]; then
  error "No patients linked to cohort ${COHORT_ID} in /list-all-patients"
  exit 1
fi

group_json="$(curl -fsS "${HAPI_BASE_URL}/Group/${COHORT_ID}")"
group_member_count="$(echo "$group_json" | jq '[.member[]?] | length')"
if [ "$group_member_count" -lt 1 ]; then
  error "HAPI Group/${COHORT_ID} has no members"
  exit 1
fi

log "Running external FHIR ingestion validation suite"
"$ROOT_DIR/ci/external_fhir_ingestion_validation.sh"

log "Synthetic + FHIR validation passed for cohort ${COHORT_ID}"
