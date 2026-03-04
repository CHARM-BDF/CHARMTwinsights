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

log "Starting Synthea + HAPI stack for PDF validation"
(
  cd "$APP_DIR"
  docker compose up -d --build hapi_db hapi synthea_server
)

log "Waiting for Synthea and HAPI health checks"
wait_for_http "${SYNTHEA_BASE_URL}/health" 240 3
wait_for_http "${HAPI_BASE_URL}/\$meta" 240 3

cohort_id="ci-pdf-validation-$(date +%s)"
payload="$(jq -nc --arg cohort "$cohort_id" '{
  num_patients: 1,
  num_years: 1,
  cohort_id: $cohort,
  exporter: "fhir",
  use_population_sampling: false,
  state: "Massachusetts"
}')"

log "Submitting synthetic precondition for PDF checks (cohort ${cohort_id})"
create_response="$(
  curl -fsS -X POST "${SYNTHEA_BASE_URL}/synthetic-patients" \
    -H "Content-Type: application/json" \
    -d "$payload"
)"

job_id="$(echo "$create_response" | jq -r '.job_id // empty')"
if [ -z "$job_id" ]; then
  error "Missing job_id in synthetic create response: ${create_response}"
  exit 1
fi

log "Polling synthetic precondition job ${job_id}"
max_attempts=180
sleep_seconds=2
attempt=1
job_json=""
job_state=""

while [ "$attempt" -le "$max_attempts" ]; do
  job_json="$(curl -fsS "${SYNTHEA_BASE_URL}/synthetic-patients/jobs/${job_id}")"
  job_state="$(echo "$job_json" | jq -r '.status // empty')"

  if [ "$job_state" = "completed" ]; then
    break
  fi

  if [ "$job_state" = "failed" ] || [ "$job_state" = "cancelled" ]; then
    error "Synthetic precondition failed with status=${job_state}: $(echo "$job_json" | jq -c '.')"
    exit 1
  fi

  sleep "$sleep_seconds"
  attempt=$((attempt + 1))
done

if [ "$attempt" -gt "$max_attempts" ]; then
  error "Timed out waiting for synthetic precondition job ${job_id}"
  exit 1
fi

patient_id="$(curl -fsS "${SYNTHEA_BASE_URL}/list-all-patients?cohort_id=${cohort_id}&limit=1" | jq -r '.patients[0].id // empty')"
if [ -z "$patient_id" ]; then
  error "No patient found for cohort ${cohort_id}"
  exit 1
fi
log "Using generated patient ${patient_id} for PDF checks"

validate_pdf_response() {
  local url="$1"
  local label="$2"
  local body_file
  local headers_file
  local status
  local prefix
  local content_type
  local byte_count

  body_file="$(mktemp)"
  headers_file="$(mktemp)"

  status="$(
    curl -sS -o "$body_file" -D "$headers_file" -w '%{http_code}' "$url"
  )"

  if [ "$status" != "200" ]; then
    error "${label} returned HTTP ${status}"
    error "${label} response headers: $(tr '\n' ' ' < "$headers_file")"
    rm -f "$body_file" "$headers_file"
    exit 1
  fi

  content_type="$(awk 'BEGIN{IGNORECASE=1} /^content-type:/{print tolower($2)}' "$headers_file" | tr -d '\r')"
  if [[ "$content_type" != application/pdf* ]]; then
    error "${label} missing application/pdf content-type: ${content_type}"
    rm -f "$body_file" "$headers_file"
    exit 1
  fi

  prefix="$(LC_ALL=C head -c 4 "$body_file")"
  if [ "$prefix" != "%PDF" ]; then
    error "${label} did not return a PDF binary (missing %PDF header)"
    rm -f "$body_file" "$headers_file"
    exit 1
  fi

  byte_count="$(wc -c < "$body_file")"
  if [ "$byte_count" -lt 1000 ]; then
    error "${label} PDF appears too small (${byte_count} bytes)"
    rm -f "$body_file" "$headers_file"
    exit 1
  fi

  rm -f "$body_file" "$headers_file"
}

log "Validating /patient/{id}/pdf success response"
validate_pdf_response "${SYNTHEA_BASE_URL}/patient/${patient_id}/pdf" "patient PDF"

log "Validating /random-patient/pdf success response"
validate_pdf_response "${SYNTHEA_BASE_URL}/random-patient/pdf" "random patient PDF"

missing_status="$(
  curl -sS -o /tmp/ci_pdf_missing_response.txt -w '%{http_code}' \
    "${SYNTHEA_BASE_URL}/patient/ci-does-not-exist-$(date +%s)/pdf"
)"

if [ "$missing_status" -lt 400 ]; then
  error "Expected missing-patient PDF request to fail, got HTTP ${missing_status}"
  exit 1
fi

rm -f /tmp/ci_pdf_missing_response.txt

log "PDF validation target passed"
