#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$ROOT_DIR/app"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

ROUTER_PORT="${ROUTER_PORT:-8000}"
SYNTHEA_PORT="${SYNTHEA_PORT:-8003}"
HAPI_PORT="${HAPI_PORT:-8080}"
STAT_PORT="${STAT_PORT:-8001}"
MODEL_PORT="${MODEL_PORT:-8004}"

ROUTER_BASE_URL="http://localhost:${ROUTER_PORT}"
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

log "Building built-in model images for router validation"
(
  cd "$APP_DIR/model_server/models"
  ./build_model_images.sh
)

log "Starting router dependencies"
(
  cd "$APP_DIR"
  docker compose up -d --build \
    hapi_db \
    hapi \
    synthea_server \
    stat_server_py \
    model_server_db \
    model_server
)

log "Starting router service without compose dependency gating"
(
  cd "$APP_DIR"
  docker compose up -d --build --no-deps router
)

log "Waiting for dependency health endpoints"
wait_for_http "${SYNTHEA_BASE_URL}/health" 300 3
wait_for_http "${HAPI_BASE_URL}/\$meta" 300 3
wait_for_http "${STAT_BASE_URL}/health" 300 3
wait_for_http "${MODEL_BASE_URL}/health" 300 3
wait_for_http "${ROUTER_BASE_URL}/openapi.json" 300 3

log "Checking router modeling endpoints"
max_attempts=60
sleep_seconds=2
attempt=1
models_json=""

while [ "$attempt" -le "$max_attempts" ]; do
  models_json="$(curl -fsS "${ROUTER_BASE_URL}/modeling/models")"
  if echo "$models_json" | jq -e 'map(select(.image == "coxcopdmodel:latest")) | length > 0' >/dev/null; then
    break
  fi
  sleep "$sleep_seconds"
  attempt=$((attempt + 1))
done

if [ "$attempt" -gt "$max_attempts" ]; then
  error "Router /modeling/models did not report coxcopdmodel:latest in time: ${models_json}"
  exit 1
fi

prediction_payload='{
  "image": "coxcopdmodel:latest",
  "input": [
    {
      "ethnicity": "Not Hispanic or Latino",
      "sex_at_birth": "Male",
      "obesity": 1.0,
      "diabetes": 0.0,
      "cardiovascular_disease": 1.0,
      "smoking_status": 0.0,
      "alcohol_use": 0.0,
      "bmi": 30.54,
      "age_at_time_0": 50.0
    }
  ]
}'
request_json "POST" "${ROUTER_BASE_URL}/modeling/predict" "$prediction_payload"
if [ "$REQUEST_STATUS" != "200" ]; then
  error "Router /modeling/predict failed with HTTP ${REQUEST_STATUS}: ${REQUEST_BODY}"
  exit 1
fi
if ! echo "$REQUEST_BODY" | jq -e '
  (.predictions | type == "array" and length == 1)
  and (.predictions[0].partial_hazard | tonumber > 0)
  and (
    (.predictions[0].survival_probability_5_years | tonumber) as $surv
    | ($surv >= 0 and $surv <= 1)
  )
' >/dev/null; then
  error "Router /modeling/predict response validation failed: ${REQUEST_BODY}"
  exit 1
fi

timestamp="$(date +%s)"
cohort_id="ci-router-${timestamp}"

log "Running router synthetic generation job"
synthetic_payload="$(jq -nc --arg cohort "$cohort_id" '{
  num_patients: 1,
  num_years: 1,
  cohort_id: $cohort,
  exporter: "fhir",
  use_population_sampling: false,
  state: "Massachusetts"
}')"

request_json "POST" "${ROUTER_BASE_URL}/synthetic/synthea/synthetic-patients" "$synthetic_payload"
if [ "$REQUEST_STATUS" != "200" ]; then
  error "Router synthetic job creation failed with HTTP ${REQUEST_STATUS}: ${REQUEST_BODY}"
  exit 1
fi

job_id="$(echo "$REQUEST_BODY" | jq -r '.job_id // empty')"
if [ -z "$job_id" ]; then
  error "Router synthetic job response missing job_id: ${REQUEST_BODY}"
  exit 1
fi

log "Polling router synthetic job ${job_id}"
attempt=1
job_json=""
job_status=""

while [ "$attempt" -le 180 ]; do
  job_json="$(curl -fsS "${ROUTER_BASE_URL}/synthetic/synthea/synthetic-patients/jobs/${job_id}")"
  job_status="$(echo "$job_json" | jq -r '.status // empty')"

  if [ "$job_status" = "completed" ]; then
    break
  fi

  if [ "$job_status" = "failed" ] || [ "$job_status" = "cancelled" ]; then
    error "Router synthetic job ended with status=${job_status}: $(echo "$job_json" | jq -c '.')"
    exit 1
  fi

  sleep 2
  attempt=$((attempt + 1))
done

if [ "$attempt" -gt 180 ]; then
  error "Timed out waiting for router synthetic job ${job_id}"
  exit 1
fi

if ! echo "$job_json" | jq -e '.result.total_patients // 0 | tonumber >= 1' >/dev/null; then
  error "Router synthetic job completed without generated patients: ${job_json}"
  exit 1
fi

patients_json="$(curl -fsS "${ROUTER_BASE_URL}/synthetic/synthea/list-all-patients")"
patient_id="$(
  echo "$patients_json" | jq -r --arg cohort "$cohort_id" '
    .patients[]
    | select((.cohort_ids // []) | index($cohort))
    | .id
    ' | head -n 1
)"

if [ -z "$patient_id" ]; then
  error "Could not locate generated patient for cohort ${cohort_id} via router"
  exit 1
fi

log "Checking router stats proxy endpoint for patient ${patient_id}"
patient_json="$(curl -fsS "${ROUTER_BASE_URL}/stats/patients/${patient_id}")"
if ! echo "$patient_json" | jq -e '
  (.patient_id // .id // "") | tostring | length > 0
' >/dev/null; then
  error "Router /stats/patients/{id} returned invalid payload: ${patient_json}"
  exit 1
fi

log "Checking router timeseries endpoints"
timeseries_info="$(curl -fsS "${ROUTER_BASE_URL}/synthetic/timeseries/model-information")"
if ! echo "$timeseries_info" | jq -e '
  .model_name == "TimeAutoDiff"
  and (.seq_len | tonumber >= 1)
  and (.feature_names | type == "array" and length >= 1)
' >/dev/null; then
  error "Router timeseries model-information validation failed: ${timeseries_info}"
  exit 1
fi

timeseries_single="$(
  curl -fsS -X POST "${ROUTER_BASE_URL}/synthetic/timeseries/generate-raw-1-patient" \
    -H "Content-Type: application/json" \
    -d '{"ethnicity":0,"gender":1,"age_group":2,"mortality_label":0}'
)"
if ! echo "$timeseries_single" | jq -e '
  . as $root
  | (.metadata.seq_len | tonumber >= 1)
  and (.metadata.feature_names | type == "array" and length >= 1)
  and (.timeseries | type == "array")
  and ((.timeseries | length) == (.metadata.seq_len | tonumber))
  and (
    all(
      .timeseries[];
      (
        has("timestep")
        and ([$root.metadata.feature_names[] as $feature | has($feature)] | all(.[]; . == true))
      )
    )
  )
' >/dev/null; then
  error "Router timeseries single-patient validation failed: ${timeseries_single}"
  exit 1
fi

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
    error "${label} did not return PDF content (missing %PDF header)"
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

log "Checking router PDF proxy endpoint"
validate_pdf_response "${ROUTER_BASE_URL}/synthetic/synthea/random-patient/pdf" "router random patient PDF"

log "Checking router ingestion endpoint"
fixture_patient_id="ci-router-ext-patient-${timestamp}"
fixture_obs_id="ci-router-ext-obs-${timestamp}"
ingest_payload="$(jq -nc \
  --arg cohort "$cohort_id" \
  --arg patient_id "$fixture_patient_id" \
  --arg obs_id "$fixture_obs_id" \
  '{
    cohort_id: $cohort,
    datatype: "external",
    bundle: {
      resourceType: "Bundle",
      type: "collection",
      entry: [
        {
          resource: {
            resourceType: "Patient",
            id: $patient_id,
            gender: "male",
            birthDate: "1974-06-24",
            name: [{family: "RouterCi", given: ["Patient"]}]
          }
        },
        {
          resource: {
            resourceType: "Observation",
            id: $obs_id,
            status: "final",
            code: {
              coding: [{system: "http://loinc.org", code: "39156-5", display: "Body mass index (BMI) [Ratio]"}],
              text: "Body Mass Index"
            },
            subject: {reference: ("Patient/" + $patient_id)},
            valueQuantity: {
              value: 30.54,
              unit: "kg/m2",
              system: "http://unitsofmeasure.org",
              code: "kg/m2"
            }
          }
        }
      ]
    }
  }'
)"

request_json "POST" "${ROUTER_BASE_URL}/ingest/fhir" "$ingest_payload"
if [ "$REQUEST_STATUS" != "200" ]; then
  error "Router /ingest/fhir failed with HTTP ${REQUEST_STATUS}: ${REQUEST_BODY}"
  exit 1
fi

if ! echo "$REQUEST_BODY" | jq -e --arg cohort "$cohort_id" '
  .success == true
  and .cohort_id == $cohort
  and (.patient_ids | type == "array" and length >= 1)
' >/dev/null; then
  error "Router /ingest/fhir response validation failed: ${REQUEST_BODY}"
  exit 1
fi

log "Router validation target passed"
