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

log "Starting Synthea + HAPI stack for timeseries validation"
(
  cd "$APP_DIR"
  docker compose up -d --build hapi_db hapi synthea_server
)

log "Waiting for Synthea and HAPI health checks"
wait_for_http "${SYNTHEA_BASE_URL}/health" 240 3
wait_for_http "${HAPI_BASE_URL}/\$meta" 240 3

log "Validating timeseries model information endpoint"
model_info_json="$(curl -fsS "${SYNTHEA_BASE_URL}/synthetic-vitals-timeseries/model-information")"
if ! echo "$model_info_json" | jq -e '
  .model_name == "TimeAutoDiff"
  and (.seq_len | tonumber) >= 1
  and (.feature_definitions | type == "object")
  and ((.feature_definitions | keys | length) >= 1)
' >/dev/null; then
  error "Invalid timeseries model-information payload: ${model_info_json}"
  exit 1
fi

log "Validating single-patient timeseries generation"
single_json="$(
  curl -fsS -X POST "${SYNTHEA_BASE_URL}/synthetic-vitals-timeseries/generate-raw-1-patient" \
    -H "Content-Type: application/json" \
    -d '{"ethnicity":0,"gender":1,"age_group":2,"mortality_label":0}'
)"

if ! echo "$single_json" | jq -e '
  . as $root
  | .patient.ethnicity.value == 0
  and .patient.gender.value == 1
  and .patient.age_group.value == 2
  and .patient.mortality_label.value == 0
  and (.metadata.seq_len | tonumber) >= 1
  and (.metadata.n_features | tonumber) >= 1
  and (.metadata.feature_names | type == "array" and length >= 1)
  and (.timeseries | type == "array")
  and ((.timeseries | length) == (.metadata.seq_len | tonumber))
  and (all(.timeseries[]; has("timestep")))
  and (
    all(
      .timeseries[];
      (
        [$root.metadata.feature_names[] as $feature | has($feature)]
        | all(.[]; . == true)
      )
    )
  )
' >/dev/null; then
  error "Invalid single-patient timeseries payload: ${single_json}"
  exit 1
fi

log "Validating multi-patient timeseries generation"
n_patients=2
multi_json="$(
  curl -fsS -X POST \
    "${SYNTHEA_BASE_URL}/synthetic-vitals-timeseries/generate-raw-n-patients?n_patients=${n_patients}"
)"

if ! echo "$multi_json" | jq -e --argjson n "$n_patients" '
  . as $root
  | .n_patients == $n
  and (.patients | type == "array" and length == $n)
  and (.metadata.seq_len | tonumber) >= 1
  and (.metadata.n_features | tonumber) >= 1
  and (.metadata.feature_names | type == "array" and length >= 1)
  and (
    all(
      .patients[];
      (
        (.timeseries | type == "array")
        and ((.timeseries | length) == ($root.metadata.seq_len | tonumber))
        and (all(.timeseries[]; has("timestep")))
        and (
          all(
            .timeseries[];
            (
              [$root.metadata.feature_names[] as $feature | has($feature)]
              | all(.[]; . == true)
            )
          )
        )
      )
    )
  )
' >/dev/null; then
  error "Invalid multi-patient timeseries payload: ${multi_json}"
  exit 1
fi

log "Timeseries validation target passed"
