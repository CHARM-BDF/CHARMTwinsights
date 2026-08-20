#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$ROOT_DIR/app"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

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

log "Building built-in model images"
(
  cd "$APP_DIR/model_server/models"
  ./build_model_images.sh
)

log "Starting model server stack"
(
  cd "$APP_DIR"
  docker compose up -d --build model_server
)

log "Waiting for model server health endpoint"
wait_for_http "http://localhost:8004/health" 180 3

log "Running model schema validation suite"
APP_PORT=8004 "$APP_DIR/model_server/models/test_validation.sh"

log "Running model registration + metadata + prediction smoke checks"
"$ROOT_DIR/ci/model_registry_prediction_smoke.sh"

log "Running model JSON Schema smoke checks"
"$ROOT_DIR/ci/model_jsonschema_smoke.sh"

log "Model validation target passed"
