#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$ROOT_DIR/app"
MODELS_DIR="$APP_DIR/model_server/models"
MODEL_SERVER_PORT="${MODEL_SERVER_PORT:-8004}"
MODEL_SERVER_BASE_URL="${MODEL_SERVER_BASE_URL:-http://localhost:${MODEL_SERVER_PORT}}"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

require_cmd curl
require_cmd jq

log "Checking registered built-in models via ${MODEL_SERVER_BASE_URL}/models"
models_json="$(curl -fsS "${MODEL_SERVER_BASE_URL}/models")"

expected_images=()
while IFS= read -r -d '' metadata_file; do
  image="$(jq -r '.image' "$metadata_file")"
  [ -n "$image" ] && expected_images+=("$image")
done < <(find "$MODELS_DIR" -mindepth 2 -maxdepth 2 -name model_metadata.json -print0)

if [ "${#expected_images[@]}" -eq 0 ]; then
  error "No built-in model metadata files found under $MODELS_DIR"
  exit 1
fi

registered_count="$(echo "$models_json" | jq 'length')"
log "Model server reports ${registered_count} registered model(s)"

for image in "${expected_images[@]}"; do
  log "Validating model registration and prediction for ${image}"

  if ! echo "$models_json" | jq -e --arg image "$image" 'map(select(.image == $image)) | length > 0' >/dev/null; then
    error "Expected built-in model not found in /models: ${image}"
    exit 1
  fi

  encoded_image="$(printf '%s' "$image" | jq -sRr @uri)"
  model_json="$(curl -fsS "${MODEL_SERVER_BASE_URL}/models/${encoded_image}")"

  if ! echo "$model_json" | jq -e --arg image "$image" '
    .image == $image
    and (.title | type == "string" and length > 0)
    and (.short_description | type == "string" and length > 0)
    and (.authors | type == "string" and length > 0)
    and (.examples | type == "array" and length > 0)
    and (.readme | type == "string" and length > 0)
    and (.input_schema | type == "object")
    and (.output_schema | type == "object")
  ' >/dev/null; then
    error "Model metadata validation failed for ${image}"
    exit 1
  fi

  payload="$(echo "$model_json" | jq -c --arg image "$image" '{image: $image, input: [.examples[0]]}')"
  predict_raw="$(
    curl -sS -w '\n%{http_code}' -X POST "${MODEL_SERVER_BASE_URL}/predict" \
      -H "Content-Type: application/json" \
      -d "$payload"
  )"
  predict_status="$(echo "$predict_raw" | tail -n 1)"
  predict_body="$(echo "$predict_raw" | sed '$d')"

  if [ "$predict_status" != "200" ]; then
    error "Prediction failed for ${image} (HTTP ${predict_status}): ${predict_body}"
    exit 1
  fi

  if ! echo "$predict_body" | jq -e '
    (.predictions | type == "array" and length > 0)
    and (.stdout | type == "string")
    and (.stderr | type == "string")
  ' >/dev/null; then
    error "Prediction response validation failed for ${image}: ${predict_body}"
    exit 1
  fi
done

log "Model registration + metadata + prediction smoke checks passed"
