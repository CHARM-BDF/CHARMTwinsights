#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODEL_SERVER_PORT="${MODEL_SERVER_PORT:-8004}"
MODEL_SERVER_BASE_URL="${MODEL_SERVER_BASE_URL:-http://localhost:${MODEL_SERVER_PORT}}"
IMAGE_TAG="${IMAGE_TAG:-coxcopdmodel:latest}"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

require_cmd curl
require_cmd jq

url="${MODEL_SERVER_BASE_URL}/models/${IMAGE_TAG}/jsonschema"
log "Fetching JSON Schema via ${url}"
schema_json="$(curl -fsS "$url")"

input_type="$(echo "$schema_json" | jq -r '.input_schema.type')"
if [ "$input_type" != "object" ]; then
  error "Expected input_schema.type == object, got: $input_type"
  exit 1
fi

props_count="$(echo "$schema_json" | jq '.input_schema.properties | length')"
if [ "$props_count" -lt 1 ]; then
  error "Expected input_schema.properties to be non-empty"
  exit 1
fi

# The Cox model's sex_at_birth is an enum -> must convert to a JSON Schema enum array.
enum_len="$(echo "$schema_json" | jq '.input_schema.properties.sex_at_birth.enum | length')"
if [ "$enum_len" -lt 2 ]; then
  error "Expected sex_at_birth to be an enum with >=2 values, got length: $enum_len"
  exit 1
fi

log "JSON Schema smoke test passed (properties=${props_count}, sex_at_birth enum=${enum_len})"
