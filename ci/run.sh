#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: ./ci/run.sh <target>

Targets:
  model-validation   Run model server validation checks (docker-based)
  all                Run all currently defined CI targets
EOF
}

target="${1:-all}"

case "$target" in
  model-validation)
    "$ROOT_DIR/ci/model_validation.sh"
    ;;
  all)
    "$ROOT_DIR/ci/model_validation.sh"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown target: $target" >&2
    usage
    exit 1
    ;;
esac
