#!/usr/bin/env bash

log() {
  printf '[ci] %s\n' "$*"
}

error() {
  printf '[ci][error] %s\n' "$*" >&2
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    error "Missing required command: $1"
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local timeout_seconds="${2:-120}"
  local interval_seconds="${3:-2}"
  local elapsed=0

  while [ "$elapsed" -lt "$timeout_seconds" ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$interval_seconds"
    elapsed=$((elapsed + interval_seconds))
  done

  error "Timed out waiting for $url after ${timeout_seconds}s"
  return 1
}
