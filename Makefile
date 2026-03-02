SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  local-ci-checks            Run local CI checks (all current CI targets)"
	@echo "  local-ci-synthetic-fhir    Run synthetic generation + FHIR + external ingestion validation CI target"
	@echo "  local-ci-model-validation  Run model validation CI target only"
	@echo "  build                      Build all app and model images"
	@echo "  up                         Start the app stack in detached mode"
	@echo "  down                       Stop the app stack"
	@echo "  gen-patients               Generate sample synthetic patients"
	@echo "  test-external-ingest       Run external FHIR ingest test script"
	@echo "  test-models                Run model prediction smoke script"
	@echo "  test-model-validation      Run model schema validation script"
	@echo "  test-analytics             Run stats server smoke script"
	@echo "  test-mcp                   Run MCP server smoke script"

.PHONY: local-ci-checks
local-ci-checks:
	./ci/run.sh all

.PHONY: local-ci-synthetic-fhir
local-ci-synthetic-fhir:
	./ci/run.sh synthetic-fhir-validation

.PHONY: local-ci-model-validation
local-ci-model-validation:
	./ci/run.sh model-validation

.PHONY: build
build:
	cd app && ./build_all.sh

.PHONY: up
up:
	cd app && docker compose up --detach

.PHONY: down
down:
	cd app && docker compose down

.PHONY: gen-patients
gen-patients:
	cd app && ./synthea_server/gen_patients.sh

.PHONY: test-external-ingest
test-external-ingest:
	cd app && ./synthea_server/test_external_ingest.sh

.PHONY: test-models
test-models:
	cd app && ./model_server/models/test_predict_models.sh

.PHONY: test-model-validation
test-model-validation:
	cd app && APP_PORT=8004 ./model_server/models/test_validation.sh

.PHONY: test-analytics
test-analytics:
	cd app && ./stat_server_py/test_stats.sh

.PHONY: test-mcp
test-mcp:
	cd app && ./mcp_server/test_mcp.sh
