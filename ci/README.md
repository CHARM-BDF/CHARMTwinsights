# CI / Continuous Integration

Automated validation for CHARMTwinsights. CI scripts live in this directory; GitHub Actions calls them directly, so **the same tests run locally and in CI**.

## Quick Start

```bash
# From project root
./ci/run.sh <target>

# Run everything
./ci/run.sh all
```

## Available Targets

| Target | What it validates |
|--------|-------------------|
| `synthetic-fhir-validation` | Synthetic generation jobs, HAPI persistence, external FHIR ingestion (success + failure paths) |
| `model-validation` | Built-in model image builds, model server startup, schema validation, API smoke checks (listing, metadata, prediction) |
| `mcp-copd-workflow-validation` | Deterministic MCP COPD workflow: patient search/data retrieval, model metadata, input mapping, execution assertions |
| `timeseries-validation` | TimeAutoDiff model info, single/multi-patient synthetic timeseries (payload shape, sequence length, feature consistency) |
| `pdf-validation` | Patient/random PDF binary validation (`Content-Type`, `%PDF` signature, minimum size) and missing-patient failure paths |
| `router-validation` | Router proxy smoke checks across modeling, synthetic jobs, stats, timeseries, PDF, and external FHIR ingestion |
| `all` | All of the above, sequentially |

## How It Works

### Entry Point

[`run.sh`](run.sh) is the single entry point. It accepts a target name and dispatches to the corresponding validation script.

### Shared Library

[`lib.sh`](lib.sh) provides common helpers used by all validation scripts:

- `log` / `error` — prefixed logging (`[ci]` / `[ci][error]`)
- `require_cmd` — fail fast if a required CLI tool is missing
- `wait_for_http` — poll a URL until it responds (with configurable timeout), used to wait for services to become healthy

### Validation Scripts

Each target has its own script (e.g., [`router_validation.sh`](router_validation.sh), [`model_validation.sh`](model_validation.sh)). Scripts follow a common pattern:

1. **Source `lib.sh`** for shared helpers
2. **Set up a `cleanup` trap** to tear down Docker Compose services on exit
3. **Build and start** the required subset of services via `docker compose`
4. **Wait for health endpoints** using `wait_for_http`
5. **Run validation curls/checks** against the running services
6. **Exit non-zero on failure** — the trap handles cleanup

### GitHub Actions

The workflow is defined in [`.github/workflows/ci-model-validation.yaml`](../.github/workflows/ci-model-validation.yaml). Each target runs as a **separate parallel job** on `ubuntu-latest`, so failures are isolated and don't block other targets.

Triggers: push or PR to `ci-checks`, `main-dev`, or `main`.

There is also a separate [`validate-metadata.yaml`](../.github/workflows/validate-metadata.yaml) workflow for BDF metadata schema validation (runs against the upstream ARPA-H-BDF schema).

## Running Locally

### Basic Usage

```bash
./ci/run.sh model-validation
```

### Keep Services Running for Debugging

By default, services are torn down after the script finishes (pass or fail). To keep them up:

```bash
CI_KEEP_SERVICES=1 ./ci/run.sh router-validation
```

This lets you inspect logs, hit endpoints manually, and iterate without waiting for a full rebuild each time.

### Port Configuration

Validation scripts use default development ports (router: 8000, synthea: 8003, HAPI: 8080, etc.) via environment variables that can be overridden if needed. See the top of each script for the specific variables.

## Adding a New CI Target

1. Create a new script in `ci/` (e.g., `my_feature_validation.sh`)
2. Source `lib.sh`, set up a cleanup trap, build/start services, run checks
3. Add a case to [`run.sh`](run.sh) and to the `all` target
4. Add a job to [`.github/workflows/ci-model-validation.yaml`](../.github/workflows/ci-model-validation.yaml)
5. Document the target in both this README and the project root README

## Troubleshooting

- **Timeout waiting for services:** Increase timeout in the script or check Docker resource limits. On Mac, ensure Docker Desktop has sufficient memory.
- **Port conflicts:** Make sure no local services are using the same ports, or override via environment variables.
- **Flaky failures:** Some tests (especially synthetic generation) involve background job polling. If a 500 error occurs during polling, check for race conditions in the service code (this has happened before with thread-safety bugs in job serialization).
- **Stale images:** Try `docker compose build --no-cache <service>` in the `app/` directory.
