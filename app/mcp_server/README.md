# CHARMTwinsight MCP Server

This is a [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server that provides AI assistants with tools to interact with the CHARMTwinsight stack.

## Features

This MCP server exposes three main categories of tools:

### 1. Synthetic Patient Data Generation (Synthea)

- `create_synthetic_patients_job()` - Generate synthetic FHIR patient data
- `get_synthetic_job_status()` - Check job progress
- `list_synthetic_jobs()` - View recent generation jobs
- `cancel_synthetic_job()` - Cancel a running job
- `get_available_states()` - Get US states for patient generation
- `get_cities_for_state()` - Get cities within a state
- `list_all_cohorts()` - View all patient cohorts
- `delete_cohort()` - Remove a cohort and its patients

### 2. Patient Data Access (HAPI FHIR)

- `get_patient_by_id()` - Get patient demographics
- `get_patient_everything()` - Get complete patient medical record
- `search_patients()` - Search for patients by criteria
- `get_patient_conditions()` - Get patient's medical conditions

### 3. Predictive Models

- `list_available_models()` - List all registered ML models
- `get_model_metadata()` - Get model details including README
- `execute_model()` - Run a model with input data

## Architecture

The MCP server connects to backed services via the internal docker network:

```
AI Assistant (Claude/ChatGPT/etc.)
    ↓ MCP Protocol
MCP Server (port 8006)
    ↓ Internal REST API
    ├─→ Synthea Server (port 8003)
    ├─→ Stat Server Python (port 8001)
    └─→ Model Server (port 8004)
```

## Resources

The MCP server also exposes documentation resources that AI assistants can read:

- `readme://synthea-server` - Synthea service documentation
- `readme://stat-server` - Statistics server documentation
- `readme://model-server` - Model server documentation

## Health Check

Check if the MCP server is running:

```bash
curl http://localhost:8006/health
```