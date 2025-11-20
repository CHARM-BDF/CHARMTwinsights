# MCP Server Implementation Summary

## What Was Built

An MCP (Model Context Protocol) server has been added to the CHARMTwinsight stack, providing AI assistants with programmatic access to:

1. **Synthetic Patient Data Generation** (Synthea server)
2. **Patient Data Access** (HAPI FHIR via stat_server_py)
3. **Predictive Model Execution** (Model server)

## Files Created

### Core Implementation
- `app/mcp_server/mcp_server/main.py` - Main MCP server with 17 tools + 3 resources
- `app/mcp_server/mcp_server/__init__.py` - Package initialization
- `app/mcp_server/pyproject.toml` - Python dependencies (FastMCP, requests, pydantic)
- `app/mcp_server/poetry.lock` - Locked dependency versions
- `app/mcp_server/Dockerfile` - Container definition using Python 3.11

### Documentation & Testing
- `app/mcp_server/README.md` - Complete user guide with examples
- `app/mcp_server/SUMMARY.md` - This file
- `app/mcp_server/test_mcp.sh` - Test script to verify server health

### Configuration Changes
- `app/docker-compose.yml` - Added mcp_server service with health checks
- `app/docker-compose.override.yaml` - Exposed port 8006 for development
- `README.md` - Updated main documentation with MCP section

## MCP Tools Implemented

### Synthetic Data Generation (8 tools)
1. `create_synthetic_patients_job()` - Start patient generation
2. `get_synthetic_job_status()` - Check job progress
3. `list_synthetic_jobs()` - View recent jobs
4. `cancel_synthetic_job()` - Cancel running job
5. `get_available_states()` - List US states
6. `get_cities_for_state()` - List cities in a state
7. `list_all_cohorts()` - View all patient cohorts
8. `delete_cohort()` - Remove cohort and patients

### Patient Data Access (4 tools)
9. `get_patient_by_id()` - Get patient demographics
10. `get_patient_everything()` - Get complete medical record (FHIR $everything)
11. `search_patients()` - Search by name/gender/birthdate
12. `get_patient_conditions()` - Get patient's conditions

### Model Execution (3 tools)
13. `list_available_models()` - List registered ML models
14. `get_model_metadata()` - Get model details + README
15. `execute_model()` - Run predictions

### Documentation Resources (3 resources)
- `readme://synthea-server` - Synthea documentation
- `readme://stat-server` - Stats server documentation  
- `readme://model-server` - Model server documentation

## Architecture

```
AI Assistant (Claude/ChatGPT/etc.)
    ↓ MCP Protocol (SSE transport)
MCP Server (localhost:8006)
    ↓ Internal REST APIs
    ├─→ Synthea Server (synthea_server:8000)
    ├─→ Stat Server (stat_server_py:8000)
    ├─→ Model Server (model_server:8000)
    └─→ HAPI FHIR (hapi:8080)
```

The MCP server acts as a bridge, translating MCP tool calls into REST API requests to the appropriate backend services.

## Usage

### 1. Build and Start

```bash
cd app
./build_all.sh
docker compose up --detach
```

The MCP server will be available at `http://localhost:8006`

### 2. Test the Server

```bash
cd app/mcp_server
./test_mcp.sh
```

### 3. Connect an AI Assistant

#### Claude Desktop
Add to `~/.config/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "charmtwinsight": {
      "url": "http://localhost:8006",
      "transport": {
        "type": "sse"
      }
    }
  }
}
```

#### Cursor IDE
Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "charmtwinsight": {
      "url": "http://localhost:8006",
      "transport": {
        "type": "sse"
      }
    }
  }
}
```

### 4. Use Natural Language

Once connected, interact naturally:

- "Generate 50 synthetic patients in Massachusetts and put them in cohort 'pilot-study'"
- "Show me all the medical conditions for patient abc-123"
- "What machine learning models are available?"
- "Run the CoxCOPDModel on this patient data: [...]"

## Key Features

### Async Job Management
Large patient generation requests are handled asynchronously with:
- Real-time progress tracking
- Chunk-based processing (100 patients per chunk)
- Incremental results (partial cohorts available if job fails)
- Job cancellation support

### Comprehensive Patient Data
The `get_patient_everything()` tool retrieves:
- Demographics
- Conditions
- Observations
- Procedures
- Medications
- Encounters
- And more...

### Model README Integration
The `get_model_metadata()` tool returns full markdown documentation for each model, including:
- What the model predicts
- Input/output formats
- Performance metrics
- Usage examples
- Citations

This allows AI assistants to understand and properly use models without external documentation.

## Development

### Rebuilding After Changes

```bash
cd app
docker compose build mcp_server
docker compose up mcp_server
```

### Adding New Tools

1. Edit `mcp_server/main.py`
2. Add new tool function with `@mcp.tool()` decorator
3. Document parameters and return values in docstring
4. Rebuild and test

### Debugging

View logs:
```bash
docker compose logs mcp_server -f
```

Check health:
```bash
curl http://localhost:8006/health
```

## Transport: SSE (Server-Sent Events)

The MCP server uses SSE transport, which:
- Works over standard HTTP
- Is firewall-friendly
- Supports real-time streaming
- Is compatible with most MCP clients

The server is started with:
```bash
python -m fastmcp run mcp_server.main:mcp --transport sse
```

## Security Considerations

- MCP server runs inside Docker network
- Only exposes port 8006 on localhost (via docker-compose.override.yaml)
- All backend communication uses internal Docker DNS
- No direct external access to backend services
- For production, consider:
  - Authentication/authorization
  - HTTPS with TLS
  - Network policies
  - Rate limiting

## Resources

- [MCP Protocol Documentation](https://modelcontextprotocol.io)
- [FastMCP Framework](https://gofastmcp.com)
- [MCP Server README](README.md)
- [CHARMTwinsight Main README](../../README.md)

## Future Enhancements

Potential additions:
- Authentication/authorization middleware
- Rate limiting for resource-intensive operations
- Caching for frequently accessed data
- Webhooks for long-running job notifications
- Additional analytics tools
- Data export/import tools
- Batch operations for bulk processing

