# CHARMTwinsight MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server that gives AI assistants access to the CHARMTwinsight stack.

## Tools

### Patient Data
- `search_patients()` - Search for patients by name, gender, or birthdate
- `get_patient_demographics()` - Get patient demographics (name, address, etc.)
- `get_patient_all_structured_data()` - Get all structured clinical data (Observations, Conditions, Procedures, Medications, etc.)
- `get_patient_narrative_data()` - Get narrative/free-text data (DiagnosticReports, DocumentReferences) **WARNING: This information is in general very large and likely to exceed the context window.**
- `get_patient_resource_type()` - Get specific FHIR resource types (e.g., just Observations and Conditions)

### Predictive Models
- `list_available_models()` - List registered ML models
- `get_model_metadata()` - Get model details and documentation
- `execute_model()` - Run a model with input data

## Resources

The server also exposes documentation resources:
- `readme://workflow` - Recommended workflow for agent-assisted modeling
- `readme://patient-data` - Patient data access documentation
- `readme://models` - Model execution documentation

## Setup

Start the stack:

```bash
cd app
./build_all.sh
docker compose up -d
```

Verify the MCP server is running:

```bash
curl http://localhost:8006/health
```

## Configuration

Configure your AI assistant to connect at `http://localhost:8006/mcp`.

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
    "mcpServers": {
      "charmtwinsight": {
        "command": "npx",
        "args": [
          "mcp-remote",
          "http://localhost:8006/mcp"
        ]
      }
    }
}
```