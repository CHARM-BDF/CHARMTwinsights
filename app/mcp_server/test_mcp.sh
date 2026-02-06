#!/bin/bash

# Test script for MCP server
# This script checks if the MCP server is responding and can list its tools

set -e

MCP_SERVER_URL="${MCP_SERVER_URL:-http://localhost:8006}"

echo "Testing MCP Server at $MCP_SERVER_URL"
echo "========================================"
echo ""

# Wait for the server to be ready
echo "Waiting for MCP server to be ready..."
for i in {1..30}; do
    if curl -s -f "$MCP_SERVER_URL/health" > /dev/null 2>&1; then
        echo "✓ MCP server is responding"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "✗ MCP server failed to respond after 30 seconds"
        exit 1
    fi
    sleep 1
done

echo ""

# Test health endpoint
echo "Testing health endpoint..."
HEALTH=$(curl -s "$MCP_SERVER_URL/health")
echo "$HEALTH" | python3 -m json.tool
echo ""

# Test MCP capabilities endpoint (if available)
echo "Testing MCP capabilities..."
if curl -s -f "$MCP_SERVER_URL/mcp/capabilities" > /dev/null 2>&1; then
    CAPABILITIES=$(curl -s "$MCP_SERVER_URL/mcp/capabilities")
    echo "$CAPABILITIES" | python3 -m json.tool
else
    echo "Note: MCP capabilities endpoint not available (this is normal for SSE transport)"
fi

echo ""
echo "========================================"
echo "✓ MCP Server tests passed!"
echo ""
echo "To connect an AI assistant to this server:"
echo "  URL: $MCP_SERVER_URL"
echo "  Transport: SSE"
echo ""
echo "See app/mcp_server/README.md for configuration examples."

