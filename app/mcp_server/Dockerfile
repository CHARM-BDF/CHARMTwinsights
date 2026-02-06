FROM python:3.11-slim

WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Install dependencies (no dev dependencies, no virtual env, no root package)
RUN poetry config virtualenvs.create false && \
    poetry install --only main --no-root --no-interaction --no-ansi

# Copy application code
COPY mcp_server ./mcp_server

# Expose the default MCP HTTP port
EXPOSE 8000

# Run the MCP server
CMD ["python", "-m", "mcp_server.main"]

