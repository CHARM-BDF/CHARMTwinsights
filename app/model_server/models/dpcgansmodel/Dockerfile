FROM python:3.11-slim

RUN pip install poetry

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-root

COPY dpcgan_model.pkl predict.py predict ./
COPY input_schema.yaml output_schema.yaml ./

RUN chmod +x predict

# No ENTRYPOINT or CMD, just use the script directly
