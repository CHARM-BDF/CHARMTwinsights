FROM python:3.11-slim

WORKDIR /app

COPY predict.py predict input_schema.yaml output_schema.yaml examples.json README.md ./

RUN chmod +x predict
