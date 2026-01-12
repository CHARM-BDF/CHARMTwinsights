#!/bin/bash

APP_PORT=${APP_PORT:-8000}

echo -e "Registering Cox PH COPD model to the model server...\n"
curl -X POST http://localhost:$APP_PORT/modeling/models \
  -H "Content-Type: application/json" \
  -d '{
    "image": "coxcopdmodel:latest",
    "title": "Cox PH Model for COPD Prediction",
    "short_description": "A survival model to predict risk and survival probability for COPD based on demographics and comorbidities.",
    "authors": "Lakshmi Anandan",
    "examples": [
      {
        "ethnicity": "Not Hispanic or Latino",
        "sex_at_birth": "Female",
        "obesity": 0.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 0.0,
        "smoking_status": 0.0,
        "alcohol_use": 0.0,
        "bmi": 25.0,
        "age_at_time_0": 50.0
      },
      {
        "ethnicity": "Hispanic or Latino",
        "sex_at_birth": "Male",
        "obesity": 1.0,
        "diabetes": 1.0,
        "cardiovascular_disease": 1.0,
        "smoking_status": 1.0,
        "alcohol_use": 1.0,
        "bmi": 32.0,
        "age_at_time_0": 65.0
      }
    ],
    "readme": "## CoxCOPDModel\\nA Cox Proportional Hazards model for predicting COPD risk and survival probability based on patient demographics and comorbidities."
  }'
