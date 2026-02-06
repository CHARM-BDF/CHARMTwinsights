#!/bin/bash

APP_PORT=${APP_PORT:-8000}

echo -e "\n\nPushing reachable-from demo model to the model server...\n"
curl -X POST "http://localhost:$APP_PORT/modeling/models" \
  -H "Content-Type: application/json" \
  -d '{
    "image": "reachablefrommodel:latest",
    "title": "Reachable From Demo Model",
    "short_description": "Demonstrates LinkML reachable_from enum expansion.",
    "authors": "CHARMTwinsight Team",
    "examples": [
      {
        "biological_sex": "PATO:0000383",
        "age_years": 34
      }
    ],
    "readme": "## ReachableFromModel\nThis demo model exercises LinkML reachable_from expansion during registration. The input schema uses the public PATO ontology at http://purl.obolibrary.org/obo/pato.obo."
  }'

echo -e "\n"
