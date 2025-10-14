#!/bin/bash

APP_PORT=${APP_PORT:-8000}
SYNTHEA_PORT=${SYNTHEA_PORT:-8003}
ROUTER_BASE_URL="http://localhost:$APP_PORT"
SYNTHEA_BASE_URL="http://localhost:$SYNTHEA_PORT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== CHARMTwinsights Synthea Server Test Suite ===${NC}"
echo "Router URL: $ROUTER_BASE_URL"
echo "Synthea URL: $SYNTHEA_BASE_URL"
echo

# Function to wait for server to be ready
wait_for_server() {
  local url="$1"
  local service_name="$2"
  echo -e "${YELLOW}Waiting for $service_name to be ready...${NC}"
  
  while true; do
    if curl -s "$url" > /dev/null 2>&1; then
      echo -e "${GREEN}✓ $service_name is ready${NC}"
      break
    else
      echo "  Waiting for $service_name..."
      sleep 2
    fi
  done
}

# Function to test API endpoint
test_api() {
  local method="$1"
  local url="$2"
  local data="$3"
  local test_name="$4"
  local expect_success="$5"
  
  echo -e "\n${BLUE}Testing: $test_name${NC}"
  
  if [ "$method" = "POST" ] && [ -n "$data" ]; then
    response=$(curl -s -w "\n%{http_code}" -X POST "$url" \
      -H "Content-Type: application/json" \
      -d "$data")
  else
    response=$(curl -s -w "\n%{http_code}" -X "$method" "$url")
  fi
  
  http_code=$(echo "$response" | tail -n1)
  body=$(echo "$response" | sed '$d')
  
  if [ "$expect_success" = "true" ]; then
    if [ "$http_code" -eq 200 ]; then
      echo -e "${GREEN}✓ PASS${NC} (HTTP $http_code)"
      if echo "$body" | grep -q "job_id"; then
        job_id=$(echo "$body" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
        echo "  Job ID: $job_id"
      fi
    else
      echo -e "${RED}✗ FAIL${NC} (HTTP $http_code)"
      echo "  Response: $body"
    fi
  else
    if [ "$http_code" -ge 400 ]; then
      echo -e "${GREEN}✓ PASS${NC} (Expected failure - HTTP $http_code)"
      if echo "$body" | grep -q "cohort_id"; then
        echo "  Validation message: $(echo "$body" | grep -o '"msg":"[^"]*"' | cut -d'"' -f4)"
      fi
    else
      echo -e "${RED}✗ FAIL${NC} (Expected failure but got HTTP $http_code)"
      echo "  Response: $body"
    fi
  fi
}

# Wait for services to be ready
wait_for_server "$SYNTHEA_BASE_URL" "Synthea Server"

echo -e "\n${BLUE}=== 1. FHIR Resource ID Validation Tests ===${NC}"

# Valid FHIR IDs (should succeed)
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 2, "num_years": 1, "cohort_id": "valid-cohort-123"}' \
  "Valid FHIR ID with hyphens and numbers" "true"

# Invalid FHIR IDs (should fail)
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 2, "num_years": 1, "cohort_id": "invalid_with_underscore"}' \
  "Invalid FHIR ID with underscore" "false"

echo -e "\n${BLUE}=== 2. Input Validation Tests ===${NC}"

# Zero patients/years (should fail)
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 0, "num_years": 1, "cohort_id": "test-zero-patients"}' \
  "Zero patients validation" "false"

test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 2, "num_years": 0, "cohort_id": "test-zero-years"}' \
  "Zero years validation" "false"

echo -e "\n${BLUE}=== 3. Geographic Options Tests ===${NC}"

# Specific state and city
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 2, "num_years": 1, "cohort_id": "california-test", "state": "California", "city": "Los Angeles"}' \
  "Specific state and city (California/Los Angeles)" "true"

# State only
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 2, "num_years": 1, "cohort_id": "texas-test", "state": "Texas"}' \
  "Specific state only (Texas)" "true"

# Population sampling (default behavior)
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 3, "num_years": 1, "cohort_id": "population-sampling", "use_population_sampling": true}' \
  "Population-weighted state sampling" "true"

echo -e "\n${BLUE}=== 4. Export Format Tests ===${NC}"

# FHIR export
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 2, "num_years": 1, "cohort_id": "fhir-export", "exporter": "fhir"}' \
  "FHIR export format" "true"

# CSV export  
test_api "POST" "$SYNTHEA_BASE_URL/synthetic-patients" \
  '{"num_patients": 2, "num_years": 1, "cohort_id": "csv-export", "exporter": "csv"}' \
  "CSV export format" "true"

echo -e "\n${BLUE}=== 6. Direct Download Endpoint Tests ===${NC}"

# Test direct download with valid ID
echo -e "\n${BLUE}Testing: Direct download with valid FHIR ID${NC}"
download_response=$(curl -s -w "\n%{http_code}" -X POST "$SYNTHEA_BASE_URL/generate-download-synthetic-patients" \
  -H "Content-Type: application/json" \
  -d '{"num_patients": 2, "num_years": 1, "cohort_id": "download-test", "exporter": "fhir"}' \
  --output /tmp/synthea_test.zip)

download_code=$(echo "$download_response" | tail -n1)
if [ "$download_code" -eq 200 ]; then
  echo -e "${GREEN}✓ PASS${NC} (HTTP $download_code)"
  if [ -f /tmp/synthea_test.zip ]; then
    file_size=$(ls -lh /tmp/synthea_test.zip | awk '{print $5}')
    echo "  Downloaded ZIP file size: $file_size"
    rm -f /tmp/synthea_test.zip
  fi
else
  echo -e "${RED}✗ FAIL${NC} (HTTP $download_code)"
fi

echo -e "\n${BLUE}=== 8. API Information Endpoints ===${NC}"

# Test demographics endpoints
test_api "GET" "$SYNTHEA_BASE_URL/demographics/states" "" "Available states list" "true"
test_api "GET" "$SYNTHEA_BASE_URL/demographics/cities/California" "" "Cities in California" "true"

# Test modules endpoint
test_api "GET" "$SYNTHEA_BASE_URL/modules" "" "Synthea modules list" "true"

echo -e "\n${GREEN}=== Test Suite Complete ===${NC}"
echo -e "${YELLOW}Note: Some tests create background jobs. Use the following to monitor:${NC}"
echo "  - List jobs: curl -s $SYNTHEA_BASE_URL/synthetic-patients/jobs"
echo "  - List patients: curl -s $SYNTHEA_BASE_URL/list-all-patients"
echo "  - List cohorts: curl -s $SYNTHEA_BASE_URL/list-all-cohorts"
echo

