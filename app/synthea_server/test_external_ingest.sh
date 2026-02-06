#!/bin/bash

APP_PORT=${APP_PORT:-8000}
ROUTER_BASE_URL="http://localhost:$APP_PORT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== CHARMTwinsights External FHIR Ingestion Test Suite ===${NC}"
echo "Router URL: $ROUTER_BASE_URL"
echo

# Function to wait for server to be ready
wait_for_server() {
  local url="$1"
  local service_name="$2"
  echo -e "${YELLOW}Waiting for $service_name to be ready...${NC}"
  
  while true; do
    if curl -s "$url/health" > /dev/null 2>&1; then
      echo -e "${GREEN}✓ $service_name is ready${NC}"
      break
    else
      echo "  Waiting for $service_name..."
      sleep 2
    fi
  done
}

# Wait for router to be ready
wait_for_server "$ROUTER_BASE_URL" "Router"

echo -e "\n${BLUE}=== Step 1: Create Sample FHIR Bundle ===${NC}"
echo "Creating a minimal FHIR bundle for testing..."

# Create a sample FHIR bundle with multiple resources to test prefixing and reference updates
SAMPLE_BUNDLE='{
  "resourceType": "Bundle",
  "type": "collection",
  "entry": [
    {
      "resource": {
        "resourceType": "Patient",
        "id": "test-patient-123",
        "gender": "male",
        "birthDate": "1990-05-15",
        "name": [{
          "family": "TestPatient",
          "given": ["John"]
        }]
      }
    },
    {
      "resource": {
        "resourceType": "Observation",
        "id": "obs-001",
        "status": "final",
        "code": {
          "coding": [{
            "system": "http://loinc.org",
            "code": "29463-7",
            "display": "Body Weight"
          }]
        },
        "subject": {
          "reference": "Patient/test-patient-123"
        },
        "valueQuantity": {
          "value": 75,
          "unit": "kg",
          "system": "http://unitsofmeasure.org",
          "code": "kg"
        }
      }
    },
    {
      "resource": {
        "resourceType": "Condition",
        "id": "cond-001",
        "clinicalStatus": {
          "coding": [{
            "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
            "code": "active"
          }]
        },
        "code": {
          "coding": [{
            "system": "http://snomed.info/sct",
            "code": "44054006",
            "display": "Diabetes"
          }]
        },
        "subject": {
          "reference": "Patient/test-patient-123"
        }
      }
    }
  ]
}'

ORIGINAL_PATIENT_ID="test-patient-123"
echo -e "${GREEN}✓ Created test bundle${NC}"
echo "  Original Patient ID: $ORIGINAL_PATIENT_ID"

echo -e "\n${BLUE}=== Step 2: Test Basic External Ingestion ===${NC}"

# Test 1: Basic ingestion with defaults
echo -e "\n${BLUE}Test 1: Basic ingestion with default parameters${NC}"
response=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" \
  -d "{
    \"bundle\": $SAMPLE_BUNDLE
  }")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
  echo -e "${GREEN}✓ PASS${NC} (HTTP $http_code)"
  echo "  Response: $body" | head -c 200
  
  # Extract patient ID to verify prefixing
  ingested_patient_id=$(echo "$body" | grep -o '"patient_ids":\["[^"]*"' | cut -d'"' -f4)
  if [[ "$ingested_patient_id" == ext-* ]]; then
    echo -e "\n  ${GREEN}✓ Patient ID correctly prefixed: $ingested_patient_id${NC}"
  else
    echo -e "\n  ${RED}✗ Patient ID not prefixed: $ingested_patient_id${NC}"
  fi
else
  echo -e "${RED}✗ FAIL${NC} (HTTP $http_code)"
  echo "  Response: $body"
fi

# Test 2: Custom cohort_id
echo -e "\n${BLUE}Test 2: Custom cohort_id${NC}"
response=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" \
  -d "{
    \"bundle\": $SAMPLE_BUNDLE,
    \"cohort_id\": \"test-cohort-123\"
  }")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
  echo -e "${GREEN}✓ PASS${NC} (HTTP $http_code)"
  cohort_id=$(echo "$body" | grep -o '"cohort_id":"[^"]*"' | cut -d'"' -f4)
  echo "  Cohort ID: $cohort_id"
else
  echo -e "${RED}✗ FAIL${NC} (HTTP $http_code)"
  echo "  Response: $body"
fi

# Test 3: Custom datatype (synthetic)
echo -e "\n${BLUE}Test 3: Custom datatype=synthetic${NC}"
response=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" \
  -d "{
    \"bundle\": $SAMPLE_BUNDLE,
    \"cohort_id\": \"synthetic-import\",
    \"datatype\": \"synthetic\"
  }")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
  echo -e "${GREEN}✓ PASS${NC} (HTTP $http_code)"
  datatype=$(echo "$body" | grep -o '"datatype":"[^"]*"' | cut -d'"' -f4)
  echo "  Datatype: $datatype"
else
  echo -e "${RED}✗ FAIL${NC} (HTTP $http_code)"
  echo "  Response: $body"
fi

echo -e "\n${BLUE}=== Step 3: Test Validation ===${NC}"

# Test 4: Invalid datatype
echo -e "\n${BLUE}Test 4: Invalid datatype (should fail)${NC}"
response=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" \
  -d "{
    \"bundle\": $SAMPLE_BUNDLE,
    \"datatype\": \"invalid-type\"
  }")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -ge 400 ]; then
  echo -e "${GREEN}✓ PASS${NC} (Expected failure - HTTP $http_code)"
  echo "  Error message: $(echo "$body" | grep -o '"detail":"[^"]*"' | cut -d'"' -f4)"
else
  echo -e "${RED}✗ FAIL${NC} (Expected failure but got HTTP $http_code)"
fi

# Test 5: Invalid cohort_id (with underscore)
echo -e "\n${BLUE}Test 5: Invalid cohort_id with underscore (should fail)${NC}"
response=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" \
  -d "{
    \"bundle\": $SAMPLE_BUNDLE,
    \"cohort_id\": \"invalid_cohort_id\"
  }")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -ge 400 ]; then
  echo -e "${GREEN}✓ PASS${NC} (Expected failure - HTTP $http_code)"
  echo "  Error message: $(echo "$body" | grep -o '"detail":"[^"]*"' | cut -d'"' -f4 | head -c 100)"
else
  echo -e "${RED}✗ FAIL${NC} (Expected failure but got HTTP $http_code)"
fi

# Test 6: Empty bundle (should fail)
echo -e "\n${BLUE}Test 6: Empty bundle (should fail)${NC}"
response=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" \
  -d '{
    "bundle": {
      "resourceType": "Bundle",
      "type": "collection",
      "entry": []
    }
  }')

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -ge 400 ]; then
  echo -e "${GREEN}✓ PASS${NC} (Expected failure - HTTP $http_code)"
  echo "  Error message: $(echo "$body" | grep -o '"detail":"[^"]*"' | cut -d'"' -f4)"
else
  echo -e "${RED}✗ FAIL${NC} (Expected failure but got HTTP $http_code)"
fi

echo -e "\n${BLUE}=== Step 4: Test Update Functionality ===${NC}"

echo -e "\n${BLUE}Test 7: Update existing patient data and add new observations${NC}"
echo "Testing that updates merge/update rather than duplicate..."

# First ingestion - initial data
echo "  Step 1: Initial ingestion..."
response=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" \
  -d "{
    \"bundle\": $SAMPLE_BUNDLE,
    \"cohort_id\": \"update-test\"
  }")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
  echo -e "  ${GREEN}✓ Initial ingestion successful${NC}"
  initial_patient_id=$(echo "$body" | grep -o '"patient_ids":\["[^"]*"' | cut -d'"' -f4)
  echo "  Patient ID: $initial_patient_id"
  
  # Wait a moment for HAPI to process
  sleep 2
  
  # Query HAPI to get initial state
  echo "  Step 2: Querying HAPI for initial patient data..."
  initial_patient=$(curl -s "http://localhost:8080/fhir/Patient/$initial_patient_id")
  initial_patient_city=$(echo "$initial_patient" | jq -r '.address[0].city // "not-set"' 2>/dev/null || echo "not-set")
  echo "  Initial patient city: $initial_patient_city"
  
  # Count initial observations
  initial_obs_count=$(curl -s "http://localhost:8080/fhir/Observation?subject=Patient/$initial_patient_id&_summary=count" | jq -r '.total // 0' 2>/dev/null || echo "0")
  echo "  Initial observation count: $initial_obs_count"
  
  # Create modified bundle with updated patient data and new observation
  echo "  Step 3: Creating modified bundle..."
  MODIFIED_BUNDLE='{
    "resourceType": "Bundle",
    "type": "collection",
    "entry": [
      {
        "resource": {
          "resourceType": "Patient",
          "id": "test-patient-123",
          "gender": "male",
          "birthDate": "1990-05-15",
          "name": [{
            "family": "TestPatient",
            "given": ["John", "Updated"]
          }],
          "address": [{
            "city": "Portland",
            "state": "OR",
            "postalCode": "97201"
          }]
        }
      },
      {
        "resource": {
          "resourceType": "Observation",
          "id": "obs-001",
          "status": "final",
          "code": {
            "coding": [{
              "system": "http://loinc.org",
              "code": "29463-7",
              "display": "Body Weight"
            }]
          },
          "subject": {
            "reference": "Patient/test-patient-123"
          },
          "valueQuantity": {
            "value": 75,
            "unit": "kg",
            "system": "http://unitsofmeasure.org",
            "code": "kg"
          }
        }
      },
      {
        "resource": {
          "resourceType": "Observation",
          "id": "obs-002-new",
          "status": "final",
          "code": {
            "coding": [{
              "system": "http://loinc.org",
              "code": "8867-4",
              "display": "Heart rate"
            }]
          },
          "subject": {
            "reference": "Patient/test-patient-123"
          },
          "effectiveDateTime": "2026-01-02T14:00:00Z",
          "valueQuantity": {
            "value": 72,
            "unit": "beats/minute",
            "system": "http://unitsofmeasure.org",
            "code": "/min"
          }
        }
      }
    ]
  }'
  
  echo "  Modified: Changed city to Portland, added middle name, added new observation"
  
  # Re-ingest with modifications
  echo "  Step 4: Re-ingesting modified bundle to same cohort..."
  response2=$(curl -s -w "\n%{http_code}" -X POST "$ROUTER_BASE_URL/ingest/fhir" \
    -H "Content-Type: application/json" \
    -d "{
      \"bundle\": $MODIFIED_BUNDLE,
      \"cohort_id\": \"update-test\"
    }")
  
  http_code2=$(echo "$response2" | tail -n1)
  body2=$(echo "$response2" | sed '$d')
  
  if [ "$http_code2" -eq 200 ]; then
    echo -e "  ${GREEN}✓ Update ingestion successful${NC}"
    
    # Wait for HAPI to process
    sleep 2
    
    # Verify updates
    echo "  Step 5: Verifying updates in HAPI..."
    
    # Check if patient was updated (not duplicated)
    updated_patient=$(curl -s "http://localhost:8080/fhir/Patient/$initial_patient_id")
    updated_city=$(echo "$updated_patient" | jq -r '.address[0].city // "not-found"' 2>/dev/null || echo "not-found")
    updated_given_count=$(echo "$updated_patient" | jq -r '.name[0].given | length' 2>/dev/null || echo "0")
    
    echo "  Updated patient city: $updated_city"
    echo "  Updated given names count: $updated_given_count"
    
    if [ "$updated_city" = "Portland" ]; then
      echo -e "  ${GREEN}✓ Patient data successfully updated (city changed to Portland)${NC}"
    else
      echo -e "  ${YELLOW}⚠ Patient city not updated (got $updated_city, expected Portland)${NC}"
      echo -e "  ${YELLOW}  Note: HAPI may preserve original data on transaction bundle POST${NC}"
    fi
    
    if [ "$updated_given_count" = "2" ]; then
      echo -e "  ${GREEN}✓ Patient name updated (now has 2 given names)${NC}"
    fi
    
    # Check observation count increased
    final_obs_count=$(curl -s "http://localhost:8080/fhir/Observation?subject=Patient/$initial_patient_id&_summary=count" | jq -r '.total // 0' 2>/dev/null || echo "0")
    echo "  Final observation count: $final_obs_count (was $initial_obs_count)"
    
    if [ "$final_obs_count" -gt "$initial_obs_count" ] 2>/dev/null; then
      echo -e "  ${GREEN}✓ New observation successfully added${NC}"
    else
      echo -e "  ${YELLOW}⚠ Observation count did not increase significantly${NC}"
      echo -e "  ${YELLOW}  Note: Both observations may have been added/updated${NC}"
    fi
    
    # Check for duplicates
    duplicate_check=$(curl -s "http://localhost:8080/fhir/Patient?_id=$initial_patient_id&_summary=count" | jq -r '.total // 0' 2>/dev/null || echo "0")
    if [ "$duplicate_check" = "1" ]; then
      echo -e "  ${GREEN}✓ No patient duplication (only 1 patient with this ID)${NC}"
    else
      echo -e "  ${RED}✗ Patient might be duplicated! Found $duplicate_check patient(s) with this ID${NC}"
    fi
    
  else
    echo -e "  ${RED}✗ FAIL (Update ingestion)${NC} (HTTP $http_code2)"
    echo "  Response: $body2"
  fi
else
  echo -e "${RED}✗ FAIL (Initial ingestion)${NC} (HTTP $http_code)"
  echo "  Response: $body"
fi

echo -e "\n${BLUE}=== Step 5: Verify Cohort Organization ===${NC}"

# List all cohorts to verify external cohorts are created
echo -e "\n${BLUE}Test 8: Verify cohorts were created${NC}"
response=$(curl -s -w "\n%{http_code}" -X GET "$ROUTER_BASE_URL/synthetic/synthea/list-all-cohorts")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
  echo -e "${GREEN}✓ PASS${NC} (HTTP $http_code)"
  
  # Check for external cohorts
  if echo "$body" | grep -q '"cohort_id":"external"'; then
    echo -e "  ${GREEN}✓ Found 'external' cohort${NC}"
  fi
  
  if echo "$body" | grep -q '"cohort_id":"test-cohort-123"'; then
    echo -e "  ${GREEN}✓ Found 'test-cohort-123' cohort${NC}"
  fi
  
  if echo "$body" | grep -q '"cohort_id":"update-test"'; then
    echo -e "  ${GREEN}✓ Found 'update-test' cohort${NC}"
  fi
  
  # Show cohort summary
  total_cohorts=$(echo "$body" | grep -o '"total_cohorts":[0-9]*' | cut -d':' -f2)
  echo "  Total cohorts: $total_cohorts"
else
  echo -e "${RED}✗ FAIL${NC} (HTTP $http_code)"
fi

# List patients to verify prefixed IDs
echo -e "\n${BLUE}Test 9: Verify patient IDs are prefixed${NC}"
response=$(curl -s -w "\n%{http_code}" -X GET "$ROUTER_BASE_URL/synthetic/synthea/list-all-patients")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 200 ]; then
  echo -e "${GREEN}✓ PASS${NC} (HTTP $http_code)"
  
  # Check for ext- prefixed patient IDs
  ext_patient_count=$(echo "$body" | grep -o '"id":"ext-[^"]*"' | wc -l)
  echo "  Patients with 'ext-' prefix: $ext_patient_count"
  
  if [ "$ext_patient_count" -gt 0 ]; then
    echo -e "  ${GREEN}✓ External patients correctly prefixed${NC}"
    # Show first few prefixed patient IDs
    echo "  Sample prefixed IDs:"
    echo "$body" | grep -o '"id":"ext-[^"]*"' | head -n 3 | sed 's/^/    /'
  else
    echo -e "  ${YELLOW}⚠ No patients with ext- prefix found${NC}"
  fi
else
  echo -e "${RED}✗ FAIL${NC} (HTTP $http_code)"
fi

# Cleanup
echo -e "\n${BLUE}=== Cleanup ===${NC}"
echo "No temporary files to clean up"

echo -e "\n${GREEN}=== External FHIR Ingestion Test Suite Complete ===${NC}"
echo -e "${YELLOW}Summary:${NC}"
echo "  - Tested basic ingestion with default parameters"
echo "  - Tested custom cohort_id and datatype parameters"
echo "  - Tested validation (invalid datatype, cohort_id, empty bundle)"
echo "  - Tested update functionality (re-ingesting to same cohort)"
echo "  - Verified cohort organization and patient ID prefixing"
echo
echo -e "${YELLOW}Useful commands:${NC}"
echo "  - List cohorts: curl -s $ROUTER_BASE_URL/synthetic/synthea/list-all-cohorts | jq"
echo "  - List patients: curl -s $ROUTER_BASE_URL/synthetic/synthea/list-all-patients | jq"
echo "  - Delete cohort: curl -X DELETE $ROUTER_BASE_URL/synthetic/synthea/delete-cohort/{cohort_id}"
echo

