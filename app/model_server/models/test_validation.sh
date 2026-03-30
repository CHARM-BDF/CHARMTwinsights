#!/bin/bash

# Test script for LinkML schema validation
# Tests valid and invalid inputs for all built-in models
# Tests model registration with API-provided JSON schema objects
# Returns exit code 0 if all tests pass, 1 if any fail
#
# Schema Format Coverage:
# - IrisModel: Uses JSON schema files (input_schema.json, output_schema.json)
# - CoxCOPDModel: Uses YAML schema files (input_schema.yaml, output_schema.yaml)
# - DPCGANSModel: Uses YAML schema files with permissive output
# - API Registration: Tests JSON schema objects in request body (not escaped strings)

APP_PORT=${APP_PORT:-8004}
BASE_URL="http://localhost:$APP_PORT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Helper function to run a test
run_test() {
    local test_name="$1"
    local expected_status="$2"
    local curl_data="$3"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    echo -e "${BLUE}Test $TOTAL_TESTS: $test_name${NC}"

    # Make request and capture both response and HTTP status
    response=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/predict" \
        -H "Content-Type: application/json" \
        -d "$curl_data")

    # Extract HTTP status code (last line)
    http_status=$(echo "$response" | tail -n 1)
    # Extract response body (everything except last line)
    response_body=$(echo "$response" | sed '$d')

    # Check if status matches expected
    if [ "$http_status" -eq "$expected_status" ]; then
        echo -e "${GREEN}✓ PASS${NC} - Got expected status $http_status"
        PASSED_TESTS=$((PASSED_TESTS + 1))

        # Show response for debugging (truncated if too long)
        if [ ${#response_body} -gt 200 ]; then
            echo "Response: ${response_body:0:200}..."
        else
            echo "Response: $response_body"
        fi
    else
        echo -e "${RED}✗ FAIL${NC} - Expected status $expected_status, got $http_status"
        echo "Response: $response_body"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi

    echo ""
}

# Print header
echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}LinkML Schema Validation Test Suite${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""

# ====================
# IrisModel Tests (JSON Schema Format)
# ====================
echo -e "${YELLOW}--- IrisModel Tests (JSON Schema) ---${NC}"
echo ""

run_test "IrisModel: Valid input" 200 '{
    "image": "irismodel:latest",
    "input": [{
        "sepal_length_cm": 5.1,
        "sepal_width_cm": 3.5,
        "petal_length_cm": 1.4,
        "petal_width_cm": 0.2
    }]
}'

run_test "IrisModel: Missing required field (petal_width_cm)" 400 '{
    "image": "irismodel:latest",
    "input": [{
        "sepal_length_cm": 5.1,
        "sepal_width_cm": 3.5,
        "petal_length_cm": 1.4
    }]
}'

run_test "IrisModel: Wrong data type (string instead of number)" 400 '{
    "image": "irismodel:latest",
    "input": [{
        "sepal_length_cm": "not_a_number",
        "sepal_width_cm": 3.5,
        "petal_length_cm": 1.4,
        "petal_width_cm": 0.2
    }]
}'

run_test "IrisModel: Multiple items, one invalid" 400 '{
    "image": "irismodel:latest",
    "input": [
        {
            "sepal_length_cm": 5.1,
            "sepal_width_cm": 3.5,
            "petal_length_cm": 1.4,
            "petal_width_cm": 0.2
        },
        {
            "sepal_length_cm": 7.0,
            "sepal_width_cm": 3.2,
            "petal_length_cm": 4.7
        }
    ]
}'

# ====================
# DPCGANSModel Tests
# ====================
echo -e "${YELLOW}--- DPCGANSModel Tests ---${NC}"
echo ""

run_test "DPCGANSModel: Valid input (permissive output schema)" 200 '{
    "image": "dpcgansmodel:latest",
    "input": [{
        "num_rows": 5,
        "max_retries": 50,
        "max_rows_multiplier": 10,
        "float_rtol": 0.01,
        "graceful_reject_sampling": true
    }]
}'

run_test "DPCGANSModel: Missing multiple required fields" 400 '{
    "image": "dpcgansmodel:latest",
    "input": [{
        "max_retries": 50
    }]
}'

run_test "DPCGANSModel: Wrong type for num_rows (string instead of integer)" 400 '{
    "image": "dpcgansmodel:latest",
    "input": [{
        "num_rows": "five",
        "max_retries": 50,
        "max_rows_multiplier": 10,
        "float_rtol": 0.01,
        "graceful_reject_sampling": true
    }]
}'

# ====================
# CoxCOPDModel Tests (YAML Schema Format with Enums)
# ====================
echo -e "${YELLOW}--- CoxCOPDModel Tests (YAML Schema with Enums) ---${NC}"
echo ""

run_test "CoxCOPDModel: Valid input with correct enum values" 200 '{
    "image": "coxcopdmodel:latest",
    "input": [{
        "ethnicity": "Not Hispanic or Latino",
        "sex_at_birth": "Female",
        "obesity": 0.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 0.0,
        "smoking_status": 1.0,
        "alcohol_use": 0.0,
        "bmi": 25.5,
        "age_at_time_0": 65
    }]
}'

run_test "CoxCOPDModel: Valid input with alternate enum values" 200 '{
    "image": "coxcopdmodel:latest",
    "input": [{
        "ethnicity": "Hispanic or Latino",
        "sex_at_birth": "Male",
        "obesity": 1.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 0.0,
        "smoking_status": 0.0,
        "alcohol_use": 1.0,
        "bmi": 28.0,
        "age_at_time_0": 55
    }]
}'

run_test "CoxCOPDModel: Invalid ethnicity enum value" 400 '{
    "image": "coxcopdmodel:latest",
    "input": [{
        "ethnicity": "Caucasian",
        "sex_at_birth": "Female",
        "obesity": 0.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 0.0,
        "smoking_status": 0.0,
        "alcohol_use": 0.0,
        "bmi": 25.0,
        "age_at_time_0": 50
    }]
}'

run_test "CoxCOPDModel: Invalid sex_at_birth enum value" 400 '{
    "image": "coxcopdmodel:latest",
    "input": [{
        "ethnicity": "Hispanic or Latino",
        "sex_at_birth": "Unknown",
        "obesity": 0.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 0.0,
        "smoking_status": 0.0,
        "alcohol_use": 0.0,
        "bmi": 25.0,
        "age_at_time_0": 50
    }]
}'

run_test "CoxCOPDModel: Both enum values invalid" 400 '{
    "image": "coxcopdmodel:latest",
    "input": [{
        "ethnicity": "White",
        "sex_at_birth": "M",
        "obesity": 0.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 0.0,
        "smoking_status": 0.0,
        "alcohol_use": 0.0,
        "bmi": 25.0,
        "age_at_time_0": 50
    }]
}'

run_test "CoxCOPDModel: Missing multiple required fields" 400 '{
    "image": "coxcopdmodel:latest",
    "input": [{
        "ethnicity": "Not Hispanic or Latino",
        "sex_at_birth": "Male"
    }]
}'

run_test "CoxCOPDModel: Extra unexpected fields" 400 '{
    "image": "coxcopdmodel:latest",
    "input": [{
        "ethnicity": "Not Hispanic or Latino",
        "sex_at_birth": "Male",
        "obesity": 0.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 0.0,
        "smoking_status": 1.0,
        "alcohol_use": 0.0,
        "bmi": 25.5,
        "age_at_time_0": 65,
        "extra_field": "should_not_be_here",
        "another_extra": 123
    }]
}'

# ====================
# API Registration Tests (JSON Schema Objects in Request Body)
# ====================
echo -e "${YELLOW}--- API Registration Tests (JSON Schema Objects) ---${NC}"
echo ""

# Helper function to run a registration test
run_registration_test() {
    local test_name="$1"
    local expected_status="$2"
    local curl_data="$3"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    echo -e "${BLUE}Test $TOTAL_TESTS: $test_name${NC}"

    # Make request and capture both response and HTTP status
    response=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/models" \
        -H "Content-Type: application/json" \
        -d "$curl_data")

    # Extract HTTP status code (last line)
    http_status=$(echo "$response" | tail -n 1)
    # Extract response body (everything except last line)
    response_body=$(echo "$response" | sed '$d')

    # Check if status matches expected
    if [ "$http_status" -eq "$expected_status" ]; then
        echo -e "${GREEN}✓ PASS${NC} - Got expected status $http_status"
        PASSED_TESTS=$((PASSED_TESTS + 1))

        # Show response for debugging (truncated if too long)
        if [ ${#response_body} -gt 200 ]; then
            echo "Response: ${response_body:0:200}..."
        else
            echo "Response: $response_body"
        fi
    else
        echo -e "${RED}✗ FAIL${NC} - Expected status $expected_status, got $http_status"
        echo "Response: $response_body"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi

    echo ""
}

# Test: Re-register IrisModel with API-provided JSON schemas (should succeed)
run_registration_test "Register with API JSON schema objects" 200 '{
    "image": "irismodel:latest",
    "title": "Iris Model (API Schema Test)",
    "short_description": "Test registration with API-provided JSON schema objects",
    "authors": "Test Suite",
    "examples": [{"sepal_length_cm": 5.1, "sepal_width_cm": 3.5, "petal_length_cm": 1.4, "petal_width_cm": 0.2}],
    "readme": "# Iris Model Test\nThis is a test registration with API-provided schemas.",
    "input_schema": {
        "id": "https://test/iris/input",
        "name": "iris_test_input",
        "prefixes": {"linkml": "https://w3id.org/linkml/"},
        "imports": ["linkml:types"],
        "default_range": "string",
        "classes": {
            "IrisTestInputItem": {
                "attributes": {
                    "sepal_length_cm": {"range": "float", "required": true},
                    "sepal_width_cm": {"range": "float", "required": true},
                    "petal_length_cm": {"range": "float", "required": true},
                    "petal_width_cm": {"range": "float", "required": true}
                }
            }
        }
    },
    "output_schema": {
        "id": "https://test/iris/output",
        "name": "iris_test_output",
        "prefixes": {"linkml": "https://w3id.org/linkml/"},
        "imports": ["linkml:types"],
        "default_range": "string",
        "classes": {
            "IrisTestOutputItem": {
                "attributes": {
                    "prediction": {"range": "integer", "required": true}
                }
            }
        }
    }
}'

# Test: Prediction using API-registered model (validates the schema was applied)
run_test "Predict with API-registered schemas" 200 '{
    "image": "irismodel:latest",
    "input": [{"sepal_length_cm": 5.1, "sepal_width_cm": 3.5, "petal_length_cm": 1.4, "petal_width_cm": 0.2}]
}'

# Test: Registration with invalid examples (should fail schema validation)
run_registration_test "Register with examples that fail schema validation" 400 '{
    "image": "irismodel:latest",
    "title": "Iris Model (Bad Examples)",
    "short_description": "This should fail because examples dont match schema",
    "authors": "Test Suite",
    "examples": [{"bad_field": "wrong"}],
    "readme": "# Test",
    "input_schema": {
        "id": "https://test/iris/input",
        "name": "iris_test_input",
        "prefixes": {"linkml": "https://w3id.org/linkml/"},
        "imports": ["linkml:types"],
        "default_range": "string",
        "classes": {
            "IrisTestInputItem": {
                "attributes": {
                    "sepal_length_cm": {"range": "float", "required": true},
                    "sepal_width_cm": {"range": "float", "required": true},
                    "petal_length_cm": {"range": "float", "required": true},
                    "petal_width_cm": {"range": "float", "required": true}
                }
            }
        }
    },
    "output_schema": {
        "id": "https://test/iris/output",
        "name": "iris_test_output",
        "prefixes": {"linkml": "https://w3id.org/linkml/"},
        "imports": ["linkml:types"],
        "default_range": "string",
        "classes": {
            "IrisTestOutputItem": {
                "attributes": {
                    "prediction": {"range": "integer", "required": true}
                }
            }
        }
    }
}'

# ====================
# ReachableFromModel Tests (reachable_from enum expansion)
# ====================
echo -e "${YELLOW}--- ReachableFromModel Tests (reachable_from) ---${NC}"
echo ""

run_test "ReachableFromModel: Valid ontology term" 200 '{
    "image": "reachablefrommodel:latest",
    "input": [{
        "biological_sex": "PATO:0000383",
        "age_years": 34
    }]
}'

run_test "ReachableFromModel: Unknown custom value" 200 '{
    "image": "reachablefrommodel:latest",
    "input": [{
        "biological_sex": "Unknown",
        "age_years": 34
    }]
}'

run_test "ReachableFromModel: Invalid ontology term" 400 '{
    "image": "reachablefrommodel:latest",
    "input": [{
        "biological_sex": "PATO:9999998",
        "age_years": 34
    }]
}'

# ====================
# Summary
# ====================
echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}Test Summary${NC}"
echo -e "${YELLOW}======================================${NC}"
echo "Total Tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"
echo ""

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi
