# CHARMTwinsights Model Development Guide

This guide helps you create and deploy machine learning models for CHARMTwinsights without needing to be a Docker expert.

## Quick Start

1. **Choose your language**: Copy the appropriate template
   - Python: `cp -r python-model my-model`
   - R: `cp -r r-model my-model`

2. **Develop your model**: Replace the template code with your model

3. **Validate**: Run `python validate-dockerfile.py my-model/Dockerfile`

4. **Build**: `docker build -t my-model:latest my-model/`

5. **Register**: Use the API to register your model

## Template Overview

### Files You Need

```
my-model/
├── Dockerfile          # Container definition (minimal editing needed)
├── predict.py/.R       # Your prediction code (EDIT THIS)
├── predict             # Execution script (don't modify)
├── README.md           # Model documentation (EDIT THIS)
├── examples.json       # Test examples (EDIT THIS)
├── input_schema.yaml   # Input validation schema (EDIT THIS) - or .json
├── output_schema.yaml  # Output validation schema (EDIT THIS) - or .json
├── pyproject.toml      # Python deps (EDIT THIS)
└── DESCRIPTION         # R deps (EDIT THIS)
```

**Note:** Schema files can be YAML (`.yaml`) or JSON (`.json`) format.

### Critical Rules

**NEVER add these to your Dockerfile:**
- `CMD` or `ENTRYPOINT` directives
- These break integration with the model server

**Always include:**
- A working `/predict` script
- `README.md` with model documentation
- `examples.json` with valid test data
- `input_schema.yaml` or `input_schema.json` defining input structure
- `output_schema.yaml` or `output_schema.json` defining output structure

## Python Models

### 1. Copy Template
```bash
cp -r model-templates/python-model my-python-model
cd my-python-model
```

### 2. Edit Dependencies
Update `pyproject.toml` with your required packages:
```toml
[tool.poetry.dependencies]
python = "^3.11"
scikit-learn = "^1.3.0"
torch = "^2.0.0"  # Add your packages here
```

### 3. Implement Your Model
Edit `predict.py`:

```python
def load_model():
    # Load your trained model
    return joblib.load('my_model.pkl')

def preprocess_input(input_data):
    # Your preprocessing logic
    return processed_data

def postprocess_output(predictions, input_data):
    # Format your outputs
    return results
```

### 4. Update Metadata
- Edit `README.md` with your model description
- Edit `examples.json` with valid test inputs
- Copy your model files (`.pkl`, `.joblib`, etc.) and update Dockerfile

### 5. Test and Deploy
```bash
# Validate Dockerfile
python ../validate-dockerfile.py Dockerfile

# Build image
docker build -t my-python-model:latest .

# Register with CHARMTwinsights
curl -X POST http://localhost:8000/modeling/models \
  -H "Content-Type: application/json" \
  -d '{
    "image": "my-python-model:latest",
    "title": "My Python Model",
    "short_description": "Description of what it does",
    "authors": "Your Name"
  }'
```

## R Models

### 1. Copy Template
```bash
cp -r model-templates/r-model my-r-model
cd my-r-model
```

### 2. Edit Dependencies
Update `DESCRIPTION` with your required R packages:
```
Imports: 
    jsonlite,
    readr,
    randomForest,
    caret
```

### 3. Implement Your Model
Edit `predict.R`:

```r
load_model <- function() {
  # Load your trained model
  return(readRDS("my_model.rds"))
}

preprocess_input <- function(input_data) {
  # Your preprocessing logic
  return(processed_data)
}

postprocess_output <- function(predictions, input_data) {
  # Format your outputs
  return(results)
}
```

### 4. Update Metadata
- Edit `README.md` with your model description
- Edit `examples.json` with valid test inputs  
- Copy your model files (`.rds`, `.RData`, etc.) and update Dockerfile

### 5. Test and Deploy
```bash
# Validate Dockerfile
python ../validate-dockerfile.py Dockerfile

# Build image
docker build -t my-r-model:latest .

# Register with CHARMTwinsights
curl -X POST http://localhost:8000/modeling/models \
  -H "Content-Type: application/json" \
  -d '{
    "image": "my-r-model:latest",
    "title": "My R Model", 
    "short_description": "Description of what it does",
    "authors": "Your Name"
  }'
```

## API Registration

### Container-Based Metadata (Recommended)
If your model includes `README.md` and `examples.json`, you only need:

```json
{
  "image": "my-model:latest",
  "title": "Model Title",
  "short_description": "Brief description", 
  "authors": "Your Name"
}
```

The README and examples will be extracted from the container automatically.

### Full API Metadata
You can also provide everything via API:

```json
{
  "image": "my-model:latest",
  "title": "Model Title",
  "short_description": "Brief description",
  "authors": "Your Name",
  "examples": [{"feature1": 1.0, "feature2": "value"}],
  "readme": "# My Model\nDescription..."
}
```

API-provided metadata always overrides container metadata.

## Input/Output Format

### Input Format
Your model receives JSON data:
```json
[
  {"feature1": 1.0, "feature2": "category_a", "id": "patient_1"},
  {"feature1": 2.0, "feature2": "category_b", "id": "patient_2"}
]
```

### Output Format
Your model should return JSON results:
```json
[
  {"prediction": 0.85, "confidence": 0.92, "id": "patient_1"},
  {"prediction": 0.73, "confidence": 0.88, "id": "patient_2"}
]
```

## Input/Output Validation with LinkML

CHARMTwinsights uses **LinkML schemas** to validate model inputs and outputs. Schemas are **required** for all models and ensure data quality at three critical points:

1. **Registration**: Examples are validated against your input schema
2. **Pre-execution**: Inputs are validated before running your model
3. **Post-execution**: Outputs are validated to ensure contract compliance

### What is LinkML?

[LinkML](https://linkml.io/) (Linked Data Modeling Language) is a schema language for describing data structures. It's similar to JSON Schema but simpler and more readable.

### Schema Format Support

Schemas can be provided in **YAML** or **JSON** format:

| Context | Supported Formats |
|---------|-------------------|
| **Container files** | `.yaml` or `.json` (either works) |
| **API registration** | JSON object in request body |

YAML is often preferred for hand-written schemas (more readable), while JSON objects are standard for REST API payloads.

### Required Schema Files

Your model must include two schema files:

```
my-model/
├── input_schema.yaml   # Defines expected input structure (REQUIRED)
├── output_schema.yaml  # Defines expected output structure (REQUIRED)
├── predict.py          # Your model code
├── ...
```

Or use JSON format:

```
my-model/
├── input_schema.json   # Defines expected input structure (REQUIRED)
├── output_schema.json  # Defines expected output structure (REQUIRED)
├── predict.py          # Your model code
├── ...
```

### Input Schema Example

Create `input_schema.yaml` to define **one input item** (not the list):

```yaml
id: https://w3id.org/charmtwinsights/my-model/input
name: my_model_input
description: Input schema for my model

prefixes:
  linkml: https://w3id.org/linkml/

imports:
  - linkml:types

default_range: string

classes:
  InputItem:
    description: A single input record for prediction
    attributes:
      age:
        description: Patient age in years
        range: float
        required: true

      bmi:
        description: Body mass index
        range: float
        required: true

      smoking_status:
        description: Current smoking status
        range: string
        required: true

      id:
        description: Optional identifier for tracking
        range: string
        required: false
```

### Output Schema Example

Create `output_schema.yaml` to define **one output item**:

```yaml
id: https://w3id.org/charmtwinsights/my-model/output
name: my_model_output
description: Output schema for my model

prefixes:
  linkml: https://w3id.org/linkml/

imports:
  - linkml:types

default_range: string

classes:
  OutputItem:
    description: A single prediction result
    attributes:
      prediction:
        description: Primary prediction value
        range: float
        required: true

      confidence:
        description: Confidence score (0.0 to 1.0)
        range: float
        required: false

      id:
        description: Identifier from input (if provided)
        range: string
        required: false
```

### Common Data Types

| Type | Description | Example |
|------|-------------|---------|
| `string` | Text values | `"category_a"` |
| `integer` | Whole numbers | `42` |
| `float` | Decimal numbers | `3.14` |
| `boolean` | True/false | `true` |

### Key Attributes

- **`required: true`** - Field must be present
- **`required: false`** - Field is optional (default)
- **`description`** - Human-readable explanation
- **`range`** - Data type for the field

### Defining Categorical/Enum Fields

For categorical fields with a fixed set of valid values, use LinkML enums:

#### Approach 1: Separate Enum Definitions

Define enums at the schema level and reference them in attributes:

```yaml
enums:
  SexAtBirthEnum:
    description: Biological sex at birth
    permissible_values:
      MALE:
        description: Male
      FEMALE:
        description: Female
      OTHER:
        description: Other
      UNKNOWN:
        description: Unknown or not disclosed

classes:
  InputItem:
    attributes:
      sex_at_birth:
        description: Biological sex at birth
        range: SexAtBirthEnum  # Reference the enum
        required: true
```

#### Approach 2: Inline permissible_values

For single-use categorical fields:

```yaml
classes:
  InputItem:
    attributes:
      risk_level:
        description: Risk assessment category
        permissible_values:
          LOW:
            description: Low risk (score 0-33)
          MODERATE:
            description: Moderate risk (score 34-66)
          HIGH:
            description: High risk (score 67-100)
        required: true
```

### Updating Your Dockerfile

Add your schema files to the Dockerfile:

```dockerfile
# Copy schema files (REQUIRED) - use .yaml or .json
COPY input_schema.yaml output_schema.yaml ./

# Or if using JSON format:
# COPY input_schema.json output_schema.json ./
```

### Providing Schemas via API

Alternatively, you can provide schemas as JSON objects in the registration API request:

```json
{
  "image": "my-model:latest",
  "title": "My Model",
  "short_description": "Description",
  "authors": "Your Name",
  "input_schema": {
    "id": "https://w3id.org/my-model/input",
    "name": "my_input",
    "prefixes": {"linkml": "https://w3id.org/linkml/"},
    "imports": ["linkml:types"],
    "default_range": "string",
    "classes": {
      "InputItem": {
        "attributes": {
          "feature1": {"range": "float", "required": true},
          "feature2": {"range": "string", "required": true}
        }
      }
    }
  },
  "output_schema": {
    "id": "https://w3id.org/my-model/output",
    "name": "my_output",
    "prefixes": {"linkml": "https://w3id.org/linkml/"},
    "imports": ["linkml:types"],
    "default_range": "string",
    "classes": {
      "OutputItem": {
        "attributes": {
          "prediction": {"range": "float", "required": true}
        }
      }
    }
  }
}
```

API-provided schemas override container schemas if both are present.

**Note:** API schemas are proper JSON objects (not escaped strings). Container schemas can be YAML or JSON files.

### More Information

- [LinkML Documentation](https://linkml.io/linkml/)
- [LinkML Schema Reference](https://linkml.io/linkml/schemas/index.html)
- See built-in models for complete examples: `app/model_server/models/`

## Troubleshooting

### Build Errors
1. **Python package conflicts**: Check `pyproject.toml` versions
2. **R package missing**: Add to `DESCRIPTION` file
3. **File not found**: Check COPY paths in Dockerfile

### Registration Errors
1. **"Examples required"**: Add `examples.json` or provide via API
2. **"README required"**: Add `README.md` or provide via API
3. **"Image not found"**: Build your Docker image first
4. **"Input schema is required"**: Add `input_schema.yaml` or `input_schema.json` to container, or provide JSON via API
5. **"Output schema is required"**: Add `output_schema.yaml` or `output_schema.json` to container, or provide JSON via API
6. **"Examples validation failed"**: Your `examples.json` doesn't match your input schema

### Runtime Errors
1. **Permission denied**: Make sure `predict` script is executable
2. **Module not found**: Check your dependencies are installed
3. **File paths**: Use relative paths from `/app` directory
4. **"Input validation failed"**: Prediction request data doesn't match your input schema
5. **"Output validation failed"**: Your model's output doesn't match your output schema

### Common Dockerfile Mistakes
```dockerfile
# ❌ WRONG - breaks model server integration
CMD ["python", "predict.py"]
ENTRYPOINT ["./predict"]

# ✅ CORRECT - let model server handle execution
# (no CMD/ENTRYPOINT)
```

Use the validator to catch these:
```bash
python validate-dockerfile.py Dockerfile
```

## Advanced Topics

### Custom Base Images
You can use custom base images, but ensure they:
- Have Python 3.11+ or R 4.0+
- Include basic JSON parsing libraries
- Don't set CMD/ENTRYPOINT

### Model Versioning
Use image tags for model versions:
```bash
docker build -t my-model:v1.0 .
docker build -t my-model:v1.1 .
```

### Large Models
For large model files:
1. Use `.dockerignore` to exclude unnecessary files
2. Consider model registries for sharing artifacts
3. Use multi-stage builds if needed

### GPU Models
For GPU-enabled models:
1. Use appropriate base images (`nvidia/cuda`)
2. Install GPU-specific libraries
3. Test with GPU-enabled Docker runtime

## Examples

See the `examples/` directory for complete working examples:
- `examples/simple-classifier/` - Basic scikit-learn model
- `examples/deep-learning/` - PyTorch model example
- `examples/r-regression/` - R linear model example

## Reachable-From (Ontology-backed enums)

There is a minimal example that demonstrates `reachable_from` enum expansion using the public PATO ontology:

- `model-templates/examples/reachable-from-demo/`

**What this enables**
- Schema authors can restrict enum values to terms reachable from a root ontology term.
- The model server expands `reachable_from` at **registration time** and stores the expanded schema.
- Predictions validate against the expanded schema, so runtime calls are fast and deterministic.

**Important behavior**
- Expansion happens when a model is registered. If the ontology changes later, you must re-register the model to pick up changes.
- Ontologies are downloaded on demand at registration time (not during prediction) and are not retained after expansion.
- Prefer `.obo` URLs for smaller downloads; other formats may be significantly larger.

This model uses a LinkML schema with:

```
reachable_from:
  source_ontology: "http://purl.obolibrary.org/obo/pato.obo"
  source_nodes:
    - "PATO:0000047"
  include_self: true
```

It is intended as a reference for how to structure schemas that need to enforce ontology-backed enums.

**Allowing values outside the ontology**
If you need to allow values not in the ontology (e.g., missing/unknown), add explicit `permissible_values` entries.
In order to be LinkML conformant, the keys for permissible values must match the `text:` entries exactly:

```
prefixes:
  CHARM: https://example.org/charm/

permissible_values:
  Unknown:
    text: Unknown
    meaning: CHARM:Unknown
    description: Unknown/missing value
```

**Supported enum features (current model server behavior)**
- `reachable_from` on an enum
- `include` / `minus` entries that contain `reachable_from`
- `include` / `minus` entries that contain `permissible_values`

Other advanced enum features in LinkML may parse, but only the above are actively expanded by the model server.
