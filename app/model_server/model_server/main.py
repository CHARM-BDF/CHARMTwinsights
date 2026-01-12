import json
import shutil
import uuid
import os
import logging
import time
import glob
from pathlib import Path
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
import docker
from pymongo import MongoClient
from typing import List, Any, Optional, Dict
from model_server.validation import ValidationError, validate_items, extract_target_class, normalize_schema_to_string, parse_schema

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def schema_to_response_format(schema: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Convert a stored schema string to a dict for API responses.

    Schemas are stored as strings in MongoDB (either from container YAML/JSON files
    or from API-provided dicts that were normalized to JSON strings).
    For API responses, we parse them back to dicts so they appear as proper JSON objects.

    Args:
        schema: Schema string from MongoDB, or None

    Returns:
        Parsed schema dict, or None if input was None
    """
    if schema is None:
        return None
    try:
        return parse_schema(schema)
    except Exception as e:
        logger.warning(f"Failed to parse schema for response: {e}")
        # Return the raw string if parsing fails (shouldn't happen with valid schemas)
        return {"_raw": schema, "_parse_error": str(e)}


app = FastAPI()
client = docker.from_env()

# === MongoDB connection ===
MONGO_HOST = os.environ.get("MODEL_SERVER_MONGO_HOST", "model_server_db")
MONGO_PORT = int(os.environ.get("MODEL_SERVER_MONGO_PORT", 27017))
MONGO_DB = os.environ.get("MODEL_SERVER_MONGO_DB", "modeldb")

mongo_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = mongo_client[MONGO_DB]
models_collection = db.models

# Path to built-in model metadata
BUILTIN_MODELS_PATH = os.environ.get("BUILTIN_MODELS_PATH", "/app/builtin_models")

class RegisterRequest(BaseModel):
    image: str  # e.g., "irismodel:1.0.0"
    title: str
    short_description: str
    authors: str
    examples: Optional[List[Any]] = None  # Optional - can be extracted from container
    readme: Optional[str] = None  # Optional - can be extracted from container
    input_schema: Optional[Dict[str, Any]] = None  # Optional - LinkML schema as JSON object (or extract from container .yaml/.json)
    output_schema: Optional[Dict[str, Any]] = None  # Optional - LinkML schema as JSON object (or extract from container .yaml/.json)

def wait_for_mongodb():
    """Wait for MongoDB to be ready"""
    max_retries = 30
    for attempt in range(max_retries):
        try:
            mongo_client.admin.command('ping')
            logger.info("MongoDB is ready")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                logger.info(f"Waiting for MongoDB... (attempt {attempt + 1}/{max_retries})")
                time.sleep(2)
            else:
                logger.error(f"Failed to connect to MongoDB after {max_retries} attempts: {e}")
                raise

def _run_model_container(image: str, input_data: any, model_metadata: dict = None) -> dict:
    """Run a model container with file-based I/O and capture stdout/stderr"""
    session_id = uuid.uuid4().hex
    input_file = f"input_{session_id}.json"
    output_file = f"output_{session_id}.json"

    input_path = os.path.join("/shared-tmp", input_file)
    output_path = os.path.join("/shared-tmp", output_file)

    try:
        # Validate input data against schema if available
        if model_metadata and model_metadata.get("input_schema"):
            logger.info(f"Validating input data against schema for {image}")
            try:
                target_class = extract_target_class(model_metadata["input_schema"])
                validate_items(
                    items=input_data,
                    schema=model_metadata["input_schema"],
                    target_class_name=target_class,
                    data_type="input"
                )
                logger.info(f"Input validation passed for {image}")
            except ValidationError:
                raise  # Re-raise ValidationError for proper handling
            except Exception as e:
                raise ValidationError(f"Input validation failed: {e}", [])

        # Write input data to file
        with open(input_path, "w") as f:
            json.dump(input_data, f)
            f.flush()

        # Run the model container with both input and output file paths
        logger.info(f"Running model {image} with file-based I/O...")
        
        # First try the new file-based I/O pattern
        try:
            container_result = client.containers.run(
                image,
                command=["./predict", f"/shared-tmp/{input_file}", f"/shared-tmp/{output_file}"],
                volumes={
                    "app_shared_tmp": {"bind": "/shared-tmp", "mode": "rw"}
                },
                remove=True,
                stdout=True,
                stderr=True,
                detach=False
            )
            
            # Check if output file was created (new pattern worked)
            if os.path.exists(output_path):
                logger.info(f"Model {image} uses new file-based I/O pattern")
                new_pattern_used = True
            else:
                raise Exception("Model did not create output file - falling back to legacy pattern")
                
        except Exception as e:
            logger.warning(f"New I/O pattern failed for {image}, trying legacy pattern: {e}")
            new_pattern_used = False
            
            # Fallback to old stdout-based pattern
            container_result = client.containers.run(
                image,
                command=["./predict", f"/shared-tmp/{input_file}"],
                volumes={
                    "app_shared_tmp": {"bind": "/shared-tmp", "mode": "rw"}
                },
                remove=True,
                stdout=True,
                stderr=True,
                detach=False
            )
            
            # Parse predictions from stdout (legacy pattern)
            try:
                stdout_output = container_result.decode('utf-8') if container_result else ""
                predictions = json.loads(stdout_output.strip())
                
                # Create output file for consistency
                with open(output_path, 'w') as f:
                    json.dump(predictions, f)
                    
                logger.info(f"Model {image} uses legacy stdout pattern - converted to file-based")
            except json.JSONDecodeError as json_err:
                raise Exception(f"Legacy pattern failed - stdout is not valid JSON: {json_err}")
        
        # Capture and parse stdout/stderr from container result
        raw_output = container_result.decode('utf-8') if container_result else ""
        
        # Try to split stdout/stderr based on simple heuristics
        # In new pattern, models should send informational messages to stderr
        # and only results to stdout
        lines = raw_output.split('\n')
        stdout_lines = []
        stderr_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
                
            # Simple heuristic: lines with certain patterns go to stderr
            stderr_indicators = [
                'Loading', 'Processing', 'Generated', 'written to', 'Error:', 'Warning:',
                'Model loaded', 'Completed processing', 'records', 'Starting', 'Successfully generated'
            ]
            
            if any(phrase in line for phrase in stderr_indicators):
                stderr_lines.append(line)
            else:
                stdout_lines.append(line)
        
        stdout_output = '\n'.join(stdout_lines).strip()
        stderr_output = '\n'.join(stderr_lines).strip()

        # Read predictions from output file
        if not os.path.exists(output_path):
            raise Exception(f"Model did not create output file: {output_file}")

        with open(output_path, 'r') as f:
            predictions = json.load(f)

        # Validate output data against schema if available
        if model_metadata and model_metadata.get("output_schema"):
            logger.info(f"Validating output data against schema for {image}")
            try:
                target_class = extract_target_class(model_metadata["output_schema"])
                # Handle both list and single-item outputs
                output_list = predictions if isinstance(predictions, list) else [predictions]
                validate_items(
                    items=output_list,
                    schema=model_metadata["output_schema"],
                    target_class_name=target_class,
                    data_type="output"
                )
                logger.info(f"Output validation passed for {image}")
            except ValidationError as e:
                # Output validation failure is a model implementation error
                raise Exception(f"Output validation failed (model does not conform to declared schema): {e}")
            except Exception as e:
                raise Exception(f"Output validation failed: {e}")

        logger.info(f"Model {image} execution successful")

        return {
            "predictions": predictions,
            "stdout": stdout_output,
            "stderr": stderr_output,
            "model_logs": {
                "input_file": input_file,
                "output_file": output_file,
                "session_id": session_id
            }
        }
        
    except json.JSONDecodeError as e:
        raise Exception(f"Model output file contains invalid JSON: {e}")
    except ValidationError:
        raise  # Re-raise ValidationError for proper 400 error handling
    except Exception as e:
        # If output file doesn't exist, try to get any error info from stdout
        error_info = container_result.decode('utf-8') if 'container_result' in locals() else "No output"
        raise Exception(f"Model execution failed: {e}. Container output: {error_info}")
    finally:
        # Clean up temporary files
        for temp_file in [input_path, output_path]:
            if os.path.exists(temp_file):
                os.remove(temp_file)

def _extract_container_metadata(image: str) -> dict:
    """Extract readme, examples, and schemas from container files.

    Schemas can be provided as either YAML (.yaml) or JSON (.json) files.
    YAML is checked first for backwards compatibility.
    """
    temp_readme_path = None
    temp_examples_path = None
    metadata = {}

    try:
        logger.info(f"Attempting to extract metadata from container: {image}")

        # Create unique temp files for this extraction
        session_id = uuid.uuid4().hex
        temp_readme_path = os.path.join("/shared-tmp", f"readme_{session_id}.md")
        temp_examples_path = os.path.join("/shared-tmp", f"examples_{session_id}.json")
        temp_input_schema_path = os.path.join("/shared-tmp", f"input_schema_{session_id}.yaml")
        temp_output_schema_path = os.path.join("/shared-tmp", f"output_schema_{session_id}.yaml")

        # Try to copy README.md from container
        try:
            client.containers.run(
                image,
                command=["cp", "/app/README.md", f"/shared-tmp/readme_{session_id}.md"],
                volumes={
                    "app_shared_tmp": {"bind": "/shared-tmp", "mode": "rw"}
                },
                remove=True,
                detach=False
            )

            if os.path.exists(temp_readme_path):
                with open(temp_readme_path, 'r') as f:
                    metadata['readme'] = f.read()
                logger.info(f"Extracted README from container: {image}")

        except Exception as e:
            logger.debug(f"No README.md found in container {image}: {e}")

        # Try to copy examples.json from container
        try:
            client.containers.run(
                image,
                command=["cp", "/app/examples.json", f"/shared-tmp/examples_{session_id}.json"],
                volumes={
                    "app_shared_tmp": {"bind": "/shared-tmp", "mode": "rw"}
                },
                remove=True,
                detach=False
            )

            if os.path.exists(temp_examples_path):
                with open(temp_examples_path, 'r') as f:
                    examples_data = json.load(f)
                metadata['examples'] = examples_data
                logger.info(f"Extracted examples from container: {image}")

        except Exception as e:
            logger.debug(f"No examples.json found in container {image}: {e}")

        # Try to copy input_schema from container (YAML or JSON)
        input_schema_extracted = False
        for ext in ['.yaml', '.json']:
            if input_schema_extracted:
                break
            try:
                temp_input_schema_path = os.path.join("/shared-tmp", f"input_schema_{session_id}{ext}")
                client.containers.run(
                    image,
                    command=["cp", f"/app/input_schema{ext}", f"/shared-tmp/input_schema_{session_id}{ext}"],
                    volumes={
                        "app_shared_tmp": {"bind": "/shared-tmp", "mode": "rw"}
                    },
                    remove=True,
                    detach=False
                )

                if os.path.exists(temp_input_schema_path):
                    with open(temp_input_schema_path, 'r') as f:
                        metadata['input_schema'] = f.read()
                    logger.info(f"Extracted input_schema{ext} from container: {image}")
                    input_schema_extracted = True
                    # Clean up this temp file
                    os.remove(temp_input_schema_path)

            except Exception as e:
                logger.debug(f"No input_schema{ext} found in container {image}: {e}")
                # Clean up temp file if it exists
                if os.path.exists(temp_input_schema_path):
                    os.remove(temp_input_schema_path)

        # Try to copy output_schema from container (YAML or JSON)
        output_schema_extracted = False
        for ext in ['.yaml', '.json']:
            if output_schema_extracted:
                break
            try:
                temp_output_schema_path = os.path.join("/shared-tmp", f"output_schema_{session_id}{ext}")
                client.containers.run(
                    image,
                    command=["cp", f"/app/output_schema{ext}", f"/shared-tmp/output_schema_{session_id}{ext}"],
                    volumes={
                        "app_shared_tmp": {"bind": "/shared-tmp", "mode": "rw"}
                    },
                    remove=True,
                    detach=False
                )

                if os.path.exists(temp_output_schema_path):
                    with open(temp_output_schema_path, 'r') as f:
                        metadata['output_schema'] = f.read()
                    logger.info(f"Extracted output_schema{ext} from container: {image}")
                    output_schema_extracted = True
                    # Clean up this temp file
                    os.remove(temp_output_schema_path)

            except Exception as e:
                logger.debug(f"No output_schema{ext} found in container {image}: {e}")
                # Clean up temp file if it exists
                if os.path.exists(temp_output_schema_path):
                    os.remove(temp_output_schema_path)

        return metadata

    except Exception as e:
        logger.warning(f"Failed to extract metadata from container {image}: {e}")
        return {}
    finally:
        # Clean up temporary files (schema temp files are cleaned up in their respective loops)
        for temp_file in [temp_readme_path, temp_examples_path]:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)

def _register_model_internal(metadata: dict) -> dict:
    """Internal model registration function that can be called during startup"""
    image = metadata["image"]

    # 1. Check if image exists locally
    try:
        client.images.get(image)
        logger.info(f"Found local image: {image}")
    except Exception as e:
        logger.error(f"Image not found locally: {image} - {e}")
        raise Exception(f"Image not found locally: {image}")

    try:
        # Validate examples against input schema before testing
        if metadata.get("input_schema"):
            logger.info(f"Validating examples against input schema for {image}")
            try:
                target_class = extract_target_class(metadata["input_schema"])
                validate_items(
                    items=metadata["examples"],
                    schema=metadata["input_schema"],
                    target_class_name=target_class,
                    data_type="example input"
                )
                logger.info(f"Example validation passed for {image}")
            except ValidationError as e:
                raise Exception(f"Examples do not match input schema: {e}")
            except Exception as e:
                raise Exception(f"Failed to validate examples against input schema: {e}")

        # Test the model with provided examples (pass metadata for output validation)
        logger.info(f"Testing model {image} with examples...")
        result = _run_model_container(image, metadata["examples"], metadata)

        preds = result["predictions"]
        logger.info(f"Model {image} test successful")

        # Log any stderr output during registration
        if result["stderr"]:
            logger.info(f"Model {image} stderr during registration: {result['stderr']}")

        # Store in MongoDB (including schemas)
        doc = {
            "image": image,
            "title": metadata["title"],
            "short_description": metadata["short_description"],
            "authors": metadata["authors"],
            "readme": metadata["readme"],
            "examples": metadata["examples"],
            "input_schema": metadata.get("input_schema"),
            "output_schema": metadata.get("output_schema")
        }
        # Upsert (replace if exists, insert if new)
        models_collection.replace_one({"image": image}, doc, upsert=True)
        logger.info(f"Successfully registered model: {image}")

        return {
            "status": "ok", 
            "image": image, 
            "example_predictions": preds,
            "registration_logs": {
                "stdout": result["stdout"],
                "stderr": result["stderr"]
            }
        }
    except Exception as e:
        logger.error(f"Registration failed for {image}: {e}")
        raise

def load_builtin_models():
    """Load and register built-in models from metadata files"""
    logger.info("Starting auto-registration of built-in models...")

    if not os.path.exists(BUILTIN_MODELS_PATH):
        logger.warning(f"Built-in models path does not exist: {BUILTIN_MODELS_PATH}")
        return

    # Find all model_metadata.json files
    metadata_files = glob.glob(os.path.join(BUILTIN_MODELS_PATH, "*/model_metadata.json"))

    if not metadata_files:
        logger.warning(f"No model metadata files found in: {BUILTIN_MODELS_PATH}")
        return

    registered_count = 0
    failed_count = 0

    for metadata_file in metadata_files:
        model_name = Path(metadata_file).parent.name
        try:
            logger.info(f"Loading metadata for model: {model_name}")
            with open(metadata_file, 'r') as f:
                base_metadata = json.load(f)

            # Extract container metadata if available
            image = base_metadata["image"]
            container_metadata = _extract_container_metadata(image)

            # Merge metadata with container fallback (including schemas)
            metadata = {
                "image": base_metadata["image"],
                "title": base_metadata["title"],
                "short_description": base_metadata["short_description"],
                "authors": base_metadata["authors"],
                "examples": base_metadata.get("examples") or container_metadata.get("examples"),
                "readme": base_metadata.get("readme") or container_metadata.get("readme"),
                "input_schema": base_metadata.get("input_schema") or container_metadata.get("input_schema"),
                "output_schema": base_metadata.get("output_schema") or container_metadata.get("output_schema")
            }

            # Validate required fields
            if not metadata["examples"]:
                logger.error(f"No examples found for {model_name} in JSON file or container")
                failed_count += 1
                continue

            if not metadata["readme"]:
                logger.error(f"No README found for {model_name} in JSON file or container")
                failed_count += 1
                continue

            # Schemas are required
            if not metadata["input_schema"]:
                logger.error(f"No input_schema found for {model_name} in JSON file or container")
                failed_count += 1
                continue

            if not metadata["output_schema"]:
                logger.error(f"No output_schema found for {model_name} in JSON file or container")
                failed_count += 1
                continue

            # Register the model
            _register_model_internal(metadata)
            registered_count += 1

        except Exception as e:
            logger.error(f"Failed to register built-in model {model_name}: {e}")
            failed_count += 1

    logger.info(f"Built-in model registration complete: {registered_count} successful, {failed_count} failed")

    if failed_count > 0:
        raise Exception(f"Failed to register {failed_count} built-in models")

@app.on_event("startup")
async def startup_event():
    """Initialize built-in models on startup"""
    logger.info("Model server starting up...")
    
    # Wait for MongoDB to be ready
    wait_for_mongodb()
    
    # Load and register built-in models
    try:
        load_builtin_models()
        logger.info("Model server startup complete")
    except Exception as e:
        logger.error(f"Failed to load built-in models: {e}")
        # Don't fail startup, but log the error
        # In production, you might want to fail startup instead

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        # Check MongoDB connection
        mongodb_connected = False
        mongodb_error = None
        model_count = 0
        
        try:
            mongo_client.admin.command('ping')
            model_count = models_collection.count_documents({})
            mongodb_connected = True
        except Exception as e:
            mongodb_error = str(e)
        
        # Service is healthy if it can respond (MongoDB issues are dependency problems)
        service_status = "healthy"
        
        return {
            "status": service_status,
            "service": "model_server",
            "models_registered": model_count,
            "dependencies": {
                "mongodb": {
                    "connected": mongodb_connected,
                    "url": f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}",
                    "error": mongodb_error if not mongodb_connected else None
                }
            }
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "model_server",
            "error": str(e),
            "models_registered": 0,
            "dependencies": {
                "mongodb": {
                    "connected": False,
                    "url": f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}",
                    "error": "Health check failed"
                }
            }
        }

@app.post("/models")
def register_model(req: RegisterRequest):
    """Register a model via API with container metadata fallback"""
    try:
        # Try to pull image if not available locally
        try:
            client.images.pull(req.image)
        except Exception:
            pass  # Image might already be local

        # Extract metadata from container if available
        container_metadata = _extract_container_metadata(req.image)

        # Build final metadata with API override priority
        # API schemas are dicts, container schemas are strings - normalize all to strings for consistent storage
        api_input_schema = normalize_schema_to_string(req.input_schema) if req.input_schema is not None else None
        api_output_schema = normalize_schema_to_string(req.output_schema) if req.output_schema is not None else None

        metadata = {
            "image": req.image,
            "title": req.title,
            "short_description": req.short_description,
            "authors": req.authors,
            "examples": req.examples if req.examples is not None else container_metadata.get("examples"),
            "readme": req.readme if req.readme is not None else container_metadata.get("readme"),
            "input_schema": api_input_schema if api_input_schema is not None else container_metadata.get("input_schema"),
            "output_schema": api_output_schema if api_output_schema is not None else container_metadata.get("output_schema")
        }

        # Validate that required fields are present
        if metadata["examples"] is None:
            raise HTTPException(
                status_code=400,
                detail="Examples are required. Provide via API 'examples' field or include /app/examples.json in container."
            )

        if metadata["readme"] is None:
            raise HTTPException(
                status_code=400,
                detail="README is required. Provide via API 'readme' field or include /app/README.md in container."
            )

        # Schemas are required
        if metadata["input_schema"] is None:
            raise HTTPException(
                status_code=400,
                detail="Input schema is required. Provide via API 'input_schema' field (JSON format) or include /app/input_schema.yaml or /app/input_schema.json in container."
            )

        if metadata["output_schema"] is None:
            raise HTTPException(
                status_code=400,
                detail="Output schema is required. Provide via API 'output_schema' field (JSON format) or include /app/output_schema.yaml or /app/output_schema.json in container."
            )

        return _register_model_internal(metadata)
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Registration failed: {e}")

@app.post("/predict")
def predict(
    image: str = Body(..., embed=True),
    input: List[Any] = Body(..., embed=True)
):
    """Make a prediction using a registered model with file-based I/O"""
    # 1. Confirm the model is registered
    m = models_collection.find_one({"image": image})
    if not m:
        raise HTTPException(status_code=404, detail="Model not registered")

    # 2. Run the model with file-based I/O (pass model metadata for validation)
    try:
        result = _run_model_container(image, input, m)
        return {
            "predictions": result["predictions"],
            "stdout": result["stdout"],
            "stderr": result["stderr"]
        }
    except ValidationError as e:
        # Input validation failed - return 400 Bad Request
        raise HTTPException(status_code=400, detail=f"Validation error: {e}")
    except Exception as e:
        # Other errors (including output validation) - return 500
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

@app.get("/models")
def list_models():
    # Return all models with core metadata (not including README or full schemas)
    models = []
    for m in models_collection.find({}, {"_id": 0}):
        models.append({
            "image": m["image"],
            "title": m.get("title", ""),
            "short_description": m.get("short_description", ""),
            "authors": m.get("authors", ""),
            "examples": m.get("examples", []),
            "has_input_schema": m.get("input_schema") is not None,
            "has_output_schema": m.get("output_schema") is not None,
        })
    return models

@app.get("/models/{image_tag}")
def model_info(image_tag: str):
    m = models_collection.find_one({"image": image_tag})
    if not m:
        raise HTTPException(status_code=404, detail="Model not found")
    # Build response with all metadata, including README and schemas
    # Parse schema strings to dicts so they appear as proper JSON objects in the response
    return {
        "image": m["image"],
        "title": m.get("title", ""),
        "short_description": m.get("short_description", ""),
        "authors": m.get("authors", ""),
        "examples": m.get("examples", []),
        "readme": m.get("readme", ""),
        "input_schema": schema_to_response_format(m.get("input_schema")),
        "output_schema": schema_to_response_format(m.get("output_schema")),
    }
