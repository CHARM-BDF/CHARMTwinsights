"""
LinkML-based validation for model inputs and outputs.

Schemas can be provided in multiple formats:
- API input: JSON object (dict) - preferred for API registration
- Container files: Both .yaml and .json files are supported (read as strings)

Internal functions accept Union[str, Dict] to handle both cases.
"""
import logging
import tempfile
import os
import json
from typing import List, Dict, Any, Union, Optional
import yaml
from linkml.validator import validate

logger = logging.getLogger(__name__)


def normalize_schema_to_string(schema: Union[str, Dict[str, Any], None]) -> Optional[str]:
    """
    Normalize a schema to a string for internal storage and LinkML validation.

    - If dict (from API): convert to JSON string
    - If string (from container file): return as-is
    - If None: return None

    Args:
        schema: Schema as dict, string, or None

    Returns:
        Optional[str]: Schema as string, or None if input was None
    """
    if schema is None:
        return None
    if isinstance(schema, dict):
        return json.dumps(schema)
    return schema  # Already a string


def parse_schema(schema: Union[str, Dict[str, Any]]) -> dict:
    """
    Parse a schema to a dictionary for inspection.

    Accepts either:
    - dict (from API): returned as-is
    - string (from container file): parsed as JSON or YAML

    Args:
        schema: Schema as dict or string

    Returns:
        dict: Parsed schema dictionary

    Raises:
        ValueError: If string schema is neither valid JSON nor YAML
    """
    # If already a dict, return it
    if isinstance(schema, dict):
        return schema

    # Try JSON first (faster, more strict)
    try:
        return json.loads(schema)
    except json.JSONDecodeError:
        pass

    # Fall back to YAML (for container-extracted schemas)
    try:
        return yaml.safe_load(schema)
    except yaml.YAMLError as e:
        raise ValueError(f"Schema is neither valid JSON nor YAML: {e}")


class ValidationError(Exception):
    """Custom exception for validation errors with detailed messages"""
    def __init__(self, message: str, errors: List[Dict[str, Any]]):
        super().__init__(message)
        self.errors = errors


def should_skip_validation(schema: Union[str, Dict[str, Any]], target_class_name: str) -> bool:
    """
    Check if validation should be skipped for this schema.

    Validation is skipped when:
    - The target class has no attributes defined (permissive schema)
    - The schema contains a 'skip_validation: true' marker

    This allows models with dynamic output structures (like generative models)
    to still have a schema file for documentation while skipping validation.

    Args:
        schema: Schema as dict (from API) or string (from container file)
        target_class_name: Name of the class to check
    """
    try:
        schema_dict = parse_schema(schema)

        # Check for explicit skip marker
        if schema_dict.get('skip_validation', False):
            return True

        # Check if target class has no attributes
        classes = schema_dict.get('classes', {})
        target_class = classes.get(target_class_name, {})
        attributes = target_class.get('attributes', {})

        # Skip if no attributes defined
        if not attributes:
            logger.info(f"Schema class '{target_class_name}' has no attributes - skipping validation")
            return True

        return False
    except Exception:
        # If we can't parse, don't skip - let validation handle errors
        return False


def validate_items(
    items: List[Dict[str, Any]],
    schema: Union[str, Dict[str, Any]],
    target_class_name: str,
    data_type: str = "input"
) -> None:
    """
    Validate a list of data items against a LinkML schema.

    Args:
        items: List of dictionaries to validate
        schema: LinkML schema as dict (from API) or string (from container file)
        target_class_name: Class name in schema to validate against
        data_type: "input" or "output" for error messages

    Raises:
        ValidationError: If any items fail validation, with detailed error info
    """
    if not schema:
        # Should never happen - schemas are required
        raise ValueError(f"Schema is required for {data_type} validation")

    # Check if validation should be skipped (permissive schema)
    if should_skip_validation(schema, target_class_name):
        logger.info(f"Skipping {data_type} validation (permissive schema)")
        return

    validation_errors = []

    # Normalize schema to string for LinkML (which requires a file path)
    schema_str = normalize_schema_to_string(schema)

    # Write schema to a temporary file (linkml.validator.validate requires a file path)
    # LinkML accepts YAML files, but JSON is valid YAML, so we write as .yaml regardless
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as schema_file:
        schema_file.write(schema_str)
        schema_path = schema_file.name

    try:
        for idx, item in enumerate(items):
            try:
                # validate() takes: instance (dict), schema (file path), target_class (str)
                report = validate(item, schema_path, target_class_name)

                # Check if validation passed
                if report.results:
                    # Validation failed for this item
                    error_messages = [result.message for result in report.results]
                    validation_errors.append({
                        "item_index": idx,
                        "item": item,
                        "errors": error_messages
                    })

            except Exception as e:
                # Schema parsing or validation error
                validation_errors.append({
                    "item_index": idx,
                    "item": item,
                    "errors": [f"Validation exception: {str(e)}"]
                })
    finally:
        # Clean up the temporary file
        os.unlink(schema_path)

    if validation_errors:
        # Format a comprehensive error message
        error_summary = f"{len(validation_errors)} of {len(items)} {data_type} items failed validation:\n"
        for err in validation_errors[:5]:  # Show first 5 errors
            error_summary += f"\n  Item {err['item_index']}:\n"
            for msg in err['errors'][:3]:  # Show first 3 messages per item
                error_summary += f"    - {msg}\n"

        if len(validation_errors) > 5:
            error_summary += f"\n  ... and {len(validation_errors) - 5} more items with errors"

        raise ValidationError(error_summary, validation_errors)


def extract_target_class(schema: Union[str, Dict[str, Any]]) -> str:
    """
    Extract the target class name from a LinkML schema.

    Assumes the schema has exactly one class definition, which is typical
    for simple input/output schemas.

    Args:
        schema: LinkML schema as dict (from API) or string (from container file)

    Returns:
        str: The class name to use for validation

    Raises:
        ValueError: If schema is invalid or has multiple classes
    """
    try:
        schema_dict = parse_schema(schema)

        if 'classes' not in schema_dict:
            raise ValueError("Schema does not contain 'classes' section")

        classes = schema_dict['classes']

        if len(classes) == 0:
            raise ValueError("Schema has no class definitions")

        if len(classes) > 1:
            # Multiple classes - use first one and warn
            class_name = list(classes.keys())[0]
            logger.warning(f"Schema has multiple classes, using first one: {class_name}")
            return class_name

        # Single class - return it
        return list(classes.keys())[0]

    except ValueError:
        # Re-raise ValueError from parse_schema
        raise
    except Exception as e:
        raise ValueError(f"Failed to parse schema: {e}")
