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
from typing import List, Dict, Any, Union, Optional, Tuple
import yaml
from linkml.validator import validate
from oaklib import get_adapter
from oaklib.interfaces import OboGraphInterface

logger = logging.getLogger(__name__)

_OAK_ADAPTER_CACHE: Dict[str, OboGraphInterface] = {}
_LABEL_CACHE: Dict[str, Optional[str]] = {}


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


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_permissible_values(pv: Any) -> Dict[str, Dict[str, Any]]:
    if pv is None:
        return {}
    if isinstance(pv, dict):
        return pv
    raise ValueError("permissible_values must be a dict if provided")


def _get_adapter(source_ontology: str) -> OboGraphInterface:
    adapter = _OAK_ADAPTER_CACHE.get(source_ontology)
    if adapter is not None:
        return adapter
    adapter = get_adapter(source_ontology)
    if not isinstance(adapter, OboGraphInterface):
        raise ValueError(f"Ontology adapter for '{source_ontology}' does not support graph traversal")
    _OAK_ADAPTER_CACHE[source_ontology] = adapter
    return adapter


def _get_label(adapter: OboGraphInterface, curie: str) -> Optional[str]:
    cached = _LABEL_CACHE.get(curie)
    if cached is not None or curie in _LABEL_CACHE:
        return cached
    try:
        label = adapter.label(curie)
    except Exception:
        label = None
    _LABEL_CACHE[curie] = label
    return label


def _expand_reachable_from_expression(expr: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    source_ontology = expr.get("source_ontology")
    source_nodes = _as_list(expr.get("source_nodes"))
    if not source_ontology or not source_nodes:
        raise ValueError("reachable_from requires 'source_ontology' and non-empty 'source_nodes'")

    relationship_types = _as_list(expr.get("relationship_types"))
    predicates = relationship_types if relationship_types else None
    include_self = bool(expr.get("include_self", expr.get("reflexive", False)))
    is_direct = bool(expr.get("is_direct", False))

    adapter = _get_adapter(source_ontology)

    expanded: Dict[str, Dict[str, Any]] = {}
    for node in source_nodes:
        try:
            if is_direct and hasattr(adapter, "children"):
                descendants = adapter.children(node, predicates=predicates)
            else:
                descendants = adapter.descendants(node, predicates=predicates)
        except Exception as e:
            raise ValueError(f"Failed to expand reachable_from for {node}: {e}")

        term_set = set(descendants or [])
        if include_self:
            term_set.add(node)

        for term in term_set:
            term_label = _get_label(adapter, term)
            entry = {"meaning": term}
            if term_label:
                entry["text"] = term_label
            expanded[term] = entry

    return expanded


def _collect_reachable_from(enum_def: Dict[str, Any]) -> List[Dict[str, Any]]:
    exprs: List[Dict[str, Any]] = []

    def add_expr(value: Any) -> None:
        for expr in _as_list(value):
            if not isinstance(expr, dict):
                raise ValueError("reachable_from entries must be objects")
            exprs.append(expr)

    add_expr(enum_def.get("reachable_from"))

    for key in ("include", "minus"):
        for entry in _as_list(enum_def.get(key)):
            if isinstance(entry, dict) and "reachable_from" in entry:
                add_expr(entry.get("reachable_from"))

    return exprs


def schema_has_reachable_from(schema_dict: Dict[str, Any]) -> bool:
    enums = schema_dict.get("enums", {}) or {}
    for enum_def in enums.values():
        if not isinstance(enum_def, dict):
            continue
        if _collect_reachable_from(enum_def):
            return True
    return False


def expand_reachable_from_schema(schema: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    schema_dict = parse_schema(schema)
    enums = schema_dict.get("enums", {}) or {}

    for enum_name, enum_def in enums.items():
        if not isinstance(enum_def, dict):
            raise ValueError(f"Enum definition for {enum_name} must be an object")

        reachable_exprs = _collect_reachable_from(enum_def)
        if not reachable_exprs:
            continue

        existing_pv = _normalize_permissible_values(enum_def.get("permissible_values"))
        expanded_pv: Dict[str, Dict[str, Any]] = {}

        for expr in reachable_exprs:
            expanded_pv.update(_expand_reachable_from_expression(expr))

        # Include permissible_values from "include" entries
        for include_entry in _as_list(enum_def.get("include")):
            if isinstance(include_entry, dict) and "permissible_values" in include_entry:
                expanded_pv.update(_normalize_permissible_values(include_entry.get("permissible_values")))

        # Merge expanded values, then re-apply explicit values so they win
        merged_pv = dict(expanded_pv)
        merged_pv.update(existing_pv)

        # Apply "minus" entries after merge
        for minus_entry in _as_list(enum_def.get("minus")):
            if not isinstance(minus_entry, dict):
                continue
            if "permissible_values" in minus_entry:
                for key in _normalize_permissible_values(minus_entry.get("permissible_values")).keys():
                    merged_pv.pop(key, None)
            if "reachable_from" in minus_entry:
                for expr in _as_list(minus_entry.get("reachable_from")):
                    if not isinstance(expr, dict):
                        raise ValueError("reachable_from entries must be objects")
                    for key in _expand_reachable_from_expression(expr).keys():
                        merged_pv.pop(key, None)

        enum_def["permissible_values"] = merged_pv
        enum_def.pop("reachable_from", None)

        for key in ("include", "minus"):
            cleaned_entries = []
            for entry in _as_list(enum_def.get(key)):
                if isinstance(entry, dict) and "reachable_from" in entry:
                    entry = dict(entry)
                    entry.pop("reachable_from", None)
                    if not entry:
                        continue
                cleaned_entries.append(entry)
            if cleaned_entries:
                enum_def[key] = cleaned_entries
            else:
                enum_def.pop(key, None)

    return schema_dict


def expand_and_normalize_schema(schema: Union[str, Dict[str, Any], None]) -> Tuple[Optional[str], Optional[str]]:
    if schema is None:
        return None, None
    original_str = normalize_schema_to_string(schema)
    schema_dict = parse_schema(schema)
    if not schema_has_reachable_from(schema_dict):
        return original_str, original_str
    expanded_dict = expand_reachable_from_schema(schema_dict)
    expanded_str = json.dumps(expanded_dict)
    return original_str, expanded_str


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
