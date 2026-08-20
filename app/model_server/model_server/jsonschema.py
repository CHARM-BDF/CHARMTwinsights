"""Pure LinkML -> JSON Schema conversion.

Kept in its own module (no docker/mongo imports) so it can be unit-tested in
isolation. main.py imports linkml_to_jsonschema from here.
"""
import json
from typing import Any, Dict, List, Optional, Union

from model_server.validation import parse_schema


def linkml_to_jsonschema(schema: Optional[Union[str, Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    """Convert a LinkML schema (YAML/JSON string or dict) into a minimal draft-07 JSON Schema.

    Focuses on the first class definition and supports the LinkML constructs used
    by existing models (attributes, required, enums, multivalued, nested classes).
    """
    if schema is None:
        return None

    schema_dict = parse_schema(schema)
    classes = schema_dict.get("classes", {})
    if not classes:
        raise ValueError("Schema does not contain any class definitions")

    enums = schema_dict.get("enums", {})
    default_range = schema_dict.get("default_range", "string")
    root_class_name = list(classes.keys())[0]
    definitions: Dict[str, Any] = {}

    def coerce_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value)
        except Exception:
            return str(value)

    def range_to_schema(range_name: Optional[str]) -> Dict[str, Any]:
        if not range_name:
            range_name = default_range
        if range_name in enums:
            permissible = enums[range_name].get("permissible_values", {})
            values = list(permissible.keys())
            return {"type": "string", "enum": values}
        if range_name in classes:
            ensure_class(range_name)
            return {"$ref": f"#/definitions/{range_name}"}
        if range_name in {"integer", "int"}:
            return {"type": "integer"}
        if range_name in {"float", "double", "number"}:
            return {"type": "number"}
        if range_name in {"boolean", "bool"}:
            return {"type": "boolean"}
        return {"type": "string"}

    def ensure_class(class_name: str) -> None:
        if class_name in definitions:
            return
        class_def = classes.get(class_name, {})
        attributes = class_def.get("attributes", {})
        properties: Dict[str, Any] = {}
        required: List[str] = []

        for attr_name, attr_def in attributes.items():
            prop_schema = range_to_schema(attr_def.get("range"))
            if attr_def.get("multivalued"):
                prop_schema = {"type": "array", "items": prop_schema}
            description = coerce_text(attr_def.get("description"))
            if description:
                prop_schema["description"] = description
            properties[attr_name] = prop_schema
            if attr_def.get("required"):
                required.append(attr_name)

        schema_obj: Dict[str, Any] = {
            "type": "object",
            "properties": properties,
            "additionalProperties": False,
        }
        if required:
            schema_obj["required"] = required
        class_description = coerce_text(class_def.get("description"))
        if class_description:
            schema_obj["description"] = class_description
        definitions[class_name] = schema_obj

    ensure_class(root_class_name)

    root_schema = definitions.get(root_class_name, {})
    jsonschema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": coerce_text(schema_dict.get("title", root_class_name)) or root_class_name,
        **root_schema,
    }
    if len(definitions) > 1:
        jsonschema["definitions"] = definitions
    return jsonschema
