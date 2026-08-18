from model_server.jsonschema import linkml_to_jsonschema

COX_INPUT = {
    "id": "https://example.org/cox/input",
    "name": "cox_input",
    "default_range": "string",
    "enums": {
        "SexAtBirthEnum": {
            "permissible_values": {"Female": {}, "Male": {}}
        }
    },
    "classes": {
        "CoxCOPDInputItem": {
            "description": "Patient data",
            "attributes": {
                "sex_at_birth": {"range": "SexAtBirthEnum", "required": True},
                "bmi": {"range": "float", "required": True},
                "diabetes": {"range": "float", "required": False},
                "notes": {"range": "string"},
            },
        }
    },
}


def test_returns_none_for_none():
    assert linkml_to_jsonschema(None) is None


def test_enum_becomes_string_enum():
    js = linkml_to_jsonschema(COX_INPUT)
    assert js["type"] == "object"
    assert js["properties"]["sex_at_birth"] == {"type": "string", "enum": ["Female", "Male"]}


def test_float_becomes_number():
    js = linkml_to_jsonschema(COX_INPUT)
    assert js["properties"]["bmi"]["type"] == "number"


def test_required_collected():
    js = linkml_to_jsonschema(COX_INPUT)
    assert set(js["required"]) == {"sex_at_birth", "bmi"}


def test_default_range_string():
    js = linkml_to_jsonschema(COX_INPUT)
    assert js["properties"]["notes"]["type"] == "string"


def test_accepts_yaml_string():
    yaml_schema = """
classes:
  Item:
    attributes:
      age:
        range: integer
        required: true
"""
    js = linkml_to_jsonschema(yaml_schema)
    assert js["properties"]["age"]["type"] == "integer"
    assert js["required"] == ["age"]
