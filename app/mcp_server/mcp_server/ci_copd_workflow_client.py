#!/usr/bin/env python3
"""Deterministic MCP COPD workflow validation client."""

from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import date, datetime
from typing import Any

from fastmcp import Client


def fail(message: str, context: Any | None = None) -> None:
    if context is None:
        raise RuntimeError(message)
    snippet = json.dumps(context, default=str)[:2000]
    raise RuntimeError(f"{message}\nContext: {snippet}")


def canonical_key(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def parse_markdown_table(markdown: str) -> list[dict[str, str]]:
    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    table_lines: list[str] = []
    collecting = False

    for line in lines:
        if line.startswith("|"):
            collecting = True
            table_lines.append(line)
        elif collecting:
            break

    if len(table_lines) < 2:
        return []

    def split_row(row: str) -> list[str]:
        return [cell.strip() for cell in row.strip("|").split("|")]

    header = [canonical_key(x) for x in split_row(table_lines[0])]
    data_lines = table_lines[2:]
    rows: list[dict[str, str]] = []

    for line in data_lines:
        cells = split_row(line)
        if not cells or all(not cell for cell in cells):
            continue
        if len(cells) < len(header):
            cells.extend([""] * (len(header) - len(cells)))
        row = {header[i]: cells[i] for i in range(len(header))}
        rows.append(row)

    return rows


def get_markdown_section(markdown: str, title: str) -> str:
    lines = markdown.splitlines()
    header = f"## {title}"
    in_section = False
    section_lines: list[str] = []

    for line in lines:
        if line.strip() == header:
            in_section = True
            continue
        if in_section and line.startswith("## "):
            break
        if in_section:
            section_lines.append(line)

    return "\n".join(section_lines).strip()


def parse_demographics_markdown(markdown: str) -> dict[str, str]:
    data: dict[str, str] = {}
    pattern = re.compile(r"^\*\*(.+?)\*\*:\s*(.*)\s*$")
    for raw_line in markdown.splitlines():
        line = raw_line.strip()
        match = pattern.match(line)
        if match:
            key = canonical_key(match.group(1))
            data[key] = match.group(2).strip()
    return data


def parse_numeric_prefix(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"[-+]?\d*\.?\d+", value)
    if not match:
        return None
    return float(match.group(0))


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except Exception:
        return None


def has_keyword(values: list[str], keywords: list[str]) -> bool:
    return any(any(keyword in value for keyword in keywords) for value in values)


def normalize_call_result(result: Any) -> Any:
    structured = getattr(result, "structuredContent", None)
    if structured not in (None, {}):
        return structured

    text_parts: list[str] = []
    for item in getattr(result, "content", []) or []:
        text = getattr(item, "text", None)
        if text is not None:
            text_parts.append(text)
        else:
            text_parts.append(str(item))

    raw = "\n".join(text_parts).strip()
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except Exception:
        return raw


async def call_and_extract(client: Client, tool_name: str, arguments: dict[str, Any] | None = None) -> Any:
    result = await client.call_tool(tool_name, arguments or {})
    if getattr(result, "isError", False):
        fail(f"MCP tool '{tool_name}' returned error", normalize_call_result(result))
    return normalize_call_result(result)


def map_copd_input(
    demographics: dict[str, str],
    observations: list[dict[str, str]],
    conditions: list[dict[str, str]],
    fallback_birthdate: str,
) -> dict[str, Any]:
    obs_rows = [{k: (v or "") for k, v in row.items()} for row in observations]
    cond_rows = [{k: (v or "") for k, v in row.items()} for row in conditions]

    bmi_row = next((row for row in obs_rows if row.get("code_text", "").lower() == "body mass index"), None)
    if bmi_row is None:
        fail("Could not find BMI observation row", obs_rows)

    bmi = parse_numeric_prefix(bmi_row.get("value_with_unit"))
    if bmi is None:
        fail("Failed to parse numeric BMI from value_with_unit", bmi_row)

    smoking_row = next((row for row in obs_rows if "smoking" in row.get("code_text", "").lower()), None)
    smoking_status = 0.0
    if smoking_row:
        smoking_value = smoking_row.get("value_with_unit", "").lower()
        if any(token in smoking_value for token in ["never smoked", "never smoker", "never"]):
            smoking_status = 0.0
        elif any(token in smoking_value for token in ["current", "former", "ex-smoker", "ex smoker", "smoker"]):
            smoking_status = 1.0

    alcohol_row = next((row for row in obs_rows if "alcohol" in row.get("code_text", "").lower()), None)
    alcohol_use = 0.0
    if alcohol_row:
        alcohol_value = alcohol_row.get("value_with_unit", "").lower()
        alcohol_numeric = parse_numeric_prefix(alcohol_value)
        if alcohol_numeric is not None:
            alcohol_use = 1.0 if alcohol_numeric > 0 else 0.0
        elif any(token in alcohol_value for token in ["yes", "current", "drinks", "positive", "true"]):
            alcohol_use = 1.0

    condition_texts = [row.get("condition_text", "").lower() for row in cond_rows]
    condition_codes = [row.get("condition_code", "").lower() for row in cond_rows]

    diabetes = 1.0 if has_keyword(condition_texts, ["diabetes"]) else 0.0
    cardiovascular_disease = 1.0 if has_keyword(
        condition_texts,
        ["ischemic heart disease", "coronary", "cardiovascular disease"],
    ) else 0.0
    obesity = 1.0 if bmi >= 30.0 or has_keyword(condition_texts, ["obesity"]) or "414916001" in condition_codes else 0.0

    ethnicity = demographics.get("ethnicity", "").strip() or "Not Hispanic or Latino"

    birth_sex = demographics.get("birth_sex", "").strip().upper()
    if birth_sex == "M":
        sex_at_birth = "Male"
    elif birth_sex == "F":
        sex_at_birth = "Female"
    else:
        gender = demographics.get("gender", "").strip().lower()
        if gender == "male":
            sex_at_birth = "Male"
        elif gender == "female":
            sex_at_birth = "Female"
        else:
            fail("Unable to determine sex_at_birth from demographics", demographics)

    birth_date_raw = demographics.get("birth_date", "").strip() or fallback_birthdate
    birth_date_value = parse_iso_date(birth_date_raw)
    if birth_date_value is None:
        fail("Could not parse birth_date", birth_date_raw)

    reference_date = parse_iso_date(bmi_row.get("effective_date"))
    if reference_date is None:
        for row in obs_rows:
            reference_date = parse_iso_date(row.get("effective_date"))
            if reference_date is not None:
                break
    if reference_date is None:
        reference_date = date.today()

    age_at_time_0 = reference_date.year - birth_date_value.year - (
        (reference_date.month, reference_date.day) < (birth_date_value.month, birth_date_value.day)
    )

    return {
        "ethnicity": ethnicity,
        "sex_at_birth": sex_at_birth,
        "obesity": float(obesity),
        "diabetes": float(diabetes),
        "cardiovascular_disease": float(cardiovascular_disease),
        "smoking_status": float(smoking_status),
        "alcohol_use": float(alcohol_use),
        "bmi": float(bmi),
        "age_at_time_0": float(age_at_time_0),
    }


def assert_expected_mapping(mapped: dict[str, Any]) -> None:
    expected = {
        "ethnicity": "Not Hispanic or Latino",
        "sex_at_birth": "Male",
        "obesity": 1.0,
        "diabetes": 0.0,
        "cardiovascular_disease": 1.0,
        "smoking_status": 0.0,
        "alcohol_use": 0.0,
        "bmi": 30.54,
        "age_at_time_0": 50.0,
    }

    for key, expected_value in expected.items():
        actual_value = mapped.get(key)
        if key == "bmi":
            if abs(float(actual_value) - float(expected_value)) > 1e-6:
                fail(f"Unexpected mapped BMI: expected {expected_value}, got {actual_value}", mapped)
        elif actual_value != expected_value:
            fail(f"Unexpected mapped field '{key}': expected {expected_value}, got {actual_value}", mapped)


async def main() -> None:
    patient_id = os.environ.get("CI_MCP_PATIENT_ID", "").strip()
    patient_given = os.environ.get("CI_MCP_PATIENT_GIVEN", "").strip()
    patient_family = os.environ.get("CI_MCP_PATIENT_FAMILY", "").strip()
    patient_birthdate = os.environ.get("CI_MCP_PATIENT_BIRTHDATE", "").strip()

    if not all([patient_id, patient_given, patient_family, patient_birthdate]):
        fail(
            "Missing required MCP CI env vars",
            {
                "CI_MCP_PATIENT_ID": bool(patient_id),
                "CI_MCP_PATIENT_GIVEN": bool(patient_given),
                "CI_MCP_PATIENT_FAMILY": bool(patient_family),
                "CI_MCP_PATIENT_BIRTHDATE": bool(patient_birthdate),
            },
        )

    print(f"[mcp-ci] Target patient: {patient_id} ({patient_given} {patient_family})")
    async with Client("http://localhost:8000/mcp", auto_initialize=False, timeout=60) as client:
        init_error: str | None = None
        for attempt in range(1, 61):
            try:
                await client.initialize(timeout=10)
                print(f"[mcp-ci] MCP initialize succeeded on attempt {attempt}")
                init_error = None
                break
            except Exception as exc:  # noqa: BLE001
                init_error = str(exc)
                await asyncio.sleep(2)

        if init_error is not None:
            fail("Failed to initialize MCP client after retries", {"error": init_error})

        tools = await client.list_tools()
        tool_names = {tool.name for tool in tools}
        required_tools = {
            "search_patients",
            "get_patient_demographics",
            "get_patient_resource_type",
            "list_available_models",
            "get_model_metadata",
            "execute_model",
        }
        missing_tools = sorted(required_tools - tool_names)
        if missing_tools:
            fail("Missing required MCP tools", {"missing_tools": missing_tools, "available_tools": sorted(tool_names)})
        print(f"[mcp-ci] Required tools available: {sorted(required_tools)}")

        search_output = await call_and_extract(
            client,
            "search_patients",
            {"name": patient_family, "birthdate": patient_birthdate, "count": 10},
        )
        if not isinstance(search_output, str):
            fail("search_patients output was not markdown text", search_output)
        search_rows = parse_markdown_table(search_output)
        if not search_rows:
            fail("Could not parse search_patients markdown table", search_output)

        matching_rows = [
            row
            for row in search_rows
            if row.get("patient_id") == patient_id and patient_family.lower() in row.get("family_name", "").lower()
        ]
        if not matching_rows:
            fail("search_patients did not return expected fixture patient", {"patient_id": patient_id, "rows": search_rows})
        print(f"[mcp-ci] search_patients located fixture patient in {len(matching_rows)} row(s)")

        demographics_output = await call_and_extract(client, "get_patient_demographics", {"patient_id": patient_id})
        if not isinstance(demographics_output, str):
            fail("get_patient_demographics output was not markdown text", demographics_output)
        demographics = parse_demographics_markdown(demographics_output)
        if demographics.get("birth_date") != patient_birthdate:
            fail("Unexpected birth_date in demographics output", demographics)
        if patient_family.lower() not in demographics.get("family_name", "").lower():
            fail("Expected family_name not found in demographics output", demographics)
        print("[mcp-ci] Demographics markdown parsed successfully")

        resource_output = await call_and_extract(
            client,
            "get_patient_resource_type",
            {"patient_id": patient_id, "resource_types": ["Observation", "Condition"]},
        )
        if not isinstance(resource_output, str):
            fail("get_patient_resource_type output was not markdown text", resource_output)

        observation_section = get_markdown_section(resource_output, "Observation")
        condition_section = get_markdown_section(resource_output, "Condition")
        observation_rows = parse_markdown_table(observation_section)
        condition_rows = parse_markdown_table(condition_section)

        if not observation_rows:
            fail("No observation rows parsed from MCP resource output", resource_output)
        if not condition_rows:
            fail("No condition rows parsed from MCP resource output", resource_output)
        print(f"[mcp-ci] Parsed {len(observation_rows)} observation row(s) and {len(condition_rows)} condition row(s)")

        models_output = await call_and_extract(client, "list_available_models", {})
        if isinstance(models_output, dict):
            models = models_output.get("models", [])
        elif isinstance(models_output, list):
            models = models_output
        else:
            fail("list_available_models returned unsupported payload", models_output)

        if not any(isinstance(model, dict) and model.get("image") == "coxcopdmodel:latest" for model in models):
            fail("coxcopdmodel:latest not found in available models", models)
        print("[mcp-ci] coxcopdmodel:latest is listed")

        metadata_output = await call_and_extract(client, "get_model_metadata", {"image_tag": "coxcopdmodel:latest"})
        if not isinstance(metadata_output, dict):
            fail("get_model_metadata returned non-dict payload", metadata_output)
        if not str(metadata_output.get("title", "")).strip():
            fail("Model metadata title is missing", metadata_output)
        if not str(metadata_output.get("readme", "")).strip():
            fail("Model metadata readme is missing", metadata_output)
        if metadata_output.get("input_schema") in (None, "", {}):
            fail("Model metadata input_schema is missing", metadata_output)
        print("[mcp-ci] Model metadata validated")

        mapped_input = map_copd_input(demographics, observation_rows, condition_rows, patient_birthdate)
        assert_expected_mapping(mapped_input)
        print(f"[mcp-ci] Deterministic mapped input: {json.dumps(mapped_input, sort_keys=True)}")

        prediction_output = await call_and_extract(
            client,
            "execute_model",
            {"image_tag": "coxcopdmodel:latest", "input_data": [mapped_input]},
        )
        if not isinstance(prediction_output, dict):
            fail("execute_model returned non-dict payload", prediction_output)

        predictions = prediction_output.get("predictions")
        if not isinstance(predictions, list) or len(predictions) != 1:
            fail("execute_model predictions payload is invalid", prediction_output)

        prediction = predictions[0]
        if not isinstance(prediction, dict):
            fail("prediction item is not a dictionary", prediction_output)

        if "partial_hazard" not in prediction or "survival_probability_5_years" not in prediction:
            fail("prediction missing required keys", prediction_output)

        partial_hazard = float(prediction["partial_hazard"])
        survival_probability = float(prediction["survival_probability_5_years"])

        if partial_hazard <= 0:
            fail("partial_hazard must be > 0", prediction)
        if survival_probability < 0 or survival_probability > 1:
            fail("survival_probability_5_years must be in [0, 1]", prediction)

        print(
            "[mcp-ci] Prediction validated: "
            f"partial_hazard={partial_hazard:.6f}, "
            f"survival_probability_5_years={survival_probability:.6f}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:  # noqa: BLE001
        print(f"[mcp-ci][error] {exc}")
        raise
