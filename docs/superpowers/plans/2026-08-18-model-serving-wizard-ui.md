# Model Serving in the Guided Wizard UI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the mock model-serving screens in the Vue guided wizard with real, schema-driven model browsing, registration, and single-record prediction, powered by a server-side LinkML→JSON Schema conversion and an off-the-shelf Vue form library.

**Architecture:** The model_server gains a pure LinkML→JSON Schema converter and a `GET /models/{tag}/jsonschema` endpoint; the router proxies it as `GET /modeling/models/{tag}/jsonschema`. The Vue app fetches that JSON Schema and renders an input form with JSONForms (vue-vanilla), submits to `POST /modeling/predict`, and wires the existing browse/register screens to the real `/modeling/*` endpoints.

**Tech Stack:** Python 3 / FastAPI / Poetry / pytest (model_server, router); Vue 3 + Vite + axios + `@jsonforms/vue` + `@jsonforms/vue-vanilla` (vueapp_guided); bash + curl + jq (CI smoke tests).

## Global Constraints

- Frontend API base: always call the **router** via `store.apiBase` (`import.meta.env.VITE_API_BASE || 'http://localhost:8000'`). Never call the model_server directly from the browser.
- Vue app uses **no UI framework** — plain CSS with design tokens (`var(--surface)`, `var(--border)`, `var(--text)`, `var(--text-muted)`, `var(--radius-sm)`, `var(--radius-md)`, `var(--surface-alt)`). New styles must use these tokens.
- The Vue app uses `<script setup>` SFCs, a hand-rolled reactive store (`src/state.js`), and per-flow data under `store.flowData[flowId]`. Follow these patterns; do not add Pinia or vue-router.
- Predict request shape (router + model_server): `{ "image": "<tag>", "input": [ <record>, ... ] }`. MVP sends exactly one record.
- LinkML→JSON Schema conversion happens **server-side only**; the Vue app consumes finished JSON Schema.
- Model registration assumes the Docker image already exists on the model_server host; the browser sends metadata/schemas only.
- Cohort-apply / FHIR→model feature mapping is **out of scope**; `ApplyModelFlow.vue` remains a mock placeholder.

---

### Task 1: LinkML→JSON Schema converter (pure module, unit-tested)

**Files:**
- Create: `app/model_server/model_server/jsonschema.py`
- Create: `app/model_server/tests/__init__.py` (empty)
- Create: `app/model_server/tests/test_jsonschema.py`
- Modify: `app/model_server/pyproject.toml` (add a dev dependency group with pytest)

**Interfaces:**
- Consumes: `parse_schema(schema: Union[str, dict]) -> dict` from `model_server.validation` (already exists at `validation.py:52`).
- Produces: `linkml_to_jsonschema(schema: Optional[Union[str, dict]]) -> Optional[dict]` in `model_server.jsonschema`. Returns a draft-07 JSON Schema object for the first class in the LinkML schema: enums → `{"type":"string","enum":[...]}`, `float/double/number` → `{"type":"number"}`, `integer/int` → `{"type":"integer"}`, `boolean/bool` → `{"type":"boolean"}`, other/unknown → `{"type":"string"}`, `multivalued` → array wrapper, nested classes → `$ref` into `definitions`, `required` attributes collected into `required`. Returns `None` when `schema is None`.

- [ ] **Step 1: Add pytest dev dependency to `app/model_server/pyproject.toml`**

Add this group near the other `[tool.poetry...]` sections (create it if absent):

```toml
[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
```

- [ ] **Step 2: Write the failing test** — `app/model_server/tests/test_jsonschema.py`

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app/model_server && poetry run pytest tests/test_jsonschema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'model_server.jsonschema'`

(If `poetry run pytest` reports pytest not installed, run `poetry install --with dev` first, then re-run.)

- [ ] **Step 4: Write the converter** — `app/model_server/model_server/jsonschema.py`

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd app/model_server && poetry run pytest tests/test_jsonschema.py -v`
Expected: PASS (6 passed)

- [ ] **Step 6: Commit**

```bash
git add app/model_server/model_server/jsonschema.py app/model_server/tests/ app/model_server/pyproject.toml
git commit -m "feat(model_server): add pure LinkML->JSON Schema converter with unit tests"
```

---

### Task 2: Expose `/jsonschema` through model_server and router (+ CI smoke)

**Files:**
- Modify: `app/model_server/model_server/main.py` (import converter; add route after the `GET /models/{image_tag}` handler at line 730, near end of file)
- Modify: `app/router/router/routers/modeling.py` (add proxy after `model_info`, before `POST /models` at line 148)
- Create: `ci/model_jsonschema_smoke.sh` (tests the model_server route directly)
- Modify: `ci/model_validation.sh` (invoke the new smoke script while the model_server is up, after line 47)
- Modify: `ci/router_validation.sh` (add an inline proxy assertion after the existing modeling-models check)

Rationale for the two-file test split: `ci/model_validation.sh` brings up only the model_server (`:8004`) and tests it directly — the right place to verify the converter + route. `ci/router_validation.sh` brings up the gateway and already exercises `/modeling/models` and `/modeling/predict` — the right place to verify the thin proxy. Both are already wired into `ci/run.sh`'s `all` target, so no `run.sh` change is needed.

**Interfaces:**
- Consumes: `linkml_to_jsonschema` from `model_server.jsonschema` (Task 1); `settings.model_server_url` (`router/config.py`).
- Produces:
  - model_server `GET /models/{image_tag}/jsonschema` → `{"image": str, "input_schema": <json-schema>, "output_schema": <json-schema>}`; 404 if model missing; 500 on conversion failure.
  - router `GET /modeling/models/{image_tag}/jsonschema` → same body, proxied; propagates upstream status; 500 "Model server unreachable" on connection error.

- [ ] **Step 1: Add the model_server route** — in `app/model_server/model_server/main.py`

Add to the existing validation import block (currently `from model_server.validation import (...)` at line 14) a new import line right after it:

```python
from model_server.jsonschema import linkml_to_jsonschema
```

Then append this route at the end of the file (after the `GET /models/{image_tag}` handler):

```python
@app.get("/models/{image_tag}/jsonschema")
def model_jsonschema(image_tag: str):
    m = models_collection.find_one({"image": image_tag})
    if not m:
        raise HTTPException(status_code=404, detail="Model not found")
    try:
        # Prefer the ontology-expanded schema so reachable_from enums are materialized.
        input_source = m.get("input_schema_expanded") or m.get("input_schema")
        input_schema = linkml_to_jsonschema(input_source)
        output_schema = linkml_to_jsonschema(m.get("output_schema"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to convert LinkML schema: {e}")
    return {
        "image": m["image"],
        "input_schema": input_schema,
        "output_schema": output_schema,
    }
```

- [ ] **Step 2: Add the router proxy** — in `app/router/router/routers/modeling.py`, insert after the `model_info` handler (ends line 145), before `@router.post("/models")` (line 148)

```python
@router.get("/models/{image_tag}/jsonschema", response_class=JSONResponse)
async def model_jsonschema(image_tag: str = Path(..., example="coxcopdmodel:latest")):
    """
    Get a model's input/output schemas as JSON Schema (converted from LinkML server-side).
    """
    url = f"{settings.model_server_url}/models/{image_tag}/jsonschema"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Model server error: {e.response.text}")
        detail = e.response.text or f"Error fetching jsonschema for {image_tag}"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except httpx.RequestError as e:
        logger.error(f"Error fetching jsonschema for {image_tag}: {e}")
        raise HTTPException(status_code=500, detail="Model server unreachable")
```

- [ ] **Step 3: Write the model_server smoke test** — `ci/model_jsonschema_smoke.sh`

This targets the model_server **directly** (matching `ci/model_registry_prediction_smoke.sh`), so it fits inside `model_validation.sh` where only the model_server is up. The `coxcopdmodel:latest` tag contains a colon, which is legal in a URL path segment — no encoding needed (the existing model detail calls use it raw).

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODEL_SERVER_PORT="${MODEL_SERVER_PORT:-8004}"
MODEL_SERVER_BASE_URL="${MODEL_SERVER_BASE_URL:-http://localhost:${MODEL_SERVER_PORT}}"
IMAGE_TAG="${IMAGE_TAG:-coxcopdmodel:latest}"

# shellcheck source=ci/lib.sh
source "$ROOT_DIR/ci/lib.sh"

require_cmd curl
require_cmd jq

url="${MODEL_SERVER_BASE_URL}/models/${IMAGE_TAG}/jsonschema"
log "Fetching JSON Schema via ${url}"
schema_json="$(curl -fsS "$url")"

input_type="$(echo "$schema_json" | jq -r '.input_schema.type')"
if [ "$input_type" != "object" ]; then
  error "Expected input_schema.type == object, got: $input_type"
  exit 1
fi

props_count="$(echo "$schema_json" | jq '.input_schema.properties | length')"
if [ "$props_count" -lt 1 ]; then
  error "Expected input_schema.properties to be non-empty"
  exit 1
fi

# The Cox model's sex_at_birth is an enum -> must convert to a JSON Schema enum array.
enum_len="$(echo "$schema_json" | jq '.input_schema.properties.sex_at_birth.enum | length')"
if [ "$enum_len" -lt 2 ]; then
  error "Expected sex_at_birth to be an enum with >=2 values, got length: $enum_len"
  exit 1
fi

log "JSON Schema smoke test passed (properties=${props_count}, sex_at_birth enum=${enum_len})"
```

- [ ] **Step 4: Invoke it from `ci/model_validation.sh`**

In `ci/model_validation.sh`, after line 47 (the `"$ROOT_DIR/ci/model_registry_prediction_smoke.sh"` invocation, while the model_server is still up), add:

```bash
log "Running model JSON Schema smoke checks"
"$ROOT_DIR/ci/model_jsonschema_smoke.sh"
```

- [ ] **Step 5: Add the router proxy assertion to `ci/router_validation.sh`**

In `ci/router_validation.sh`, immediately after the `if [ "$attempt" -gt "$max_attempts" ]; then ... fi` block that verifies `/modeling/models` reports `coxcopdmodel:latest`, and BEFORE the `prediction_payload='{` line, insert:

```bash
log "Checking router modeling jsonschema endpoint"
schema_json="$(curl -fsS "${ROUTER_BASE_URL}/modeling/models/coxcopdmodel:latest/jsonschema")"
if ! echo "$schema_json" | jq -e '
  (.input_schema.type == "object")
  and (.input_schema.properties | length >= 1)
  and (.input_schema.properties.sex_at_birth.enum | length >= 2)
' >/dev/null; then
  error "Router /modeling/models/{tag}/jsonschema validation failed: ${schema_json}"
  exit 1
fi
```

- [ ] **Step 6: Make the smoke script executable**

Run: `chmod +x ci/model_jsonschema_smoke.sh`

- [ ] **Step 7: Run the model validation target to verify end-to-end**

Run: `bash ci/run.sh model-validation`
Expected: existing checks pass AND `JSON Schema smoke test passed (...)` appears. (This target builds/brings up the model_server and tears it down via its own trap. Requires docker.)

Optionally verify the proxy too: `bash ci/run.sh router-validation` (heavier — brings up the full stack).

- [ ] **Step 8: Commit**

```bash
git add app/model_server/model_server/main.py app/router/router/routers/modeling.py ci/model_jsonschema_smoke.sh ci/model_validation.sh ci/router_validation.sh
git commit -m "feat(modeling): expose LinkML-derived JSON Schema via model_server and router"
```

---

### Task 3: Frontend dependencies + shared model API module

**Files:**
- Modify: `app/vueapp_guided/package.json` (add JSONForms deps)
- Create: `app/vueapp_guided/src/api/models.js`

**Interfaces:**
- Consumes: `store.apiBase` from `src/state.js`; `axios`.
- Produces `src/api/models.js` named exports:
  - `listModels(): Promise<Array>` → `GET /modeling/models`
  - `getModel(imageTag): Promise<Object>` → `GET /modeling/models/{tag}`
  - `getModelJsonSchema(imageTag): Promise<{image, input_schema, output_schema}>` → `GET /modeling/models/{tag}/jsonschema`
  - `predict(image, input): Promise<{predictions, stdout, stderr}>` → `POST /modeling/predict` with `{image, input}`
  - `registerModel(payload): Promise<Object>` → `POST /modeling/models`

- [ ] **Step 1: Add dependencies** — in `app/vueapp_guided/package.json`, add to `"dependencies"`:

```json
"@jsonforms/vue": "^3.2.1",
"@jsonforms/vue-vanilla": "^3.2.1"
```

- [ ] **Step 2: Install**

Run: `cd app/vueapp_guided && npm install`
Expected: installs without peer-dependency errors (JSONForms 3.x supports Vue 3).

- [ ] **Step 3: Create the API module** — `app/vueapp_guided/src/api/models.js`

```js
import axios from 'axios'
import { store } from '../state.js'

function base() {
  return store.apiBase
}

export async function listModels() {
  const { data } = await axios.get(`${base()}/modeling/models`)
  // model_server returns a JSON array of model summaries.
  return Array.isArray(data) ? data : (data.models ?? [])
}

export async function getModel(imageTag) {
  const { data } = await axios.get(
    `${base()}/modeling/models/${encodeURIComponent(imageTag)}`,
  )
  return data
}

export async function getModelJsonSchema(imageTag) {
  const { data } = await axios.get(
    `${base()}/modeling/models/${encodeURIComponent(imageTag)}/jsonschema`,
  )
  return data
}

export async function predict(image, input) {
  const { data } = await axios.post(`${base()}/modeling/predict`, { image, input })
  return data
}

export async function registerModel(payload) {
  const { data } = await axios.post(`${base()}/modeling/models`, payload)
  return data
}
```

- [ ] **Step 4: Verify the build compiles**

Run: `cd app/vueapp_guided && npm run build`
Expected: build succeeds (no import/syntax errors). This is the available verification — the app has no unit-test harness.

- [ ] **Step 5: Commit**

```bash
git add app/vueapp_guided/package.json app/vueapp_guided/package-lock.json app/vueapp_guided/src/api/models.js
git commit -m "feat(vueapp_guided): add JSONForms deps and shared model API module"
```

---

### Task 4: New "Run a prediction" flow (schema-driven form)

**Files:**
- Create: `app/vueapp_guided/src/flows/RunPredictionFlow.vue`
- Modify: `app/vueapp_guided/src/state.js` (add a `run` entry to `FLOWS`)
- Modify: `app/vueapp_guided/src/App.vue` (add `run` → `RunPredictionFlow` to `flowMap`)

**Interfaces:**
- Consumes: `listModels`, `getModel`, `getModelJsonSchema`, `predict` from `src/api/models.js` (Task 3); `Wizard.vue`; `store`, `goHome` from `src/state.js`.
- Produces: a flow registered under id `'run'`.

- [ ] **Step 1: Register the flow in `src/state.js`** — add this object to the `FLOWS` array, immediately after the `apply` entry (keep it in the "Predictive models" category):

```js
  {
    id: 'run',
    title: 'Run a prediction',
    subtitle: 'Pick a model, fill in its inputs from an auto-generated form, see the result',
    icon: '🔮',
    accent: '#f97316',
    category: 'Predictive models',
  },
```

Also update the `currentFlow` comment near the top of `state.js` to include `'run'` in the list of valid flow ids.

- [ ] **Step 2: Register the component in `src/App.vue`** — import it and add to `flowMap`:

```js
import RunPredictionFlow from './flows/RunPredictionFlow.vue'
```

Add `run: RunPredictionFlow,` to the `flowMap` object alongside the other flows.

- [ ] **Step 3: Create the flow** — `app/vueapp_guided/src/flows/RunPredictionFlow.vue`

```vue
<template>
  <Wizard
    title="Run a prediction"
    subtitle="Pick a registered model, fill in its inputs, and run it on a single record."
    accent="#f97316"
    :steps="steps"
    finishLabel="Done"
    @finish="onFinish"
  >
    <!-- Step 0: pick a model -->
    <template #step-0>
      <h2>Pick a model</h2>
      <p v-if="loadingModels" class="muted">Loading models…</p>
      <p v-else-if="modelsError" class="error">{{ modelsError }}</p>
      <p v-else-if="!models.length" class="muted">No models are registered yet.</p>
      <div v-else class="model-grid">
        <button
          v-for="m in models"
          :key="m.image"
          class="model-card"
          :class="{ selected: data.image === m.image }"
          @click="selectModel(m.image)"
        >
          <div class="model-card-title">{{ m.title || m.image }}</div>
          <div class="pill">{{ m.image }}</div>
          <p class="model-card-sub muted">{{ m.short_description }}</p>
        </button>
      </div>
    </template>

    <!-- Step 1: fill inputs (schema-driven) -->
    <template #step-1>
      <h2>Inputs for {{ data.image }}</h2>
      <p v-if="loadingSchema" class="muted">Loading schema…</p>
      <p v-else-if="schemaError" class="error">{{ schemaError }}</p>
      <template v-else-if="inputSchema">
        <div class="form-actions">
          <button class="ghost" type="button" @click="useExample">Use example</button>
        </div>
        <div class="jsonforms-host">
          <JsonForms
            :data="data.formData"
            :schema="inputSchema"
            :renderers="renderers"
            @change="onFormChange"
          />
        </div>
        <p v-if="data.formErrors.length" class="muted">
          {{ data.formErrors.length }} field(s) need attention before running.
        </p>
      </template>
    </template>

    <!-- Step 2: result -->
    <template #step-2>
      <h2>Prediction</h2>
      <div class="form-actions">
        <button
          class="primary"
          type="button"
          :disabled="running || data.formErrors.length > 0"
          @click="runPrediction"
        >
          {{ running ? 'Running…' : 'Run prediction' }}
        </button>
      </div>
      <p v-if="runError" class="error">{{ runError }}</p>

      <table v-if="predictionRows.length" class="results-table">
        <thead>
          <tr><th v-for="c in resultColumns" :key="c">{{ c }}</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, i) in predictionRows" :key="i">
            <td v-for="c in resultColumns" :key="c">{{ formatCell(row[c]) }}</td>
          </tr>
        </tbody>
      </table>

      <details v-if="lastResult">
        <summary>Raw output</summary>
        <pre class="schema">{{ JSON.stringify(lastResult.predictions, null, 2) }}</pre>
      </details>
      <details v-if="lastResult && lastResult.stderr">
        <summary>Model logs (stderr)</summary>
        <pre class="schema">{{ lastResult.stderr }}</pre>
      </details>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, onMounted } from 'vue'
import { JsonForms } from '@jsonforms/vue'
import { vanillaRenderers } from '@jsonforms/vue-vanilla'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'
import { listModels, getModel, getModelJsonSchema, predict } from '../api/models.js'

const renderers = Object.freeze([...vanillaRenderers])

const steps = [{ label: 'Model' }, { label: 'Inputs' }, { label: 'Result' }]

const data = store.flowData.run ?? reactive({
  image: '',
  formData: {},
  formErrors: [],
})
store.flowData.run = data

const models = ref([])
const loadingModels = ref(false)
const modelsError = ref('')

const inputSchema = ref(null)
const outputSchema = ref(null)
const loadingSchema = ref(false)
const schemaError = ref('')

const running = ref(false)
const runError = ref('')
const lastResult = ref(null)

onMounted(async () => {
  loadingModels.value = true
  modelsError.value = ''
  try {
    models.value = await listModels()
  } catch (e) {
    modelsError.value = errText(e, 'Failed to load models')
  } finally {
    loadingModels.value = false
  }
})

async function selectModel(image) {
  data.image = image
  data.formData = {}
  data.formErrors = []
  lastResult.value = null
  inputSchema.value = null
  outputSchema.value = null
  loadingSchema.value = true
  schemaError.value = ''
  try {
    const js = await getModelJsonSchema(image)
    inputSchema.value = js.input_schema
    outputSchema.value = js.output_schema
  } catch (e) {
    schemaError.value = errText(e, 'Failed to load model schema')
  } finally {
    loadingSchema.value = false
  }
}

function onFormChange(event) {
  data.formData = event.data
  data.formErrors = event.errors ?? []
}

async function useExample() {
  try {
    const detail = await getModel(data.image)
    const ex = detail.examples && detail.examples[0]
    if (ex) data.formData = { ...ex }
  } catch (e) {
    schemaError.value = errText(e, 'Failed to load example')
  }
}

async function runPrediction() {
  running.value = true
  runError.value = ''
  lastResult.value = null
  try {
    lastResult.value = await predict(data.image, [data.formData])
  } catch (e) {
    runError.value = errText(e, 'Prediction failed')
  } finally {
    running.value = false
  }
}

const predictionRows = computed(() => {
  const preds = lastResult.value?.predictions
  if (!Array.isArray(preds)) return []
  return preds.map((p) => (p && typeof p === 'object' && !Array.isArray(p) ? p : { value: p }))
})

const resultColumns = computed(() => {
  const fromSchema = outputSchema.value?.properties
    ? Object.keys(outputSchema.value.properties)
    : []
  if (fromSchema.length) return fromSchema
  const first = predictionRows.value[0]
  return first ? Object.keys(first) : []
})

function formatCell(v) {
  if (v === null || v === undefined) return ''
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function errText(e, fallback) {
  return e?.response?.data?.detail || e?.message || fallback
}

function onFinish() {
  goHome()
}
</script>

<style scoped>
.model-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 1rem;
  margin-top: 0.5rem;
}
.model-card {
  text-align: left;
  padding: 1rem 1.2rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  cursor: pointer;
  color: inherit;
  transition: border-color 0.15s ease;
}
.model-card:hover { border-color: #f97316; }
.model-card.selected { border-color: #f97316; background: var(--surface-alt); }
.model-card-title { font-weight: 600; margin-bottom: 0.3rem; }
.model-card-sub { font-size: 0.9rem; margin-top: 0.4rem; }
.form-actions { display: flex; gap: 0.5rem; margin: 0.5rem 0 1rem; }
.results-table { width: 100%; border-collapse: collapse; margin-top: 0.8rem; }
.results-table th, .results-table td {
  padding: 0.55rem 0.7rem; text-align: left;
  border-bottom: 1px solid var(--border); font-size: 0.9rem;
}
.results-table th {
  font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;
  color: var(--text-muted); font-weight: 500; border-bottom-width: 2px;
}
.schema {
  margin: 0.4rem 0 0; padding: 0.6rem 0.8rem;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius-sm); font-size: 0.8rem; overflow-x: auto;
}
.error { color: #dc2626; }
</style>
```

- [ ] **Step 4: Verify the build compiles**

Run: `cd app/vueapp_guided && npm run build`
Expected: build succeeds.

- [ ] **Step 5: Manual smoke (if the stack is up)**

Run: `cd app/vueapp_guided && npm run dev`, open the dev URL, choose **Run a prediction**, pick the iris or cox demo model, click **Use example**, advance to Result, click **Run prediction**, confirm a result table renders. (Requires router + model_server running with built-in models; skip if unavailable and note it.)

- [ ] **Step 6: Commit**

```bash
git add app/vueapp_guided/src/flows/RunPredictionFlow.vue app/vueapp_guided/src/state.js app/vueapp_guided/src/App.vue
git commit -m "feat(vueapp_guided): add schema-driven Run a prediction flow"
```

---

### Task 5: Wire ModelStudioFlow browse + register to real endpoints

**Files:**
- Modify: `app/vueapp_guided/src/flows/ModelStudioFlow.vue`

**Interfaces:**
- Consumes: `listModels`, `getModel`, `registerModel` from `src/api/models.js` (Task 3).
- Produces: no new exports; replaces mock data/`alert()` with live calls.

- [ ] **Step 1: Replace the mock browse list.** In `ModelStudioFlow.vue`, remove the hard-coded `mockModels` array and instead load models on mount. **Replace** the existing `import { reactive } from 'vue'` line (do not add a second `vue` import) with these two lines:

```js
import { reactive, ref, onMounted } from 'vue'
import { listModels, getModel, registerModel } from '../api/models.js'

const models = ref([])
const loadError = ref('')

onMounted(async () => {
  try {
    models.value = await listModels()
  } catch (e) {
    loadError.value = e?.response?.data?.detail || e?.message || 'Failed to load models'
  }
})
```

Update the browse template (step 1, `data.mode === 'browse'`) to iterate `models` instead of `mockModels`, remove the "Stub" banner, and render `m.title`, `m.image`, `m.authors`, `m.short_description`. On expand, fetch details lazily:

```js
async function toggleExpand(image) {
  if (data.expandedModel === image) { data.expandedModel = null; return }
  data.expandedModel = image
  if (!data.details[image]) {
    try {
      data.details[image] = await getModel(image)
    } catch (e) {
      data.details[image] = { _error: e?.response?.data?.detail || e?.message || 'Failed to load model' }
    }
  }
}
```

In the expanded detail block, render `data.details[image]?.readme`, and the input/output schemas from `data.details[image]` (show as JSON in a `<pre class="schema">`), replacing the `inputSchemaStub`/`outputSchemaStub` mock fields.

- [ ] **Step 2: Extend the flow's reactive data** to hold fetched details. Update the `data` initializer to include a `details` map:

```js
const data = store.flowData.models ?? reactive({
  mode: 'browse',
  expandedModel: null,
  details: {},
  reg: {
    image: '', title: '', description: '', authors: '', readme: '',
    inputSchema: '', outputSchema: '', examples: '',
  },
})
store.flowData.models = data
```

- [ ] **Step 3: Wire registration.** Replace `onFinish`'s `alert('Submit registration (stub).')` with a real call that builds the `RegisterRequest` payload. JSON textareas are parsed leniently (blank → omitted):

```js
const regResult = ref(null)
const regError = ref('')

function parseMaybeJson(text) {
  const t = (text || '').trim()
  if (!t) return undefined
  return JSON.parse(t)  // may throw; caught in onFinish
}

async function onFinish() {
  if (data.mode !== 'register') { goHome(); return }
  regError.value = ''
  regResult.value = null
  try {
    const payload = {
      image: data.reg.image,
      title: data.reg.title,
      short_description: data.reg.description,
      authors: data.reg.authors,
    }
    if (data.reg.readme?.trim()) payload.readme = data.reg.readme
    const inputSchema = parseMaybeJson(data.reg.inputSchema)
    if (inputSchema) payload.input_schema = inputSchema
    const outputSchema = parseMaybeJson(data.reg.outputSchema)
    if (outputSchema) payload.output_schema = outputSchema
    const examples = parseMaybeJson(data.reg.examples)
    if (examples) payload.examples = examples

    regResult.value = await registerModel(payload)
  } catch (e) {
    regError.value = e?.response?.data?.detail || e?.message || 'Registration failed'
  }
}
```

Note: do NOT call `goHome()` on the register path — keep the wizard open so the result can be shown.

- [ ] **Step 4: Show registration outcome.** In the register step-2 template, add below the fields:

```html
<p v-if="regError" class="error">{{ regError }}</p>
<div v-if="regResult" class="reg-result">
  <p><strong>Registered {{ regResult.image }}.</strong></p>
  <details v-if="regResult.example_predictions">
    <summary>Example predictions</summary>
    <pre class="schema">{{ JSON.stringify(regResult.example_predictions, null, 2) }}</pre>
  </details>
  <details v-if="regResult.registration_logs">
    <summary>Registration logs</summary>
    <pre class="schema">{{ regResult.registration_logs.stdout }}{{ regResult.registration_logs.stderr }}</pre>
  </details>
  <button class="ghost" type="button" @click="goHome">Done</button>
</div>
```

Keep the existing caveat text that the Docker image must already exist on the model_server host (add a `<p class="muted">` note near the image-tag field if not present).

- [ ] **Step 5: Verify the build compiles**

Run: `cd app/vueapp_guided && npm run build`
Expected: build succeeds.

- [ ] **Step 6: Commit**

```bash
git add app/vueapp_guided/src/flows/ModelStudioFlow.vue
git commit -m "feat(vueapp_guided): wire model browse and register to real endpoints"
```

---

### Task 6: Style the JSONForms vue-vanilla renderers to the design tokens

**Files:**
- Create: `app/vueapp_guided/src/styles/jsonforms.css`
- Modify: `app/vueapp_guided/src/main.js` (import the stylesheet globally)

**Interfaces:**
- Consumes: the vue-vanilla renderer DOM/class names emitted by `<JsonForms>` in Task 4.
- Produces: global styling so generated forms match the app.

- [ ] **Step 1: Create the stylesheet** — `app/vueapp_guided/src/styles/jsonforms.css`

vue-vanilla emits class names such as `vertical-layout`, `vertical-layout-item`, `control`, `control-label`, `input`, `select`, and validation classes. Style them with the app tokens:

```css
.jsonforms-host .vertical-layout { display: flex; flex-direction: column; gap: 0.9rem; }
.jsonforms-host .control { display: flex; flex-direction: column; gap: 0.3rem; }
.jsonforms-host .control-label { font-size: 0.9rem; color: var(--text); font-weight: 500; }
.jsonforms-host .control-label.required::after { content: ' *'; color: #dc2626; }
.jsonforms-host input,
.jsonforms-host select,
.jsonforms-host textarea {
  padding: 0.5rem 0.6rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--surface);
  color: var(--text);
  font: inherit;
}
.jsonforms-host input:focus,
.jsonforms-host select:focus { outline: 2px solid #f97316; outline-offset: 0; }
.jsonforms-host .control-description { font-size: 0.8rem; color: var(--text-muted); }
.jsonforms-host .validation { font-size: 0.8rem; color: #dc2626; }
```

- [ ] **Step 2: Import it globally** — in `app/vueapp_guided/src/main.js`, add near the existing `import './style.css'`:

```js
import './styles/jsonforms.css'
```

- [ ] **Step 3: Verify the build compiles and inspect visually**

Run: `cd app/vueapp_guided && npm run build`, then `npm run dev` and open the Run a prediction → Inputs step for a demo model. Confirm labels, inputs, selects (enum fields), and required markers render in the app's style. Adjust class selectors if vue-vanilla's actual emitted class names differ (inspect the DOM; the exact names are version-specific).

- [ ] **Step 4: Commit**

```bash
git add app/vueapp_guided/src/styles/jsonforms.css app/vueapp_guided/src/main.js
git commit -m "style(vueapp_guided): theme JSONForms vue-vanilla to design tokens"
```

---

## Notes for the implementer

- **Verification honesty:** the Vue app has no automated test harness, so frontend tasks verify via `npm run build` (catches import/syntax errors only) plus manual smoke through the running stack. Do not claim a flow "works" without either running it or explicitly stating it was not run.
- **vue-vanilla class names are version-specific.** Task 6 lists the common ones; confirm against the installed version's DOM and adjust selectors rather than assuming.
- **Enum coverage depends on the expanded schema.** Task 2 converts `input_schema_expanded or input_schema`; a model whose enums come from `reachable_from` ontologies only renders full dropdowns if the model server successfully expanded them at registration.
- **Design spec:** `docs/superpowers/specs/2026-08-18-model-serving-wizard-ui-design.md`.
