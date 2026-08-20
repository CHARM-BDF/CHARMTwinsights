# Model Serving in the Guided Wizard UI — Design

**Date:** 2026-08-18
**Branch:** `adding_guided_flow_UI`
**Status:** Approved (spec review waived by user)

## Goal

Wire real model-serving into the Vue guided wizard (`app/vueapp_guided`), replacing the
current mock stubs. The central hard problem is **adapting the input form to a model whose
input fields are not known a priori** — the model server declares its input contract at
runtime as a LinkML schema, and the UI must generate a matching form.

The chosen strategy: **convert LinkML → JSON Schema server-side**, and render it in the
wizard with an **off-the-shelf JSON-Schema form library** (JSONForms + vue-vanilla).

## Non-goals (explicitly out of scope)

- **Cohort-apply / FHIR→model feature mapping.** We do not know a priori how to map patient
  FHIR resources to model input fields, and no metadata exists to derive it. `ApplyModelFlow.vue`
  stays a mock placeholder for future work.
- **Batch / multi-record input.** MVP submits a single input record. The `/predict` API is
  batch-capable (`input` is an array), so multi-record entry is a later, easy extension.
- **Model image upload from the browser.** Registration assumes the Docker image already exists
  on the model_server host; the UI only sends metadata/schemas.
- **A frontend test harness.** `vueapp_guided` has none today; we do not stand one up here.

## Background (verified)

- Router (`app/router`, host `:8000`) is a thin FastAPI gateway; every `/modeling/*` handler
  in `app/router/router/routers/modeling.py` is a short httpx pass-through to the model_server
  (`settings.model_server_url`, default `http://model_server:8000`, host-published `:8004`).
- Existing model routes: `GET /modeling/models`, `GET /modeling/models/{tag}`,
  `POST /modeling/models`, `POST /modeling/predict`, `GET /modeling/health`.
- The model_server stores model metadata (incl. LinkML `input_schema` and the ontology-expanded
  `input_schema_expanded`) in MongoDB. `parse_schema` (LinkML string→dict) already exists in
  `app/model_server/model_server/validation.py:52` and is imported by `main.py`.
- A working `linkml_to_jsonschema()` converter + `GET /models/{tag}/jsonschema` endpoint exist
  **only on the `modelui` branch** (`app/model_server/model_server/main.py`). They are the assets
  to promote. That branch's React app feeds the JSON Schema straight to `@rjsf` — the pattern we
  replicate in Vue.
- Testing is shell/CI based: `ci/model_validation.sh`, `ci/model_registry_prediction_smoke.sh`.

## Architecture & data flow

```
Vue wizard (vueapp_guided)              router (:8000)                model_server (:8004)
  RunPredictionFlow  ──► GET  /modeling/models                 ──► GET  /models
                     ──► GET  /modeling/models/{tag}/jsonschema ──► GET  /models/{tag}/jsonschema  ★NEW
                     ──► POST /modeling/predict                 ──► POST /predict
  ModelStudioFlow    ──► GET  /modeling/models                 ──► GET  /models
   (browse+register) ──► GET  /modeling/models/{tag}            ──► GET  /models/{tag}
                     ──► POST /modeling/models                  ──► POST /models
```

The router stays a dumb proxy. All LinkML→JSON Schema intelligence lives once, in the
model_server, where the schemas already are. The Vue side stays thin: fetch JSON Schema →
hand to JSONForms.

## Backend changes

### 1. model_server — `app/model_server/model_server/main.py`
- Port `linkml_to_jsonschema(schema: Optional[str]) -> Optional[Dict]` and its inner helpers
  (`range_to_schema`, `ensure_class`, `coerce_text`) from the `modelui` branch verbatim; it
  depends only on `parse_schema`, already available.
- Add route `GET /models/{image_tag}/jsonschema`:
  - Look up the model doc; 404 if not registered.
  - Convert **`input_schema_expanded or input_schema`** (prefer expanded so `reachable_from`
    enums are fully materialized) and `output_schema` via `linkml_to_jsonschema`.
  - Return `{ "image": image_tag, "input_schema": <draft-07>, "output_schema": <draft-07> }`.

### 2. router — `app/router/router/routers/modeling.py`
- Add `GET /modeling/models/{image_tag}/jsonschema`, mirroring the existing `model_info`
  handler: httpx GET to `{settings.model_server_url}/models/{image_tag}/jsonschema`, 15s timeout,
  same `HTTPStatusError` / `RequestError` mapping (propagate status; 500 "Model server
  unreachable").

## Frontend changes — `app/vueapp_guided`

### Dependencies
- Add `@jsonforms/vue` and `@jsonforms/vue-vanilla` to `package.json`.

### Shared API module — `src/api/models.js` (new)
Thin axios helpers against `store.apiBase`, shared by both flows (justified departure from the
inline-axios convention, to avoid duplication across two flows):
- `listModels()` → `GET /modeling/models`
- `getModel(imageTag)` → `GET /modeling/models/{tag}`
- `getModelJsonSchema(imageTag)` → `GET /modeling/models/{tag}/jsonschema`
- `predict(image, input)` → `POST /modeling/predict`
- `registerModel(payload)` → `POST /modeling/models`

### New flow — `src/flows/RunPredictionFlow.vue`
Register in `src/state.js` `FLOWS` (new id `run`, category "Predictive models") and in
`App.vue`'s `flowMap`. Three wizard steps:
1. **Pick model** — `listModels()`; selectable cards (image, title, short_description).
2. **Fill inputs** — on select, `getModelJsonSchema(tag)`; render `input_schema` with JSONForms.
   "Use example" button prefills form data from `getModel(tag).examples[0]`. AJV validation
   (via JSONForms) gates advancing.
3. **Result** — `predict(image, [record])`; render `predictions` as a table (columns from the
   output-schema field names) with a raw-JSON fallback; show model `stderr` in a collapsible.

### Wire existing `src/flows/ModelStudioFlow.vue`
- **Browse tab**: replace `mockModels` with `listModels()`; on expand, `getModel(tag)` for
  README + schemas (render schema summary; JSON is fine).
- **Register tab**: `registerModel(payload)` with the existing `RegisterRequest`-shaped form;
  surface returned `registration_logs` and `example_predictions` as confirmation. Keep the
  visible caveat that the Docker image must already exist on the model_server host.

### Styling
Scoped stylesheet mapping vue-vanilla CSS classes (`.vertical-layout`, `.control`, `.input`,
etc.) to the app's existing design tokens so generated forms match the hand-rolled look.

## Error handling

- JSONForms/AJV validation errors shown inline before submit.
- Backend `400` (schema validation failure) and `500` (model server unreachable) surfaced as
  error banners showing the returned `detail`.
- Empty model list → friendly empty-state; unreachable model server → error banner.

## Testing

- **Backend (CI):** extend `ci/model_validation.sh` (or add a sibling script) to curl
  `GET /modeling/models/{tag}/jsonschema` for a known built-in model and assert: response is a
  JSON object with a `properties` map, and a known enum field (e.g. Cox model `sex_at_birth`)
  is rendered as an `enum` array. Keep it consistent with existing shell-based CI.
- **Frontend:** no harness exists; verify manually via the `run` skill — launch the guided app,
  open Run a prediction, select a demo model (iris or cox), prefill the example, submit, confirm
  the prediction renders. Documented as a known gap, not addressed in this build.

## Components & boundaries

- `linkml_to_jsonschema` (model_server): pure function, LinkML dict/string → draft-07 JSON
  Schema. Testable in isolation.
- `GET /models/{tag}/jsonschema` (model_server) + router proxy: read-only, no side effects.
- `src/api/models.js`: single boundary for all model HTTP calls.
- `RunPredictionFlow.vue`: owns only run-flow state; delegates form rendering to JSONForms.
- `ModelStudioFlow.vue`: browse/register only; unchanged wizard structure, mocks swapped for
  real calls.

## Open risks

- vue-vanilla's default widgets/styling are basic; the token-mapping stylesheet is the main
  polish effort.
- LinkML→JSON Schema is intentionally lossy (first-class-only; patterns/min-max/unions degrade
  to string). Acceptable for current models; note it for future schema features.
