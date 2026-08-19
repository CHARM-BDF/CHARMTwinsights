# CHARMTwinsights Guided Flow UI (prototype)

A Vue 3 + Vite single-page app that reorganizes CHARMTwinsights functionality into
**goal-driven guided flows** rather than service-oriented menus.

This is a new, separate app living alongside the existing `vueapp/`. Nothing in that
directory is modified.

## Status

Shell, wizard framework, and most flows fully wired to the router API. Per flow
(landing-page order):

| Flow | Status |
|---|---|
| Ingest external FHIR data | ✅ wired (`POST /ingest/fhir`) |
| Export FHIR data | ✅ wired. Whole store or selected cohorts, zipped: per-type NDJSON (Bulk Data layout), per-patient bundles (Synthea-style), or flat CSV |
| Generate synthetic FHIR cohort (Synthea) | ✅ wired (job submit + progress polling) |
| Generate synthetic ICU vitals (TimeAutoDiff) | ✅ wired (single + batch, client-side charts) |
| Browse cohorts & patients | ✅ wired (analytics, drill-down, timeline, FHIR/PDF export, delete) |
| Find digital twins | ✅ wired. Subject picker, attribute checkboxes, search existing data via `POST /twins/find`, or generate a candidate cohort first |
| Browse & register models | ✅ wired. Lists the registry (`GET /modeling/models`), renders each model's README (markdown) and input/output schemas (highlighted JSON); registers new models (`POST /modeling/models`) |
| Run a prediction | ✅ wired. Pick a model, fill a **schema-driven form** auto-generated from its input schema, then run a single-record `POST /modeling/predict`; includes a copy-paste **curl** panel showing the exact request |
| Apply a model to a cohort | 🚫 **temporarily hidden**. Flow file retained (`ApplyModelFlow.vue`), but the tile is commented out in `state.js` — it needs an authoritative FHIR→model feature-mapping design before it can be wired |
| AI assistant (MCP console) | 🚧 **future work**. Placeholder only, needs an LLM↔MCP bridge |

### Model serving

The model flows rely on a small server-side addition: the `model_server` converts each
model's LinkML input/output schema to JSON Schema (`GET /modeling/models/{tag}/jsonschema`,
proxied by the router). The UI hands that JSON Schema to **JSONForms** (vue-vanilla
renderers), so the input form — field types, enum dropdowns, per-field hints, required-field
validation — is generated entirely from the model's declared contract, with no per-model
code. Because the model server is primarily an API for other clients, the Run-a-prediction
flow also surfaces the equivalent `curl` request.

## Run locally

```bash
cd app/vueapp_guided
npm install
npm run dev
```

The dev server listens on **http://localhost:5174** (the existing vueapp uses 5173). In the
full Docker stack it is served at **http://localhost:8007**.

`VITE_API_BASE` env var overrides the default router base URL (`http://localhost:8000`).

## Structure

```
src/
  main.js                # entry (imports the global stylesheets below)
  style.css              # global styles + design tokens
  App.vue                # shell, routes landing vs active flow
  state.js               # shared reactive store + FLOWS metadata
  api/
    models.js            # model-serving API calls (list / get / jsonschema / predict / register)
  utils/
    format.js            # markdown (marked + DOMPurify) and JSON syntax highlighting
  styles/
    jsonforms.css        # theming for the JSONForms vue-vanilla form renderers
    content.css          # styling for rendered markdown + highlighted JSON
  components/
    TopBar.vue           # header with home button + breadcrumbs
    Landing.vue          # flow tiles, grouped by category
    Wizard.vue           # generic step container w/ progress, Back/Next
    StatusPill.vue       # (stub) health indicator
    SampleDataBanner.vue # offer to load sample data
  flows/
    IngestionFlow.vue
    ExportFlow.vue
    SyntheaFhirFlow.vue
    IcuVitalsFlow.vue
    BrowsingFlow.vue
    DigitalTwinFlow.vue
    ModelStudioFlow.vue      # browse & register models
    RunPredictionFlow.vue    # schema-driven single-record prediction
    ApplyModelFlow.vue       # cohort-apply (currently hidden from the landing)
    AgentConsoleFlow.vue
```

## Key dependencies

- **@jsonforms/vue** + **@jsonforms/vue-vanilla** — schema-driven form rendering
- **marked** + **dompurify** — safe markdown rendering for model READMEs
- **axios** — HTTP
