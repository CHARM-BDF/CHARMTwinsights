# CHARMTwinsights Guided Flow UI (prototype)

A Vue 3 + Vite single-page app that reorganizes CHARMTwinsights functionality into
**goal-driven guided flows** rather than service-oriented menus.

This is a new, separate app living alongside the existing `vueapp/`. Nothing in that
directory is modified.

## Status

Shell, wizard framework, and several fully wired flows. Per flow:

| Flow | Status |
|---|---|
| Generate synthetic FHIR cohort (Synthea) | ✅ wired (job submit + progress polling) |
| Browse cohorts & patients | ✅ wired (analytics, drill-down, timeline, FHIR/PDF export, delete) |
| Ingest external FHIR data | ✅ wired |
| Export FHIR data | ✅ wired. Whole store or selected cohorts, zipped: per-type NDJSON (Bulk Data layout), per-patient bundles (Synthea-style), or flat CSV |
| Generate synthetic ICU vitals (TimeAutoDiff) | ✅ wired (single + batch, client-side charts) |
| Browse & register models | 🔴 mockup. Backend endpoints exist, wiring pending |
| Apply a model to a cohort | 🔴 mockup. Needs feature-mapping + batch-predict backend |
| Find digital twins | ✅ wired. Subject picker, attribute checkboxes, search existing data via `POST /twins/find`, or generate a candidate cohort first |
| AI assistant (MCP console) | 🚧 **future work**. Placeholder only, needs an LLM↔MCP bridge |

## Run locally

```bash
cd app/vueapp_guided
npm install
npm run dev
```

The dev server listens on **http://localhost:5174** (the existing vueapp uses 5173).

`VITE_API_BASE` env var overrides the default router base URL (`http://localhost:8000`).

## Structure

```
src/
  main.js              # entry
  style.css            # global styles
  App.vue              # shell, routes landing vs active flow
  state.js             # shared reactive store (currentFlow, step, data)
  components/
    TopBar.vue         # header with home button + breadcrumbs
    Landing.vue        # 7 flow tiles
    Wizard.vue         # generic step container w/ progress, Back/Next
    StatusPill.vue     # (stub) health indicator
  flows/
    SyntheaFhirFlow.vue
    IcuVitalsFlow.vue
    BrowsingFlow.vue
    IngestionFlow.vue
    ExportFlow.vue
    DigitalTwinFlow.vue
    ModelStudioFlow.vue
    AgentConsoleFlow.vue
```
