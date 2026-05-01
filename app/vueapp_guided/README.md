# CHARMTwinsights — Guided Flow UI (prototype)

A Vue 3 + Vite single-page app that reorganizes CHARMTwinsights functionality into
**goal-driven guided flows** rather than service-oriented menus.

This is a new, separate app living alongside the existing `vueapp/` — nothing in that
directory is modified.

## Status

Navigation skeleton only. Most steps are stubs pending backend wiring.

## Flows

1. **Generate synthetic FHIR cohort** (Synthea)
2. **Generate synthetic ICU vitals** (TimeAutoDiff)
3. **Browse cohorts & patients** (includes FHIR / PDF export)
4. **Ingest external FHIR data** (with validation)
5. **Find digital twins** (similarity search)
6. **Predictive models** (browse / predict / register)
7. **AI assistant** (MCP console)

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
  App.vue              # shell — routes landing vs active flow
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
    DigitalTwinFlow.vue
    ModelStudioFlow.vue
    AgentConsoleFlow.vue
```
