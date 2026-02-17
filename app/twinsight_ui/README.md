# Twinsight UI (React + TypeScript)

`twinsight_ui` is the foundation web interface for CHARMTwinsight. It is built as a standalone service and is currently mock-first for workflow validation.

## Goals

- Provide a production-grade shell for cohort workflows and predictive analytics
- Keep UI contracts decoupled from backend response shape via typed adapters
- Support endpoint mode switching (`mock`, `direct`)
- Establish accessibility and testing baseline (unit, integration, Playwright smoke)

## Tech Stack

- React + TypeScript + Vite
- React Router
- TanStack Query
- React Hook Form + Zod
- CSS Modules + design tokens
- MSW (browser + node test server)
- Vitest + Testing Library + jest-axe
- Playwright + axe-core

## Available Routes

- `/dashboard`
- `/cohorts/new`
- `/cohorts`
- `/cohorts/:cohortId`
- `/models`
- `/runs`
- `/settings`
- `/copilot`

## Local Development

```bash
cd app/twinsight_ui
npm install
npm run mock:worker
npm run dev
```

Default dev URL: `http://localhost:5173`

## Docker

This service is integrated in `app/docker-compose.yml` as `twinsight_ui` and exposed at:

- `http://localhost:8010`

## Runtime Configuration

Environment variables:

- `VITE_SERVICE_MODE` (`mock` by default)
- `VITE_UI_PRIMARY` (`false` by default)
- `VITE_SYNTHEA_BASE` (default `http://localhost:8003`)
- `VITE_STAT_BASE` (default `http://localhost:8001`)
- `VITE_MODEL_BASE` (default `http://localhost:8004`)
- `VITE_MCP_BASE` (default `http://localhost:8006`)

If the MSW worker script is missing, the app uses a temporary fetch fallback for mock mode. Generate the full worker script with `npm run mock:worker` for normal browser interception.

## Test Commands

```bash
npm run lint
npm run typecheck
npm run test
npm run test:e2e
```

## Current Scope

This version intentionally does not execute live write flows against backend services. It is designed for workflow validation and future live integration.

`direct` mode is currently a read-first integration surface:

- Live reads are wired for cohorts and models
- Write intents (generation and run creation) are disabled
- Dashboard includes a connection-health panel for endpoint readiness checks
