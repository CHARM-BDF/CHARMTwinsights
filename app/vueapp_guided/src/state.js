import { reactive } from 'vue'

// Single reactive store shared across the app.
// Intentionally lightweight — no Pinia, no router — to keep the footprint
// matched to the existing vueapp's minimal stack.

export const store = reactive({
  // Which flow is active. null = landing page.
  // One of: 'synthea', 'vitals', 'browse', 'ingest', 'twins', 'models', 'apply', 'agent'
  currentFlow: null,

  // Current step index within the active flow (0-based).
  currentStep: 0,

  // Per-flow working data, keyed by flow id. Each flow owns its shape.
  flowData: {},

  // API base URL — pulled from Vite env with a sensible default.
  apiBase: import.meta.env.VITE_API_BASE || 'http://localhost:8000',
})

export function startFlow(flowId) {
  store.currentFlow = flowId
  store.currentStep = 0
  // Each flow hydrates store.flowData[flowId] itself on mount, so inputs
  // survive leaving and re-entering a flow within a session.
}

export function goHome() {
  store.currentFlow = null
  store.currentStep = 0
}

export function nextStep(totalSteps) {
  if (store.currentStep < totalSteps - 1) {
    store.currentStep += 1
  }
}

export function prevStep() {
  if (store.currentStep > 0) {
    store.currentStep -= 1
  }
}

export function goToStep(index) {
  store.currentStep = index
}

export function resetFlowData(flowId) {
  store.flowData[flowId] = {}
  store.currentStep = 0
}

// Metadata for all flows — used by Landing and TopBar.
// Order here controls the landing page order; category controls grouping.
export const FLOWS = [
  {
    id: 'ingest',
    title: 'Ingest external FHIR data',
    subtitle: 'Validate, tag, and store FHIR bundles from external sources',
    icon: '⬇️',
    accent: '#059669',
    category: 'Data management',
  },
  {
    id: 'export',
    title: 'Export FHIR data',
    subtitle: 'Download everything or selected cohorts — FHIR NDJSON, per-patient bundles, or a flat CSV table',
    icon: '📤',
    accent: '#0d9488',
    category: 'Data management',
  },
  {
    id: 'synthea',
    title: 'Generate synthetic FHIR patients',
    subtitle: 'Synthea-based cohort with demographics, geography, medical history',
    icon: '🌱',
    accent: '#2563eb',
    category: 'Synthetic data',
  },
  {
    id: 'vitals',
    title: 'Generate synthetic ICU vitals',
    subtitle: 'TimeAutoDiff — 10 vital signs over 25 timesteps, optionally conditioned',
    icon: '📈',
    accent: '#0891b2',
    category: 'Synthetic data',
  },
  {
    id: 'browse',
    title: 'Browse cohorts & patients',
    subtitle: 'Explore patients, population analytics, export as FHIR or PDF',
    icon: '🔍',
    accent: '#7c3aed',
    category: 'Exploration',
  },
  {
    id: 'twins',
    title: 'Find digital twins',
    subtitle: 'Pick a subject and matching attributes — search existing cohorts or generate candidates',
    icon: '🧑‍🤝‍🧑',
    accent: '#db2777',
    category: 'Digital twins',
  },
  {
    id: 'models',
    title: 'Browse & register models',
    subtitle: 'See registered ML models, view their schemas, or register a new one',
    icon: '⚙️',
    accent: '#ea580c',
    category: 'Predictive models',
  },
  {
    id: 'apply',
    title: 'Apply a model to a cohort',
    subtitle: 'Run a registered model over a whole cohort and review results',
    icon: '▶️',
    accent: '#f59e0b',
    category: 'Predictive models',
  },
  {
    id: 'agent',
    title: 'AI assistant (MCP)',
    subtitle: 'Chat-based access to patient data and model execution',
    icon: '✨',
    accent: '#4f46e5',
    category: 'Assistance',
    badge: 'Future work',
  },
]

export function getFlow(id) {
  return FLOWS.find((f) => f.id === id) || null
}
