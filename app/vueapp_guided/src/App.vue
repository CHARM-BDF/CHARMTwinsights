<template>
  <div class="app-shell">
    <TopBar />

    <main class="content">
      <Landing v-if="!store.currentFlow" />

      <component v-else :is="activeFlowComponent" />
    </main>

    <footer class="app-footer">
      <span class="muted">CHARMTwinsights · Guided flow prototype</span>
      <span class="muted">API base: <code>{{ store.apiBase }}</code></span>
    </footer>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { store } from './state.js'
import TopBar from './components/TopBar.vue'
import Landing from './components/Landing.vue'

import SyntheaFhirFlow from './flows/SyntheaFhirFlow.vue'
import IcuVitalsFlow from './flows/IcuVitalsFlow.vue'
import BrowsingFlow from './flows/BrowsingFlow.vue'
import IngestionFlow from './flows/IngestionFlow.vue'
import ExportFlow from './flows/ExportFlow.vue'
import DigitalTwinFlow from './flows/DigitalTwinFlow.vue'
import ModelStudioFlow from './flows/ModelStudioFlow.vue'
import ApplyModelFlow from './flows/ApplyModelFlow.vue'
import AgentConsoleFlow from './flows/AgentConsoleFlow.vue'

const flowMap = {
  ingest: IngestionFlow,
  export: ExportFlow,
  synthea: SyntheaFhirFlow,
  vitals: IcuVitalsFlow,
  browse: BrowsingFlow,
  twins: DigitalTwinFlow,
  models: ModelStudioFlow,
  apply: ApplyModelFlow,
  agent: AgentConsoleFlow,
}

const activeFlowComponent = computed(() => flowMap[store.currentFlow] || null)
</script>

<style scoped>
.app-shell {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
}

.content {
  flex: 1;
  width: 100%;
  max-width: 1100px;
  margin: 0 auto;
  padding: 2rem 1.5rem 3rem;
}

.app-footer {
  padding: 1rem 1.5rem;
  border-top: 1px solid var(--border);
  background: var(--surface);
  display: flex;
  justify-content: space-between;
  gap: 1rem;
  flex-wrap: wrap;
}

.app-footer code {
  font-size: 0.85em;
  background: var(--surface-alt);
  padding: 1px 6px;
  border-radius: 4px;
}
</style>
