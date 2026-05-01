<template>
  <Wizard
    title="Browse cohorts & patients"
    subtitle="Pick a cohort, explore patients, view population analytics, export as FHIR or PDF."
    icon="🔍"
    accent="#7c3aed"
    :steps="steps"
    finishLabel="Done"
    @finish="onFinish"
  >
    <!-- Step 0: Cohort picker -->
    <template #step-0>
      <h2>Pick a cohort</h2>
      <p class="muted">Powered by <code>GET /synthetic/synthea/list-all-cohorts</code>.</p>

      <div class="cohort-list">
        <button
          v-for="c in mockCohorts"
          :key="c.id"
          class="cohort-row"
          :class="{ selected: data.cohortId === c.id }"
          @click="data.cohortId = c.id"
        >
          <div class="cohort-main">
            <div class="cohort-name">{{ c.id }}</div>
            <div class="cohort-meta">
              <span class="pill">{{ c.source }}</span>
              <span class="muted">{{ c.size }} patients</span>
            </div>
          </div>
          <span class="chevron">›</span>
        </button>
      </div>
      <p class="muted" style="margin-top: 0.8rem">
        <span class="stub-banner">Stub</span>
        Cohort list is hard-coded. Real version queries the router.
      </p>
    </template>

    <!-- Step 1: Overview & analytics -->
    <template #step-1>
      <h2>Cohort overview — <span style="color: #7c3aed">{{ data.cohortId }}</span></h2>

      <div class="kpi-row">
        <div class="kpi-card">
          <div class="kpi-label">Patients</div>
          <div class="kpi-value">—</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Median age</div>
          <div class="kpi-value">—</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">F / M split</div>
          <div class="kpi-value">—</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Source</div>
          <div class="kpi-value">—</div>
        </div>
      </div>

      <div class="tabs">
        <button
          v-for="t in tabs"
          :key="t"
          class="tab"
          :class="{ active: data.tab === t }"
          @click="data.tab = t"
        >
          {{ t }}
        </button>
      </div>

      <div class="tab-panel">
        <div class="stub-banner">Stub</div>
        <p style="margin-top: 0.8rem">
          This panel will embed the matching
          <code>/stats/visualize-{{ data.tab.toLowerCase() }}</code> chart, plus the
          <em>by-gender</em> and <em>by-age</em> variants as toggles.
        </p>
      </div>
    </template>

    <!-- Step 2: Patient drilldown -->
    <template #step-2>
      <h2>Patients in cohort</h2>
      <p class="muted">
        Searchable patient list — select one to see its full record, download PDF, or export FHIR.
      </p>

      <div class="field">
        <input type="text" placeholder="Search by name…" />
      </div>

      <div class="patient-list">
        <div v-for="p in mockPatients" :key="p.id" class="patient-row">
          <div class="patient-main">
            <div class="patient-name">{{ p.name }}</div>
            <div class="patient-meta">
              <span class="muted">{{ p.id }}</span>
              <span class="muted">·</span>
              <span class="muted">{{ p.gender }}, {{ p.age }}y</span>
            </div>
          </div>
          <div class="patient-actions">
            <button class="ghost" title="View record">View</button>
            <button class="ghost" title="Download PDF">📄 PDF</button>
            <button class="ghost" title="Export FHIR">⤓ FHIR</button>
          </div>
        </div>
      </div>
    </template>

    <!-- Step 3: Export -->
    <template #step-3>
      <h2>Export the whole cohort</h2>
      <div class="stub-banner">Partial backend</div>
      <p style="margin-top: 1rem">
        Per-patient PDF / FHIR export already exists. Bulk cohort export will need a small
        new router endpoint — <code>GET /cohorts/{id}/export.fhir</code> and <code>.csv</code>.
      </p>

      <div class="export-grid">
        <button class="export-card">
          <div class="export-icon">📦</div>
          <div>
            <div class="export-title">Download as FHIR bundle</div>
            <div class="export-sub">Single .json with every resource in the cohort</div>
          </div>
        </button>
        <button class="export-card">
          <div class="export-icon">📊</div>
          <div>
            <div class="export-title">Download as CSV</div>
            <div class="export-sub">Flattened patient × feature table</div>
          </div>
        </button>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive } from 'vue'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

const mockCohorts = [
  { id: 'Cohort-Monday-20260218-072436', source: 'synthetic', size: 50 },
  { id: 'mobile-app-users', source: 'external', size: 12 },
  { id: 'research-cohort', source: 'synthetic', size: 200 },
]

const mockPatients = [
  { id: 'abc-001', name: 'Jane Q. Sample', gender: 'F', age: 47 },
  { id: 'abc-002', name: 'John A. Sample', gender: 'M', age: 62 },
  { id: 'abc-003', name: 'Alex M. Sample', gender: 'F', age: 34 },
]

const tabs = ['Conditions', 'Observations', 'Procedures', 'Medications', 'Diagnostics']

const data = reactive({
  cohortId: '',
  tab: 'Conditions',
})
store.flowData.browse = data

const steps = [
  { label: 'Pick cohort' },
  { label: 'Overview' },
  { label: 'Patients' },
  { label: 'Export' },
]

function onFinish() {
  goHome()
}
</script>

<style scoped>
.cohort-list,
.patient-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.cohort-row,
.patient-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  padding: 0.8rem 1rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  cursor: pointer;
  text-align: left;
  color: inherit;
  transition: all 0.12s ease;
}
.cohort-row:hover,
.patient-row:hover {
  border-color: #7c3aed;
  background: #faf5ff;
}
.cohort-row.selected {
  border-color: #7c3aed;
  background: #f5f3ff;
}

.cohort-main,
.patient-main {
  flex: 1;
  min-width: 0;
}
.cohort-name,
.patient-name {
  font-weight: 600;
  margin-bottom: 0.2rem;
}
.cohort-meta,
.patient-meta {
  display: flex;
  gap: 0.7rem;
  font-size: 0.85rem;
  align-items: center;
}

.chevron {
  color: var(--text-muted);
  font-size: 1.3rem;
}

.patient-actions {
  display: flex;
  gap: 0.3rem;
}

.kpi-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 0.8rem;
  margin-bottom: 1.2rem;
}
.kpi-card {
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.8rem 1rem;
}
.kpi-label {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: 0.3rem;
}
.kpi-value {
  font-size: 1.4rem;
  font-weight: 700;
}

.tabs {
  display: flex;
  gap: 0.3rem;
  border-bottom: 1px solid var(--border);
  margin-bottom: 1rem;
  flex-wrap: wrap;
}
.tab {
  border: none;
  background: transparent;
  padding: 0.6rem 0.9rem;
  color: var(--text-muted);
  border-bottom: 2px solid transparent;
  border-radius: 0;
  cursor: pointer;
}
.tab:hover {
  color: var(--text);
}
.tab.active {
  color: #7c3aed;
  border-bottom-color: #7c3aed;
  font-weight: 600;
}

.tab-panel {
  padding: 1rem 0;
}

.export-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 1rem;
  margin-top: 1rem;
}
.export-card {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 1rem 1.2rem;
  text-align: left;
  cursor: pointer;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  color: inherit;
}
.export-card:hover {
  border-color: #7c3aed;
}
.export-icon {
  font-size: 1.7rem;
}
.export-title {
  font-weight: 600;
  margin-bottom: 0.2rem;
}
.export-sub {
  font-size: 0.85rem;
  color: var(--text-muted);
}
</style>
