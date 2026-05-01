<template>
  <Wizard
    title="Generate synthetic FHIR patients"
    subtitle="Create a cohort of synthetic patients with Synthea, stored in the FHIR repository."
    icon="🧬"
    accent="#2563eb"
    :steps="steps"
    finishLabel="Start generation"
    @finish="onFinish"
  >
    <!-- Step 0: Population -->
    <template #step-0>
      <h2>Define the population</h2>
      <p class="muted">Count, age, gender mix. Geographic distribution next.</p>

      <div class="field-row">
        <div class="field">
          <label>Number of patients</label>
          <input type="number" min="1" max="100000" v-model.number="data.numPatients" />
        </div>
        <div class="field">
          <label>Years of medical history</label>
          <input type="number" min="1" max="100" v-model.number="data.numYears" />
        </div>
      </div>

      <div class="field-row">
        <div class="field">
          <label>Minimum age</label>
          <input type="number" min="0" max="140" v-model.number="data.minAge" />
        </div>
        <div class="field">
          <label>Maximum age</label>
          <input type="number" min="0" max="140" v-model.number="data.maxAge" />
        </div>
      </div>

      <div class="field">
        <label>Gender distribution</label>
        <select v-model="data.gender">
          <option value="both">Both (balanced)</option>
          <option value="female">Female only</option>
          <option value="male">Male only</option>
        </select>
      </div>
    </template>

    <!-- Step 1: Geography -->
    <template #step-1>
      <h2>Geographic distribution</h2>
      <p class="muted">
        Sample patients from a state / city, or let Synthea use population-weighted sampling.
      </p>

      <div class="field">
        <label>Sampling mode</label>
        <select v-model="data.samplingMode">
          <option value="population">Population-weighted (all states)</option>
          <option value="state">Specific state</option>
        </select>
      </div>

      <div v-if="data.samplingMode === 'state'" class="field-row">
        <div class="field">
          <label>State</label>
          <select v-model="data.state">
            <option value="">(choose a state)</option>
            <option value="Massachusetts">Massachusetts</option>
            <option value="California">California</option>
            <option value="Texas">Texas</option>
            <option value="New York">New York</option>
          </select>
          <p class="muted" style="margin-top:0.4em">
            Full state list will be loaded from <code>/synthetic/synthea/demographics/states</code>.
          </p>
        </div>
        <div class="field">
          <label>City (optional)</label>
          <input type="text" v-model="data.city" placeholder="e.g. Boston" />
        </div>
      </div>
    </template>

    <!-- Step 2: Name & review -->
    <template #step-2>
      <h2>Name your cohort &amp; review</h2>

      <div class="field-row">
        <div class="field">
          <label>Cohort ID</label>
          <input type="text" v-model="data.cohortId" placeholder="auto-generated if blank" />
          <p class="muted" style="margin-top:0.4em">
            FHIR resource-id format: letters, numbers, hyphens, periods.
          </p>
        </div>
        <div class="field">
          <label>Export format</label>
          <select v-model="data.exporter">
            <option value="fhir">FHIR</option>
            <option value="csv">CSV</option>
          </select>
        </div>
      </div>

      <div class="summary">
        <h3>Summary</h3>
        <ul>
          <li><strong>{{ data.numPatients }}</strong> patients, <strong>{{ data.numYears }}</strong> years of history</li>
          <li>Ages <strong>{{ data.minAge }}–{{ data.maxAge }}</strong>, gender: <strong>{{ data.gender }}</strong></li>
          <li>
            Geography:
            <strong v-if="data.samplingMode === 'population'">population-weighted</strong>
            <strong v-else>{{ data.state || '(state not chosen)' }}{{ data.city ? `, ${data.city}` : '' }}</strong>
          </li>
          <li>Cohort: <strong>{{ data.cohortId || '(auto-generate)' }}</strong>, export: {{ data.exporter }}</li>
        </ul>
      </div>
    </template>

    <!-- Step 3: Running -->
    <template #step-3>
      <h2>Generation job</h2>
      <div class="stub-banner">Stub</div>
      <p style="margin-top: 1rem">
        In the working version this step will create an async job via
        <code>POST /synthetic/synthea/synthetic-patients</code>, show live progress, and
        offer a CTA to jump straight into the <strong>Browse cohorts</strong> flow when done.
      </p>

      <div class="fake-progress">
        <div class="fake-progress-bar"></div>
      </div>
      <p class="muted">Simulated progress — not hitting the backend yet.</p>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive } from 'vue'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

// Local reactive step data (would normally be persisted in store.flowData)
const data = reactive({
  numPatients: 50,
  numYears: 5,
  minAge: 0,
  maxAge: 90,
  gender: 'both',
  samplingMode: 'population',
  state: '',
  city: '',
  cohortId: '',
  exporter: 'fhir',
})

// Mirror into shared store so TopBar breadcrumbs etc. can observe.
store.flowData.synthea = data

const steps = [
  { label: 'Population' },
  { label: 'Geography' },
  { label: 'Name & review' },
  { label: 'Launch' },
]

function onFinish() {
  // Stub — in real impl this dispatches the job POST.
  alert('Generation job would be launched here.')
  goHome()
}
</script>

<style scoped>
.summary {
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 1rem 1.2rem;
  margin-top: 0.5rem;
}
.summary h3 {
  font-size: 0.85rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: 0.6rem;
}
.summary ul {
  margin: 0;
  padding-left: 1.2rem;
}
.summary li {
  margin-bottom: 0.3rem;
  color: var(--text);
}

.fake-progress {
  margin-top: 1.2rem;
  height: 6px;
  background: var(--surface-alt);
  border-radius: 3px;
  overflow: hidden;
}
.fake-progress-bar {
  height: 100%;
  width: 45%;
  background: var(--accent);
  animation: shimmer 2s ease-in-out infinite;
}
@keyframes shimmer {
  0% { transform: translateX(-100%); }
  100% { transform: translateX(220%); }
}
</style>
