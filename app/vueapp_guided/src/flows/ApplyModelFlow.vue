<template>
  <Wizard
    title="Apply a model to a cohort"
    subtitle="Pick a registered model, choose who to run it on, review the predictions."
    icon="⚡"
    accent="#f59e0b"
    :steps="steps"
    finishLabel="Run predictions"
    @finish="onFinish"
  >
    <!-- Step 0: Pick a model -->
    <template #step-0>
      <h2>Pick a model</h2>
      <p class="muted">
        Loaded from the model registry. Click a card to select it.
      </p>

      <div class="model-grid">
        <button
          v-for="m in mockModels"
          :key="m.image"
          class="model-card"
          :class="{ selected: data.modelImage === m.image }"
          @click="data.modelImage = m.image"
        >
          <div class="model-card-head">
            <div class="model-card-title">{{ m.title }}</div>
            <span class="pill">{{ m.image }}</span>
          </div>
          <div class="model-card-sub muted">{{ m.short_description }}</div>
        </button>
      </div>
    </template>

    <!-- Step 1: Pick the target cohort / patients -->
    <template #step-1>
      <h2>Choose what to run it on</h2>

      <div class="field">
        <label>Target type</label>
        <div class="radio-row">
          <label class="radio-inline">
            <input type="radio" value="cohort" v-model="data.targetType" />
            Whole cohort
          </label>
          <label class="radio-inline">
            <input type="radio" value="subset" v-model="data.targetType" />
            Subset of a cohort (filter later)
          </label>
          <label class="radio-inline">
            <input type="radio" value="single" v-model="data.targetType" />
            A single patient
          </label>
          <label class="radio-inline">
            <input type="radio" value="manual" v-model="data.targetType" />
            Manual input (JSON)
          </label>
        </div>
      </div>

      <div v-if="data.targetType === 'cohort' || data.targetType === 'subset'" class="field">
        <label>Cohort</label>
        <select v-model="data.cohortId">
          <option value="">(select cohort)</option>
          <option v-for="c in mockCohorts" :key="c.id" :value="c.id">
            {{ c.id }} · {{ c.size }} patients ({{ c.source }})
          </option>
        </select>
        <p class="muted" style="margin-top: 0.4rem">
          Will call <code>GET /synthetic/synthea/list-all-cohorts</code> in the real version.
        </p>
      </div>

      <div v-if="data.targetType === 'subset'" class="field">
        <label>Optional filter (condition, age, gender…)</label>
        <input type="text" v-model="data.filter" placeholder="e.g. age >= 60 AND has Hypertension" />
        <p class="muted" style="margin-top: 0.4rem">
          <span class="stub-banner">Planned</span>
          Will use the Digital Twin query builder's features internally.
        </p>
      </div>

      <div v-if="data.targetType === 'single'" class="field">
        <label>Patient ID</label>
        <input type="text" v-model="data.patientId" placeholder="e.g. abc-001" />
      </div>

      <div v-if="data.targetType === 'manual'" class="field">
        <label>Manual input (JSON array of records)</label>
        <textarea rows="6" v-model="data.manualJson" placeholder='[{"age": 65, "bmi": 30, ...}]'></textarea>
        <p class="muted" style="margin-top: 0.4rem">
          Fields must match the model's LinkML input schema.
        </p>
      </div>
    </template>

    <!-- Step 2: Feature mapping (how to build model input from FHIR) -->
    <template #step-2>
      <h2>Feature mapping</h2>
      <p class="muted">
        The model expects specific fields; the cohort stores FHIR resources. Confirm how to
        map one to the other.
      </p>
      <div class="stub-banner">New backend work</div>
      <p style="margin-top: 0.8rem">
        For each model input field, pick a source: FHIR Observation code, Condition presence,
        computed age / BMI, etc. A good default mapping will be offered per known model.
      </p>

      <table class="mapping-table">
        <thead>
          <tr>
            <th>Model input</th>
            <th>Source</th>
            <th>Default value</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="f in mockModelFields" :key="f.name">
            <td>
              <div><strong>{{ f.name }}</strong></div>
              <div class="muted" style="font-size: 0.8rem">{{ f.type }}</div>
            </td>
            <td>
              <select>
                <option>{{ f.suggestedSource }}</option>
                <option>Constant value</option>
                <option>Leave null</option>
              </select>
            </td>
            <td>
              <input type="text" :placeholder="f.defaultHint" />
            </td>
          </tr>
        </tbody>
      </table>
    </template>

    <!-- Step 3: Review -->
    <template #step-3>
      <h2>Review &amp; launch</h2>

      <div class="summary">
        <h3>About to run</h3>
        <ul>
          <li>Model: <strong>{{ data.modelImage || '(none selected)' }}</strong></li>
          <li>
            Target:
            <strong v-if="data.targetType === 'cohort'">
              whole cohort <code>{{ data.cohortId || '—' }}</code>
            </strong>
            <strong v-else-if="data.targetType === 'subset'">
              subset of <code>{{ data.cohortId || '—' }}</code>, filter: <em>{{ data.filter || '(none)' }}</em>
            </strong>
            <strong v-else-if="data.targetType === 'single'">
              patient <code>{{ data.patientId || '—' }}</code>
            </strong>
            <strong v-else>manual input ({{ (() => { try { return JSON.parse(data.manualJson).length } catch { return '?' } })() }} records)</strong>
          </li>
        </ul>
      </div>

      <p class="muted" style="margin-top: 0.8rem">
        Clicking <strong>Run predictions</strong> will POST to <code>/modeling/predict</code>
        per batch (or a new <code>/modeling/predict-on-cohort</code> endpoint for large cohorts).
      </p>
    </template>

    <!-- Step 4: Results -->
    <template #step-4>
      <h2>Predictions</h2>
      <div class="stub-banner">Stub</div>
      <p style="margin-top: 0.8rem">
        The working version renders a results table and summary charts:
        score distribution, risk strata, etc. Options to save as a new cohort
        attribute, download CSV, or attach as Observations in HAPI.
      </p>

      <table class="results-table">
        <thead>
          <tr>
            <th>Patient</th>
            <th>Input preview</th>
            <th>Prediction</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in mockResults" :key="r.patient">
            <td><code>{{ r.patient }}</code></td>
            <td class="muted">{{ r.inputPreview }}</td>
            <td>
              <div class="result-value">{{ r.value }}</div>
              <div class="muted" style="font-size: 0.8rem">{{ r.label }}</div>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="result-actions">
        <button class="ghost">⤓ Download as CSV</button>
        <button class="ghost">💾 Attach to patients as FHIR Observations</button>
        <button class="ghost">📊 View distribution</button>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive } from 'vue'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

const mockModels = [
  {
    image: 'coxcopdmodel:latest',
    title: 'Cox PH: COPD risk',
    short_description: 'Survival model predicting 5-year COPD outcomes.',
  },
  {
    image: 'irismodel:latest',
    title: 'Iris classifier (demo)',
    short_description: 'Classic demo classifier. Will only work with iris-shaped inputs.',
  },
  {
    image: 'reachablefrommodel:latest',
    title: 'Reachable-from (demo)',
    short_description: 'Graph reachability demo model.',
  },
]

const mockCohorts = [
  { id: 'Cohort-Monday-20260218-072436', source: 'synthetic', size: 50 },
  { id: 'mobile-app-users', source: 'external', size: 12 },
  { id: 'research-cohort', source: 'synthetic', size: 200 },
]

const mockModelFields = [
  { name: 'age_at_time_0', type: 'float', suggestedSource: 'Computed patient age', defaultHint: '50' },
  { name: 'sex_at_birth', type: 'Female | Male', suggestedSource: 'Patient.gender', defaultHint: 'Female' },
  { name: 'bmi', type: 'float', suggestedSource: 'Observation LOINC 39156-5', defaultHint: '25.0' },
  { name: 'diabetes', type: '0.0 | 1.0', suggestedSource: 'Condition presence (E11*)', defaultHint: '0.0' },
  { name: 'smoking_status', type: '0.0 | 1.0', suggestedSource: 'Observation LOINC 72166-2', defaultHint: '0.0' },
]

const mockResults = [
  { patient: 'abc-001', inputPreview: 'F, 58, BMI 27, T2D=1', value: '0.82', label: 'high 5y hazard' },
  { patient: 'abc-002', inputPreview: 'M, 62, BMI 31, T2D=1', value: '0.91', label: 'high 5y hazard' },
  { patient: 'abc-003', inputPreview: 'F, 34, BMI 22, T2D=0', value: '0.14', label: 'low 5y hazard' },
  { patient: 'abc-004', inputPreview: 'M, 71, BMI 28, T2D=0', value: '0.56', label: 'moderate 5y hazard' },
]

// Reuse previously entered values if the user left and came back.
const data = store.flowData.apply ?? reactive({
  modelImage: '',
  targetType: 'cohort',
  cohortId: '',
  filter: '',
  patientId: '',
  manualJson: '',
})
store.flowData.apply = data

const steps = [
  { label: 'Model' },
  { label: 'Target' },
  { label: 'Mapping' },
  { label: 'Review' },
  { label: 'Results' },
]

function onFinish() {
  alert('Would POST to /modeling/predict with the mapped inputs.')
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
  transition: all 0.15s ease;
}
.model-card:hover { border-color: #f59e0b; }
.model-card.selected {
  border-color: #f59e0b;
  background: #fffbeb;
}
.model-card-head {
  display: flex;
  justify-content: space-between;
  gap: 0.5rem;
  align-items: flex-start;
  margin-bottom: 0.4rem;
}
.model-card-title {
  font-weight: 600;
}
.model-card-sub {
  font-size: 0.9rem;
  line-height: 1.4;
}

.radio-row {
  display: flex;
  flex-wrap: wrap;
  gap: 1rem;
  padding-top: 0.2rem;
}
.radio-inline {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  margin-bottom: 0;
  color: var(--text);
  font-size: 0.9rem;
  cursor: pointer;
}

textarea {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.85rem;
  resize: vertical;
}

.mapping-table,
.results-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 0.8rem;
}
.mapping-table th,
.mapping-table td,
.results-table th,
.results-table td {
  padding: 0.55rem 0.7rem;
  text-align: left;
  border-bottom: 1px solid var(--border);
  font-size: 0.9rem;
  vertical-align: top;
}
.mapping-table th,
.results-table th {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  font-weight: 500;
  border-bottom-width: 2px;
}

.result-value {
  font-weight: 700;
  font-size: 1.05rem;
}

.result-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-top: 1rem;
}

.summary {
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 1rem 1.2rem;
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
</style>
