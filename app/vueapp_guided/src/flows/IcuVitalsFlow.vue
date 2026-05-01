<template>
  <Wizard
    title="Generate synthetic ICU vitals"
    subtitle="TimeAutoDiff — 10 vital signs over 25 hourly timesteps."
    icon="📈"
    accent="#0891b2"
    :steps="steps"
    finishLabel="Generate"
    @finish="onFinish"
  >
    <!-- Step 0: Mode -->
    <template #step-0>
      <h2>Single patient or batch?</h2>

      <div class="mode-grid">
        <button
          class="mode-card"
          :class="{ selected: data.mode === 'single' }"
          @click="data.mode = 'single'"
        >
          <div class="mode-icon">👤</div>
          <div>
            <div class="mode-title">Single patient</div>
            <div class="mode-sub">Control conditioning (ethnicity, gender, age, outcome)</div>
          </div>
        </button>
        <button
          class="mode-card"
          :class="{ selected: data.mode === 'batch' }"
          @click="data.mode = 'batch'"
        >
          <div class="mode-icon">🔢</div>
          <div>
            <div class="mode-title">Batch</div>
            <div class="mode-sub">N patients with randomized conditioning</div>
          </div>
        </button>
      </div>
    </template>

    <!-- Step 1: Conditioning (or batch size) -->
    <template #step-1>
      <template v-if="data.mode === 'single'">
        <h2>Conditioning</h2>
        <p class="muted">Leave a field blank to let the model sample randomly.</p>

        <div class="field-row">
          <div class="field">
            <label>Ethnicity</label>
            <select v-model="data.ethnicity">
              <option :value="null">(random)</option>
              <option :value="0">White</option>
              <option :value="1">Black / African American</option>
              <option :value="2">Asian</option>
              <option :value="3">Other / Unknown</option>
            </select>
          </div>
          <div class="field">
            <label>Gender</label>
            <select v-model="data.gender">
              <option :value="null">(random)</option>
              <option :value="0">Female</option>
              <option :value="1">Male</option>
            </select>
          </div>
        </div>

        <div class="field-row">
          <div class="field">
            <label>Age group</label>
            <select v-model="data.ageGroup">
              <option :value="null">(random)</option>
              <option :value="0">0–30</option>
              <option :value="1">30–50</option>
              <option :value="2">50–70</option>
              <option :value="3">70–100</option>
            </select>
          </div>
          <div class="field">
            <label>ICU outcome</label>
            <select v-model="data.mortality">
              <option :value="null">(random)</option>
              <option :value="0">Survived</option>
              <option :value="1">Died</option>
            </select>
          </div>
        </div>
      </template>

      <template v-else>
        <h2>Batch size</h2>
        <div class="field">
          <label>Number of patients to generate</label>
          <input type="number" min="1" max="10000" v-model.number="data.nPatients" />
        </div>
        <p class="muted">
          Each patient will be sampled with random conditioning across the four demographic features.
        </p>
      </template>
    </template>

    <!-- Step 2: Run & visualize -->
    <template #step-2>
      <h2>Generate</h2>
      <div class="stub-banner">Stub</div>
      <p style="margin-top: 1rem">
        On the working version this step will call
        <code>POST /synthetic/timeseries/generate-raw-{{ data.mode === 'single' ? '1-patient' : 'n-patients' }}</code>
        and embed the
        <code>/generate-visualization-1-patient</code> interactive Plotly HTML for single-patient previews.
      </p>

      <div class="preview-placeholder">
        <span>📊 Plotly preview goes here</span>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive } from 'vue'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

const data = reactive({
  mode: 'single',
  ethnicity: null,
  gender: null,
  ageGroup: null,
  mortality: null,
  nPatients: 10,
})
store.flowData.vitals = data

const steps = [
  { label: 'Mode' },
  { label: 'Configure' },
  { label: 'Generate' },
]

function onFinish() {
  alert('Vitals generation would run here.')
  goHome()
}
</script>

<style scoped>
.mode-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1rem;
  margin-top: 0.5rem;
}
.mode-card {
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
  transition: all 0.15s ease;
}
.mode-card:hover {
  border-color: var(--accent);
}
.mode-card.selected {
  border-color: #0891b2;
  background: #ecfeff;
}
.mode-icon {
  font-size: 1.8rem;
}
.mode-title {
  font-weight: 600;
  margin-bottom: 0.2rem;
}
.mode-sub {
  font-size: 0.85rem;
  color: var(--text-muted);
}

.preview-placeholder {
  margin-top: 1.2rem;
  height: 220px;
  border: 1px dashed var(--border);
  border-radius: var(--radius-sm);
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-muted);
  background: repeating-linear-gradient(
    45deg,
    var(--surface) 0,
    var(--surface) 10px,
    var(--surface-alt) 10px,
    var(--surface-alt) 20px
  );
}
</style>
