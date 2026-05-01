<template>
  <Wizard
    title="Find digital twins"
    subtitle="Similarity-ranked search across the cohort. Build a query manually or seed from a patient."
    icon="👯"
    accent="#db2777"
    :steps="steps"
    finishLabel="Save matches as cohort"
    @finish="onFinish"
  >
    <!-- Step 0: Seed choice -->
    <template #step-0>
      <h2>How would you like to start?</h2>

      <div class="seed-grid">
        <button
          class="seed-card"
          :class="{ selected: data.seedMode === 'manual' }"
          @click="data.seedMode = 'manual'"
        >
          <div class="seed-icon">✍️</div>
          <div>
            <div class="seed-title">Build a query</div>
            <div class="seed-sub">Pick conditions, meds, demographics</div>
          </div>
        </button>

        <button
          class="seed-card"
          :class="{ selected: data.seedMode === 'patient' }"
          @click="data.seedMode = 'patient'"
        >
          <div class="seed-icon">👤</div>
          <div>
            <div class="seed-title">Seed from a patient</div>
            <div class="seed-sub">Use an existing patient's profile as the starting point</div>
          </div>
        </button>
      </div>

      <div v-if="data.seedMode === 'patient'" class="field" style="margin-top: 1.2rem">
        <label>Seed patient ID</label>
        <input type="text" v-model="data.seedPatientId" placeholder="e.g. abc-001" />
        <p class="muted" style="margin-top: 0.3rem">
          The next step will show the extracted profile, which you can edit before searching.
        </p>
      </div>
    </template>

    <!-- Step 1: Profile builder -->
    <template #step-1>
      <h2>Profile</h2>
      <p class="muted">
        Edit the features that describe the target profile. Empty sections are ignored.
      </p>

      <div class="profile-section">
        <h3>Demographics</h3>
        <div class="field-row">
          <div class="field">
            <label>Gender</label>
            <select v-model="data.profile.gender">
              <option value="">any</option>
              <option value="female">female</option>
              <option value="male">male</option>
            </select>
          </div>
          <div class="field">
            <label>Age range</label>
            <div style="display: flex; gap: 0.5rem">
              <input type="number" v-model.number="data.profile.minAge" placeholder="min" />
              <input type="number" v-model.number="data.profile.maxAge" placeholder="max" />
            </div>
          </div>
        </div>
      </div>

      <div class="profile-section">
        <h3>Conditions</h3>
        <input type="text" placeholder="e.g. Hypertension, Type 2 diabetes (comma-separated)" v-model="data.profile.conditions" />
      </div>

      <div class="profile-section">
        <h3>Medications</h3>
        <input type="text" placeholder="e.g. Metformin, Lisinopril" v-model="data.profile.medications" />
      </div>

      <div class="profile-section">
        <h3>Procedures</h3>
        <input type="text" placeholder="(optional)" v-model="data.profile.procedures" />
      </div>

      <div class="stub-banner" style="margin-top: 0.6rem">New endpoint needed</div>
      <p class="muted" style="margin-top: 0.4rem">
        Search will POST to a new <code>/twins/find</code> route that computes a similarity
        score (e.g. Jaccard on condition/med/procedure sets, weighted by demographic match).
      </p>
    </template>

    <!-- Step 2: Scoring options -->
    <template #step-2>
      <h2>Scoring &amp; scope</h2>

      <div class="field-row">
        <div class="field">
          <label>Top K results</label>
          <input type="number" v-model.number="data.topK" min="1" max="500" />
        </div>
        <div class="field">
          <label>Restrict to cohort (optional)</label>
          <input type="text" v-model="data.scopeCohort" placeholder="e.g. research-cohort" />
        </div>
      </div>

      <div class="field">
        <label>Include sources</label>
        <div style="display: flex; gap: 1rem; padding-top: 0.2rem">
          <label class="checkbox-inline">
            <input type="checkbox" v-model="data.includeSynthetic" /> synthetic
          </label>
          <label class="checkbox-inline">
            <input type="checkbox" v-model="data.includeExternal" /> external
          </label>
        </div>
      </div>

      <div class="field">
        <label>Weighting preset</label>
        <select v-model="data.weighting">
          <option value="balanced">Balanced — conditions, meds, demographics equal</option>
          <option value="clinical">Clinical-heavy — prioritize conditions &amp; procedures</option>
          <option value="pharma">Pharmacology-heavy — prioritize medications</option>
        </select>
      </div>
    </template>

    <!-- Step 3: Results -->
    <template #step-3>
      <h2>Top matches</h2>
      <div class="stub-banner">Stub</div>
      <p style="margin-top: 0.8rem">
        Ranked results would appear here. The "Finish" button saves the matched patients
        as a new cohort.
      </p>

      <div class="match-list">
        <div v-for="m in mockMatches" :key="m.id" class="match-row">
          <div class="match-main">
            <div class="match-name">{{ m.name }}</div>
            <div class="match-meta muted">
              {{ m.id }} · {{ m.gender }}, {{ m.age }}y · source: {{ m.source }}
            </div>
          </div>
          <div class="match-score" :style="{ '--sc': m.score }">
            <div class="score-bar"><div class="score-fill"></div></div>
            <span class="score-value">{{ m.score.toFixed(2) }}</span>
          </div>
          <button class="ghost">View</button>
        </div>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive } from 'vue'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

const mockMatches = [
  { id: 'abc-042', name: 'Patient 42', gender: 'F', age: 58, source: 'synthetic', score: 0.91 },
  { id: 'abc-118', name: 'Patient 118', gender: 'F', age: 61, source: 'synthetic', score: 0.84 },
  { id: 'ext-007', name: 'Patient 7', gender: 'F', age: 55, source: 'external', score: 0.77 },
  { id: 'abc-309', name: 'Patient 309', gender: 'F', age: 63, source: 'synthetic', score: 0.72 },
]

const data = reactive({
  seedMode: 'manual',
  seedPatientId: '',
  profile: {
    gender: '',
    minAge: null,
    maxAge: null,
    conditions: '',
    medications: '',
    procedures: '',
  },
  topK: 20,
  scopeCohort: '',
  includeSynthetic: true,
  includeExternal: true,
  weighting: 'balanced',
})
store.flowData.twins = data

const steps = [
  { label: 'Seed' },
  { label: 'Profile' },
  { label: 'Scoring' },
  { label: 'Results' },
]

function onFinish() {
  alert('Save matches as a new cohort — stub.')
  goHome()
}
</script>

<style scoped>
.seed-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1rem;
  margin-top: 0.5rem;
}
.seed-card {
  display: flex;
  gap: 1rem;
  padding: 1rem 1.2rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  text-align: left;
  cursor: pointer;
  color: inherit;
  transition: all 0.15s ease;
}
.seed-card:hover { border-color: #db2777; }
.seed-card.selected {
  border-color: #db2777;
  background: #fdf2f8;
}
.seed-icon { font-size: 1.7rem; }
.seed-title { font-weight: 600; margin-bottom: 0.2rem; }
.seed-sub { font-size: 0.85rem; color: var(--text-muted); }

.profile-section {
  margin-bottom: 1.2rem;
}
.profile-section h3 {
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: 0.5rem;
}

.checkbox-inline {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  color: var(--text);
  font-size: 0.9rem;
  margin-bottom: 0;
}

.match-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  margin-top: 1rem;
}
.match-row {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0.75rem 1rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
}
.match-main { flex: 1; min-width: 0; }
.match-name { font-weight: 600; margin-bottom: 0.2rem; }
.match-meta { font-size: 0.85rem; }

.match-score {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  width: 140px;
}
.score-bar {
  flex: 1;
  height: 6px;
  background: var(--surface-alt);
  border-radius: 3px;
  overflow: hidden;
}
.score-fill {
  height: 100%;
  width: calc(var(--sc) * 100%);
  background: #db2777;
}
.score-value {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--text);
  min-width: 36px;
  text-align: right;
}
</style>
