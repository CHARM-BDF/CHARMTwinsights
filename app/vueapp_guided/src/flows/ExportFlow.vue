<template>
  <Wizard
    title="Export FHIR data"
    subtitle="Download the whole store or selected cohorts as zipped NDJSON — one file per resource type, ready for Hugging Face."
    icon="📦"
    accent="#0d9488"
    :steps="steps"
    finishLabel="Done"
    @finish="goHome()"
  >
    <!-- ═══════════════ Step 0: Scope ═══════════════ -->
    <template #step-0>
      <h2>What should be exported?</h2>

      <div class="mode-grid">
        <button
          class="mode-card"
          :class="{ selected: data.scope === 'all' }"
          @click="data.scope = 'all'"
        >
          <div class="mode-icon">🗄️</div>
          <div>
            <div class="mode-title">Everything</div>
            <div class="mode-sub">All patients and their clinical records, plus reference data</div>
          </div>
        </button>
        <button
          class="mode-card"
          :class="{ selected: data.scope === 'cohorts' }"
          @click="data.scope = 'cohorts'"
        >
          <div class="mode-icon">🗂️</div>
          <div>
            <div class="mode-title">Selected cohorts</div>
            <div class="mode-sub">Only resources tagged into the cohorts you pick below</div>
          </div>
        </button>
      </div>

      <template v-if="data.scope === 'cohorts'">
        <div v-if="cohortsLoading" class="state-msg"><span class="spinner"></span> Loading cohorts…</div>
        <div v-else-if="cohortsError" class="alert-err">
          ✗ {{ cohortsError }}
          <button class="ghost small-btn" style="margin-left:0.8rem" @click="loadCohorts">Retry</button>
        </div>
        <div v-else-if="!cohorts.length" class="state-msg muted">No cohorts found in the store.</div>
        <div v-else class="cohort-list">
          <label v-for="c in cohorts" :key="c.cohort_id" class="cohort-row">
            <input type="checkbox" :value="c.cohort_id" v-model="data.cohortIds" />
            <span class="mono cohort-name">{{ c.cohort_id }}</span>
            <span class="pill" :class="c.source">{{ c.source }}</span>
            <span class="muted cohort-count">{{ c.patient_count }} patients</span>
          </label>
        </div>
      </template>

      <p v-if="data.scope === 'cohorts' && !data.cohortIds.length" class="muted" style="margin-top:0.6rem; font-size:0.88rem">
        Pick at least one cohort to continue.
      </p>
    </template>

    <!-- ═══════════════ Step 1: Download ═══════════════ -->
    <template #step-1>
      <h2>Review &amp; download</h2>

      <div class="summary">
        <h3>About to export</h3>
        <ul>
          <li v-if="data.scope === 'all'">
            <strong>Everything</strong> — all cohorts
            <template v-if="totalPatients != null"> (~<strong>{{ totalPatients }}</strong> patients)</template>,
            clinical records, and reference data
          </li>
          <template v-else>
            <li><strong>{{ data.cohortIds.length }}</strong> cohort(s): <code>{{ data.cohortIds.join(', ') }}</code></li>
            <li>~<strong>{{ selectedPatients }}</strong> patients and every resource tagged into those cohorts</li>
          </template>
          <li>Format: <strong>zip of NDJSON files</strong>, one per FHIR resource type (Bulk Data layout), with <code>manifest.json</code> and a dataset-card <code>README.md</code></li>
        </ul>
      </div>

      <div class="hf-note">
        <div class="hf-note-title">🤗 Hugging Face-ready</div>
        <p class="muted" style="margin:0.2rem 0 0.5rem">
          Each line is one FHIR resource, so the files load directly — and
          <code>push_to_hub()</code> converts to Parquet automatically:
        </p>
        <pre class="hf-snippet">from datasets import load_dataset

ds = load_dataset("json", data_files={
    "patients": "Patient.ndjson",
    "conditions": "Condition.ndjson",
})</pre>
      </div>

      <div class="dl-row">
        <a class="primary dl-btn" :href="exportUrl" @click="downloadStarted = true">
          ⬇ Download zip
        </a>
        <span v-if="downloadStarted" class="muted" style="font-size:0.85rem">
          Preparing on the server — the download starts when the zip is built
          (large stores can take a minute or two). Check your browser's downloads.
        </span>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch, onMounted } from 'vue'
import axios from 'axios'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

// Reuse previously entered values if the user left and came back.
const data = store.flowData.export ?? reactive({
  scope: 'all', // 'all' | 'cohorts'
  cohortIds: [],
})
store.flowData.export = data

const downloadStarted = ref(false)

// ─── cohorts ─────────────────────────────────────────────────────────────────
const cohorts = ref([])
const cohortsLoading = ref(false)
const cohortsError = ref('')

async function loadCohorts() {
  cohortsLoading.value = true
  cohortsError.value = ''
  try {
    const { data: resp } = await axios.get(`${store.apiBase}/synthetic/synthea/list-all-cohorts`)
    cohorts.value = resp.cohorts ?? []
  } catch (e) {
    cohortsError.value = e?.response?.data?.detail ?? e.message ?? 'Failed to load cohorts'
  } finally {
    cohortsLoading.value = false
  }
}
onMounted(loadCohorts)

const totalPatients = computed(() => {
  const n = cohorts.value.reduce((a, c) => a + (c.patient_count || 0), 0)
  return n || null
})
const selectedPatients = computed(() =>
  cohorts.value
    .filter((c) => data.cohortIds.includes(c.cohort_id))
    .reduce((a, c) => a + (c.patient_count || 0), 0),
)

const exportUrl = computed(() => {
  if (data.scope === 'all') return `${store.apiBase}/export/fhir`
  const qs = data.cohortIds.map((c) => `cohort_id=${encodeURIComponent(c)}`).join('&')
  return `${store.apiBase}/export/fhir?${qs}`
})

// ─── steps ───────────────────────────────────────────────────────────────────
const steps = reactive([
  { label: 'Scope', canAdvance: true },
  { label: 'Download' },
])

watch(
  [() => data.scope, () => data.cohortIds.length],
  () => {
    steps[0].canAdvance = data.scope === 'all' || data.cohortIds.length > 0
    downloadStarted.value = false
  },
  { immediate: true },
)
</script>

<style scoped>
.mode-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 1rem;
  margin: 0.5rem 0 1.2rem;
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
.mode-card:hover { border-color: #0d9488; }
.mode-card.selected { border-color: #0d9488; background: #f0fdfa; }
.mode-icon { font-size: 1.8rem; }
.mode-title { font-weight: 600; margin-bottom: 0.2rem; }
.mode-sub { font-size: 0.85rem; color: var(--text-muted); }

.cohort-list {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}
.cohort-row {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  padding: 0.55rem 0.9rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--surface);
  cursor: pointer;
  margin: 0;
  font-size: 0.9rem;
}
.cohort-row:hover { border-color: #0d9488; }
.cohort-row:has(input:checked) { border-color: #0d9488; background: #f0fdfa; }
.cohort-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cohort-count { flex-shrink: 0; font-size: 0.83rem; }

.pill {
  font-size: 0.72rem;
  font-weight: 600;
  border-radius: 999px;
  padding: 0.1rem 0.5rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  color: var(--text-muted);
  flex-shrink: 0;
}
.pill.synthea, .pill.synthetic { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.pill.external { background: #ecfdf5; border-color: #a7f3d0; color: #047857; }

.state-msg {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  padding: 0.8rem 0;
  color: var(--text-muted);
}
.alert-err {
  background: #fef2f2;
  border: 1px solid #fecaca;
  border-radius: var(--radius-sm);
  color: #b91c1c;
  padding: 0.7rem 1rem;
  margin: 0.5rem 0;
}
.small-btn { padding: 0.3rem 0.65rem; font-size: 0.82rem; }

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
.summary ul { margin: 0; padding-left: 1.2rem; }
.summary li { margin-bottom: 0.3rem; }

.hf-note {
  margin-top: 1rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.8rem 1rem;
  background: var(--surface);
}
.hf-note-title { font-weight: 600; font-size: 0.9rem; }
.hf-snippet {
  margin: 0;
  padding: 0.6rem 0.8rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  font-size: 0.8rem;
  overflow-x: auto;
}

.dl-row {
  display: flex;
  align-items: center;
  gap: 0.9rem;
  flex-wrap: wrap;
  margin-top: 1.2rem;
}
.dl-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  text-decoration: none;
  padding: 0.55rem 1.2rem;
  border-radius: var(--radius-sm);
}

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  flex-shrink: 0;
  border: 2px solid var(--border);
  border-top-color: #0d9488;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
