<template>
  <Wizard
    title="Export FHIR data"
    subtitle="Download the whole store or selected cohorts as a zip. Choose FHIR NDJSON, per-patient bundles, or a flat CSV table."
    accent="#0d9488"
    :steps="steps"
    finishLabel="⬇ Download Exported Data"
    @finish="startDownload"
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

    <!-- ═══════════════ Step 1: Format & download ═══════════════ -->
    <template #step-1>
      <h2>Format &amp; download</h2>

      <p class="muted" style="margin: -0.2rem 0 0.9rem; font-size:0.9rem">
        Pick one or more. Several formats come back in one zip, each in its own folder.
      </p>

      <div class="mode-grid">
        <button
          v-for="f in FORMATS"
          :key="f.key"
          class="mode-card"
          :class="{ selected: data.formats.includes(f.key) }"
          @click="toggleFormat(f.key)"
        >
          <div class="mode-icon">{{ f.icon }}</div>
          <div>
            <div class="mode-title">
              <span class="fmt-check">{{ data.formats.includes(f.key) ? '☑' : '☐' }}</span>
              {{ f.title }}
            </div>
            <div class="mode-sub">{{ f.sub }}</div>
          </div>
        </button>
      </div>

      <p v-if="bothFhirLayouts" class="muted redundancy-note">
        Note: NDJSON and per-patient bundles hold the same resources in two layouts.
        Picking both roughly doubles the build time for duplicate data.
      </p>

      <div class="summary">
        <h3>About to export</h3>
        <ul>
          <li v-if="data.scope === 'all'">
            <strong>Everything</strong>. All cohorts
            <template v-if="totalPatients != null"> (~<strong>{{ totalPatients }}</strong> patients)</template>
          </li>
          <template v-else>
            <li><strong>{{ data.cohortIds.length }}</strong> cohort(s): <code>{{ data.cohortIds.join(', ') }}</code></li>
            <li>~<strong>{{ selectedPatients }}</strong> patients</li>
          </template>
          <li v-for="f in selectedFormats" :key="f.key">
            <template v-if="data.formats.length > 1"><code>{{ f.key }}/</code> · </template>
            <span v-html="f.summary"></span>
          </li>
        </ul>
      </div>

      <p v-if="downloadStarted" class="muted" style="font-size:0.85rem; margin-top:1rem">
        Preparing on the server. The download starts when the zip is built
        (large stores can take a minute or two). Check your browser's downloads.
      </p>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch, onMounted } from 'vue'
import axios from 'axios'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

const FORMATS = [
  {
    key: 'ndjson',
    icon: '🧬',
    title: 'FHIR NDJSON',
    sub: 'Full fidelity. One file per resource type (Bulk Data layout), providers included',
    summary: 'Format: <strong>one NDJSON file per FHIR resource type</strong>. Full records '
      + 'plus the provider/reference resources they point at, with <code>manifest.json</code> '
      + '(per-type count verification) and a <code>README.md</code>',
  },
  {
    key: 'bundles',
    icon: '🗃️',
    title: 'Per-patient bundles',
    sub: 'The layout Synthea generates. One Bundle file per patient, plus provider files',
    summary: 'Format: <strong>one FHIR Bundle file per patient</strong> (all of their resources), '
      + 'plus <code>practitionerInformation.json</code> and <code>hospitalInformation.json</code>.'
      + ' This is the same file structure Synthea generates.',
  },
  {
    key: 'flat',
    icon: '📊',
    title: 'Flat table (CSV)',
    sub: 'ML-ready. One row per patient, 0/1 columns per condition/medication/procedure',
    summary: 'Format: <strong><code>patients_flat.csv</code></strong>. Demographics + indicator '
      + 'columns, with <code>data_dictionary.json</code> mapping columns to labels',
  },
]

// Reuse previously entered values if the user left and came back.
const data = store.flowData.export ?? reactive({
  scope: 'all', // 'all' | 'cohorts'
  cohortIds: [],
  formats: ['ndjson'], // any of 'ndjson' | 'bundles' | 'flat'
})
store.flowData.export = data
// Sessions restored from the single-format version carried `format`.
if (!Array.isArray(data.formats)) data.formats = [data.format || 'ndjson']
if (!data.formats.length) data.formats = ['ndjson']

const downloadStarted = ref(false)

function toggleFormat(key) {
  const i = data.formats.indexOf(key)
  if (i === -1) data.formats.push(key)
  else if (data.formats.length > 1) data.formats.splice(i, 1) // never leave zero selected
}

// Keep the display order stable regardless of click order.
const selectedFormats = computed(() => FORMATS.filter((f) => data.formats.includes(f.key)))
const bothFhirLayouts = computed(
  () => data.formats.includes('ndjson') && data.formats.includes('bundles'),
)

function startDownload() {
  downloadStarted.value = true
  // The server sets Content-Disposition: attachment, so this downloads
  // without navigating away from the wizard.
  const a = document.createElement('a')
  a.href = exportUrl.value
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
}

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
  const parts = data.scope === 'all'
    ? []
    : data.cohortIds.map((c) => `cohort_id=${encodeURIComponent(c)}`)
  for (const f of selectedFormats.value) parts.push(`format=${f.key}`)
  return `${store.apiBase}/export/fhir?${parts.join('&')}`
})

// ─── steps ───────────────────────────────────────────────────────────────────
const steps = reactive([
  { label: 'Scope', canAdvance: true },
  { label: 'Download' },
])

watch(
  [() => data.scope, () => data.cohortIds.length, () => data.formats.length],
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
.fmt-check { color: #0d9488; margin-right: 0.15rem; }
.redundancy-note {
  font-size: 0.85rem;
  margin: -0.4rem 0 0.9rem;
  padding: 0.5rem 0.8rem;
  background: var(--surface-alt);
  border-left: 3px solid #f59e0b;
  border-radius: var(--radius-sm);
}
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
