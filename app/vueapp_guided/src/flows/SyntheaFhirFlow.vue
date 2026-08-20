<template>
  <Wizard
    title="Generate synthetic FHIR patients"
    subtitle="Create a cohort of synthetic patients with Synthea, stored in the FHIR repository."
    accent="#2563eb"
    :steps="steps"
    finishLabel="Start generation"
    @finish="onFinish"
  >
    <!-- Step 0: Population -->
    <template #step-0>
      <h2>Define the population</h2>
      <p class="muted">Count, age range, and gender mix.</p>

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
        Sample patients from a state / city, or use population-weighted sampling across all states.
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
            <option v-if="statesLoading" disabled>(loading…)</option>
            <option v-for="s in availableStates" :key="s" :value="s">{{ s }}</option>
          </select>
          <p v-if="statesError" class="err-text" style="margin-top:0.3rem; font-size:0.85rem">
            ✗ {{ statesError }}
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
          <input
            type="text"
            v-model="data.cohortId"
            placeholder="auto-generated if blank"
            :class="{ 'input-error': data.cohortId && !cohortIdValid }"
          />
          <div v-if="data.cohortId && !cohortIdValid" class="err-text" style="margin-top:0.3rem; font-size:0.85rem">
            ✗ Letters, numbers, hyphens, periods only (1–64 chars). No underscores.
          </div>
          <div v-else class="muted" style="margin-top:0.3rem; font-size:0.85rem">
            FHIR ID format. Leave blank to let the server assign a sequential name.
          </div>
        </div>
        <div class="field">
          <label>Export format</label>
          <select v-model="data.exporter">
            <option value="fhir">FHIR (stored in HAPI)</option>
            <option value="csv">CSV (download only)</option>
          </select>
        </div>
      </div>

      <div class="summary">
        <h3>Summary</h3>
        <ul>
          <li><strong>{{ data.numPatients }}</strong> patients · <strong>{{ data.numYears }}</strong> yr history</li>
          <li>Ages <strong>{{ data.minAge }}–{{ data.maxAge }}</strong> · gender: <strong>{{ data.gender }}</strong></li>
          <li>
            Geography:
            <strong v-if="data.samplingMode === 'population'">population-weighted (all states)</strong>
            <strong v-else>{{ data.state || '(no state chosen)' }}{{ data.city ? `, ${data.city}` : '' }}</strong>
          </li>
          <li>Cohort ID: <strong>{{ data.cohortId || '(auto-assign)' }}</strong> · format: <strong>{{ data.exporter }}</strong></li>
        </ul>
      </div>
    </template>

    <!-- Step 3: Launch -->
    <template #step-3>
      <h2>Generation job</h2>

      <!-- Idle: pre-launch confirmation -->
      <div v-if="job.status === 'idle'" class="summary">
        <h3>About to submit</h3>
        <ul>
          <li><strong>{{ data.numPatients }}</strong> patients via Synthea</li>
          <li>Cohort: <strong>{{ data.cohortId || '(auto-assign)' }}</strong></li>
          <li>Endpoint: <code>POST {{ store.apiBase }}/synthetic/synthea/synthetic-patients</code></li>
        </ul>
        <p class="muted" style="margin-top:0.6rem">
          Generation runs in the background. This page will poll for progress.
        </p>
      </div>

      <!-- Submitting -->
      <div v-if="job.status === 'submitting'" class="submit-state loading">
        <span class="spinner"></span> Submitting job…
      </div>

      <!-- Queued or running -->
      <div v-if="job.status === 'queued' || job.status === 'running'" class="submit-state loading">
        <div style="width:100%">
          <div style="display:flex; align-items:center; gap:0.7rem; margin-bottom:0.8rem">
            <span class="spinner blue"></span>
            <span>{{ job.currentPhase || job.status }}</span>
          </div>
          <div class="progress-wrap">
            <div class="progress-bar" :style="{ width: (job.progress * 100).toFixed(0) + '%' }"></div>
          </div>
          <div class="progress-row">
            <span class="muted">{{ Math.round(job.progress * 100) }}%</span>
            <span v-if="job.eta" class="muted">~{{ job.eta }}s remaining</span>
            <span v-if="job.jobId" class="muted" style="margin-left:auto; font-size:0.8rem">
              Job <code>{{ job.jobId.slice(0, 8) }}…</code>
            </span>
          </div>
        </div>
      </div>

      <!-- Completed -->
      <div v-if="job.status === 'done'" class="submit-state ok">
        <h3>✓ Generation complete</h3>
        <ul>
          <li>Cohort: <strong>{{ job.result?.cohort_id }}</strong></li>
          <li>Patients generated: <strong>{{ job.result?.total_patients }}</strong></li>
          <li v-if="job.result?.chunks_processed > 1">Processed in <strong>{{ job.result.chunks_processed }}</strong> chunks</li>
          <li v-if="job.result?.patient_ids?.length">
            Sample IDs:
            <code>{{ job.result.patient_ids.slice(0, 4).join(', ') }}</code>
            <span v-if="job.result.patient_ids.length > 4" class="muted"> (+{{ job.result.patient_ids.length - 4 }} more)</span>
          </li>
        </ul>
        <button class="primary" style="margin-top:0.8rem" @click="goHome()">← Return home</button>
      </div>

      <!-- Failed / cancelled -->
      <div v-if="job.status === 'failed' || job.status === 'cancelled'" class="submit-state err">
        <h3>✗ Job {{ job.status }}</h3>
        <pre>{{ job.error }}</pre>
        <button class="ghost" style="margin-top:0.6rem" @click="resetJob">Try again</button>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch, onMounted, onUnmounted } from 'vue'
import axios from 'axios'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

// ---------- form data ----------
function defaultCohortId() {
  const now = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  return (
    'synthea-' +
    `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}` +
    `-${pad(now.getHours())}${pad(now.getMinutes())}`
  )
}

// Reuse previously entered values if the user left and came back; otherwise
// start fresh and register the new object in the shared store.
const data = store.flowData.synthea ?? reactive({
  numPatients: 50,
  numYears: 5,
  minAge: 0,
  maxAge: 90,
  gender: 'both',
  samplingMode: 'population',
  state: '',
  city: '',
  cohortId: defaultCohortId(),
  exporter: 'fhir',
})
store.flowData.synthea = data

// ---------- state list ----------
const availableStates = ref([])
const statesLoading = ref(false)
const statesError = ref('')

onMounted(async () => {
  statesLoading.value = true
  statesError.value = ''
  try {
    const { data: resp } = await axios.get(`${store.apiBase}/synthetic/synthea/demographics/states`)
    availableStates.value = resp.states ?? []
  } catch {
    statesError.value = 'Could not load state list. Type a state name manually.'
  } finally {
    statesLoading.value = false
  }
})

// ---------- validation ----------
const FHIR_ID_RE = /^[A-Za-z0-9\-.]{1,64}$/
const cohortIdValid = computed(() => FHIR_ID_RE.test(data.cohortId))

// ---------- step gating ----------
const steps = reactive([
  { label: 'Population' },
  { label: 'Geography', canAdvance: true },
  { label: 'Configure', canAdvance: true },
  { label: 'Launch', canAdvance: true },
])

watch(
  () => [data.samplingMode, data.state],
  () => {
    steps[1].canAdvance = data.samplingMode === 'population' || !!data.state
  },
  { immediate: true },
)
watch(
  () => [data.cohortId, cohortIdValid.value],
  () => {
    steps[2].canAdvance = !data.cohortId || cohortIdValid.value
  },
  { immediate: true },
)

// ---------- job state ----------
const job = reactive({
  status: 'idle',   // idle | submitting | queued | running | done | failed | cancelled
  jobId: null,
  progress: 0,
  currentPhase: '',
  eta: null,
  result: null,
  error: null,
})
let pollInterval = null

function resetJob() {
  job.status = 'idle'
  job.jobId = null
  job.progress = 0
  job.currentPhase = ''
  job.eta = null
  job.result = null
  job.error = null
  steps[3].canAdvance = true
}

onUnmounted(() => {
  if (pollInterval) clearInterval(pollInterval)
})

async function pollJob() {
  try {
    const { data: j } = await axios.get(
      `${store.apiBase}/synthetic/synthea/synthetic-patients/jobs/${job.jobId}`,
    )
    job.progress = j.progress ?? 0
    job.currentPhase = j.current_phase ?? j.status
    job.eta = j.estimated_remaining_seconds ?? null

    if (j.status === 'completed') {
      job.status = 'done'
      job.result = j.result
      clearInterval(pollInterval)
      pollInterval = null
    } else if (j.status === 'failed') {
      job.status = 'failed'
      job.error = j.error ?? 'Unknown error'
      clearInterval(pollInterval)
      pollInterval = null
    } else if (j.status === 'cancelled') {
      job.status = 'cancelled'
      job.error = 'Job was cancelled.'
      clearInterval(pollInterval)
      pollInterval = null
    } else {
      job.status = j.status // queued or running
    }
  } catch (e) {
    console.warn('Poll error:', e)
  }
}

async function onFinish() {
  if (job.status !== 'idle') return
  job.status = 'submitting'
  steps[3].canAdvance = false

  try {
    const payload = {
      num_patients: data.numPatients,
      num_years: data.numYears,
      // 'auto' passes through the router untouched and makes the synthea
      // server assign a sequential Cohort-No-X name, matching the "sequential
      // name" promise in the help text. ('default'/blank would auto-name too,
      // but with the router's timestamp format instead.)
      cohort_id: data.cohortId || 'auto',
      exporter: data.exporter,
      min_age: data.minAge,
      max_age: data.maxAge,
      gender: data.gender,
      state: data.samplingMode === 'state' && data.state ? data.state : null,
      city: data.samplingMode === 'state' && data.city ? data.city : null,
      use_population_sampling: data.samplingMode === 'population',
    }
    const { data: resp } = await axios.post(
      `${store.apiBase}/synthetic/synthea/synthetic-patients`,
      payload,
    )
    job.jobId = resp.job_id
    job.status = resp.status ?? 'queued'
    job.currentPhase = resp.status ?? 'queued'
    pollInterval = setInterval(pollJob, 3000)
  } catch (e) {
    job.status = 'failed'
    const detail = e?.response?.data?.detail ?? e?.response?.data ?? e?.message ?? 'Unknown error'
    job.error = typeof detail === 'string' ? detail : JSON.stringify(detail, null, 2)
  }
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
.summary li { margin-bottom: 0.3rem; color: var(--text); }

.input-error {
  border-color: #fecaca !important;
  background: #fef2f2;
}
.err-text { color: #b91c1c; }

.submit-state {
  margin-top: 0.5rem;
  padding: 1rem 1.2rem;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
  background: var(--surface);
}
.submit-state.loading {
  display: flex;
  align-items: flex-start;
  gap: 0.7rem;
  color: var(--text-muted);
}
.submit-state.ok {
  background: #eff6ff;
  border-color: #bfdbfe;
}
.submit-state.ok h3 { color: #1d4ed8; margin-bottom: 0.5rem; }
.submit-state.ok ul { margin: 0; padding-left: 1.2rem; }
.submit-state.ok li { margin-bottom: 0.25rem; }
.submit-state.err {
  background: #fef2f2;
  border-color: #fecaca;
}
.submit-state.err h3 { color: #b91c1c; margin-bottom: 0.5rem; }
.submit-state pre {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.6rem 0.8rem;
  font-size: 0.8rem;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
}

.progress-wrap {
  height: 6px;
  background: var(--surface-alt);
  border-radius: 3px;
  overflow: hidden;
  margin-bottom: 0.4rem;
}
.progress-bar {
  height: 100%;
  background: #2563eb;
  border-radius: 3px;
  transition: width 0.4s ease;
  min-width: 2%;
}
.progress-row {
  display: flex;
  gap: 1rem;
  font-size: 0.82rem;
  flex-wrap: wrap;
}

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  flex-shrink: 0;
  border: 2px solid var(--border);
  border-top-color: var(--accent);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
.spinner.blue { border-top-color: #2563eb; }
@keyframes spin { to { transform: rotate(360deg); } }
</style>
