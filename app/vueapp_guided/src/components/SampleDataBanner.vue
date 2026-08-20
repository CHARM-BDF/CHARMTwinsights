<template>
  <!-- Shown only when the FHIR store has no patients yet. -->
  <div v-if="visible" class="sample-banner">
    <div class="sb-icon">🗃️</div>

    <div class="sb-body">
      <div class="sb-title">Your FHIR store is empty. Load the sample dataset?</div>

      <p class="sb-text" v-if="ds">
        A ready-made sample of <strong>{{ ds.patients }}</strong> synthetic patients in
        <strong>{{ cohortCount }}</strong> cohorts, generated with this same application
        using Synthea. It downloads about <strong>{{ sizeLabel }}</strong>
        (<strong>{{ resourceLabel }}</strong> FHIR resources) from
        <a :href="ds.url" target="_blank" rel="noopener">Hugging Face</a> and takes
        <strong>{{ timeLabel }}</strong> to load. Cohorts, sources, and the generation
        settings of each cohort come back exactly as they were.
      </p>
      <p class="sb-text" v-else>
        A ready-made sample of synthetic patients is published on Hugging Face and can be
        loaded here.
      </p>

      <p class="sb-alt">
        This is entirely optional. To build your own data instead, pick
        <strong>Generate synthetic FHIR patients</strong> below and choose your own
        population, or <strong>Ingest external FHIR data</strong> if you already have
        bundles.
      </p>

      <!-- idle -->
      <div v-if="state === 'idle'" class="sb-actions">
        <button class="primary" :disabled="!ds" @click="start">⬇ Load sample dataset</button>
        <button class="ghost" @click="dismissed = true">No thanks, I will make my own</button>
      </div>

      <!-- loading -->
      <div v-else-if="state === 'loading'" class="sb-progress">
        <div class="sb-progress-head">
          <span class="spinner"></span>
          <span>{{ job.current_phase || 'starting…' }}</span>
          <span v-if="job.estimated_remaining_seconds" class="muted sb-eta">
            about {{ etaLabel }} left
          </span>
        </div>
        <div class="sb-bar"><div class="sb-fill" :style="{ width: pct + '%' }"></div></div>
        <div class="muted sb-pct">
          {{ pct }}%<template v-if="job.total_chunks">
            · {{ job.completed_chunks }} of {{ job.total_chunks }} files</template>
          · you can keep using the app while this runs
        </div>
      </div>

      <!-- done -->
      <div v-else-if="state === 'done'" class="sb-done">
        ✓ Loaded {{ job.result?.patients_loaded }} patients across
        {{ job.result?.cohorts_created }} cohorts.
        <button class="ghost small-btn" @click="$emit('loaded')">Refresh</button>
      </div>

      <!-- error -->
      <div v-else-if="state === 'error'" class="sb-error">
        ✗ {{ error }}
        <button class="ghost small-btn" @click="state = 'idle'">Dismiss</button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import axios from 'axios'
import { store } from '../state.js'

const emit = defineEmits(['loaded'])

const storeEmpty = ref(false)
const ds = ref(null)
const dismissed = ref(false)
const state = ref('idle') // idle | loading | done | error
const error = ref('')
const job = reactive({})
let poll = null

const visible = computed(() => storeEmpty.value && !dismissed.value)
const cohortCount = computed(() => (ds.value?.cohorts || []).length || 'several')
const sizeLabel = computed(() => {
  const mb = ds.value?.download_mb ?? 0
  return mb >= 1024 ? `${(mb / 1024).toFixed(1)} GB` : `${Math.round(mb)} MB`
})
const resourceLabel = computed(() =>
  (ds.value?.total_resources ?? 0).toLocaleString(),
)
const timeLabel = computed(() => {
  const m = ds.value?.estimated_load_minutes
  if (!m) return 'a while'
  return m >= 60 ? `roughly ${(m / 60).toFixed(1)} hours` : `roughly ${m} minutes`
})
const pct = computed(() => Math.round((job.progress ?? 0) * 100))
const etaLabel = computed(() => {
  const s = job.estimated_remaining_seconds ?? 0
  return s >= 90 ? `${Math.round(s / 60)} min` : `${s}s`
})

async function check() {
  try {
    const { data } = await axios.get(
      `${store.apiBase}/synthetic/synthea/sample-data/info`,
      { timeout: 60_000 },
    )
    storeEmpty.value = !!data.store_empty
    ds.value = data.dataset
  } catch {
    storeEmpty.value = false // never block the landing page on this
  }
}
onMounted(check)

async function start() {
  state.value = 'loading'
  Object.assign(job, { progress: 0, current_phase: 'queued' })
  try {
    const { data } = await axios.post(
      `${store.apiBase}/synthetic/synthea/sample-data/load`,
      null,
      { timeout: 60_000 },
    )
    poll = setInterval(() => pollJob(data.job_id), 3000)
  } catch (e) {
    error.value = e?.response?.data?.detail ?? e.message ?? 'Could not start the load'
    state.value = 'error'
  }
}

async function pollJob(id) {
  try {
    const { data } = await axios.get(
      `${store.apiBase}/synthetic/synthea/sample-data/load/jobs/${id}`,
    )
    Object.assign(job, data)
    if (data.status === 'completed') {
      stopPoll()
      state.value = 'done'
    } else if (data.status === 'failed') {
      stopPoll()
      error.value = data.error || 'The load failed'
      state.value = 'error'
    }
  } catch {
    // transient poll failures are ignored; the next tick retries
  }
}

function stopPoll() {
  if (poll) { clearInterval(poll); poll = null }
}
onUnmounted(stopPoll)
</script>

<style scoped>
.sample-banner {
  display: flex;
  gap: 1rem;
  align-items: flex-start;
  background: #f0fdfa;
  border: 1px solid #99f6e4;
  border-left: 4px solid #0d9488;
  border-radius: var(--radius-md);
  padding: 1rem 1.2rem;
  margin-bottom: 2rem;
}
.sb-icon { font-size: 1.6rem; line-height: 1.2; flex-shrink: 0; }
.sb-body { flex: 1; min-width: 0; }
.sb-title { font-weight: 700; margin-bottom: 0.35rem; }
.sb-text, .sb-alt {
  font-size: 0.9rem;
  line-height: 1.5;
  margin: 0 0 0.5rem;
  color: var(--text);
}
.sb-alt { color: var(--text-muted); }
.sb-actions { display: flex; gap: 0.5rem; flex-wrap: wrap; margin-top: 0.7rem; }

.sb-progress { margin-top: 0.7rem; }
.sb-progress-head {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  font-size: 0.9rem;
  flex-wrap: wrap;
  margin-bottom: 0.45rem;
}
.sb-eta { font-size: 0.85rem; }
.sb-bar {
  height: 6px;
  background: #ccfbf1;
  border-radius: 3px;
  overflow: hidden;
}
.sb-fill {
  height: 100%;
  background: #0d9488;
  border-radius: 3px;
  transition: width 0.4s ease;
  min-width: 2%;
}
.sb-pct { font-size: 0.8rem; margin-top: 0.3rem; }

.sb-done, .sb-error {
  margin-top: 0.7rem;
  font-size: 0.9rem;
  display: flex;
  align-items: center;
  gap: 0.6rem;
  flex-wrap: wrap;
}
.sb-done { color: #047857; font-weight: 600; }
.sb-error { color: #b91c1c; }
.small-btn { padding: 0.3rem 0.65rem; font-size: 0.82rem; }

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  flex-shrink: 0;
  border: 2px solid #ccfbf1;
  border-top-color: #0d9488;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
