<template>
  <Wizard
    title="Browse cohorts & patients"
    subtitle="Pick a cohort, explore population analytics, and drill into individual patients."
    icon="🔍"
    accent="#7c3aed"
    :steps="steps"
    finishLabel="Done"
    @finish="onFinish"
  >
    <!-- ═══════════════ Step 0: Cohort picker ═══════════════ -->
    <template #step-0>
      <h2>Pick a cohort</h2>

      <!-- Loading -->
      <div v-if="cohortsLoading" class="state-msg">
        <span class="spinner"></span> Loading cohorts…
      </div>

      <!-- Error -->
      <div v-else-if="cohortsError" class="alert-err">
        ✗ {{ cohortsError }}
        <button class="ghost small-btn" style="margin-left:0.8rem" @click="loadCohorts">Retry</button>
      </div>

      <!-- Empty -->
      <div v-else-if="cohorts.length === 0" class="state-msg muted">
        No cohorts found. Generate synthetic patients or ingest external FHIR data first.
      </div>

      <!-- List -->
      <div v-else>
        <div class="search-row">
          <input v-model="cohortSearch" type="text" placeholder="Filter cohorts…" class="search-input" />
          <span class="muted" style="font-size:0.85rem">{{ filteredCohorts.length }} of {{ cohorts.length }}</span>
        </div>
        <div class="sort-bar">
          <span class="muted sort-label">Sort by:</span>
          <div class="sort-group">
            <button class="sort-btn" :class="{ active: sortKey === 'none' }" @click="sortKey = 'none'">Default</button>
            <button class="sort-btn" :class="{ active: sortKey === 'date' }" @click="sortKey = 'date'">📅 Date</button>
            <button class="sort-btn" :class="{ active: sortKey === 'size' }" @click="sortKey = 'size'">👥 Size</button>
          </div>
          <template v-if="sortKey !== 'none'">
            <div class="sort-group">
              <button class="sort-btn" :class="{ active: sortDir === 'desc' }" @click="sortDir = 'desc'">↓ Desc</button>
              <button class="sort-btn" :class="{ active: sortDir === 'asc' }"  @click="sortDir = 'asc'">↑ Asc</button>
            </div>
          </template>
        </div>
        <div class="item-list">
          <button
            v-for="c in filteredCohorts"
            :key="c.cohort_id"
            class="item-row"
            :class="{ selected: data.cohortId === c.cohort_id }"
            @click="selectCohort(c)"
          >
            <div class="item-main">
              <div class="item-name">{{ c.cohort_id }}</div>
              <div class="item-meta">
                <span class="pill" :class="c.source">{{ c.source }}</span>
                <span class="muted">{{ c.patient_count }} patients</span>
                <span v-if="c.created_at && c.created_at !== 'unknown'" class="muted">· {{ c.created_at }}</span>
              </div>
            </div>
            <span class="chevron">›</span>
          </button>
        </div>

        <!-- store maintenance: leftovers of cohorts deleted before the
             full trace sweep existed -->
        <div class="maint-row">
          <button
            v-if="cleanupState === 'idle' && !cleanupConfirm"
            class="ghost small-btn"
            @click="cleanupConfirm = true"
          >🧹 Clean up leftovers of deleted cohorts</button>
          <template v-if="cleanupConfirm && cleanupState === 'idle'">
            <span class="muted" style="font-size:0.85rem">
              Scans the whole store for resources still tagged to cohorts that no longer
              exist and removes them. Can take a few minutes. Continue?
            </span>
            <button class="danger-btn" @click="runCleanup">Yes, clean up</button>
            <button class="ghost small-btn" @click="cleanupConfirm = false">Cancel</button>
          </template>
          <span v-if="cleanupState === 'running'" class="state-msg" style="padding:0">
            <span class="spinner"></span> Cleaning up. Scanning the whole store…
          </span>
          <div v-if="cleanupState === 'done'" class="alert-ok" style="margin:0">✓ {{ cleanupSummary }}</div>
          <div v-if="cleanupState === 'error'" class="alert-err" style="margin:0">
            ✗ {{ cleanupError }}
            <button class="ghost small-btn" style="margin-left:0.6rem" @click="cleanupState = 'idle'">Dismiss</button>
          </div>
        </div>
      </div>
    </template>

    <!-- ═══════════════ Step 1: Cohort detail (Analytics + Patients in parallel tabs) ═══════════════ -->
    <template #step-1>
      <!-- Header with cohort ID + KPIs -->
      <div class="detail-header">
        <h2 style="margin:0">
          <span class="accent-text">{{ data.cohortId }}</span>
        </h2>
        <div class="kpi-row">
          <div class="kpi-card">
            <div class="kpi-label">Patients</div>
            <div class="kpi-value">{{ patientsLoading ? '…' : cohortPatients.length }}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Female / Male</div>
            <div class="kpi-value">{{ kpiFemale }} / {{ kpiMale }}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Source</div>
            <div class="kpi-value" style="font-size:1rem">{{ selectedCohort?.source ?? '—' }}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Created</div>
            <div class="kpi-value" style="font-size:0.85rem; font-weight:500">
              {{ selectedCohort?.created_at ?? '—' }}
            </div>
          </div>
        </div>
      </div>

      <!-- ── Parallel section tabs ── -->
      <div class="section-tabs">
        <button
          class="section-tab"
          :class="{ active: data.section === 'analytics' }"
          @click="data.section = 'analytics'"
        >📊 Analytics</button>
        <button
          class="section-tab"
          :class="{ active: data.section === 'patients' }"
          @click="data.section = 'patients'"
        >👥 Patients <span v-if="!patientsLoading && cohortPatients.length" class="count-badge">{{ cohortPatients.length }}</span></button>
      </div>

      <!-- ══ ANALYTICS PANEL ══ -->
      <div v-show="data.section === 'analytics'">
        <!-- Chart tabs -->
        <div class="tabs">
          <button
            v-for="t in chartTabs"
            :key="t.key"
            class="tab"
            :class="{ active: data.tab === t.key }"
            @click="data.tab = t.key"
          >
            {{ t.label }}
          </button>
        </div>

        <!-- Sub-mode toggle -->
        <div class="sub-toggle">
          <button
            v-for="m in subModes"
            :key="m.value"
            class="sub-btn"
            :class="{ active: data.subMode === m.value }"
            @click="data.subMode = m.value"
          >
            {{ m.label }}
          </button>
        </div>

        <!-- Chart image -->
        <div class="chart-area">
          <div v-if="chartImgError" class="chart-placeholder muted">
            No data available for this chart.
          </div>
          <img
            v-else
            :key="chartUrl"
            :src="chartUrl"
            class="chart-img"
            alt="Chart"
            @error="chartImgError = true"
            @load="chartImgError = false"
          />
        </div>

        <!-- Danger zone -->
        <details class="danger-zone">
          <summary>Danger zone</summary>
          <div style="padding:0.8rem 0 0">
            <p class="muted" style="margin-bottom:0.6rem">Permanently deletes the cohort with all its patients, clinical records, and provider resources (shared resources are only untagged).</p>
            <button v-if="deleteState === 'idle'" class="danger-btn" @click="confirmDelete = true">🗑 Delete cohort</button>
            <div v-if="confirmDelete && deleteState === 'idle'" class="confirm-row">
              <span class="muted">Are you sure? This cannot be undone.</span>
              <button class="danger-btn" @click="deleteCohort">Yes, delete</button>
              <button class="ghost" @click="confirmDelete = false">Cancel</button>
            </div>
            <div v-if="deleteState === 'loading'" class="state-msg">
              <span class="spinner"></span> Deleting…
            </div>
            <div v-if="deleteState === 'error'" class="alert-err">✗ {{ deleteError }}</div>
          </div>
        </details>
      </div>

      <!-- ══ PATIENTS PANEL ══ -->
      <div v-show="data.section === 'patients'">

        <!-- ── LIST VIEW ── -->
        <template v-if="!selectedPatient">
          <div v-if="patientsLoading" class="state-msg"><span class="spinner"></span> Loading patients…</div>
          <div v-else-if="patientsError" class="alert-err">✗ {{ patientsError }}</div>
          <div v-else>
            <div class="search-row">
              <input v-model="patientSearch" type="text" placeholder="Search by ID…" class="search-input" />
              <span class="muted" style="font-size:0.85rem">{{ displayedPatients.length }} shown</span>
            </div>
            <div v-if="cohortPatients.length === 0" class="state-msg muted">No patients found in this cohort.</div>
            <div v-else class="item-list">
              <div v-for="p in displayedPatients" :key="p.id" class="patient-row" @click="openPatient(p)" style="cursor:pointer">
                <div class="item-main">
                  <div class="item-name">{{ p.id }}</div>
                  <div class="item-meta">
                    <span class="muted">{{ p.gender }}</span>
                    <span class="muted">·</span>
                    <span class="muted">{{ ageLabel(p.birth_date) }}</span>
                    <span v-if="p.ethnicity && p.ethnicity !== 'unknown'" class="muted">· {{ p.ethnicity }}</span>
                  </div>
                </div>
                <div class="patient-actions">
                  <button class="ghost small-btn" @click.stop="openPdf(p.id)">📄 PDF</button>
                  <button class="ghost small-btn" :disabled="fhirExporting[p.id]" @click.stop="exportFhir(p.id)">
                    {{ fhirExporting[p.id] ? '…' : '⤓ FHIR' }}
                  </button>
                  <button class="primary small-btn" @click.stop="openPatient(p)">📅 View Patient's Timeline</button>
                </div>
              </div>
            </div>
            <div v-if="displayedPatients.length === 0 && cohortPatients.length > 0" class="state-msg muted">No patients match your search.</div>
          </div>
        </template>

        <!-- ── DETAIL VIEW ── -->
        <div v-else class="pdv">

          <!-- Header -->
          <div class="pdv-header">
            <button class="ghost small-btn" @click="selectedPatient = null">← Back to list</button>
            <div class="pdv-id">
              <div class="item-name">{{ selectedPatient.id }}</div>
              <div class="item-meta">
                <span class="muted">{{ selectedPatient.gender }}</span>
                <span class="muted">· DOB {{ selectedPatient.birth_date }}</span>
                <span class="muted">· {{ ageLabel(selectedPatient.birth_date) }}</span>
                <span v-if="selectedPatient.ethnicity && selectedPatient.ethnicity !== 'unknown'" class="muted">· {{ selectedPatient.ethnicity }}</span>
              </div>
            </div>
            <div class="pdv-actions">
              <button class="ghost small-btn" @click="openPdf(selectedPatient.id)">📄 PDF</button>
              <button class="ghost small-btn" :disabled="fhirExporting[selectedPatient.id]" @click="exportFhir(selectedPatient.id)">
                {{ fhirExporting[selectedPatient.id] ? '…' : '⤓ FHIR' }}
              </button>
            </div>
          </div>

          <!-- Loading / error -->
          <div v-if="bundleLoading" class="state-msg"><span class="spinner"></span> Loading patient record…</div>
          <div v-else-if="bundleError" class="alert-err">✗ {{ bundleError }}</div>

          <!-- Tabs: Records | Timeline -->
          <template v-else-if="parsedBundle">
            <div class="section-tabs">
              <button class="section-tab" :class="{ active: patientTab === 'records' }" @click="patientTab = 'records'">
                📋 Records
              </button>
              <button class="section-tab" :class="{ active: patientTab === 'timeline' }" @click="patientTab = 'timeline'">
                📅 Timeline
                <span class="count-badge">{{ parsedBundle.timeline.length }}</span>
              </button>
            </div>

            <!-- ════ RECORDS ════ -->
            <div v-show="patientTab === 'records'">
              <div class="subtabs">
                <button v-for="t in recordTabs" :key="t.key"
                  class="subtab" :class="{ active: recordsTab === t.key }"
                  @click="recordsTab = t.key"
                >
                  {{ t.icon }} {{ t.label }}
                  <span class="count-badge" v-if="parsedBundle[t.key].length">{{ parsedBundle[t.key].length }}</span>
                </button>
              </div>

              <!-- Conditions -->
              <div v-show="recordsTab === 'conditions'" class="records-panel">
                <div v-if="!parsedBundle.conditions.length" class="state-msg muted">No conditions recorded.</div>
                <table v-else class="rec-table">
                  <thead><tr><th>Condition</th><th>Status</th><th>Onset</th></tr></thead>
                  <tbody>
                    <tr v-for="c in parsedBundle.conditions" :key="c.id">
                      <td>{{ c.code?.text || c.code?.coding?.[0]?.display || '—' }}</td>
                      <td><span class="status-badge" :class="c.clinicalStatus?.coding?.[0]?.code">{{ c.clinicalStatus?.coding?.[0]?.code || '—' }}</span></td>
                      <td class="muted date-cell">{{ fmtDate(c.onsetDateTime || c.recordedDate) }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <!-- Medications -->
              <div v-show="recordsTab === 'medications'" class="records-panel">
                <div v-if="!parsedBundle.medications.length" class="state-msg muted">No medications recorded.</div>
                <table v-else class="rec-table">
                  <thead><tr><th>Medication</th><th>Status</th><th>Date</th></tr></thead>
                  <tbody>
                    <tr v-for="m in parsedBundle.medications" :key="m.id">
                      <td>{{ m.medicationCodeableConcept?.text || m.medicationCodeableConcept?.coding?.[0]?.display || '—' }}</td>
                      <td><span class="status-badge" :class="m.status">{{ m.status || '—' }}</span></td>
                      <td class="muted date-cell">{{ fmtDate(m.authoredOn || m.dateAsserted) }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <!-- Procedures -->
              <div v-show="recordsTab === 'procedures'" class="records-panel">
                <div v-if="!parsedBundle.procedures.length" class="state-msg muted">No procedures recorded.</div>
                <table v-else class="rec-table">
                  <thead><tr><th>Procedure</th><th>Status</th><th>Date</th></tr></thead>
                  <tbody>
                    <tr v-for="pr in parsedBundle.procedures" :key="pr.id">
                      <td>{{ pr.code?.text || pr.code?.coding?.[0]?.display || '—' }}</td>
                      <td><span class="status-badge" :class="pr.status">{{ pr.status || '—' }}</span></td>
                      <td class="muted date-cell">{{ fmtDate(pr.performedDateTime || pr.performedPeriod?.start) }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <!-- Observations -->
              <div v-show="recordsTab === 'observations'" class="records-panel">
                <div v-if="!parsedBundle.observations.length" class="state-msg muted">No observations recorded.</div>
                <template v-else>
                  <p v-if="parsedBundle.observations.length > 100" class="muted" style="font-size:0.82rem;margin-bottom:0.5rem">Showing first 100 of {{ parsedBundle.observations.length }}</p>
                  <table class="rec-table">
                    <thead><tr><th>Observation</th><th>Value</th><th>Date</th></tr></thead>
                    <tbody>
                      <tr v-for="o in parsedBundle.observations.slice(0, 100)" :key="o.id">
                        <td>{{ o.code?.text || o.code?.coding?.[0]?.display || '—' }}</td>
                        <td class="obs-val">{{ obsValue(o) }}</td>
                        <td class="muted date-cell">{{ fmtDate(o.effectiveDateTime || o.effectivePeriod?.start) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </template>
              </div>

              <!-- Encounters -->
              <div v-show="recordsTab === 'encounters'" class="records-panel">
                <div v-if="!parsedBundle.encounters.length" class="state-msg muted">No encounters recorded.</div>
                <table v-else class="rec-table">
                  <thead><tr><th>Type</th><th>Class</th><th>Start</th><th>End</th></tr></thead>
                  <tbody>
                    <tr v-for="e in parsedBundle.encounters" :key="e.id">
                      <td>{{ e.type?.[0]?.text || e.type?.[0]?.coding?.[0]?.display || 'Encounter' }}</td>
                      <td class="muted">{{ e.class?.code || '—' }}</td>
                      <td class="muted date-cell">{{ fmtDate(e.period?.start) }}</td>
                      <td class="muted date-cell">{{ fmtDate(e.period?.end) }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <!-- ════ TIMELINE ════ -->
            <div v-show="patientTab === 'timeline'" class="tl-panel">
              <div v-if="!parsedBundle.timeline.length" class="state-msg muted">No dated events found.</div>
              <div v-else class="tl">
                <div v-for="(ev, idx) in parsedBundle.timeline" :key="idx" class="tl-row">
                  <div class="tl-date">{{ fmtDate(ev.date) }}</div>
                  <div class="tl-track">
                    <div class="tl-dot" :class="`tlt-${typeKey(ev.type)}`"></div>
                    <div class="tl-line" v-if="idx < parsedBundle.timeline.length - 1"></div>
                  </div>
                  <div class="tl-body">
                    <span class="tl-badge" :class="`tlt-${typeKey(ev.type)}`">{{ ev.type }}</span>
                    <span class="tl-label">{{ ev.label }}</span>
                    <span v-if="ev.detail" class="tl-detail muted">{{ ev.detail }}</span>
                  </div>
                </div>
              </div>
            </div>
          </template>
        </div>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch, onMounted } from 'vue'
import axios from 'axios'
import Wizard from '../components/Wizard.vue'
import { store, goHome, nextStep, goToStep } from '../state.js'

// ─── reactive data ───────────────────────────────────────────────────────────
// Reuse the previous selection if the user left and came back.
const data = store.flowData.browse ?? reactive({
  cohortId: '',
  section: 'analytics', // 'analytics' | 'patients'
  tab: 'conditions',
  subMode: 'overall',
})
store.flowData.browse = data

// ─── cohorts ─────────────────────────────────────────────────────────────────
const cohorts         = ref([])
const cohortsLoading  = ref(false)
const cohortsError    = ref('')
const cohortSearch    = ref('')
const sortKey         = ref('none')  // 'none' | 'date' | 'size'
const sortDir         = ref('desc')  // 'desc' | 'asc'

const filteredCohorts = computed(() => {
  let list = cohorts.value
  if (cohortSearch.value.trim()) {
    const q = cohortSearch.value.toLowerCase()
    list = list.filter(c => c.cohort_id.toLowerCase().includes(q) || c.source?.toLowerCase().includes(q))
  }
  if (sortKey.value === 'date') {
    list = [...list].sort((a, b) => {
      const da = a.created_at && a.created_at !== 'unknown' ? new Date(a.created_at).getTime() : 0
      const db = b.created_at && b.created_at !== 'unknown' ? new Date(b.created_at).getTime() : 0
      return sortDir.value === 'desc' ? db - da : da - db
    })
  } else if (sortKey.value === 'size') {
    list = [...list].sort((a, b) =>
      sortDir.value === 'desc' ? b.patient_count - a.patient_count : a.patient_count - b.patient_count
    )
  }
  return list
})

const selectedCohort = computed(() =>
  cohorts.value.find(c => c.cohort_id === data.cohortId) ?? null
)

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

// ─── deleted-cohort leftovers cleanup ────────────────────────────────────────
const cleanupConfirm = ref(false)
const cleanupState = ref('idle') // idle | running | done | error
const cleanupSummary = ref('')
const cleanupError = ref('')

async function runCleanup() {
  cleanupConfirm.value = false
  cleanupState.value = 'running'
  try {
    const { data: resp } = await axios.post(
      `${store.apiBase}/synthetic/synthea/cleanup-deleted-cohorts`,
      null,
      { timeout: 1800_000 },
    )
    const dead = resp.dead_cohorts?.length ?? 0
    cleanupSummary.value = dead
      ? `Removed traces of ${dead} deleted cohort(s): ${resp.resources_deleted} resources deleted`
        + (resp.tags_stripped ? `, ${resp.tags_stripped} shared resources untagged` : '')
        + (resp.failed ? ` (${resp.failed} failures)` : '')
        + (resp.expunge?.ok ? '. Storage expunged.' : '.')
      : 'No leftovers found. The store is clean.'
    cleanupState.value = 'done'
    await loadCohorts()
  } catch (e) {
    cleanupError.value = e?.response?.data?.detail ?? e.message ?? 'Cleanup failed'
    cleanupState.value = 'error'
  }
}

// ─── patients ────────────────────────────────────────────────────────────────
const allPatients    = ref([])
const patientsLoading = ref(false)
const patientsError   = ref('')
let patientsFetched   = false

const cohortPatients = computed(() =>
  allPatients.value.filter(p => p.cohort_ids?.includes(data.cohortId))
)

const kpiFemale = computed(() => cohortPatients.value.filter(p => p.gender === 'female').length || '—')
const kpiMale   = computed(() => cohortPatients.value.filter(p => p.gender === 'male').length || '—')

async function loadPatients() {
  if (patientsFetched) return
  patientsLoading.value = true
  patientsError.value = ''
  try {
    const { data: resp } = await axios.get(`${store.apiBase}/synthetic/synthea/list-all-patients`)
    allPatients.value = resp.patients ?? []
    patientsFetched = true
  } catch (e) {
    patientsError.value = e?.response?.data?.detail ?? e.message ?? 'Failed to load patients'
  } finally {
    patientsLoading.value = false
  }
}

// Cohort-change side effects (loading patients, clearing stale detail state)
// live in a single watch near the step gating at the bottom of this script,
// after everything it touches has been declared.

// Patient search
const patientSearch = ref('')
const displayedPatients = computed(() => {
  const q = patientSearch.value.toLowerCase().trim()
  if (!q) return cohortPatients.value
  return cohortPatients.value.filter(p => p.id.toLowerCase().includes(q))
})

function ageLabel(bd) {
  if (!bd || bd === 'unknown') return '—'
  return `${Math.floor((Date.now() - new Date(bd).getTime()) / (365.25 * 86400000))} y`
}

function fmtDate(d) {
  if (!d) return '—'
  try { return new Date(d).toLocaleDateString('en-GB', { year: 'numeric', month: 'short', day: 'numeric' }) }
  catch { return d }
}

// ─── patient detail view ─────────────────────────────────────────────────────
const selectedPatient = ref(null)
const parsedBundle    = ref(null)
const bundleLoading   = ref(false)
const bundleError     = ref('')
const patientTab      = ref('records')     // 'records' | 'timeline'
const recordsTab      = ref('conditions')  // which sub-tab

const recordTabs = [
  { key: 'conditions',   icon: '🩺', label: 'Conditions' },
  { key: 'medications',  icon: '💊', label: 'Medications' },
  { key: 'procedures',   icon: '🔧', label: 'Procedures' },
  { key: 'observations', icon: '📊', label: 'Observations' },
  { key: 'encounters',   icon: '🏥', label: 'Encounters' },
]

async function openPatient(patient) {
  selectedPatient.value = patient
  parsedBundle.value    = null
  bundleLoading.value   = true
  bundleError.value     = ''
  patientTab.value      = 'records'
  recordsTab.value      = 'conditions'
  try {
    // _count=1000: HAPI pages $everything at 20 entries by default, which
    // silently truncates the record view for any realistic patient.
    const { data: bundle } = await axios.get(
      `${store.apiBase}/stats/patients/${patient.id}/$everything?_count=1000`
    )
    parsedBundle.value = parseBundle(bundle)
  } catch (e) {
    bundleError.value = e?.response?.data?.detail ?? e.message ?? 'Failed to load patient record'
  } finally {
    bundleLoading.value = false
  }
}

function parseBundle(bundle) {
  const resources = (bundle.entry ?? []).map(e => e.resource).filter(Boolean)
  const byType = {}
  for (const r of resources) {
    if (!byType[r.resourceType]) byType[r.resourceType] = []
    byType[r.resourceType].push(r)
  }
  return {
    conditions:   byType.Condition ?? [],
    medications:  [...(byType.MedicationRequest ?? []), ...(byType.MedicationStatement ?? [])],
    procedures:   byType.Procedure ?? [],
    observations: byType.Observation ?? [],
    encounters:   byType.Encounter ?? [],
    timeline:     buildTimeline(resources),
  }
}

function buildTimeline(resources) {
  const events = []
  for (const r of resources) {
    let date = null, label = '', detail = ''
    const t = r.resourceType
    if (t === 'Condition') {
      date   = r.onsetDateTime || r.recordedDate
      label  = r.code?.text || r.code?.coding?.[0]?.display || 'Condition'
      detail = r.clinicalStatus?.coding?.[0]?.code ?? ''
    } else if (t === 'MedicationRequest' || t === 'MedicationStatement') {
      date   = r.authoredOn || r.dateAsserted
      label  = r.medicationCodeableConcept?.text || r.medicationCodeableConcept?.coding?.[0]?.display || 'Medication'
      detail = r.status ?? ''
    } else if (t === 'Procedure') {
      date   = r.performedDateTime || r.performedPeriod?.start
      label  = r.code?.text || r.code?.coding?.[0]?.display || 'Procedure'
      detail = r.status ?? ''
    } else if (t === 'Observation') {
      date   = r.effectiveDateTime || r.effectivePeriod?.start
      label  = r.code?.text || r.code?.coding?.[0]?.display || 'Observation'
      detail = obsValue(r)
    } else if (t === 'Encounter') {
      date   = r.period?.start
      label  = r.type?.[0]?.text || r.type?.[0]?.coding?.[0]?.display || 'Encounter'
      detail = r.class?.code ?? ''
    } else if (t === 'Immunization') {
      date   = r.occurrenceDateTime || r.occurrenceString
      label  = r.vaccineCode?.text || r.vaccineCode?.coding?.[0]?.display || 'Immunization'
    } else if (t === 'AllergyIntolerance') {
      date   = r.recordedDate || r.onsetDateTime
      label  = r.code?.text || r.code?.coding?.[0]?.display || 'Allergy'
      detail = r.criticality ?? ''
    }
    if (date && label) events.push({ type: t, date, label, detail })
  }
  return events.sort((a, b) => new Date(b.date) - new Date(a.date))
}

function obsValue(r) {
  if (r.valueQuantity) {
    const v = r.valueQuantity.value?.toFixed?.(1) ?? r.valueQuantity.value
    return `${v} ${r.valueQuantity.unit ?? ''}`.trim()
  }
  return r.valueCodeableConcept?.text ?? r.valueString ?? '—'
}

const TYPE_KEY = {
  Condition: 'condition', MedicationRequest: 'medication', MedicationStatement: 'medication',
  Procedure: 'procedure', Observation: 'observation', Encounter: 'encounter',
  Immunization: 'immunization', AllergyIntolerance: 'allergy',
}
function typeKey(t) { return TYPE_KEY[t] ?? 'other' }

// ─── per-patient actions ─────────────────────────────────────────────────────
function openPdf(patientId) {
  window.open(`${store.apiBase}/synthetic/synthea/patient/${patientId}/pdf`, '_blank')
}

const fhirExporting = reactive({})

async function exportFhir(patientId) {
  fhirExporting[patientId] = true
  try {
    const { data: bundle } = await axios.get(
      `${store.apiBase}/stats/patients/${patientId}/$everything?_count=1000`,
    )
    const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' })
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    a.href     = url
    a.download = `patient-${patientId}-everything.json`
    a.click()
    URL.revokeObjectURL(url)
  } catch (e) {
    alert(`FHIR export failed: ${e?.response?.data?.detail ?? e.message}`)
  } finally {
    fhirExporting[patientId] = false
  }
}

// ─── analytics chart ─────────────────────────────────────────────────────────
const chartTabs = [
  { key: 'conditions',   label: 'Conditions' },
  { key: 'observations', label: 'Observations' },
  { key: 'procedures',   label: 'Procedures' },
  { key: 'medications',  label: 'Medications' },
  { key: 'diagnostics',  label: 'Diagnostics' },
]
const subModes = [
  { value: 'overall',   label: 'Overall' },
  { value: 'by-gender', label: 'By gender' },
  { value: 'by-age',    label: 'By age' },
]

const chartImgError = ref(false)
watch([() => data.tab, () => data.subMode, () => data.cohortId], () => {
  chartImgError.value = false
})

const chartUrl = computed(() => {
  if (!data.cohortId) return ''
  const suffix = data.subMode === 'overall' ? '' : `-${data.subMode}`
  return `${store.apiBase}/stats/visualize-${data.tab}${suffix}?cohort_id=${encodeURIComponent(data.cohortId)}&limit=15`
})

// ─── cohort delete ────────────────────────────────────────────────────────────
const confirmDelete = ref(false)
const deleteState   = ref('idle')  // idle | loading | done | error
const deleteError   = ref('')

async function deleteCohort() {
  confirmDelete.value = false
  deleteState.value = 'loading'
  try {
    await axios.delete(`${store.apiBase}/synthetic/synthea/delete-cohort/${data.cohortId}`)
    // Refresh the list, clear the (now dangling) selection, and return to the
    // cohort picker. Staying on the detail step of a deleted cohort is a trap.
    await loadCohorts()
    data.cohortId = ''
    patientsFetched = false
    allPatients.value = []
    goToStep(0)
  } catch (e) {
    deleteError.value = e?.response?.data?.detail ?? e.message ?? 'Delete failed'
    deleteState.value = 'error'
  }
}

// ─── step gating ─────────────────────────────────────────────────────────────
const steps = reactive([
  { label: 'Pick cohort', canAdvance: false },
  { label: 'Browse',      canAdvance: true },
])

// Single cohort-change watch: gates the stepper, loads patients (also on mount,
// when a restored selection means the initial value is already set), and clears
// state left over from a previously viewed cohort.
watch(() => data.cohortId, (id) => {
  steps[0].canAdvance = !!id
  selectedPatient.value = null
  parsedBundle.value = null
  bundleError.value = ''
  confirmDelete.value = false
  deleteState.value = 'idle'
  deleteError.value = ''
  if (id) loadPatients()
}, { immediate: true })

function onFinish() {
  goHome()
}

function selectCohort(c) {
  data.cohortId = c.cohort_id
  nextStep(steps.length)
}
</script>

<style scoped>
/* ─── layout helpers ─── */
.accent-text { color: #7c3aed; }

.detail-header {
  display: flex;
  flex-direction: column;
  gap: 0.8rem;
  margin-bottom: 1rem;
}

/* ─── section tabs (Analytics / Patients) ─── */
.section-tabs {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 1.4rem;
  padding: 0.3rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
}
.section-tab {
  flex: 1;
  padding: 0.7rem 1.4rem;
  border: none;
  background: transparent;
  border-radius: calc(var(--radius-sm) - 2px);
  font-size: 1rem;
  font-weight: 600;
  color: var(--text-muted);
  cursor: pointer;
  transition: background 0.14s, color 0.14s, box-shadow 0.14s;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.45rem;
  white-space: nowrap;
}
.section-tab:hover { color: var(--text); background: color-mix(in srgb, #7c3aed 6%, white); }
.section-tab.active {
  background: var(--surface);
  color: #7c3aed;
  box-shadow: 0 1px 4px rgba(0,0,0,0.10);
  font-weight: 700;
}
.count-badge {
  background: #7c3aed;
  color: #fff;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 700;
  padding: 0.05rem 0.45rem;
  line-height: 1.4;
}

.state-msg {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  padding: 1rem 0;
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
.alert-ok {
  background: #f0fdf4;
  border: 1px solid #bbf7d0;
  border-radius: var(--radius-sm);
  color: #15803d;
  padding: 0.7rem 1rem;
  margin: 0.5rem 0;
}

/* ─── search ─── */
.search-row {
  display: flex;
  align-items: center;
  gap: 0.8rem;
  margin-bottom: 0.7rem;
}
.search-input {
  flex: 1;
}

/* ─── cohort list ─── */
.item-list {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
}
.item-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  padding: 0.75rem 1rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  cursor: pointer;
  text-align: left;
  color: inherit;
  transition: all 0.12s ease;
}
.item-row:hover  { border-color: #7c3aed; background: #faf5ff; }
.item-row.selected { border-color: #7c3aed; background: #f5f3ff; }

.item-main { flex: 1; min-width: 0; }
.item-name {
  font-weight: 600;
  margin-bottom: 0.15rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.9rem;
}
.item-meta {
  display: flex;
  gap: 0.5rem;
  font-size: 0.82rem;
  align-items: center;
  flex-wrap: wrap;
}
.chevron { color: var(--text-muted); font-size: 1.3rem; }

/* ─── pills ─── */
.pill {
  font-size: 0.72rem;
  font-weight: 600;
  border-radius: 999px;
  padding: 0.1rem 0.5rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  color: var(--text-muted);
}
.pill.synthetic { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.pill.external  { background: #ecfdf5; border-color: #a7f3d0; color: #047857; }
.pill.unknown   { background: var(--surface-alt); }

/* ─── KPI row ─── */
.kpi-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 0.7rem;
  margin-bottom: 1.2rem;
}
.kpi-card {
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.75rem 1rem;
}
.kpi-label {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: 0.3rem;
}
.kpi-value { font-size: 1.4rem; font-weight: 700; }

/* ─── analytics tabs ─── */
.tabs {
  display: flex;
  gap: 0;
  border-bottom: 1px solid var(--border);
  margin-bottom: 0.5rem;
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
  font-size: 0.9rem;
}
.tab:hover { color: var(--text); }
.tab.active { color: #7c3aed; border-bottom-color: #7c3aed; font-weight: 600; }

.sub-toggle {
  display: flex;
  gap: 0.3rem;
  margin-bottom: 0.8rem;
}
.sub-btn {
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.3rem 0.75rem;
  font-size: 0.82rem;
  cursor: pointer;
  color: var(--text-muted);
}
.sub-btn.active {
  background: #f5f3ff;
  border-color: #7c3aed;
  color: #7c3aed;
  font-weight: 600;
}

.chart-area {
  min-height: 200px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--surface);
  overflow: hidden;
}
.chart-img {
  width: 100%;
  max-width: 900px;
  height: auto;
  display: block;
}
.chart-placeholder { padding: 2rem; font-size: 0.9rem; }

/* ─── danger zone ─── */
.danger-zone {
  margin-top: 1.5rem;
  border: 1px solid #fecaca;
  border-radius: var(--radius-sm);
  padding: 0.6rem 1rem;
}
.danger-zone summary {
  cursor: pointer;
  color: #b91c1c;
  font-size: 0.85rem;
  font-weight: 600;
}
.danger-btn {
  background: #fef2f2;
  border: 1px solid #fecaca;
  border-radius: var(--radius-sm);
  color: #b91c1c;
  padding: 0.4rem 0.9rem;
  cursor: pointer;
  font-size: 0.85rem;
}
.danger-btn:hover { background: #fee2e2; }
.confirm-row {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  margin-top: 0.5rem;
  flex-wrap: wrap;
}

/* ─── patient list ─── */
.patient-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 0.6rem;
  padding: 0.75rem 1rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  transition: border-color 0.12s;
  margin-bottom: 0.4rem;
}
.patient-row:hover { border-color: #7c3aed; }
.patient-actions { display: flex; gap: 0.3rem; flex-shrink: 0; }

.small-btn {
  padding: 0.3rem 0.65rem;
  font-size: 0.82rem;
}

/* ─── patient detail ─── */
.patient-detail {
  width: 100%;
  padding-top: 0.6rem;
  border-top: 1px solid var(--border);
}
.detail-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}
.detail-table th, .detail-table td {
  text-align: left;
  padding: 0.3rem 0.6rem;
  border-bottom: 1px solid var(--border);
}
.detail-table th {
  width: 120px;
  font-weight: 600;
  color: var(--text-muted);
  background: var(--surface-alt);
}

/* ─── maintenance row ─── */
.maint-row {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  flex-wrap: wrap;
  margin-top: 0.8rem;
  padding-top: 0.7rem;
  border-top: 1px dashed var(--border);
}

/* ─── sort bar ─── */
.sort-bar {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex-wrap: wrap;
  margin-bottom: 0.7rem;
  font-size: 0.83rem;
}
.sort-label { white-space: nowrap; }
.sort-group {
  display: flex;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
}
.sort-btn {
  padding: 0.28rem 0.7rem;
  background: var(--surface);
  border: none;
  border-right: 1px solid var(--border);
  cursor: pointer;
  color: var(--text-muted);
  font-size: 0.82rem;
  white-space: nowrap;
}
.sort-btn:last-child { border-right: none; }
.sort-btn:hover { background: var(--surface-alt); color: var(--text); }
.sort-btn.active { background: #ede9fe; color: #7c3aed; font-weight: 600; }

/* ─── patient detail view (pdv) ─── */
.pdv { }
.pdv-header {
  display: flex;
  align-items: flex-start;
  gap: 0.8rem;
  flex-wrap: wrap;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.75rem 1rem;
  margin-bottom: 1rem;
}
.pdv-id { flex: 1; min-width: 0; }
.pdv-actions { display: flex; gap: 0.3rem; flex-shrink: 0; align-self: center; }

/* ─── sub-tabs (records) ─── */
.subtabs {
  display: flex;
  flex-wrap: wrap;
  gap: 0.3rem;
  margin-bottom: 0.8rem;
}
.subtab {
  display: flex;
  align-items: center;
  gap: 0.35rem;
  padding: 0.35rem 0.8rem;
  font-size: 0.83rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  cursor: pointer;
  color: var(--text-muted);
}
.subtab:hover { border-color: #7c3aed; color: var(--text); }
.subtab.active { background: #f5f3ff; border-color: #7c3aed; color: #7c3aed; font-weight: 600; }

/* ─── records table ─── */
.records-panel { overflow-x: auto; }
.rec-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}
.rec-table th {
  text-align: left;
  padding: 0.45rem 0.7rem;
  background: var(--surface-alt);
  border-bottom: 2px solid var(--border);
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--text-muted);
  white-space: nowrap;
}
.rec-table td {
  padding: 0.4rem 0.7rem;
  border-bottom: 1px solid var(--border);
  vertical-align: top;
}
.rec-table tr:last-child td { border-bottom: none; }
.rec-table tr:hover td { background: #faf5ff; }
.date-cell { white-space: nowrap; font-size: 0.82rem; }
.obs-val   { font-family: ui-monospace, Menlo, monospace; font-size: 0.82rem; }

/* ─── status badges ─── */
.status-badge {
  display: inline-block;
  padding: 0.1rem 0.45rem;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 600;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  color: var(--text-muted);
  white-space: nowrap;
}
.status-badge.active     { background: #dcfce7; border-color: #86efac; color: #15803d; }
.status-badge.completed  { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.status-badge.resolved   { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.status-badge.stopped    { background: #fff7ed; border-color: #fed7aa; color: #c2410c; }
.status-badge.cancelled  { background: #fef2f2; border-color: #fecaca; color: #b91c1c; }

/* ─── timeline ─── */
.tl-panel { padding-top: 0.2rem; }
.tl { display: flex; flex-direction: column; }
.tl-row {
  display: grid;
  grid-template-columns: 110px 28px 1fr;
  gap: 0 0.5rem;
  align-items: flex-start;
}
.tl-date {
  text-align: right;
  font-size: 0.78rem;
  color: var(--text-muted);
  padding-top: 0.2rem;
  white-space: nowrap;
}
.tl-track {
  display: flex;
  flex-direction: column;
  align-items: center;
}
.tl-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-top: 0.25rem;
  border: 2px solid white;
  box-shadow: 0 0 0 1px var(--border);
}
.tl-line {
  width: 2px;
  flex: 1;
  min-height: 18px;
  background: var(--border);
  margin: 2px 0;
}
.tl-body {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 0.3rem 0.5rem;
  padding-bottom: 0.8rem;
  font-size: 0.85rem;
}
.tl-badge {
  font-size: 0.68rem;
  font-weight: 700;
  padding: 0.08rem 0.45rem;
  border-radius: 999px;
  white-space: nowrap;
  flex-shrink: 0;
}
.tl-label  { color: var(--text); font-weight: 500; }
.tl-detail { font-size: 0.8rem; }

/* timeline type colours */
.tlt-condition   { background: #fee2e2; color: #b91c1c; }
.tlt-condition.tl-dot   { background: #ef4444; }
.tlt-medication  { background: #dcfce7; color: #15803d; }
.tlt-medication.tl-dot  { background: #22c55e; }
.tlt-procedure   { background: #f3e8ff; color: #7c3aed; }
.tlt-procedure.tl-dot   { background: #a855f7; }
.tlt-observation { background: #dbeafe; color: #1d4ed8; }
.tlt-observation.tl-dot { background: #3b82f6; }
.tlt-encounter   { background: #fef3c7; color: #b45309; }
.tlt-encounter.tl-dot   { background: #f59e0b; }
.tlt-immunization { background: #ccfbf1; color: #0f766e; }
.tlt-immunization.tl-dot{ background: #14b8a6; }
.tlt-allergy     { background: #fce7f3; color: #be185d; }
.tlt-allergy.tl-dot     { background: #ec4899; }
.tlt-other       { background: var(--surface-alt); color: var(--text-muted); }
.tlt-other.tl-dot       { background: #94a3b8; }

/* ─── spinner ─── */
.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  flex-shrink: 0;
  border: 2px solid var(--border);
  border-top-color: #7c3aed;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
