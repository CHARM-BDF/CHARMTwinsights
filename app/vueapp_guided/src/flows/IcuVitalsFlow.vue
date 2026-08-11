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
            <div class="mode-sub">Control conditioning; get interactive Plotly chart</div>
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
            <div class="mode-sub">N patients with randomized conditioning; download as JSON</div>
          </div>
        </button>
      </div>
    </template>

    <!-- Step 1: Conditioning (single) or batch size -->
    <template #step-1>
      <template v-if="data.mode === 'single'">
        <h2>Conditioning</h2>
        <p class="muted">Leave any field at "(random)" and the model will sample it freely.</p>

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
          Each patient is sampled with random conditioning across all four demographic features.
          Returns raw JSON you can download.
        </p>
      </template>
    </template>

    <!-- Step 2: Generate & results -->
    <template #step-2>
      <h2>{{ data.mode === 'single' ? 'Visualization' : 'Batch results' }}</h2>

      <!-- ── IDLE (before "Generate" is clicked) ── -->
      <div v-if="genStatus === 'idle'" class="pre-launch">
        <template v-if="data.mode === 'single'">
          <p>
            Calls
            <code>GET {{ store.apiBase }}/synthetic/timeseries/generate-visualization-1-patient</code>
            and embeds the interactive Plotly chart below.
          </p>
          <div class="conditioning-chips">
            <span class="chip" v-if="data.ethnicity !== null">{{ ethnicityLabel }}</span>
            <span class="chip" v-if="data.gender !== null">{{ genderLabel }}</span>
            <span class="chip" v-if="data.ageGroup !== null">{{ ageGroupLabel }}</span>
            <span class="chip" v-if="data.mortality !== null">{{ mortalityLabel }}</span>
            <span class="chip muted-chip" v-if="data.ethnicity === null && data.gender === null && data.ageGroup === null && data.mortality === null">
              all conditions randomized
            </span>
          </div>
        </template>
        <template v-else>
          <p>
            Posts to
            <code>POST {{ store.apiBase }}/synthetic/timeseries/generate-raw-n-patients</code>
            and returns <strong>{{ data.nPatients }}</strong> synthetic patient records as JSON.
          </p>
        </template>
      </div>

      <!-- ── LOADING ── -->
      <div v-if="genStatus === 'loading'" class="submit-state loading">
        <span class="spinner"></span>
        <span>{{ data.mode === 'single' ? 'Generating visualization…' : `Generating ${data.nPatients} patients…` }}</span>
      </div>

      <!-- ── SINGLE: embedded viz iframe ── -->
      <div v-if="genStatus === 'done' && data.mode === 'single'" class="viz-wrap">
        <div v-if="iframeLoading" class="viz-loading">
          <span class="spinner"></span> Loading chart…
        </div>
        <iframe
          :src="vizSrc"
          class="viz-iframe"
          :class="{ hidden: iframeLoading }"
          frameborder="0"
          @load="iframeLoading = false"
        ></iframe>
        <div class="viz-actions">
          <a :href="vizSrc" target="_blank" class="ghost small-btn">↗ Open in new tab</a>
          <button class="ghost small-btn" @click="downloadSingle" :disabled="!singleRawResult">⬇ Download JSON</button>
          <button class="ghost small-btn" @click="regenerate">↺ Regenerate</button>
        </div>
        <p class="muted" style="font-size:0.82rem; margin:0">
          Note: the chart and the downloaded JSON are two independent samples from the
          model — the JSON is not the exact series shown above.
        </p>
      </div>

      <!-- ── BATCH: summary + download ── -->
      <div v-if="genStatus === 'done' && data.mode === 'batch'" class="submit-state ok">
        <h3>✓ Batch generated</h3>
        <p><strong>{{ batchResult.length }}</strong> patients · 25 timesteps · 10 vital signs each</p>
        <button class="primary" style="margin-top:0.6rem" @click="downloadBatch">⬇ Download JSON</button>
        <button class="ghost" style="margin-top:0.6rem; margin-left:0.4rem" @click="showBatchCharts = !showBatchCharts">
          📊 {{ showBatchCharts ? 'Hide charts' : 'Show charts' }}
        </button>

        <!-- ── BATCH CHARTS ── -->
        <div v-if="showBatchCharts" class="batch-charts">
          <!-- Chart type tabs -->
          <div class="chart-type-tabs">
            <button
              :class="{ active: chartMode === 'patient' }"
              @click="chartMode = 'patient'"
            >Per patient — 10 vitals</button>
            <button
              :class="{ active: chartMode === 'feature' }"
              @click="chartMode = 'feature'"
            >All patients — one vital</button>
          </div>

          <!-- Per-patient controls -->
          <div v-if="chartMode === 'patient'" class="chart-controls">
            <label>Patient</label>
            <select v-model.number="selectedPatientIdx">
              <option v-for="(p, i) in batchResult" :key="i" :value="i">
                Patient {{ i + 1 }}
                ({{ ethnicityLabels[p.demographics?.ethnicity] ?? '?' }},
                {{ p.demographics?.gender === 0 ? 'F' : 'M' }},
                {{ ageGroupLabels[p.demographics?.age_group] ?? '?' }},
                {{ p.demographics?.mortality_label === 0 ? 'survived' : 'died' }})
              </option>
            </select>
          </div>

          <!-- By-feature controls -->
          <div v-if="chartMode === 'feature'" class="chart-controls">
            <label>Vital sign</label>
            <select v-model="selectedFeature">
              <option v-for="f in featureNames" :key="f" :value="f">{{ f }}</option>
            </select>
            <label style="margin-left:1rem">Overlay</label>
            <select v-model="aggregateLine">
              <option value="mean">Mean</option>
              <option value="median">Median</option>
            </select>
          </div>

          <!-- Canvas -->
          <div class="canvas-wrap">
            <canvas ref="batchChartCanvas"></canvas>
          </div>
        </div>

        <details style="margin-top:0.8rem">
          <summary>Preview (first 3 patients)</summary>
          <div class="preview-table-wrap">
            <table class="preview-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ethnicity</th>
                  <th>Gender</th>
                  <th>Age group</th>
                  <th>Outcome</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(p, i) in batchResult.slice(0, 3)" :key="i">
                  <td>{{ i + 1 }}</td>
                  <td>{{ ethnicityLabels[p.demographics?.ethnicity] ?? p.demographics?.ethnicity }}</td>
                  <td>{{ p.demographics?.gender === 0 ? 'Female' : 'Male' }}</td>
                  <td>{{ ageGroupLabels[p.demographics?.age_group] ?? p.demographics?.age_group }}</td>
                  <td>{{ p.demographics?.mortality_label === 0 ? 'Survived' : 'Died' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </details>
      </div>

      <!-- ── ERROR ── -->
      <div v-if="genStatus === 'error'" class="submit-state err">
        <h3>✗ Generation failed</h3>
        <pre>{{ genError }}</pre>
        <button class="ghost" style="margin-top:0.6rem" @click="genStatus = 'idle'">Try again</button>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch, nextTick, onBeforeUnmount } from 'vue'
import { Chart, registerables } from 'chart.js'
Chart.register(...registerables)
import axios from 'axios'
import Wizard from '../components/Wizard.vue'
import { store } from '../state.js'

// ---------- form data ----------
// Reuse previously entered values if the user left and came back.
const data = store.flowData.vitals ?? reactive({
  mode: 'single',
  ethnicity: null,
  gender: null,
  ageGroup: null,
  mortality: null,
  nPatients: 10,
})
store.flowData.vitals = data

// ---------- label maps ----------
const ethnicityLabels = { 0: 'White', 1: 'Black/AA', 2: 'Asian', 3: 'Other' }
const ageGroupLabels  = { 0: '0–30', 1: '30–50', 2: '50–70', 3: '70–100' }
const ethnicityLabel  = computed(() => ethnicityLabels[data.ethnicity] ?? '')
const genderLabel     = computed(() => data.gender === 0 ? 'Female' : data.gender === 1 ? 'Male' : '')
const ageGroupLabel   = computed(() => ageGroupLabels[data.ageGroup] ?? '')
const mortalityLabel  = computed(() => data.mortality === 0 ? 'Survived' : data.mortality === 1 ? 'Died' : '')

// ---------- steps ----------
const steps = [
  { label: 'Mode' },
  { label: 'Configure' },
  { label: 'Generate' },
]

// ---------- generation state ----------
const genStatus     = ref('idle')  // idle | loading | done | error
const genError      = ref('')
const vizSrc        = ref('')
const iframeLoading = ref(false)
const batchRawResp  = ref(null)   // full response object from backend
const batchResult   = ref([])     // just the patients array for display
const singleRawResult = ref(null) // full response for single patient

function buildVizUrl() {
  const params = new URLSearchParams()
  if (data.ethnicity  !== null) params.set('ethnicity',      data.ethnicity)
  if (data.gender     !== null) params.set('gender',         data.gender)
  if (data.ageGroup   !== null) params.set('age_group',      data.ageGroup)
  if (data.mortality  !== null) params.set('mortality_label', data.mortality)
  const qs = params.toString()
  return `${store.apiBase}/synthetic/timeseries/generate-visualization-1-patient${qs ? '?' + qs : ''}`
}

async function onFinish() {
  if (genStatus.value === 'loading') return

  if (data.mode === 'single') {
    genStatus.value = 'loading'
    singleRawResult.value = null
    // Fetch raw JSON and set viz URL in parallel
    const rawPromise = axios.post(
      `${store.apiBase}/synthetic/timeseries/generate-raw-1-patient`,
      { ethnicity: data.ethnicity, gender: data.gender, age_group: data.ageGroup, mortality_label: data.mortality },
      { timeout: 120_000 },
    ).then(r => { singleRawResult.value = r.data }).catch(() => {})
    await new Promise(r => setTimeout(r, 80))
    vizSrc.value = buildVizUrl()
    iframeLoading.value = true
    genStatus.value = 'done'
    await rawPromise
  } else {
    genStatus.value = 'loading'
    try {
      const { data: resp } = await axios.post(
        `${store.apiBase}/synthetic/timeseries/generate-raw-n-patients`,
        { n_patients: data.nPatients },
        { timeout: 300_000 },
      )
      batchRawResp.value  = resp
      batchResult.value   = Array.isArray(resp?.patients) ? resp.patients : (Array.isArray(resp) ? resp : [])
      genStatus.value = 'done'
    } catch (e) {
      const detail = e?.response?.data?.detail ?? e?.response?.data ?? e?.message ?? 'Unknown error'
      genError.value = typeof detail === 'string' ? detail : JSON.stringify(detail, null, 2)
      genStatus.value = 'error'
    }
  }
}

function regenerate() {
  genStatus.value = 'idle'
  vizSrc.value = ''
  singleRawResult.value = null
}

function triggerDownload(content, filename) {
  const blob = new Blob([JSON.stringify(content, null, 2)], { type: 'application/json' })
  const url  = URL.createObjectURL(blob)
  const a    = document.createElement('a')
  a.href     = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

function downloadSingle() {
  if (!singleRawResult.value) return
  triggerDownload(singleRawResult.value, `icu-vitals-single-patient.json`)
}

function downloadBatch() {
  triggerDownload(batchRawResp.value ?? batchResult.value, `icu-vitals-batch-${data.nPatients}-patients.json`)
}

// ─── batch charts ─────────────────────────────────────────────────────────────
const showBatchCharts   = ref(false)
const chartMode         = ref('patient')  // 'patient' | 'feature'
const selectedPatientIdx = ref(0)
const selectedFeature   = ref('')
const aggregateLine     = ref('mean')     // 'mean' | 'median'
const batchChartCanvas  = ref(null)
let   batchChart        = null

const featureNames = computed(() =>
  batchRawResp.value?.metadata?.feature_names ?? []
)

watch(featureNames, (names) => {
  if (names.length && !selectedFeature.value) selectedFeature.value = names[0]
}, { immediate: true })

watch(
  [showBatchCharts, chartMode, selectedPatientIdx, selectedFeature, aggregateLine],
  () => { if (showBatchCharts.value) nextTick(drawBatchChart) },
)

const VIZ_COLORS = [
  '#e03131','#1971c2','#2f9e44','#f08c00','#7048e8',
  '#0ca678','#d6336c','#1098ad','#4dabf7','#a9e34b',
]

function destroyChart() {
  if (batchChart) { batchChart.destroy(); batchChart = null }
}

function drawBatchChart() {
  if (!batchChartCanvas.value || !batchResult.value.length) return
  destroyChart()
  if (chartMode.value === 'patient') drawPatientChart()
  else drawFeatureChart()
}

function drawPatientChart() {
  const patient = batchResult.value[selectedPatientIdx.value]
  if (!patient) return
  const ts     = patient.timeseries
  const labels = ts.map(t => `t${t.timestep}`)
  const features = featureNames.value

  const datasets = features.map((feat, i) => ({
    label: feat,
    data: ts.map(t => t[feat] ?? null),
    borderColor: VIZ_COLORS[i % VIZ_COLORS.length],
    backgroundColor: 'transparent',
    borderWidth: 1.5,
    pointRadius: 0,
    tension: 0.3,
  }))

  const d = patient.demographics ?? {}
  const title = [
    `Patient ${selectedPatientIdx.value + 1}`,
    `${ethnicityLabels[d.ethnicity] ?? '?'}`,
    `${d.gender === 0 ? 'Female' : 'Male'}`,
    `${ageGroupLabels[d.age_group] ?? '?'}`,
    `${d.mortality_label === 0 ? 'Survived' : 'Died'}`,
  ].join(' · ')

  batchChart = new Chart(batchChartCanvas.value, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        title: { display: true, text: title, font: { size: 13 } },
        legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 13, font: { size: 10 } } },
        y: { ticks: { font: { size: 10 } } },
      },
    },
  })
}

function computeMean(arr) {
  const v = arr.filter(x => x != null)
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null
}

function computeMedian(arr) {
  const v = arr.filter(x => x != null).sort((a, b) => a - b)
  if (!v.length) return null
  const m = Math.floor(v.length / 2)
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2
}

function drawFeatureChart() {
  const feat = selectedFeature.value
  if (!feat || !batchResult.value.length) return

  const seqLen = batchRawResp.value?.metadata?.seq_len ?? 25
  const labels = Array.from({ length: seqLen }, (_, i) => `t${i}`)
  const aggregateFn = aggregateLine.value === 'median' ? computeMedian : computeMean

  const patientDatasets = batchResult.value.map((p, i) => ({
    label: `Patient ${i + 1}`,
    data: labels.map((_, t) => p.timeseries[t]?.[feat] ?? null),
    borderColor: 'rgba(120,120,200,0.25)',
    backgroundColor: 'transparent',
    borderWidth: 1,
    pointRadius: 0,
    tension: 0.3,
  }))

  const aggData = labels.map((_, t) =>
    aggregateFn(batchResult.value.map(p => p.timeseries[t]?.[feat] ?? null))
  )
  const aggDataset = {
    label: `${aggregateLine.value}`,
    data: aggData,
    borderColor: '#e03131',
    backgroundColor: 'transparent',
    borderWidth: 2.5,
    pointRadius: 0,
    tension: 0.3,
    order: 0,
  }

  batchChart = new Chart(batchChartCanvas.value, {
    type: 'line',
    data: { labels, datasets: [...patientDatasets, aggDataset] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        title: {
          display: true,
          text: `“${feat}” — all ${batchResult.value.length} patients + ${aggregateLine.value}`,
          font: { size: 13 },
        },
        legend: {
          display: true,
          labels: {
            filter: item => item.text === aggregateLine.value,
            boxWidth: 20,
            font: { size: 11 },
          },
        },
        tooltip: { mode: 'index', intersect: false },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 13, font: { size: 10 } } },
        y: { ticks: { font: { size: 10 } } },
      },
    },
  })
}

onBeforeUnmount(destroyChart)
</script>

<style scoped>
.mode-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
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
.mode-card:hover { border-color: #0891b2; }
.mode-card.selected {
  border-color: #0891b2;
  background: #ecfeff;
}
.mode-icon  { font-size: 1.8rem; }
.mode-title { font-weight: 600; margin-bottom: 0.2rem; }
.mode-sub   { font-size: 0.85rem; color: var(--text-muted); }

.pre-launch {
  color: var(--text);
}
.pre-launch p { margin: 0 0 0.6rem; }
.conditioning-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
  margin-top: 0.4rem;
}
.chip {
  background: #ecfeff;
  border: 1px solid #a5f3fc;
  color: #0e7490;
  border-radius: 999px;
  padding: 0.15rem 0.6rem;
  font-size: 0.8rem;
  font-weight: 600;
}
.muted-chip {
  background: var(--surface-alt);
  border-color: var(--border);
  color: var(--text-muted);
  font-weight: normal;
}

.submit-state {
  margin-top: 0.5rem;
  padding: 1rem 1.2rem;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
  background: var(--surface);
}
.submit-state.loading {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  color: var(--text-muted);
}
.submit-state.ok {
  background: #ecfeff;
  border-color: #a5f3fc;
}
.submit-state.ok h3 { color: #0e7490; margin-bottom: 0.4rem; }
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

.viz-wrap {
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
  margin-top: 0.4rem;
}
.viz-loading {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  padding: 1rem;
  color: var(--text-muted);
}
.viz-iframe {
  width: 100%;
  height: 560px;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--surface);
}
.viz-iframe.hidden { display: none; }
.viz-actions {
  display: flex;
  gap: 0.5rem;
}
.small-btn {
  padding: 0.35rem 0.8rem;
  font-size: 0.85rem;
}

.preview-table-wrap { overflow-x: auto; margin-top: 0.6rem; }
.preview-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}
.preview-table th, .preview-table td {
  text-align: left;
  padding: 0.35rem 0.6rem;
  border-bottom: 1px solid var(--border);
}
.preview-table th {
  font-weight: 600;
  color: var(--text-muted);
  background: var(--surface-alt);
}

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  flex-shrink: 0;
  border: 2px solid var(--border);
  border-top-color: #0891b2;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ─── batch charts ─── */
.batch-charts {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--border);
}
.chart-type-tabs {
  display: flex;
  gap: 0.4rem;
  margin-bottom: 0.8rem;
}
.chart-type-tabs button {
  padding: 0.35rem 0.9rem;
  font-size: 0.85rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  cursor: pointer;
  color: var(--text-muted);
}
.chart-type-tabs button:hover { border-color: #0891b2; color: var(--text); }
.chart-type-tabs button.active {
  background: #ecfeff;
  border-color: #0891b2;
  color: #0e7490;
  font-weight: 600;
}
.chart-controls {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex-wrap: wrap;
  margin-bottom: 0.8rem;
  font-size: 0.88rem;
}
.chart-controls label { color: var(--text-muted); }
.chart-controls select { max-width: 340px; }
.canvas-wrap {
  position: relative;
  height: 340px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.5rem;
}
.canvas-wrap canvas { width: 100% !important; height: 100% !important; }
</style>
