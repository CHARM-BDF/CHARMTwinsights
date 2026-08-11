<template>
  <Wizard
    title="Ingest external FHIR data"
    subtitle="Upload a FHIR bundle, validate it, tag with cohort metadata, then commit."
    icon="📨"
    accent="#059669"
    :steps="steps"
    finishLabel="Commit to FHIR store"
    @finish="onFinish"
  >
    <!-- Step 0: Source (paste or upload) -->
    <template #step-0>
      <h2>Provide a FHIR bundle</h2>

      <div class="source-tabs" role="tablist">
        <button
          role="tab"
          :class="{ active: data.source === 'paste' }"
          @click="data.source = 'paste'"
        >
          📋 Paste JSON
        </button>
        <button
          role="tab"
          :class="{ active: data.source === 'upload' }"
          @click="data.source = 'upload'"
        >
          📁 Upload file
        </button>
      </div>

      <!-- Paste mode -->
      <div v-if="data.source === 'paste'">
        <textarea
          v-model="data.bundleText"
          rows="14"
          placeholder='{"resourceType": "Bundle", "type": "collection", "entry": [...]}'
          @input="onTextChange"
        ></textarea>
        <div class="hint-row">
          <span>{{ data.bundleText.length.toLocaleString() }} characters</span>
          <span v-if="parseStatus.ok" class="ok-pill">✓ valid JSON</span>
          <span v-else-if="data.bundleText.length > 0" class="err-pill">
            ✗ {{ parseStatus.error }}
          </span>
        </div>
      </div>

      <!-- Upload mode -->
      <div v-else>
        <div
          class="dropzone"
          :class="{ dragging: isDragging, filled: !!data.fileName }"
          @dragenter.prevent="isDragging = true"
          @dragover.prevent="isDragging = true"
          @dragleave.prevent="isDragging = false"
          @drop.prevent="onDrop"
          @click="$refs.fileInput.click()"
        >
          <input
            ref="fileInput"
            type="file"
            accept=".json,application/json,application/fhir+json"
            style="display: none"
            @change="onFileChange"
          />
          <div v-if="!data.fileName" class="dropzone-empty">
            <div class="dropzone-icon">📁</div>
            <div><strong>Drag a FHIR bundle here</strong> or click to browse</div>
            <div class="muted">Accepts <code>.json</code> (a single FHIR Bundle)</div>
          </div>
          <div v-else class="dropzone-filled">
            <div class="file-row">
              <span class="file-icon">📄</span>
              <div class="file-info">
                <div class="file-name">{{ data.fileName }}</div>
                <div class="muted">
                  {{ formatBytes(data.fileSize) }} ·
                  {{ data.bundleText.length.toLocaleString() }} characters
                  <span v-if="parseStatus.ok" class="ok-pill">✓ valid JSON</span>
                  <span v-else class="err-pill">✗ {{ parseStatus.error }}</span>
                </div>
              </div>
              <button class="ghost small" @click.stop="clearFile">Remove</button>
            </div>
          </div>
        </div>
        <p v-if="readError" class="err-pill" style="margin-top: 0.6rem">
          ✗ {{ readError }}
        </p>
      </div>
    </template>

    <!-- Step 1: Tag -->
    <template #step-1>
      <h2>Tag &amp; classify</h2>
      <p class="muted">These tags let you group ingested data later.</p>

      <div class="field-row">
        <div class="field">
          <label>Cohort ID</label>
          <input
            type="text"
            v-model="data.cohortId"
            placeholder="e.g. mobile-app-users"
            :class="{ 'input-error': data.cohortId && !cohortIdValid }"
          />
          <div v-if="data.cohortId && !cohortIdValid" class="err-pill" style="margin-top: 0.3rem">
            ✗ Must contain only letters, numbers, hyphens, and periods (1–64 chars).
            Underscores are not allowed.
          </div>
          <div v-else-if="!data.cohortId" class="muted" style="margin-top: 0.3rem">
            Defaults to <code>external</code> if left blank.
          </div>
          <div v-else class="muted" style="margin-top: 0.3rem">
            Auto-suggested from the current timestamp — edit if you want a custom name.
          </div>
        </div>

        <div class="field">
          <label>Data type</label>
          <select v-model="data.datatype">
            <option value="external">external</option>
            <option value="synthetic">synthetic</option>
          </select>
        </div>
      </div>

      <p class="muted" style="margin-top: 0.8rem">
        All patient IDs will be prefixed with <code>ext-</code> automatically to prevent
        collisions with synthetic data.
      </p>
    </template>

    <!-- Step 2: Commit -->
    <template #step-2>
      <h2>Review &amp; commit</h2>

      <div class="summary">
        <h3>About to ingest</h3>
        <ul>
          <li>
            Source:
            <strong>{{ data.source === 'upload' ? `file (${data.fileName})` : 'pasted JSON' }}</strong>
          </li>
          <li>Bundle size: <strong>{{ data.bundleText.length.toLocaleString() }}</strong> chars</li>
          <li>Entries in bundle (informational): <strong>{{ parsedBundle?.entry?.length ?? '—' }}</strong></li>
          <li>Patient resources (informational): <strong>{{ patientCount }}</strong></li>
          <li>Cohort ID: <strong>{{ data.cohortId || 'external' }}</strong></li>
          <li>Datatype: <strong>{{ data.datatype }}</strong></li>
        </ul>
      </div>

      <p class="muted" style="margin-top: 0.8rem">
        Submits to <code>POST {{ apiBase }}/ingest/fhir</code>. The FHIR store will validate
        the bundle and surface any errors below. Re-ingesting a patient with the same ID
        updates the existing record rather than creating a duplicate.
      </p>

      <!-- Loading -->
      <div v-if="submitState === 'loading'" class="submit-state loading">
        <span class="spinner"></span> Submitting bundle…
      </div>

      <!-- Success -->
      <div v-if="submitState === 'success'" class="submit-state ok">
        <h3>✓ Ingested successfully</h3>
        <ul>
          <li>Cohort: <strong>{{ submitResult.cohort_id }}</strong></li>
          <li>Datatype: <strong>{{ submitResult.datatype }}</strong></li>
          <li>Patients stored: <strong>{{ submitResult.patient_count }}</strong></li>
          <li v-if="submitResult.patient_ids?.length">
            IDs:
            <code>{{ submitResult.patient_ids.slice(0, 5).join(', ') }}</code>
            <span v-if="submitResult.patient_ids.length > 5" class="muted">
              (+{{ submitResult.patient_ids.length - 5 }} more)
            </span>
          </li>
        </ul>
        <details v-if="submitResult.tags_applied">
          <summary>Tags applied</summary>
          <pre>{{ JSON.stringify(submitResult.tags_applied, null, 2) }}</pre>
        </details>
      </div>

      <!-- Error -->
      <div v-if="submitState === 'error'" class="submit-state err">
        <h3>✗ Ingestion failed</h3>
        <p v-if="submitError.status">HTTP {{ submitError.status }}</p>
        <pre>{{ submitError.detail }}</pre>
      </div>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch } from 'vue'
import axios from 'axios'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

// ---------- reactive state ----------
// Build a sensible default cohort id like 'external-input-2026-05-13-1454'.
// Format chosen to satisfy the FHIR id regex (no underscores, no colons) while
// remaining human-readable and naturally sortable.
function defaultCohortId() {
  const now = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  return (
    'external-input-' +
    `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}` +
    `-${pad(now.getHours())}${pad(now.getMinutes())}`
  )
}

// Reuse previously entered values (including a pasted bundle) if the user
// left and came back; otherwise start fresh.
const data = store.flowData.ingest ?? reactive({
  source: 'paste', // 'paste' | 'upload'
  bundleText: '',
  fileName: '',
  fileSize: 0,
  cohortId: defaultCohortId(),
  datatype: 'external',
})
store.flowData.ingest = data

const isDragging = ref(false)
const readError = ref('')
const submitState = ref('idle') // 'idle' | 'loading' | 'success' | 'error'
const submitResult = ref(null)
const submitError = ref({ status: null, detail: '' })

const apiBase = computed(() => store.apiBase)

// ---------- parsing / validation ----------
const parsedBundle = computed(() => {
  if (!data.bundleText.trim()) return null
  try {
    return JSON.parse(data.bundleText)
  } catch {
    return null
  }
})

const parseStatus = computed(() => {
  if (!data.bundleText.trim()) {
    return { ok: false, error: 'no content yet' }
  }
  try {
    JSON.parse(data.bundleText)
    return { ok: true, error: '' }
  } catch (e) {
    return { ok: false, error: e.message }
  }
})

// Cheap informational count — used in the Commit summary as a heads-up,
// NOT for validation. Authoritative validation happens server-side at commit
// when the bundle is POSTed to HAPI.
const patientCount = computed(() => {
  const b = parsedBundle.value
  if (!b || !Array.isArray(b.entry)) return 0
  let n = 0
  for (const e of b.entry) {
    if (e?.resource?.resourceType === 'Patient') n += 1
  }
  return n
})

const FHIR_ID_RE = /^[A-Za-z0-9\-\.]{1,64}$/
const cohortIdValid = computed(() => FHIR_ID_RE.test(data.cohortId))

// ---------- step-level gating ----------
// Wizard reads `step.canAdvance` to enable/disable Next. We mutate it reactively.
// Only two real gates remain: (1) input must be parseable JSON; (2) cohort id
// must satisfy the FHIR id regex if the user set one. Anything FHIR-specific
// is delegated to the FHIR store at commit time.
const steps = reactive([
  { label: 'Source', canAdvance: false },
  { label: 'Tag', canAdvance: true },
  { label: 'Commit', canAdvance: true },
])

watch(
  [() => parseStatus.value.ok, () => data.bundleText.length],
  () => {
    steps[0].canAdvance = parseStatus.value.ok && data.bundleText.length > 0
  },
  { immediate: true },
)
watch(
  [() => data.cohortId, () => cohortIdValid.value],
  () => {
    // Empty is fine (defaults to "external" server-side); only block if non-empty AND invalid.
    steps[1].canAdvance = !data.cohortId || cohortIdValid.value
  },
  { immediate: true },
)

// ---------- file handling ----------
function onTextChange() {
  // Switching from upload mode back to paste invalidates the fileName label.
  if (data.fileName && data.source === 'paste') {
    data.fileName = ''
    data.fileSize = 0
  }
}

function clearFile() {
  data.bundleText = ''
  data.fileName = ''
  data.fileSize = 0
  readError.value = ''
}

function onFileChange(evt) {
  const file = evt.target.files?.[0]
  if (file) readFile(file)
}

function onDrop(evt) {
  isDragging.value = false
  const file = evt.dataTransfer?.files?.[0]
  if (file) readFile(file)
}

function readFile(file) {
  readError.value = ''
  const reader = new FileReader()
  reader.onload = () => {
    data.bundleText = String(reader.result || '')
    data.fileName = file.name
    data.fileSize = file.size
  }
  reader.onerror = () => {
    readError.value = `Could not read ${file.name}`
  }
  reader.readAsText(file)
}

function formatBytes(n) {
  if (!n && n !== 0) return ''
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / 1024 / 1024).toFixed(1)} MB`
}

// ---------- submit ----------
async function onFinish() {
  submitState.value = 'loading'
  submitResult.value = null
  submitError.value = { status: null, detail: '' }

  try {
    const payload = {
      bundle: parsedBundle.value,
      cohort_id: data.cohortId || 'external',
      datatype: data.datatype,
    }
    const { data: resp } = await axios.post(
      `${store.apiBase}/ingest/fhir`,
      payload,
      { timeout: 300_000 },
    )
    submitResult.value = resp
    submitState.value = 'success'
  } catch (e) {
    const status = e?.response?.status ?? null
    let detail = e?.response?.data?.detail ?? e?.response?.data ?? e?.message ?? 'Unknown error'
    if (typeof detail !== 'string') {
      try {
        detail = JSON.stringify(detail, null, 2)
      } catch {
        detail = String(detail)
      }
    }
    submitError.value = { status, detail }
    submitState.value = 'error'
  }
}

// Note: we no longer auto-goHome on finish. The user reviews the result and clicks
// Cancel/Home themselves. If you want auto-dismiss on success, call goHome() above.
</script>

<style scoped>
.source-tabs {
  display: flex;
  gap: 0.4rem;
  margin-bottom: 0.8rem;
}
.source-tabs button {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.5rem 0.9rem;
  font-size: 0.9rem;
  cursor: pointer;
  color: var(--text);
}
.source-tabs button.active {
  background: #ecfdf5;
  border-color: #059669;
  color: #047857;
  font-weight: 600;
}

textarea {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.85rem;
  resize: vertical;
}

.hint-row {
  display: flex;
  gap: 0.8rem;
  align-items: center;
  flex-wrap: wrap;
  font-size: 0.85rem;
  color: var(--text-muted);
  margin-top: 0.4rem;
}

.ok-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  background: #ecfdf5;
  color: #047857;
  border: 1px solid #a7f3d0;
  border-radius: 999px;
  padding: 0.1rem 0.55rem;
  font-size: 0.75rem;
  font-weight: 600;
}
.err-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  background: #fef2f2;
  color: #b91c1c;
  border: 1px solid #fecaca;
  border-radius: 999px;
  padding: 0.1rem 0.55rem;
  font-size: 0.75rem;
  font-weight: 600;
}
.err-text { color: #b91c1c; }

.dropzone {
  border: 2px dashed var(--border);
  border-radius: var(--radius-md);
  padding: 2rem 1.2rem;
  text-align: center;
  cursor: pointer;
  background: var(--surface);
  transition: all 0.15s ease;
}
.dropzone:hover,
.dropzone.dragging {
  border-color: #059669;
  background: #ecfdf5;
}
.dropzone.filled {
  text-align: left;
  padding: 1rem 1.2rem;
  cursor: default;
}
.dropzone-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.4rem;
}
.dropzone-icon { font-size: 1.6rem; }
.file-row {
  display: flex;
  align-items: center;
  gap: 0.8rem;
}
.file-icon { font-size: 1.4rem; }
.file-info { flex: 1; min-width: 0; }
.file-name {
  font-weight: 600;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.9rem;
  word-break: break-all;
}
.ghost.small {
  padding: 0.3rem 0.6rem;
  font-size: 0.8rem;
}

/* Tag step */
.input-error {
  border-color: #fecaca !important;
  background: #fef2f2;
}

/* Summary + submit-state */
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

.submit-state {
  margin-top: 1rem;
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
  background: #ecfdf5;
  border-color: #a7f3d0;
}
.submit-state.ok h3 { color: #047857; margin-bottom: 0.5rem; }
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
.submit-state details { margin-top: 0.5rem; }
.submit-state summary {
  cursor: pointer;
  font-size: 0.85rem;
  color: var(--text-muted);
}

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  border: 2px solid var(--border);
  border-top-color: #059669;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin {
  to { transform: rotate(360deg); }
}
</style>
