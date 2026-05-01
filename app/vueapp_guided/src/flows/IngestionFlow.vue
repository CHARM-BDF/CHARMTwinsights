<template>
  <Wizard
    title="Ingest external FHIR data"
    subtitle="Upload a FHIR bundle, validate it, tag with cohort metadata, then commit."
    icon="📥"
    accent="#059669"
    :steps="steps"
    finishLabel="Commit to FHIR store"
    @finish="onFinish"
  >
    <!-- Step 0: Upload -->
    <template #step-0>
      <h2>Upload a FHIR bundle</h2>
      <p class="muted">Paste JSON, or drag a file onto the box.</p>

      <textarea
        v-model="data.bundleText"
        rows="10"
        placeholder='{"resourceType": "Bundle", "type": "collection", "entry": [...]}'
      ></textarea>
      <p class="muted" style="margin-top: 0.4rem">
        {{ data.bundleText.length }} characters
      </p>
    </template>

    <!-- Step 1: Validate -->
    <template #step-1>
      <h2>Validate</h2>
      <div class="stub-banner">New endpoint needed</div>
      <p style="margin-top: 1rem">
        Validation will call a new
        <code>POST /ingest/validate</code> route that proxies HAPI's
        <code>$validate</code> operation in dry-run mode.
      </p>

      <div class="validation-result">
        <div class="validation-header">
          <span class="validation-ok">✓</span>
          <strong>Bundle looks well-formed (placeholder)</strong>
        </div>
        <ul class="validation-list">
          <li>Parsed as JSON: <strong>yes</strong></li>
          <li>Has <code>resourceType: Bundle</code>: <strong>yes</strong></li>
          <li>Entry count: <strong>—</strong></li>
          <li>HAPI <code>$validate</code> result: <em>pending backend endpoint</em></li>
        </ul>
      </div>
    </template>

    <!-- Step 2: Tag -->
    <template #step-2>
      <h2>Tag &amp; classify</h2>
      <p class="muted">These tags let you group ingested data later.</p>

      <div class="field-row">
        <div class="field">
          <label>Cohort ID</label>
          <input type="text" v-model="data.cohortId" placeholder="e.g. mobile-app-users" />
        </div>
        <div class="field">
          <label>Data type</label>
          <select v-model="data.datatype">
            <option value="external">external</option>
            <option value="synthetic">synthetic</option>
          </select>
        </div>
      </div>

      <p class="muted">
        All patient IDs will be prefixed with <code>ext-</code> automatically to prevent
        collisions with synthetic data.
      </p>
    </template>

    <!-- Step 3: Commit -->
    <template #step-3>
      <h2>Review &amp; commit</h2>

      <div class="summary">
        <h3>Summary</h3>
        <ul>
          <li>Bundle size: <strong>{{ data.bundleText.length }}</strong> chars</li>
          <li>Cohort: <strong>{{ data.cohortId || '(not set)' }}</strong></li>
          <li>Datatype: <strong>{{ data.datatype }}</strong></li>
        </ul>
      </div>

      <p class="muted" style="margin-top: 0.8rem">
        On commit we'll POST to <code>/ingest/fhir</code>. Re-ingesting a patient with the
        same ID updates their record rather than creating a duplicate.
      </p>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive } from 'vue'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

const data = reactive({
  bundleText: '',
  cohortId: '',
  datatype: 'external',
})
store.flowData.ingest = data

const steps = [
  { label: 'Upload' },
  { label: 'Validate' },
  { label: 'Tag' },
  { label: 'Commit' },
]

function onFinish() {
  alert('Commit to HAPI FHIR store would happen here.')
  goHome()
}
</script>

<style scoped>
textarea {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.85rem;
  resize: vertical;
}

.validation-result {
  background: #f0fdf4;
  border: 1px solid #bbf7d0;
  border-radius: var(--radius-sm);
  padding: 1rem 1.2rem;
  margin-top: 1rem;
}
.validation-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.6rem;
}
.validation-ok {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  background: var(--success);
  color: white;
  border-radius: 50%;
  font-size: 0.85rem;
}
.validation-list {
  margin: 0;
  padding-left: 1.2rem;
  font-size: 0.9rem;
}
.validation-list li {
  margin-bottom: 0.25rem;
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
