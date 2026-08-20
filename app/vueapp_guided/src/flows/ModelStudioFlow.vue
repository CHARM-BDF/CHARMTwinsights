<template>
  <Wizard
    title="Browse & register models"
    subtitle="See every registered model, inspect its schema, or upload a new one."
    accent="#ea580c"
    :steps="steps"
    finishLabel="Done"
    @finish="onFinish"
  >
    <!-- Step 0: What do you want to do? -->
    <template #step-0>
      <h2>What would you like to do?</h2>
      <p class="muted">
        To <em>run</em> a model across a cohort, use the "Apply a model to a cohort" flow instead.
      </p>

      <div class="mode-grid">
        <button
          class="mode-card"
          :class="{ selected: data.mode === 'browse' }"
          @click="data.mode = 'browse'"
        >
          <div class="mode-icon">🗂️</div>
          <div>
            <div class="mode-title">Browse registry</div>
            <div class="mode-sub">See every registered model and its schema / README</div>
          </div>
        </button>
        <button
          class="mode-card"
          :class="{ selected: data.mode === 'register' }"
          @click="data.mode = 'register'"
        >
          <div class="mode-icon">➕</div>
          <div>
            <div class="mode-title">Register / upload new</div>
            <div class="mode-sub">Add a Docker image + LinkML schemas to the registry</div>
          </div>
        </button>
      </div>
    </template>

    <!-- Step 1 branches on mode -->
    <template #step-1>
      <template v-if="data.mode === 'browse'">
        <h2>Registered models</h2>
        <p v-if="loadingModels" class="muted" style="margin-top: 0.8rem">Loading models…</p>
        <p v-else-if="loadError" class="error">{{ loadError }}</p>
        <p v-else-if="!models.length" class="muted" style="margin-top: 0.8rem">No models are registered yet.</p>
        <div class="model-list">
          <div
            v-for="m in models"
            :key="m.image"
            class="model-row"
            :class="{ expanded: data.expandedModel === m.image }"
          >
            <div class="model-summary" @click="toggleExpand(m.image)">
              <div class="model-main">
                <div class="model-title">{{ m.title }}</div>
                <div class="model-meta muted">{{ m.image }} · {{ m.authors }}</div>
                <p class="model-desc">{{ m.short_description }}</p>
              </div>
              <span class="chevron">{{ data.expandedModel === m.image ? '▾' : '▸' }}</span>
            </div>
            <div v-if="data.expandedModel === m.image" class="model-detail">
              <template v-if="data.details[m.image]?._error">
                <p class="error">{{ data.details[m.image]._error }}</p>
              </template>
              <template v-else-if="data.details[m.image]">
                <template v-if="data.details[m.image].readme">
                  <div class="pill">README</div>
                  <div
                    class="markdown-body"
                    v-html="renderMarkdown(data.details[m.image].readme)"
                  ></div>
                </template>
                <div class="pill">input schema</div>
                <pre
                  v-if="data.details[m.image].input_schema"
                  class="code-block"
                  v-html="highlightJson(data.details[m.image].input_schema)"
                ></pre>
                <pre v-else class="code-block">(none)</pre>
                <div class="pill">output schema</div>
                <pre
                  v-if="data.details[m.image].output_schema"
                  class="code-block"
                  v-html="highlightJson(data.details[m.image].output_schema)"
                ></pre>
                <pre v-else class="code-block">(none)</pre>
              </template>
              <p v-else class="muted">Loading details…</p>
            </div>
          </div>
        </div>
      </template>

      <template v-else>
        <h2>Register a new model</h2>
        <p class="muted">Backed by <code>POST /modeling/models</code>.</p>

        <div class="field-row">
          <div class="field">
            <label>Docker image tag</label>
            <input type="text" v-model="data.reg.image" placeholder="e.g. mymodel:latest" />
          </div>
          <div class="field">
            <label>Authors</label>
            <input type="text" v-model="data.reg.authors" />
          </div>
        </div>
        <p class="muted">
          The Docker image must already exist on the model_server host before it can be registered.
        </p>

        <div class="field">
          <label>Title</label>
          <input type="text" v-model="data.reg.title" />
        </div>
        <div class="field">
          <label>Short description</label>
          <input type="text" v-model="data.reg.description" />
        </div>
        <div class="field">
          <label>README (markdown)</label>
          <textarea rows="5" v-model="data.reg.readme" placeholder="## What this model does…"></textarea>
        </div>

        <p class="muted">
          LinkML input / output schemas can be baked into the container, or pasted on the next step.
        </p>
      </template>
    </template>

    <!-- Step 2: schema paste (register) or confirmation (browse) -->
    <template #step-2>
      <template v-if="data.mode === 'register'">
        <h2>Schemas &amp; examples</h2>
        <p class="muted">
          Optional. Leave blank if the container has <code>/app/input_schema.{yaml,json}</code>.
        </p>

        <div class="field">
          <label>LinkML input schema (JSON)</label>
          <textarea rows="6" v-model="data.reg.inputSchema" placeholder='{"classes": {...}}'></textarea>
        </div>
        <div class="field">
          <label>LinkML output schema (JSON)</label>
          <textarea rows="6" v-model="data.reg.outputSchema"></textarea>
        </div>
        <div class="field">
          <label>Example inputs (JSON array)</label>
          <textarea rows="4" v-model="data.reg.examples" placeholder='[{"age": 50, "bmi": 28}]'></textarea>
        </div>

        <p v-if="regError" class="error">{{ regError }}</p>
        <div v-if="regResult" class="reg-result">
          <p><strong>Registered {{ regResult.image }}.</strong></p>
          <details v-if="regResult.example_predictions">
            <summary>Example predictions</summary>
            <pre class="schema">{{ JSON.stringify(regResult.example_predictions, null, 2) }}</pre>
          </details>
          <details v-if="regResult.registration_logs">
            <summary>Registration logs</summary>
            <pre class="schema">{{ regResult.registration_logs.stdout }}{{ regResult.registration_logs.stderr }}</pre>
          </details>
          <button class="ghost" type="button" @click="goHome">Done</button>
        </div>
      </template>

      <template v-else>
        <h2>Schema details</h2>
        <p class="muted">
          Pick a model on the previous step to drill in. (Full detail view coming when backend
          wiring lands.)
        </p>
      </template>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, onMounted } from 'vue'
import { listModels, getModel, registerModel } from '../api/models.js'
import { renderMarkdown, highlightJson } from '../utils/format.js'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

const models = ref([])
const loadError = ref('')
const loadingModels = ref(false)

onMounted(async () => {
  loadingModels.value = true
  try {
    models.value = await listModels()
  } catch (e) {
    loadError.value = e?.response?.data?.detail || e?.message || 'Failed to load models'
  } finally {
    loadingModels.value = false
  }
})

// Reuse previously entered values if the user left and came back.
const data = store.flowData.models ?? reactive({
  mode: 'browse',
  expandedModel: null,
  details: {},
  reg: {
    image: '',
    title: '',
    description: '',
    authors: '',
    readme: '',
    inputSchema: '',
    outputSchema: '',
    examples: '',
  },
})
store.flowData.models = data

const steps = [
  { label: 'Mode' },
  { label: 'Details' },
  { label: 'Confirm' },
]

async function toggleExpand(image) {
  if (data.expandedModel === image) { data.expandedModel = null; return }
  data.expandedModel = image
  if (!data.details[image]) {
    try {
      data.details[image] = await getModel(image)
    } catch (e) {
      data.details[image] = { _error: e?.response?.data?.detail || e?.message || 'Failed to load model' }
    }
  }
}

const regResult = ref(null)
const regError = ref('')

function parseMaybeJson(text) {
  const t = (text || '').trim()
  if (!t) return undefined
  return JSON.parse(t) // may throw; caught in onFinish
}

async function onFinish() {
  if (data.mode !== 'register') { goHome(); return }
  regError.value = ''
  regResult.value = null
  try {
    const payload = {
      image: data.reg.image,
      title: data.reg.title,
      short_description: data.reg.description,
      authors: data.reg.authors,
    }
    if (data.reg.readme?.trim()) payload.readme = data.reg.readme
    const inputSchema = parseMaybeJson(data.reg.inputSchema)
    if (inputSchema) payload.input_schema = inputSchema
    const outputSchema = parseMaybeJson(data.reg.outputSchema)
    if (outputSchema) payload.output_schema = outputSchema
    const examples = parseMaybeJson(data.reg.examples)
    if (examples) payload.examples = examples

    regResult.value = await registerModel(payload)
  } catch (e) {
    regError.value = e?.response?.data?.detail || e?.message || 'Registration failed'
  }
}
</script>

<style scoped>
.mode-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 1rem;
  margin-top: 0.5rem;
}
.mode-card {
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
.mode-card:hover { border-color: #ea580c; }
.mode-card.selected {
  border-color: #ea580c;
  background: #fff7ed;
}
.mode-icon { font-size: 1.7rem; }
.mode-title { font-weight: 600; margin-bottom: 0.2rem; }
.mode-sub { font-size: 0.85rem; color: var(--text-muted); }

.model-list {
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
  margin-top: 1rem;
}
.model-row {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
  transition: border-color 0.15s ease;
}
.model-row.expanded {
  border-color: #ea580c;
}
.model-summary {
  display: flex;
  gap: 1rem;
  padding: 1rem 1.2rem;
  cursor: pointer;
  align-items: center;
}
.model-main { flex: 1; min-width: 0; }
.model-title { font-weight: 600; }
.model-meta { font-size: 0.85rem; margin-bottom: 0.3rem; }
.model-desc { font-size: 0.9rem; color: var(--text); margin: 0; }
.chevron { color: var(--text-muted); font-size: 1.1rem; }

.model-detail {
  padding: 0.8rem 1.2rem 1.2rem;
  border-top: 1px solid var(--border);
  background: var(--surface-alt);
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
}
.schema {
  margin: 0;
  padding: 0.6rem 0.8rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  font-size: 0.8rem;
  overflow-x: auto;
}

textarea {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.85rem;
  resize: vertical;
}

.error { color: #dc2626; }

.reg-result {
  margin-top: 1rem;
  padding: 1rem 1.2rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
</style>
