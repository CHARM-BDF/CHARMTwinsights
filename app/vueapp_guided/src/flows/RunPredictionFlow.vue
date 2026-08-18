<template>
  <Wizard
    title="Run a prediction"
    subtitle="Pick a registered model, fill in its inputs, and run it on a single record."
    accent="#f97316"
    :steps="steps"
    finishLabel="Done"
    @finish="onFinish"
  >
    <!-- Step 0: pick a model -->
    <template #step-0>
      <h2>Pick a model</h2>
      <p v-if="loadingModels" class="muted">Loading models…</p>
      <p v-else-if="modelsError" class="error">{{ modelsError }}</p>
      <p v-else-if="!models.length" class="muted">No models are registered yet.</p>
      <div v-else class="model-grid">
        <button
          v-for="m in models"
          :key="m.image"
          class="model-card"
          :class="{ selected: data.image === m.image }"
          @click="selectModel(m.image)"
        >
          <div class="model-card-title">{{ m.title || m.image }}</div>
          <div class="pill">{{ m.image }}</div>
          <p class="model-card-sub muted">{{ m.short_description }}</p>
        </button>
      </div>
    </template>

    <!-- Step 1: fill inputs (schema-driven) -->
    <template #step-1>
      <h2>Inputs for {{ data.image }}</h2>
      <p v-if="loadingSchema" class="muted">Loading schema…</p>
      <p v-else-if="schemaError" class="error">{{ schemaError }}</p>
      <template v-else-if="inputSchema">
        <div class="form-actions">
          <button class="ghost" type="button" @click="useExample">Use example</button>
        </div>
        <p v-if="exampleError" class="error">{{ exampleError }}</p>
        <div class="jsonforms-host">
          <JsonForms
            :data="data.formData"
            :schema="inputSchema"
            :renderers="renderers"
            @change="onFormChange"
          />
        </div>
        <p v-if="data.formErrors.length" class="muted">
          {{ data.formErrors.length }} field(s) need attention before running.
        </p>
      </template>
    </template>

    <!-- Step 2: result -->
    <template #step-2>
      <h2>Prediction</h2>
      <div class="form-actions">
        <button
          class="primary"
          type="button"
          :disabled="running || data.formErrors.length > 0"
          @click="runPrediction"
        >
          {{ running ? 'Running…' : 'Run prediction' }}
        </button>
      </div>
      <p v-if="runError" class="error">{{ runError }}</p>

      <table v-if="predictionRows.length" class="results-table">
        <thead>
          <tr><th v-for="c in resultColumns" :key="c">{{ c }}</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, i) in predictionRows" :key="i">
            <td v-for="c in resultColumns" :key="c">{{ formatCell(row[c]) }}</td>
          </tr>
        </tbody>
      </table>

      <details v-if="lastResult">
        <summary>Raw output</summary>
        <pre class="schema">{{ JSON.stringify(lastResult.predictions, null, 2) }}</pre>
      </details>
      <details v-if="lastResult && lastResult.stderr">
        <summary>Model logs (stderr)</summary>
        <pre class="schema">{{ lastResult.stderr }}</pre>
      </details>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch, onMounted } from 'vue'
import { JsonForms } from '@jsonforms/vue'
import { vanillaRenderers } from '@jsonforms/vue-vanilla'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'
import { listModels, getModel, getModelJsonSchema, predict } from '../api/models.js'

const renderers = Object.freeze([...vanillaRenderers])

const steps = reactive([
  { label: 'Model', canAdvance: false },
  { label: 'Inputs', canAdvance: false },
  { label: 'Result' },
])

const data = store.flowData.run ?? reactive({
  image: '',
  formData: {},
  formErrors: [],
})
store.flowData.run = data

const models = ref([])
const loadingModels = ref(false)
const modelsError = ref('')

const inputSchema = ref(null)
const outputSchema = ref(null)
const loadingSchema = ref(false)
const schemaError = ref('')
const exampleError = ref('')

const running = ref(false)
const runError = ref('')
const lastResult = ref(null)

watch(
  () => data.image,
  () => {
    steps[0].canAdvance = !!data.image
  },
  { immediate: true },
)

watch(
  () => [inputSchema.value, data.formErrors.length],
  () => {
    steps[1].canAdvance = !!inputSchema.value && data.formErrors.length === 0
  },
  { immediate: true },
)

onMounted(async () => {
  loadingModels.value = true
  modelsError.value = ''
  try {
    models.value = await listModels()
  } catch (e) {
    modelsError.value = errText(e, 'Failed to load models')
  } finally {
    loadingModels.value = false
  }
})

async function selectModel(image) {
  data.image = image
  data.formData = {}
  data.formErrors = []
  lastResult.value = null
  inputSchema.value = null
  outputSchema.value = null
  loadingSchema.value = true
  schemaError.value = ''
  exampleError.value = ''
  try {
    const js = await getModelJsonSchema(image)
    inputSchema.value = js.input_schema
    outputSchema.value = js.output_schema
  } catch (e) {
    schemaError.value = errText(e, 'Failed to load model schema')
  } finally {
    loadingSchema.value = false
  }
}

function onFormChange(event) {
  data.formData = event.data
  data.formErrors = event.errors ?? []
}

async function useExample() {
  exampleError.value = ''
  try {
    const detail = await getModel(data.image)
    const ex = detail.examples && detail.examples[0]
    if (ex) data.formData = { ...ex }
  } catch (e) {
    exampleError.value = errText(e, 'Failed to load example')
  }
}

async function runPrediction() {
  running.value = true
  runError.value = ''
  lastResult.value = null
  try {
    lastResult.value = await predict(data.image, [data.formData])
  } catch (e) {
    runError.value = errText(e, 'Prediction failed')
  } finally {
    running.value = false
  }
}

const predictionRows = computed(() => {
  const preds = lastResult.value?.predictions
  if (!Array.isArray(preds)) return []
  return preds.map((p) => (p && typeof p === 'object' && !Array.isArray(p) ? p : { value: p }))
})

const resultColumns = computed(() => {
  const fromSchema = outputSchema.value?.properties
    ? Object.keys(outputSchema.value.properties)
    : []
  if (fromSchema.length) return fromSchema
  const first = predictionRows.value[0]
  return first ? Object.keys(first) : []
})

function formatCell(v) {
  if (v === null || v === undefined) return ''
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function errText(e, fallback) {
  return e?.response?.data?.detail || e?.message || fallback
}

function onFinish() {
  goHome()
}
</script>

<style scoped>
.model-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 1rem;
  margin-top: 0.5rem;
}
.model-card {
  text-align: left;
  padding: 1rem 1.2rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  cursor: pointer;
  color: inherit;
  transition: border-color 0.15s ease;
}
.model-card:hover { border-color: #f97316; }
.model-card.selected { border-color: #f97316; background: var(--surface-alt); }
.model-card-title { font-weight: 600; margin-bottom: 0.3rem; }
.model-card-sub { font-size: 0.9rem; margin-top: 0.4rem; }
.form-actions { display: flex; gap: 0.5rem; margin: 0.5rem 0 1rem; }
.results-table { width: 100%; border-collapse: collapse; margin-top: 0.8rem; }
.results-table th, .results-table td {
  padding: 0.55rem 0.7rem; text-align: left;
  border-bottom: 1px solid var(--border); font-size: 0.9rem;
}
.results-table th {
  font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;
  color: var(--text-muted); font-weight: 500; border-bottom-width: 2px;
}
.schema {
  margin: 0.4rem 0 0; padding: 0.6rem 0.8rem;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius-sm); font-size: 0.8rem; overflow-x: auto;
}
.error { color: #dc2626; }
</style>
