<template>
  <Wizard
    title="Find digital twins"
    subtitle="Pick a subject, choose which of their attributes matter, then search for similar patients."
    accent="#db2777"
    :steps="steps"
    finishLabel="Done"
    @finish="goHome()"
  >
    <!-- ═══════════════ Step 0: Subject ═══════════════ -->
    <template #step-0>
      <h2>Who is the subject?</h2>
      <p class="muted">Enter a patient ID directly, or browse a cohort and pick one.</p>

      <div class="field" style="max-width: 420px">
        <label>Subject patient ID</label>
        <input type="text" v-model.trim="data.subjectId" placeholder="e.g. abc-001 or ext-007" />
      </div>

      <details class="browse-box" :open="!data.subjectId">
        <summary>🔍 Browse cohorts to pick a patient</summary>
        <div class="browse-body">
          <div class="field" style="max-width: 420px">
            <label>Cohort</label>
            <select v-model="data.browseCohort">
              <option value="">(choose a cohort)</option>
              <option v-for="c in cohorts" :key="c.cohort_id" :value="c.cohort_id">
                {{ c.cohort_id }} · {{ c.patient_count }} patients ({{ c.source }})
              </option>
            </select>
            <p v-if="cohortsError" class="err-text" style="margin-top:0.3rem; font-size:0.85rem">✗ {{ cohortsError }}</p>
          </div>

          <div v-if="data.browseCohort">
            <div v-if="patientsLoading" class="state-msg"><span class="spinner"></span> Loading patients…</div>
            <div v-else-if="patientsError" class="alert-err">✗ {{ patientsError }}</div>
            <template v-else>
              <input
                v-model="patientSearch"
                type="text"
                placeholder="Filter by ID…"
                style="max-width: 420px; margin-bottom: 0.5rem"
              />
              <div class="picker-list">
                <button
                  v-for="p in browsePatients"
                  :key="p.id"
                  class="picker-row"
                  :class="{ selected: data.subjectId === p.id }"
                  @click="pickSubject(p.id)"
                >
                  <span class="mono">{{ p.id }}</span>
                  <span class="muted">{{ p.gender }} · {{ ageLabel(p.birth_date) }}</span>
                </button>
                <div v-if="!browsePatients.length" class="state-msg muted">No patients match.</div>
              </div>
            </template>
          </div>
        </div>
      </details>
    </template>

    <!-- ═══════════════ Step 1: Attributes ═══════════════ -->
    <template #step-1>
      <h2>What should the twinning focus on?</h2>

      <div v-if="profileLoading" class="state-msg"><span class="spinner"></span> Loading subject profile…</div>
      <div v-else-if="profileError" class="alert-err">
        ✗ {{ profileError }}
        <button class="ghost small-btn" style="margin-left:0.8rem" @click="loadProfile(true)">Retry</button>
      </div>

      <template v-else-if="profile">
        <!-- Subject summary. The raw patient id stays out of the way here:
             it was picked on the previous step, and the card title still has it. -->
        <div class="subject-card" :title="`Subject record: ${profile.id}`">
          <span class="muted">{{ profile.gender ?? '?' }} · {{ profile.age != null ? profile.age + ' y' : 'age unknown' }}</span>
          <span v-if="profile.ethnicity" class="muted">· {{ profile.ethnicity }}</span>
          <span class="muted subject-counts">
            {{ profile.conditions.length }} conditions · {{ profile.medications.length }} medications ·
            {{ profile.procedures.length }} procedures
          </span>
        </div>
        <p class="muted" style="margin: 0.6rem 0 1rem">
          Check the attributes that matter. Only checked attributes contribute to the similarity score.
        </p>

        <!-- prevalence counts state -->
        <p v-if="attrCounts.status === 'building'" class="muted counts-note">
          <span class="spinner tiny"></span>
          Counting attribute prevalence across the store. Counts will appear here shortly…
        </p>
        <p v-else-if="attrCounts.status === 'ready'" class="muted counts-note">
          Each count shows how many of the <strong>{{ attrCounts.totalOthers }}</strong> other
          patients share that attribute. Low counts make strong twinning criteria.
        </p>

        <!-- Demographics group -->
        <div class="attr-group">
          <label class="attr-group-head">
            <input
              type="checkbox"
              :checked="demoAll"
              :indeterminate="demoPartial"
              @change="setDemoAll($event.target.checked)"
            />
            <span class="attr-group-title">Demographics</span>
          </label>
          <div class="attr-items">
            <label v-if="profile.gender" class="attr-item">
              <input type="checkbox" v-model="sel.gender" />
              gender: <strong>{{ profile.gender }}</strong>
              <span v-if="attrCounts.demo.gender != null" class="attr-count" :title="countTitle(attrCounts.demo.gender)">{{ attrCounts.demo.gender }}</span>
            </label>
            <label v-if="profile.age != null" class="attr-item">
              <input type="checkbox" v-model="sel.age" />
              age: <strong>{{ profile.age }} y</strong>
              <span v-if="sel.age" class="tolerance">
                ± <input type="number" min="1" max="100" v-model.number="sel.ageTolerance" class="tol-input" /> y
              </span>
              <span v-if="attrCounts.demo.age != null" class="attr-count" :title="countTitle(attrCounts.demo.age)">{{ attrCounts.demo.age }}</span>
            </label>
            <label v-if="profile.ethnicity" class="attr-item">
              <input type="checkbox" v-model="sel.ethnicity" />
              ethnicity: <strong>{{ profile.ethnicity }}</strong>
              <span v-if="attrCounts.demo.ethnicity != null" class="attr-count" :title="countTitle(attrCounts.demo.ethnicity)">{{ attrCounts.demo.ethnicity }}</span>
            </label>
          </div>
        </div>

        <!-- Clinical groups -->
        <div v-for="group in clinicalGroups" :key="group.key" class="attr-group">
          <label class="attr-group-head">
            <input
              type="checkbox"
              :checked="groupAll(group.key)"
              :indeterminate="groupPartial(group.key)"
              @change="setGroupAll(group.key, $event.target.checked)"
            />
            <span class="attr-group-title">{{ group.title }}</span>
            <span class="muted">({{ groupCheckedCount(group.key) }}/{{ profile[group.key].length }} selected)</span>
          </label>
          <div v-if="profile[group.key].length" class="attr-items">
            <label v-for="item in profile[group.key]" :key="item.label" class="attr-item">
              <input type="checkbox" v-model="sel[group.key][item.label]" />
              {{ item.label }}
              <span
                v-if="countFor(group.key, item.label) != null"
                class="attr-count"
                :title="countTitle(countFor(group.key, item.label))"
              >{{ countFor(group.key, item.label) }}</span>
            </label>
          </div>
          <div v-else class="attr-items muted" style="padding: 0.2rem 0 0.4rem">(none recorded)</div>
        </div>

        <p v-if="!anySelected" class="err-text" style="font-size: 0.88rem">
          Select at least one attribute to continue.
        </p>
      </template>
    </template>

    <!-- ═══════════════ Step 2: Mode & scope ═══════════════ -->
    <template #step-2>
      <h2>Where should we look?</h2>

      <div class="mode-grid">
        <button
          class="mode-card"
          :class="{ selected: data.mode === 'existing' }"
          @click="data.mode = 'existing'"
        >
          <div class="mode-icon">🔎</div>
          <div>
            <div class="mode-title">Search existing data</div>
            <div class="mode-sub">Score patients across all cohorts in the FHIR store</div>
          </div>
        </button>
        <button
          class="mode-card"
          :class="{ selected: data.mode === 'generate' }"
          @click="data.mode = 'generate'"
        >
          <div class="mode-icon">⚗️</div>
          <div>
            <div class="mode-title">Generate new candidates</div>
            <div class="mode-sub">Create a fresh synthetic cohort shaped like the subject, then search it</div>
          </div>
        </button>
      </div>

      <!-- existing-data options: all cohorts by default; restriction is tucked away -->
      <details v-if="data.mode === 'existing'" class="adv-scope" :open="!!data.scopeCohort">
        <summary>Advanced: restrict the search to a single cohort</summary>
        <div class="field" style="max-width: 480px; margin-top: 0.6rem">
          <select v-model="data.scopeCohort">
            <option value="">All cohorts (default)</option>
            <option v-for="c in cohorts" :key="c.cohort_id" :value="c.cohort_id">
              {{ c.cohort_id }} · {{ c.patient_count }} patients ({{ c.source }})
            </option>
          </select>
        </div>
      </details>

      <p v-if="data.mode === 'existing' && searchScope" class="muted scope-note">
        🔎 Will search <strong>{{ searchScope.patients ?? '…' }}</strong> patient records
        <template v-if="data.scopeCohort"> in cohort <code>{{ data.scopeCohort }}</code>.</template>
        <template v-else> across all <strong>{{ searchScope.cohorts }}</strong> cohorts.</template>
        The subject itself is excluded from matches.
      </p>

      <!-- generation options -->
      <template v-if="data.mode === 'generate'">
        <div class="field-row" style="margin-top: 1.2rem">
          <div class="field">
            <label>Candidates to generate</label>
            <input type="number" min="1" max="1000" v-model.number="data.genCount" />
          </div>
          <div class="field" v-if="sel.age && profile?.age != null">
            <label>Age band around subject (± years)</label>
            <input type="number" min="0" max="50" v-model.number="data.genAgeBand" />
          </div>
        </div>
        <p class="muted" style="font-size: 0.88rem">
          Generation is constrained by the demographics you selected
          <template v-if="genConstraintSummary">({{ genConstraintSummary }})</template>
          and runs as a background Synthea job. The generated cohort stays in the store,
          so you can browse it later.
        </p>
      </template>

      <div class="topk-line" style="margin-top: 0.8rem">
        Showing the top <strong>{{ data.topK }}</strong> matches
        <template v-if="!editTopK">
          <button class="link-btn" @click="editTopK = true">change</button>
        </template>
        <template v-else>
          <input
            type="number" min="1" max="500" class="topk-input"
            v-model.number="data.topK"
            @keyup.enter="editTopK = false"
          />
          <button class="link-btn" @click="editTopK = false">done</button>
        </template>
      </div>

      <div class="field">
        <label>Scoring emphasis</label>
        <div class="emph-row" role="radiogroup" aria-label="Scoring emphasis">
          <label class="emph-option" :class="{ active: data.weighting === 'equal' }">
            <input type="radio" value="equal" v-model="data.weighting" />
            Equal weight
          </label>
          <label
            v-for="c in weightableCategories"
            :key="c.key"
            class="emph-option"
            :class="{ active: data.weighting === c.key }"
          >
            <input type="radio" :value="c.key" v-model="data.weighting" />
            {{ c.title }}
          </label>
        </div>
        <p class="muted" style="margin-top: 0.4rem; font-size: 0.85rem">
          The emphasized category counts double when the per-category subscores are combined.
          Only categories with selected attributes are offered.
        </p>
      </div>
    </template>

    <!-- ═══════════════ Step 3: Results ═══════════════ -->
    <template #step-3>
      <h2>Matches</h2>

      <!-- generating -->
      <div v-if="run.state === 'generating'" class="submit-state loading">
        <div style="width:100%">
          <div style="display:flex; align-items:center; gap:0.7rem; margin-bottom:0.8rem">
            <span class="spinner"></span>
            <span>Generating {{ data.genCount }} candidates ({{ run.phase || 'queued' }})</span>
          </div>
          <div class="progress-wrap">
            <div class="progress-bar" :style="{ width: (run.progress * 100).toFixed(0) + '%' }"></div>
          </div>
          <div class="progress-row">
            <span class="muted">{{ Math.round(run.progress * 100) }}%</span>
            <span v-if="run.eta" class="muted">~{{ run.eta }}s remaining</span>
            <span class="muted" style="margin-left:auto">cohort <code>{{ run.genCohortId }}</code></span>
          </div>
        </div>
      </div>

      <!-- searching -->
      <div v-if="run.state === 'searching'" class="submit-state loading">
        <span class="spinner"></span>
        <div>
          <div>
            Scoring
            <strong v-if="searchScope?.patients != null">{{ searchScope.patients }}</strong>
            patient records{{ searchScopeLabel ? ` in ${searchScopeLabel}` : searchScope ? ` across ${searchScope.cohorts} cohorts` : ' across the store' }}…
          </div>
          <div class="muted search-progress">
            {{ searchElapsed }}s elapsed. Fetching each candidate's
            {{ selectedCategoriesLabel }}, then ranking by similarity
          </div>
        </div>
      </div>

      <!-- error -->
      <div v-if="run.state === 'error'" class="submit-state err">
        <h3>✗ Search failed</h3>
        <pre>{{ run.error }}</pre>
        <button class="ghost" style="margin-top:0.6rem" @click="restart">↻ Try again</button>
      </div>

      <!-- results -->
      <template v-if="run.state === 'done' && run.results">
        <div class="result-meta">
          <span class="muted">
            {{ run.results.matches.length }} of {{ run.results.total_candidates }} candidates shown
            <template v-if="run.genCohortId"> · generated cohort <code>{{ run.genCohortId }}</code></template>
            <template v-else-if="run.results.coverage?.cohort_id"> · cohort <code>{{ run.results.coverage.cohort_id }}</code></template>
            <template v-if="run.results.coverage?.patients_truncated"> · ⚠ candidate scan truncated at cap</template>
            <template v-if="run.results.coverage?.incomplete_feature_categories?.length">
              · ⚠ partially scanned: {{ run.results.coverage.incomplete_feature_categories.join(', ') }}
            </template>
          </span>
          <button class="ghost small-btn" @click="restart">↻ Search again</button>
        </div>

        <div v-if="!run.results.matches.length" class="state-msg muted">
          No candidates found in this scope. Try widening the scope or generating candidates.
        </div>

        <template v-else>
          <!-- ── two-sided comparison: subject vs rotating match ── -->
          <div class="comparator">
            <!-- subject column (strip stays empty, aligns with the rotator) -->
            <div class="cmp-col">
            <div class="cmp-strip"></div>
            <div class="cmp-side">
              <div class="cmp-head">
                <span class="mono cmp-id">{{ profile.id }}</span>
                <span class="cmp-tag">subject</span>
              </div>
              <div class="cmp-demo">
                <div v-for="f in demoFields" :key="f.key" class="cmp-demo-row" :class="{ crit: f.crit }">
                  <span class="cmp-demo-label">{{ f.key }}</span>
                  <span class="cmp-demo-value">{{ f.sv ?? 'not recorded' }}</span>
                </div>
              </div>
              <div v-for="cat in comparisonCats" :key="cat.key" class="cmp-cat">
                <div class="cmp-cat-title">{{ cat.title }}</div>
                <div class="cmp-items">
                  <span
                    v-for="it in cat.subject"
                    :key="it.label"
                    class="cmp-item"
                    :class="{ crit: it.crit, shared: it.shared }"
                  ><span v-if="it.shared" class="cmp-mark">✓</span>{{ it.label }}</span>
                  <span v-if="!cat.subject.length" class="muted cmp-none">(none)</span>
                </div>
              </div>
            </div>

            </div>

            <!-- match column: the rotator sits in the strip above the card -->
            <div class="cmp-col">
            <div class="cmp-strip rot-strip">
              <button class="ghost rot-btn" :disabled="run.results.matches.length < 2" @click="rotate(-1)">◀</button>
              <div class="rot-pos">
                <span class="rot-count">#{{ compareIdx + 1 }}</span>
                <span class="muted rot-total">of {{ run.results.matches.length }}</span>
              </div>
              <button class="ghost rot-btn" :disabled="run.results.matches.length < 2" @click="rotate(1)">▶</button>
            </div>

            <div class="cmp-side" v-if="currentMatch">
              <div class="cmp-head">
                <span class="mono cmp-id">{{ currentMatch.patient_id }}</span>
                <span class="cmp-score">score {{ currentMatch.score.toFixed(2) }}</span>
              </div>
              <div class="cmp-sub muted">
                <span v-for="(v, k) in currentMatch.subscores" :key="k">{{ k }} {{ v.toFixed(2) }}</span>
              </div>

              <div v-if="currentMatchProfile?.status === 'loading'" class="state-msg">
                <span class="spinner"></span> Loading record…
              </div>
              <div v-else-if="currentMatchProfile?.status === 'error'" class="alert-err">
                ✗ {{ currentMatchProfile.error }}
                <button class="ghost small-btn" @click="fetchMatchProfile(currentMatch.patient_id, true)">Retry</button>
              </div>
              <template v-else-if="currentMatchProfile?.profile">
                <div class="cmp-demo">
                  <div v-for="f in demoFields" :key="f.key" class="cmp-demo-row" :class="{ crit: f.crit }">
                    <span class="cmp-demo-label">{{ f.key }}</span>
                    <span class="cmp-demo-value">
                      {{ f.mv ?? 'not recorded' }}
                      <span v-if="f.mark" :class="f.ok ? 'ok-mark' : 'miss-mark'">{{ f.mark }}</span>
                    </span>
                  </div>
                </div>
                <div v-for="cat in comparisonCats" :key="cat.key" class="cmp-cat">
                  <div class="cmp-cat-title">{{ cat.title }}</div>
                  <div class="cmp-items">
                    <span
                      v-for="it in cat.match"
                      :key="it.label"
                      class="cmp-item"
                      :class="{ crit: it.crit, shared: it.shared, absent: it.absent }"
                    ><span v-if="it.shared || it.absent" class="cmp-mark">{{ it.absent ? '✗' : '✓' }}</span>{{ it.label }}</span>
                    <span v-if="!cat.match.length" class="muted cmp-none">(none)</span>
                  </div>
                </div>
                <div class="match-actions">
                  <button class="ghost small-btn" @click="openPdf(currentMatch.patient_id)">📄 PDF report</button>
                  <button
                    class="ghost small-btn"
                    :disabled="exporting[currentMatch.patient_id]"
                    @click="exportFhir(currentMatch.patient_id)"
                  >
                    {{ exporting[currentMatch.patient_id] ? '…' : '⤓ FHIR bundle' }}
                  </button>
                </div>
              </template>
            </div>
            </div>
          </div>

          <label class="cmp-toggle">
            <input type="checkbox" v-model="showAllAttrs" />
            Also compare attributes that were <em>not</em> selected for twinning
          </label>

          <!-- ── attribute prevalence: candidates sharing each subject attribute ── -->
          <details v-if="run.results.prevalence" class="prev-panel" open>
            <summary>
              Attribute prevalence. How many of the {{ run.results.prevalence.of }} candidates
              share each subject attribute
            </summary>
            <div class="prev-grid">
              <div v-for="grp in prevalenceGroups" :key="grp.key" class="prev-cat">
                <div class="cmp-cat-title">{{ grp.title }}</div>
                <div v-if="!grp.rows.length" class="muted cmp-none">(none)</div>
                <div v-for="row in grp.rows" :key="row.label" class="prev-row" :class="{ crit: row.crit }">
                  <span class="prev-label">{{ row.label }}</span>
                  <div class="prev-bar"><div class="prev-fill" :style="{ width: prevPct(row.count) }"></div></div>
                  <span class="prev-count">{{ row.count }}</span>
                </div>
              </div>
            </div>
          </details>

          <!-- ── compact ranked list (click a row to load it on the right side) ── -->
          <div
            v-for="(m, idx) in run.results.matches"
            :key="m.patient_id"
            class="match-row"
            :class="{ active: idx === compareIdx }"
            @click="compareIdx = idx"
          >
            <span class="match-rank">#{{ idx + 1 }}</span>
            <div class="match-main">
              <div class="mono match-id">{{ m.patient_id }}</div>
              <div class="match-meta muted">
                {{ m.gender ?? '?' }} · {{ m.age != null ? m.age + ' y' : 'age ?' }}
                <span v-for="c in m.cohort_ids" :key="c" class="pill">{{ c }}</span>
                <span v-if="m.datatype" class="pill" :class="m.datatype">{{ m.datatype }}</span>
              </div>
            </div>
            <div class="match-score" :style="{ '--sc': m.score }">
              <div class="score-bar"><div class="score-fill"></div></div>
              <span class="score-value">{{ m.score.toFixed(2) }}</span>
            </div>
          </div>
        </template>
      </template>
    </template>
  </Wizard>
</template>

<script setup>
import { reactive, ref, computed, watch, onMounted, onUnmounted } from 'vue'
import axios from 'axios'
import Wizard from '../components/Wizard.vue'
import { store, goHome } from '../state.js'

// ─── persisted inputs (survive leaving/re-entering the flow) ─────────────────
const data = store.flowData.twins ?? reactive({
  subjectId: '',
  browseCohort: '',
  mode: 'existing',      // 'existing' | 'generate'
  scopeCohort: '',
  genCount: 100,
  genAgeBand: 5,
  topK: 10,
  weighting: 'equal',
})
store.flowData.twins = data

// Top-K is a fixed default (10) shown as text; the input only appears on demand.
const editTopK = ref(false)

// ─── steps ───────────────────────────────────────────────────────────────────
const steps = reactive([
  { label: 'Subject', canAdvance: false },
  { label: 'Attributes', canAdvance: false },
  { label: 'Mode', canAdvance: true },
  { label: 'Results' },
])

// ─── cohorts (browse + scope) ────────────────────────────────────────────────
const cohorts = ref([])
const cohortsError = ref('')

async function loadCohorts() {
  cohortsError.value = ''
  try {
    const { data: resp } = await axios.get(`${store.apiBase}/synthetic/synthea/list-all-cohorts`)
    cohorts.value = resp.cohorts ?? []
  } catch (e) {
    cohortsError.value = e?.response?.data?.detail ?? e.message ?? 'Failed to load cohorts'
  }
}
onMounted(loadCohorts)

// ─── patient picker ──────────────────────────────────────────────────────────
const allPatients = ref([])
const patientsLoading = ref(false)
const patientsError = ref('')
const patientSearch = ref('')
let patientsFetched = false

const browsePatients = computed(() => {
  const q = patientSearch.value.toLowerCase().trim()
  return allPatients.value
    .filter((p) => p.cohort_ids?.includes(data.browseCohort))
    .filter((p) => !q || p.id.toLowerCase().includes(q))
})

watch(() => data.browseCohort, async (id) => {
  if (!id || patientsFetched) return
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
}, { immediate: true })

function pickSubject(id) {
  data.subjectId = id
}

function ageLabel(bd) {
  const a = ageFromBirthDate(bd)
  return a != null ? `${a} y` : 'age ?'
}

// ─── subject profile ─────────────────────────────────────────────────────────
const profile = ref(null)
const profileLoading = ref(false)
const profileError = ref('')

// Selection state: which attributes participate in the matching.
const sel = reactive({
  gender: true,
  age: true,
  ethnicity: false,
  ageTolerance: 10,
  conditions: {},
  medications: {},
  procedures: {},
})

function ageFromBirthDate(bd) {
  if (!bd || bd === 'unknown') return null
  const born = new Date(bd)
  if (isNaN(born)) return null
  const now = new Date()
  let age = now.getFullYear() - born.getFullYear()
  if (now.getMonth() < born.getMonth() || (now.getMonth() === born.getMonth() && now.getDate() < born.getDate())) age -= 1
  return age
}

// Profiles come from the twins backend, which pages HAPI exhaustively.
// $everything is paginated and silently truncates long-history patients.
async function fetchProfileFor(pid) {
  const { data: p } = await axios.get(
    `${store.apiBase}/twins/profile/${encodeURIComponent(pid)}`,
    { timeout: 120_000 },
  )
  return p
}

async function loadProfile(force = false) {
  if (!data.subjectId) return
  if (!force && profile.value?.id === data.subjectId) return
  profileLoading.value = true
  profileError.value = ''
  profile.value = null
  try {
    const p = await fetchProfileFor(data.subjectId)
    profile.value = p

    // Defaults: demographics + all conditions and medications on; procedures off.
    sel.gender = !!p.gender
    sel.age = p.age != null
    sel.ethnicity = false
    sel.conditions = Object.fromEntries(p.conditions.map((i) => [i.label, true]))
    sel.medications = Object.fromEntries(p.medications.map((i) => [i.label, true]))
    sel.procedures = Object.fromEntries(p.procedures.map((i) => [i.label, false]))
  } catch (e) {
    profileError.value = e?.response?.data?.detail ?? e.message ?? 'Failed to load subject record'
  } finally {
    profileLoading.value = false
  }
}

// Fetch the profile when the user reaches the Attributes step.
watch(() => store.currentStep, (s) => {
  if (s === 1) loadProfile()
  if (s === 3) startRun()
})

// ─── attribute prevalence counts (attributes step) ───────────────────────────
// Served by the store-wide count cache in stat_server_py: the first request
// after a store change triggers a background rebuild ("building"), everything
// after that is answered instantly.
const attrCounts = reactive({
  status: 'idle', // idle | building | ready | error
  totalOthers: 0,
  stale: false,
  demo: {},    // gender/age/ethnicity -> count
  byLabel: {}, // category -> { label -> count }
})
let countsTimer = null

function stopCountsPolling() {
  if (countsTimer) { clearTimeout(countsTimer); countsTimer = null }
}

async function fetchAttrCounts() {
  stopCountsPolling()
  const p = profile.value
  if (!p) return
  try {
    const body = {
      subject_id: p.id,
      demographics: {
        gender: p.gender ?? null,
        age: p.age ?? null,
        age_tolerance: sel.ageTolerance || 10,
        ethnicity: p.ethnicity ?? null,
      },
      conditions: p.conditions.map(({ label, codes }) => ({ label, codes })),
      medications: p.medications.map(({ label, codes }) => ({ label, codes })),
      procedures: p.procedures.map(({ label, codes }) => ({ label, codes })),
    }
    const { data: resp } = await axios.post(
      `${store.apiBase}/twins/attribute-counts`, body, { timeout: 60_000 },
    )
    if (profile.value?.id !== p.id) return // subject changed mid-flight
    if (resp.status === 'building') {
      attrCounts.status = 'building'
      countsTimer = setTimeout(fetchAttrCounts, 3000)
      return
    }
    attrCounts.totalOthers = resp.total_others ?? 0
    attrCounts.stale = !!resp.stale
    const demo = {}
    for (const r of resp.demographics ?? []) demo[r.key] = r.count
    attrCounts.demo = demo
    const byLabel = {}
    for (const cat of ['conditions', 'medications', 'procedures']) {
      byLabel[cat] = {}
      for (const r of resp[cat] ?? []) byLabel[cat][r.label] = r.count
    }
    attrCounts.byLabel = byLabel
    attrCounts.status = 'ready'
  } catch {
    attrCounts.status = 'error' // counts are decoration, so fail quietly
  }
}

function countFor(cat, label) {
  return attrCounts.byLabel[cat]?.[label] ?? null
}
function countTitle(n) {
  return `${n} of ${attrCounts.totalOthers} other patients share this attribute`
}

// (Re)fetch counts whenever a profile arrives; age-tolerance changes shift the
// age-band count, so refresh on those too (cheap, served from the cache).
watch(profile, (p) => {
  stopCountsPolling()
  attrCounts.status = 'idle'
  attrCounts.demo = {}
  attrCounts.byLabel = {}
  if (p) fetchAttrCounts()
})
watch(() => sel.ageTolerance, () => {
  if (profile.value && (attrCounts.status === 'ready' || attrCounts.status === 'error')) {
    fetchAttrCounts()
  }
})
onUnmounted(stopCountsPolling)

// Changing the subject invalidates profile, cached match records, and results.
watch(() => data.subjectId, () => {
  profile.value = null
  profileError.value = ''
  for (const k of Object.keys(matchProfiles)) delete matchProfiles[k]
  resetRun()
})

// Changing anything that feeds the search invalidates completed results, so
// re-entering the Results step runs a fresh search instead of showing stale
// matches. In-flight runs are left alone (the user is on an earlier step).
watch(
  [() => data.mode, () => data.scopeCohort, () => data.topK, () => data.weighting,
   () => data.genCount, () => data.genAgeBand, sel],
  () => {
    if (run.state === 'done' || run.state === 'error') resetRun()
  },
  { deep: true },
)

// ─── attribute group helpers ─────────────────────────────────────────────────
const clinicalGroups = [
  { key: 'conditions', title: 'Conditions' },
  { key: 'medications', title: 'Medications' },
  { key: 'procedures', title: 'Procedures' },
]

const demoFlags = computed(() => {
  const flags = []
  if (profile.value?.gender) flags.push(sel.gender)
  if (profile.value?.age != null) flags.push(sel.age)
  if (profile.value?.ethnicity) flags.push(sel.ethnicity)
  return flags
})
const demoAll = computed(() => demoFlags.value.length > 0 && demoFlags.value.every(Boolean))
const demoPartial = computed(() => demoFlags.value.some(Boolean) && !demoAll.value)
function setDemoAll(v) {
  if (profile.value?.gender) sel.gender = v
  if (profile.value?.age != null) sel.age = v
  if (profile.value?.ethnicity) sel.ethnicity = v
}

function groupCheckedCount(key) {
  return Object.values(sel[key]).filter(Boolean).length
}
function groupAll(key) {
  const n = profile.value?.[key].length ?? 0
  return n > 0 && groupCheckedCount(key) === n
}
function groupPartial(key) {
  const c = groupCheckedCount(key)
  return c > 0 && !groupAll(key)
}
function setGroupAll(key, v) {
  for (const k of Object.keys(sel[key])) sel[key][k] = v
}

const anySelected = computed(() => {
  if (!profile.value) return false
  return (
    demoFlags.value.some(Boolean) ||
    clinicalGroups.some((g) => groupCheckedCount(g.key) > 0)
  )
})

// ─── step gating ─────────────────────────────────────────────────────────────
watch(() => data.subjectId, (id) => { steps[0].canAdvance = !!id }, { immediate: true })
watch([anySelected, profileLoading], () => {
  steps[1].canAdvance = anySelected.value && !profileLoading.value
}, { immediate: true })
watch(() => [data.mode, data.genCount], () => {
  steps[2].canAdvance = data.mode === 'existing' || (data.genCount >= 1 && data.genCount <= 1000)
}, { immediate: true })

// ─── run: generate (optional) + search ───────────────────────────────────────
const run = reactive({
  state: 'idle', // idle | generating | searching | done | error
  error: '',
  jobId: null,
  progress: 0,
  phase: '',
  eta: null,
  genCohortId: '',
  results: null,
})
let pollInterval = null

function resetRun() {
  if (pollInterval) { clearInterval(pollInterval); pollInterval = null }
  run.state = 'idle'
  run.error = ''
  run.jobId = null
  run.progress = 0
  run.phase = ''
  run.eta = null
  run.genCohortId = ''
  run.results = null
  compareIdx.value = 0
}
function restart() {
  resetRun()
  startRun()
}
onUnmounted(() => { if (pollInterval) clearInterval(pollInterval) })

// ─── comparison view: subject vs rotating match ──────────────────────────────
const compareIdx = ref(0)
const showAllAttrs = ref(false)
// patient_id -> { status: 'loading'|'done'|'error', profile?, error? }
const matchProfiles = reactive({})

const currentMatch = computed(() => run.results?.matches?.[compareIdx.value] ?? null)
const currentMatchProfile = computed(() =>
  currentMatch.value ? matchProfiles[currentMatch.value.patient_id] : null,
)

function rotate(dir) {
  const n = run.results?.matches?.length ?? 0
  if (!n) return
  compareIdx.value = (compareIdx.value + dir + n) % n
}

// ─── attribute prevalence (computed server-side over ALL candidates) ─────────
const prevalenceGroups = computed(() => {
  const pv = run.results?.prevalence
  if (!pv) return []
  const demoCrit = { gender: sel.gender, age: sel.age, ethnicity: sel.ethnicity }
  return [
    {
      key: 'demographics',
      title: 'Demographics',
      rows: (pv.demographics ?? []).map((r) => ({ ...r, crit: !!demoCrit[r.key] })),
    },
    ...clinicalGroups.map((g) => ({
      key: g.key,
      title: g.title,
      rows: (pv[g.key] ?? []).map((r) => ({ ...r, crit: !!sel[g.key][r.label] })),
    })),
  ]
})

function prevPct(n) {
  const of = run.results?.prevalence?.of || 1
  return Math.round((100 * n) / of) + '%'
}

async function fetchMatchProfile(pid, force = false) {
  if (!force && matchProfiles[pid]) return
  matchProfiles[pid] = { status: 'loading' }
  try {
    matchProfiles[pid] = { status: 'done', profile: await fetchProfileFor(pid) }
  } catch (e) {
    matchProfiles[pid] = {
      status: 'error',
      error: e?.response?.data?.detail ?? e.message ?? 'Failed to load record',
    }
  }
}

// Load the shown match's record whenever the comparison target changes.
watch([currentMatch, () => run.state], () => {
  if (run.state === 'done' && currentMatch.value) fetchMatchProfile(currentMatch.value.patient_id)
}, { immediate: true })

// Key helpers mirroring the backend's matching (codes ∪ normalized label).
function normKey(s) {
  return s.toLowerCase().trim().replace(/\s+/g, ' ')
    .replace(/\s*\((disorder|finding|procedure|situation|product)\)$/, '')
}
function itemKeySet(it) {
  return new Set([...(it.codes ?? []).map(String), normKey(it.label)])
}
function intersects(a, b) {
  for (const x of a) if (b.has(x)) return true
  return false
}

// Demographic rows, aligned between the two sides. A row is shown when it was
// selected for twinning, or when "show all" is on.
const demoFields = computed(() => {
  const p = profile.value
  if (!p) return []
  const mp = currentMatchProfile.value?.profile ?? null
  const fields = []
  if (p.gender != null) {
    fields.push({
      key: 'gender', crit: sel.gender, sv: p.gender, mv: mp?.gender ?? null,
      mark: mp ? (mp.gender === p.gender ? '=' : '≠') : '',
      ok: mp?.gender === p.gender,
    })
  }
  if (p.age != null) {
    const diff = mp?.age != null ? Math.abs(mp.age - p.age) : null
    fields.push({
      key: 'age', crit: sel.age, sv: `${p.age} y`, mv: mp?.age != null ? `${mp.age} y` : null,
      mark: diff != null ? `Δ${diff}` : '',
      ok: diff != null && diff <= (sel.ageTolerance || 10),
    })
  }
  if (p.ethnicity != null || mp?.ethnicity != null) {
    const same = !!mp?.ethnicity && !!p.ethnicity && mp.ethnicity.toLowerCase() === p.ethnicity.toLowerCase()
    fields.push({
      key: 'ethnicity', crit: sel.ethnicity, sv: p.ethnicity ?? null, mv: mp?.ethnicity ?? null,
      mark: mp ? (same ? '=' : '≠') : '',
      ok: same,
    })
  }
  return fields.filter((f) => f.crit || showAllAttrs.value)
})

// Clinical categories for both sides.
// Toggle OFF: rows are the selected criteria. Left shows them, right shows ✓/✗.
// Toggle ON: both sides show their full attribute lists; shared items get ✓,
// criteria stay outlined, and criteria the match lacks appear as ✗ ghosts.
const comparisonCats = computed(() => {
  const p = profile.value
  if (!p) return []
  const mp = currentMatchProfile.value?.profile ?? null
  return clinicalGroups.map((g) => {
    const subjItems = p[g.key].map((it) => ({ ...it, crit: !!sel[g.key][it.label] }))
    const matchItems = (mp?.[g.key] ?? []).map((it) => ({ ...it }))
    for (const si of subjItems) {
      si.shared = matchItems.some((mi) => intersects(itemKeySet(si), itemKeySet(mi)))
    }
    for (const mi of matchItems) {
      mi.shared = subjItems.some((si) => intersects(itemKeySet(si), itemKeySet(mi)))
      mi.crit = subjItems.some((si) => si.crit && intersects(itemKeySet(si), itemKeySet(mi)))
    }
    const missingCrit = subjItems
      .filter((si) => si.crit && !si.shared)
      .map((si) => ({ label: si.label, crit: true, absent: true }))

    let subject, match
    if (showAllAttrs.value) {
      subject = subjItems
      match = [...matchItems, ...(mp ? missingCrit : [])]
    } else {
      subject = subjItems.filter((si) => si.crit)
      match = mp
        ? [
            ...subjItems
              .filter((si) => si.crit && si.shared)
              .map((si) => ({ label: si.label, crit: true, shared: true })),
            ...missingCrit,
          ]
        : []
    }
    return { key: g.key, title: g.title, subject, match }
  })
})

const searchScopeLabel = computed(() =>
  run.genCohortId || data.scopeCohort || '',
)

const genConstraintSummary = computed(() => {
  if (!profile.value) return ''
  const parts = []
  if (sel.gender && profile.value.gender) parts.push(profile.value.gender)
  if (sel.age && profile.value.age != null) {
    parts.push(`ages ${Math.max(0, profile.value.age - data.genAgeBand)}–${Math.min(140, profile.value.age + data.genAgeBand)}`)
  }
  return parts.join(', ')
})

function buildCriteria(cohortId) {
  const pickChecked = (key) =>
    profile.value[key].filter((it) => sel[key][it.label]).map(({ label, codes }) => ({ label, codes }))
  const wantDemo = demoFlags.value.some(Boolean)
  return {
    subject_id: data.subjectId,
    demographics: wantDemo
      ? {
          gender: sel.gender ? profile.value.gender : null,
          age: sel.age ? profile.value.age : null,
          age_tolerance: sel.ageTolerance || 10,
          ethnicity: sel.ethnicity ? profile.value.ethnicity : null,
        }
      : null,
    conditions: pickChecked('conditions'),
    medications: pickChecked('medications'),
    procedures: pickChecked('procedures'),
    cohort_id: cohortId || null,
    exclude_subject: true,
    top_k: data.topK,
    weighting: data.weighting,
  }
}

async function startRun() {
  if (run.state !== 'idle' || !profile.value) return
  if (data.mode === 'generate') await startGeneration()
  else await runSearch(data.scopeCohort || null)
}

// Scope preview: how many records / cohorts the search will cover. Exact
// store total once the count cache has answered; cohort-sum fallback (which
// can double-count patients tagged into several cohorts).
const searchScope = computed(() => {
  if (data.mode !== 'existing') return null
  if (data.scopeCohort) {
    const c = cohorts.value.find((x) => x.cohort_id === data.scopeCohort)
    return { cohorts: 1, patients: c?.patient_count ?? null }
  }
  const exact = attrCounts.status === 'ready' ? attrCounts.totalOthers + 1 : null
  const sum = cohorts.value.reduce((a, c) => a + (c.patient_count || 0), 0)
  return { cohorts: cohorts.value.length, patients: exact ?? (sum || null) }
})

const selectedCategoriesLabel = computed(() => {
  const parts = []
  if (sel.gender || sel.age || sel.ethnicity) parts.push('demographics')
  for (const g of clinicalGroups) {
    if (Object.values(sel[g.key]).some(Boolean)) parts.push(g.key)
  }
  return parts.join(', ') || 'attributes'
})

// Categories offered for scoring emphasis: only those with selected criteria.
const weightableCategories = computed(() => {
  const cats = []
  if (sel.gender || sel.age || sel.ethnicity) cats.push({ key: 'demographics', title: 'Demographics' })
  for (const g of clinicalGroups) {
    if (Object.values(sel[g.key]).some(Boolean)) cats.push({ key: g.key, title: g.title })
  }
  return cats
})

// If the emphasized category loses all its selected attributes, or an old
// session restored a legacy preset value, fall back to equal weight.
watch(weightableCategories, (cats) => {
  if (data.weighting !== 'equal' && !cats.some((c) => c.key === data.weighting)) {
    data.weighting = 'equal'
  }
}, { immediate: true })

// Elapsed-seconds ticker shown while the search itself runs.
const searchElapsed = ref(0)
let elapsedTimer = null
watch(() => run.state, (s) => {
  if (s === 'searching') {
    searchElapsed.value = 0
    elapsedTimer = setInterval(() => { searchElapsed.value += 1 }, 1000)
  } else if (elapsedTimer) {
    clearInterval(elapsedTimer)
    elapsedTimer = null
  }
})
onUnmounted(() => { if (elapsedTimer) clearInterval(elapsedTimer) })

async function runSearch(cohortId) {
  run.state = 'searching'
  try {
    const { data: resp } = await axios.post(
      `${store.apiBase}/twins/find`,
      buildCriteria(cohortId),
      { timeout: 300_000 },
    )
    run.results = resp
    compareIdx.value = 0
    run.state = 'done'
  } catch (e) {
    const detail = e?.response?.data?.detail ?? e?.response?.data ?? e?.message ?? 'Unknown error'
    run.error = typeof detail === 'string' ? detail : JSON.stringify(detail, null, 2)
    run.state = 'error'
  }
}

function genCohortId() {
  const now = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  return `twin-cand-${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`
}

async function startGeneration() {
  run.state = 'generating'
  run.genCohortId = genCohortId()
  try {
    const age = profile.value.age
    const payload = {
      num_patients: data.genCount,
      num_years: 10,
      cohort_id: run.genCohortId,
      exporter: 'fhir',
      min_age: sel.age && age != null ? Math.max(0, age - data.genAgeBand) : 0,
      max_age: sel.age && age != null ? Math.min(140, age + data.genAgeBand) : 140,
      gender: sel.gender && ['male', 'female'].includes(profile.value.gender) ? profile.value.gender : 'both',
      state: null,
      city: null,
      use_population_sampling: true,
    }
    const { data: resp } = await axios.post(`${store.apiBase}/synthetic/synthea/synthetic-patients`, payload)
    run.jobId = resp.job_id
    pollInterval = setInterval(pollGeneration, 3000)
  } catch (e) {
    const detail = e?.response?.data?.detail ?? e?.message ?? 'Failed to submit generation job'
    run.error = typeof detail === 'string' ? detail : JSON.stringify(detail, null, 2)
    run.state = 'error'
  }
}

async function pollGeneration() {
  try {
    const { data: j } = await axios.get(
      `${store.apiBase}/synthetic/synthea/synthetic-patients/jobs/${run.jobId}`,
    )
    run.progress = j.progress ?? 0
    run.phase = j.current_phase ?? j.status
    run.eta = j.estimated_remaining_seconds ?? null
    if (j.status === 'completed') {
      clearInterval(pollInterval)
      pollInterval = null
      await runSearch(run.genCohortId)
      loadCohorts() // the new cohort is now browsable elsewhere
    } else if (j.status === 'failed' || j.status === 'cancelled') {
      clearInterval(pollInterval)
      pollInterval = null
      run.error = j.error ?? `Generation job ${j.status}.`
      run.state = 'error'
    }
  } catch (e) {
    console.warn('Poll error:', e)
  }
}

// ─── per-match actions ───────────────────────────────────────────────────────
const exporting = reactive({})

function openPdf(patientId) {
  window.open(`${store.apiBase}/synthetic/synthea/patient/${patientId}/pdf`, '_blank')
}

async function exportFhir(patientId) {
  exporting[patientId] = true
  try {
    const { data: bundle } = await axios.get(`${store.apiBase}/stats/patients/${patientId}/$everything?_count=1000`)
    const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `patient-${patientId}-everything.json`
    a.click()
    URL.revokeObjectURL(url)
  } catch (e) {
    alert(`FHIR export failed: ${e?.response?.data?.detail ?? e.message}`)
  } finally {
    exporting[patientId] = false
  }
}
</script>

<style scoped>
/* ─── shared bits ─── */
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.9rem;
}
.err-text { color: #b91c1c; }
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
.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  flex-shrink: 0;
  border: 2px solid var(--border);
  border-top-color: #db2777;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ─── step 0: browse box ─── */
.browse-box {
  margin-top: 0.6rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.6rem 1rem;
}
.browse-box summary {
  cursor: pointer;
  font-size: 0.9rem;
  color: var(--text-muted);
}
.browse-body { padding-top: 0.8rem; }
.picker-list {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
  max-height: 320px;
  overflow-y: auto;
}
.picker-row {
  display: flex;
  justify-content: space-between;
  gap: 0.8rem;
  padding: 0.5rem 0.8rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  cursor: pointer;
  text-align: left;
  color: inherit;
  font-size: 0.88rem;
}
.picker-row:hover { border-color: #db2777; }
.picker-row.selected { border-color: #db2777; background: #fdf2f8; }

/* ─── step 1: subject + attribute tree ─── */
.subject-card {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 0.6rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.7rem 1rem;
}
.subject-counts { margin-left: auto; font-size: 0.83rem; }

.attr-group {
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  margin-bottom: 0.8rem;
  background: var(--surface);
}
.attr-group-head {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.6rem 0.9rem;
  cursor: pointer;
  margin: 0;
  border-bottom: 1px solid var(--border);
  background: var(--surface-alt);
  border-radius: var(--radius-sm) var(--radius-sm) 0 0;
  font-size: 0.9rem;
}
.attr-group-title {
  font-weight: 600;
  text-transform: uppercase;
  font-size: 0.78rem;
  letter-spacing: 0.05em;
}
.attr-items {
  display: flex;
  flex-wrap: wrap;
  gap: 0.2rem 1.4rem;
  padding: 0.6rem 0.9rem 0.7rem;
}
.attr-item {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 0.88rem;
  color: var(--text);
  margin: 0;
  min-width: 240px;
  cursor: pointer;
}
.tolerance { display: inline-flex; align-items: center; gap: 0.25rem; color: var(--text-muted); }
.tol-input { width: 58px; padding: 0.1rem 0.3rem; font-size: 0.85rem; }

/* ─── attribute prevalence counts ─── */
.counts-note {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  font-size: 0.85rem;
  margin: -0.2rem 0 0.9rem;
}
.spinner.tiny { width: 12px; height: 12px; border-width: 2px; }
.scope-note { margin-top: 0.9rem; font-size: 0.9rem; }

/* ─── top-K line ─── */
.topk-line {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  font-size: 0.92rem;
  color: var(--text);
  flex-wrap: wrap;
}
.link-btn {
  background: none;
  border: none;
  padding: 0;
  color: #db2777;
  font-size: 0.85rem;
  cursor: pointer;
  text-decoration: underline;
}
.link-btn:hover { color: #be185d; }
.topk-input { width: 74px; padding: 0.15rem 0.4rem; font-size: 0.88rem; }

/* ─── scoring emphasis radio pills ─── */
.emph-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
  padding-top: 0.2rem;
}
.emph-option {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  padding: 0.4rem 0.9rem;
  margin: 0;
  border: 1px solid var(--border);
  border-radius: 999px;
  background: var(--surface);
  font-size: 0.88rem;
  cursor: pointer;
  transition: all 0.12s ease;
  color: var(--text);
}
.emph-option:hover { border-color: #db2777; }
.emph-option.active {
  border-color: #db2777;
  background: #fdf2f8;
  color: #be185d;
  font-weight: 600;
}
.emph-option input { accent-color: #db2777; margin: 0; }
.search-progress { font-size: 0.82rem; margin-top: 0.25rem; }
.attr-count {
  margin-left: auto;
  font-size: 0.72rem;
  font-weight: 700;
  color: var(--text-muted);
  background: var(--surface-alt);
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 0.05rem 0.5rem;
  flex-shrink: 0;
}

/* ─── step 2: mode cards ─── */
.mode-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
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
.mode-card:hover { border-color: #db2777; }
.mode-card.selected { border-color: #db2777; background: #fdf2f8; }
.mode-icon { font-size: 1.7rem; }
.mode-title { font-weight: 600; margin-bottom: 0.2rem; }
.mode-sub { font-size: 0.85rem; color: var(--text-muted); }

/* ─── step 3: progress + results ─── */
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
.submit-state.err { background: #fef2f2; border-color: #fecaca; }
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
  background: #db2777;
  border-radius: 3px;
  transition: width 0.4s ease;
  min-width: 2%;
}
.progress-row { display: flex; gap: 1rem; font-size: 0.82rem; flex-wrap: wrap; }

.result-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.8rem;
  flex-wrap: wrap;
  margin-bottom: 0.7rem;
  font-size: 0.86rem;
}

/* ─── advanced scope disclosure ─── */
.adv-scope {
  margin-top: 1.2rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.55rem 0.9rem;
  max-width: 560px;
}
.adv-scope summary {
  cursor: pointer;
  font-size: 0.88rem;
  color: var(--text-muted);
}

/* ─── comparator ─── */
.comparator {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.8rem;
  align-items: start;
  margin-bottom: 0.7rem;
}
.cmp-col {
  display: flex;
  flex-direction: column;
  gap: 0.45rem;
  min-width: 0;
}
.cmp-strip {
  min-height: 38px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.7rem;
}
.cmp-side {
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  background: var(--surface);
  padding: 0.8rem 1rem;
  display: flex;
  flex-direction: column;
  gap: 0.7rem;
  min-width: 0;
}
.cmp-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.6rem;
  flex-wrap: wrap;
}
.cmp-id { font-weight: 700; word-break: break-all; }
.cmp-tag {
  font-size: 0.7rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  background: #fdf2f8;
  border: 1px solid #db2777;
  color: #db2777;
  border-radius: 999px;
  padding: 0.1rem 0.6rem;
}
.cmp-score {
  font-size: 0.8rem;
  font-weight: 700;
  color: #db2777;
  background: #fdf2f8;
  border: 1px solid #fbcfe8;
  border-radius: 999px;
  padding: 0.1rem 0.6rem;
  white-space: nowrap;
}
.cmp-sub {
  display: flex;
  gap: 0.7rem;
  flex-wrap: wrap;
  font-size: 0.78rem;
}
.cmp-demo {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
}
.cmp-demo-row {
  display: flex;
  justify-content: space-between;
  gap: 0.8rem;
  font-size: 0.86rem;
  padding: 0.2rem 0.45rem;
  border-radius: 4px;
}
.cmp-demo-row.crit { background: #fdf2f8; box-shadow: inset 2px 0 0 #db2777; }
.cmp-demo-label {
  color: var(--text-muted);
  text-transform: uppercase;
  font-size: 0.7rem;
  letter-spacing: 0.05em;
  padding-top: 0.15rem;
}
.cmp-demo-value { font-weight: 600; text-align: right; }
.ok-mark { color: #15803d; font-weight: 700; font-size: 0.8rem; }
.miss-mark { color: #b91c1c; font-weight: 700; font-size: 0.8rem; }

.cmp-cat { display: flex; flex-direction: column; gap: 0.3rem; }
.cmp-cat-title {
  font-size: 0.7rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
}
.cmp-items { display: flex; flex-wrap: wrap; gap: 0.3rem; }
.cmp-item {
  display: inline-flex;
  align-items: baseline;
  gap: 0.25rem;
  font-size: 0.8rem;
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 0.12rem 0.55rem;
  background: var(--surface-alt);
  color: var(--text);
}
.cmp-item.crit { border-color: #db2777; }
.cmp-item.shared { background: #dcfce7; border-color: #86efac; color: #15803d; }
.cmp-item.shared.crit { border-color: #db2777; box-shadow: 0 0 0 1px #fbcfe8; }
.cmp-item.absent {
  background: #fef2f2;
  border-color: #fecaca;
  color: #b91c1c;
  border-style: dashed;
}
.cmp-mark { font-weight: 700; }
.cmp-none { font-size: 0.8rem; }

.rot-btn { padding: 0.35rem 0.8rem; font-size: 1rem; }
.rot-pos { display: flex; align-items: baseline; gap: 0.35rem; white-space: nowrap; }
.rot-count { font-weight: 700; }
.rot-total { font-size: 0.8rem; }

.cmp-toggle {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  font-size: 0.88rem;
  color: var(--text);
  margin: 0 0 1rem;
  cursor: pointer;
}

@media (max-width: 760px) {
  .comparator { grid-template-columns: 1fr; }
  /* stacked: no need for the empty alignment strip above the subject */
  .cmp-col:first-child .cmp-strip { display: none; }
}

/* ─── attribute prevalence ─── */
.prev-panel {
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  background: var(--surface);
  padding: 0.7rem 1rem;
  margin-bottom: 1rem;
}
.prev-panel summary {
  cursor: pointer;
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--text);
}
.prev-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(290px, 1fr));
  gap: 1rem 1.6rem;
  margin-top: 0.9rem;
}
.prev-cat { min-width: 0; }
.prev-row {
  display: flex;
  align-items: center;
  gap: 0.55rem;
  padding: 0.22rem 0.45rem;
  border-radius: 4px;
  font-size: 0.84rem;
}
.prev-row.crit { background: #fdf2f8; box-shadow: inset 2px 0 0 #db2777; }
.prev-label {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.prev-bar {
  width: 90px;
  height: 6px;
  flex-shrink: 0;
  background: var(--surface-alt);
  border-radius: 3px;
  overflow: hidden;
}
.prev-fill {
  height: 100%;
  background: #db2777;
  border-radius: 3px;
}
.prev-count {
  min-width: 30px;
  text-align: right;
  font-weight: 600;
  font-size: 0.82rem;
}

/* ─── compact ranked list ─── */
.match-row {
  display: flex;
  align-items: center;
  gap: 0.9rem;
  padding: 0.6rem 0.9rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--surface);
  margin-bottom: 0.4rem;
  cursor: pointer;
  transition: border-color 0.12s ease, background 0.12s ease;
}
.match-row:hover { border-color: #db2777; }
.match-row.active { border-color: #db2777; background: #fdf2f8; }
.match-rank {
  color: var(--text-muted);
  font-size: 0.82rem;
  font-weight: 600;
  min-width: 2rem;
}
.match-main { flex: 1; min-width: 0; }
.match-id { font-weight: 600; }
.match-meta {
  font-size: 0.82rem;
  display: flex;
  gap: 0.4rem;
  align-items: center;
  flex-wrap: wrap;
}
.pill {
  font-size: 0.72rem;
  font-weight: 600;
  border-radius: 999px;
  padding: 0.08rem 0.5rem;
  background: var(--surface-alt);
  border: 1px solid var(--border);
  color: var(--text-muted);
}
.pill.synthetic { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
.pill.external { background: #ecfdf5; border-color: #a7f3d0; color: #047857; }

.match-score {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  width: 150px;
  flex-shrink: 0;
}
.score-bar {
  flex: 1;
  height: 6px;
  background: var(--surface-alt);
  border-radius: 3px;
  overflow: hidden;
}
.score-fill {
  height: 100%;
  width: calc(var(--sc) * 100%);
  background: #db2777;
}
.score-value {
  font-size: 0.85rem;
  font-weight: 600;
  min-width: 36px;
  text-align: right;
}
.match-actions { display: flex; gap: 0.4rem; margin-top: 0.2rem; flex-wrap: wrap; }
</style>
