<template>
  <div class="wizard">
    <!-- Flow header -->
    <div class="wizard-header">
      <div class="wizard-title-row">
        <span class="wizard-icon" :style="{ color: accent }">{{ headerIcon }}</span>
        <h1>{{ title }}</h1>
      </div>
      <p v-if="subtitle" class="wizard-subtitle">{{ subtitle }}</p>
    </div>

    <!-- Step indicator -->
    <ol class="stepper" :style="{ '--stepper-accent': accent }">
      <li
        v-for="(step, idx) in steps"
        :key="idx"
        class="stepper-item"
        :class="{
          active: idx === store.currentStep,
          done: idx < store.currentStep,
          clickable: allowStepClick && idx <= store.currentStep,
        }"
        @click="handleStepClick(idx)"
      >
        <span class="stepper-circle">
          <span v-if="idx < store.currentStep">✓</span>
          <span v-else>{{ idx + 1 }}</span>
        </span>
        <span class="stepper-label">{{ step.label }}</span>
      </li>
    </ol>

    <!-- Body -->
    <div class="card wizard-card">
      <div class="step-body">
        <slot :name="`step-${store.currentStep}`" :step="currentStep">
          <p class="muted">(No content slot defined for step {{ store.currentStep + 1 }})</p>
        </slot>
      </div>

      <div class="wizard-actions">
        <button class="ghost" @click="cancel">Cancel</button>

        <div class="wizard-actions-right">
          <button
            :disabled="store.currentStep === 0"
            @click="prev"
          >
            ← Back
          </button>

          <button
            v-if="!isLastStep"
            class="primary"
            :disabled="!canAdvance"
            @click="next"
          >
            Next →
          </button>
          <button
            v-else
            class="primary"
            :disabled="!canAdvance"
            @click="$emit('finish')"
          >
            {{ finishLabel }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { store, goHome, prevStep, nextStep, goToStep, getFlow } from '../state.js'

const props = defineProps({
  title: { type: String, required: true },
  subtitle: { type: String, default: '' },
  icon: { type: String, default: '' },
  accent: { type: String, default: 'var(--accent)' },
  steps: {
    type: Array,
    required: true,
    // Each step: { label: string, canAdvance?: boolean }
  },
  finishLabel: { type: String, default: 'Finish' },
  allowStepClick: { type: Boolean, default: true },
})

defineEmits(['finish'])

// The flow list in state.js is the single source of truth for icons, so the
// header here always matches the landing tile and the breadcrumb. The `icon`
// prop is only a fallback for a component rendered outside a known flow.
const headerIcon = computed(() => getFlow(store.currentFlow)?.icon || props.icon)

const currentStep = computed(() => props.steps[store.currentStep])
const isLastStep = computed(() => store.currentStep === props.steps.length - 1)

// A step can opt-in to gate advancement via step.canAdvance = false.
// Default: always allowed (since these are stubs).
const canAdvance = computed(() => {
  const s = currentStep.value
  return s?.canAdvance !== false
})

function next() {
  nextStep(props.steps.length)
}
function prev() {
  prevStep()
}
function cancel() {
  goHome()
}
function handleStepClick(idx) {
  if (!props.allowStepClick) return
  // Only allow jumping to steps already visited or the current one.
  if (idx <= store.currentStep) {
    goToStep(idx)
  }
}
</script>

<style scoped>
.wizard {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.wizard-header {
  max-width: 720px;
}
.wizard-title-row {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  margin-bottom: 0.4rem;
}
.wizard-icon {
  font-size: 1.8rem;
}
.wizard-subtitle {
  color: var(--text-muted);
  font-size: 1rem;
}

/* Stepper */
.stepper {
  --stepper-accent: var(--accent);
  display: flex;
  list-style: none;
  margin: 0;
  padding: 0;
  gap: 0;
  flex-wrap: wrap;
}
.stepper-item {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  padding: 0.55rem 0.9rem 0.55rem 0.55rem;
  color: var(--text-muted);
  font-size: 0.9rem;
  position: relative;
}
.stepper-item.clickable {
  cursor: pointer;
}
.stepper-item.clickable:hover .stepper-label {
  color: var(--text);
}
.stepper-item:not(:last-child)::after {
  content: '';
  width: 28px;
  height: 1px;
  background: var(--border);
  margin-left: 0.3rem;
}
.stepper-circle {
  width: 26px;
  height: 26px;
  border-radius: 50%;
  border: 1px solid var(--border);
  background: var(--surface);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.8rem;
  font-weight: 600;
  flex-shrink: 0;
}
.stepper-item.active .stepper-circle {
  border-color: var(--stepper-accent);
  color: var(--stepper-accent);
  background: color-mix(in srgb, var(--stepper-accent) 10%, white);
}
.stepper-item.active .stepper-label {
  color: var(--text);
  font-weight: 600;
}
.stepper-item.done .stepper-circle {
  background: var(--stepper-accent);
  border-color: var(--stepper-accent);
  color: white;
}
.stepper-item.done .stepper-label {
  color: var(--text);
}

.wizard-card {
  display: flex;
  flex-direction: column;
  gap: 1.2rem;
}

.wizard-actions {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-top: 1rem;
  border-top: 1px solid var(--border);
  gap: 0.6rem;
}
.wizard-actions-right {
  display: flex;
  gap: 0.5rem;
}
</style>
