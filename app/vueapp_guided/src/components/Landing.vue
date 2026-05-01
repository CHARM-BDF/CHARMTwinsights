<template>
  <section class="landing">
    <div class="hero">
      <h1>What would you like to do?</h1>
      <p class="lede">
        Choose a guided flow below. Each is a short, step-by-step wizard — you can always
        back out and pick something else.
      </p>
    </div>

    <div v-for="category in categories" :key="category" class="category-section">
      <h2 class="category-title">{{ category }}</h2>
      <div class="tile-grid">
        <button
          v-for="flow in flowsByCategory(category)"
          :key="flow.id"
          class="tile"
          @click="pick(flow.id)"
          :style="{ '--tile-accent': flow.accent }"
        >
          <div class="tile-icon">{{ flow.icon }}</div>
          <div class="tile-content">
            <div class="tile-title">{{ flow.title }}</div>
            <div class="tile-sub">{{ flow.subtitle }}</div>
          </div>
          <div class="tile-arrow">→</div>
        </button>
      </div>
    </div>
  </section>
</template>

<script setup>
import { computed } from 'vue'
import { FLOWS, startFlow } from '../state.js'

const categories = computed(() => {
  const seen = new Set()
  const ordered = []
  for (const f of FLOWS) {
    if (!seen.has(f.category)) {
      seen.add(f.category)
      ordered.push(f.category)
    }
  }
  return ordered
})

function flowsByCategory(cat) {
  return FLOWS.filter((f) => f.category === cat)
}

function pick(id) {
  startFlow(id)
}
</script>

<style scoped>
.landing {
  padding-top: 1rem;
}

.hero {
  margin-bottom: 2.5rem;
  max-width: 720px;
}

.hero h1 {
  font-size: 2.2rem;
  margin-bottom: 0.6rem;
}

.lede {
  color: var(--text-muted);
  font-size: 1.05rem;
}

.category-section {
  margin-bottom: 2.2rem;
}

.category-title {
  font-size: 0.8rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text-muted);
  margin-bottom: 0.8rem;
}

.tile-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
  gap: 1rem;
}

.tile {
  --tile-accent: var(--accent);
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 1.1rem 1.2rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  text-align: left;
  box-shadow: var(--shadow-sm);
  cursor: pointer;
  transition: all 0.18s ease;
  color: inherit;
}
.tile:hover {
  border-color: var(--tile-accent);
  box-shadow: var(--shadow-md);
  transform: translateY(-1px);
}
.tile:hover .tile-arrow {
  color: var(--tile-accent);
  transform: translateX(3px);
}

.tile-icon {
  font-size: 1.9rem;
  width: 46px;
  height: 46px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: color-mix(in srgb, var(--tile-accent) 10%, white);
  border-radius: var(--radius-sm);
}

.tile-content {
  flex: 1;
  min-width: 0;
}

.tile-title {
  font-weight: 600;
  font-size: 1rem;
  color: var(--text);
  margin-bottom: 0.15rem;
}

.tile-sub {
  font-size: 0.85rem;
  color: var(--text-muted);
  line-height: 1.4;
}

.tile-arrow {
  color: var(--text-muted);
  font-size: 1.3rem;
  transition: all 0.18s ease;
  flex-shrink: 0;
}
</style>
