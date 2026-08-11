<template>
  <header class="topbar">
    <div class="topbar-inner">
      <div class="brand" @click="goHome" role="button" tabindex="0">
        <div class="brand-text">
          <div class="brand-name">CHARMTwinsights</div>
          <div class="brand-sub">Guided flow</div>
        </div>
      </div>

      <nav class="breadcrumbs" v-if="activeFlow">
        <button class="ghost" @click="goHome">← Home</button>
        <span class="sep">/</span>
        <span class="crumb-title">
          <span class="crumb-icon" :style="{ color: activeFlow.accent }">
            {{ activeFlow.icon }}
          </span>
          <span class="crumb-text">{{ activeFlow.title }}</span>
        </span>
      </nav>

      <div class="topbar-right">
        <StatusPill />
      </div>
    </div>
  </header>
</template>

<script setup>
import { computed } from 'vue'
import { store, goHome, getFlow } from '../state.js'
import StatusPill from './StatusPill.vue'

const activeFlow = computed(() => getFlow(store.currentFlow))
</script>

<style scoped>
.topbar {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  box-shadow: var(--shadow-sm);
}

.topbar-inner {
  max-width: 1100px;
  margin: 0 auto;
  padding: 0.9rem 1.5rem;
  display: flex;
  align-items: center;
  gap: 1.5rem;
}

.brand {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  cursor: pointer;
  user-select: none;
}
.brand:hover .brand-name {
  color: var(--accent);
}
.brand-text {
  display: flex;
  flex-direction: column;
  line-height: 1.1;
}
.brand-name {
  font-weight: 700;
  font-size: 1.05rem;
  color: var(--text);
  transition: color 0.15s ease;
}
.brand-sub {
  font-size: 0.75rem;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.breadcrumbs {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  flex: 1;
  min-width: 0;
}
.sep {
  color: var(--text-muted);
}
.crumb-title {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  font-weight: 500;
  color: var(--text);
  min-width: 0;
}
/* Ellipsis must live on the text span itself — text-overflow on the flex
   container silently drops the text node instead of truncating it. */
.crumb-text {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.crumb-icon {
  font-size: 1.1rem;
}

.topbar-right {
  margin-left: auto;
}
</style>
