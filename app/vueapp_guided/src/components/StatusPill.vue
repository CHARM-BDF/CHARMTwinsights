<template>
  <div class="status-pill" :class="statusClass" :title="statusLabel">
    <span class="dot"></span>
    <span class="label">{{ statusLabel }}</span>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import axios from 'axios'
import { store } from '../state.js'

// Poll /health to reflect live status. If the router is unreachable we degrade
// gracefully to an "offline" label — no hard errors.
const status = ref('unknown')

let timer = null
async function poll() {
  try {
    const resp = await axios.get(`${store.apiBase}/health`, { timeout: 4000 })
    if (resp.status === 200 && resp.data?.status === 'healthy') {
      status.value = 'online'
    } else {
      status.value = 'degraded'
    }
  } catch (_e) {
    status.value = 'offline'
  }
}

onMounted(() => {
  poll()
  timer = setInterval(poll, 15000)
})
onUnmounted(() => timer && clearInterval(timer))

const statusClass = computed(() => `status-${status.value}`)
const statusLabel = computed(() => {
  switch (status.value) {
    case 'online':
      return 'System online'
    case 'degraded':
      return 'System degraded'
    case 'offline':
      return 'System unreachable'
    default:
      return 'Checking…'
  }
})
</script>

<style scoped>
.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.45em;
  padding: 0.3em 0.8em;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: var(--surface);
  font-size: 0.8rem;
  font-weight: 500;
  color: var(--text-muted);
}

.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--text-muted);
}
.status-online .dot { background: var(--success); }
.status-degraded .dot { background: var(--warning); }
.status-offline .dot { background: var(--danger); }
.status-online { color: var(--success); border-color: #bbf7d0; }
.status-degraded { color: var(--warning); border-color: #fde68a; }
.status-offline { color: var(--danger); border-color: #fecaca; }
</style>
