<template>
  <div class="agent-flow">
    <div class="agent-header">
      <div class="agent-title-row">
        <span class="agent-icon">💬</span>
        <h1>AI assistant (MCP)</h1>
      </div>
      <p class="muted">
        Chat with the Model Context Protocol server. The assistant can search patients,
        fetch records, and execute registered models on your behalf.
      </p>
      <button class="ghost" @click="goHome">← Home</button>
    </div>

    <div class="card agent-card">
      <div class="chat-log">
        <div v-for="(msg, i) in messages" :key="i" :class="['msg', msg.role]">
          <div class="msg-role">{{ msg.role === 'user' ? 'You' : 'Assistant' }}</div>
          <div class="msg-body">{{ msg.text }}</div>
        </div>
      </div>

      <div class="stub-banner" style="align-self: flex-start">Future work</div>
      <p class="muted" style="margin: 0.5rem 0 0.8rem">
        This panel is a placeholder. A future iteration will bridge an LLM to the MCP
        server (port 8006) so the assistant can search patients and run models.
      </p>

      <form class="chat-input" @submit.prevent="send">
        <input
          type="text"
          v-model="draft"
          placeholder="Ask about a patient, cohort, or model…"
        />
        <button class="primary" type="submit" :disabled="!draft.trim()">Send</button>
      </form>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { goHome } from '../state.js'

const messages = ref([
  {
    role: 'assistant',
    text: 'Hi — this is where the MCP-backed assistant will live. Try asking about a patient or a model, once wired up.',
  },
])

const draft = ref('')

function send() {
  const text = draft.value.trim()
  if (!text) return
  messages.value.push({ role: 'user', text })
  draft.value = ''
  // Stub — echo a placeholder response.
  setTimeout(() => {
    messages.value.push({
      role: 'assistant',
      text: "(stub) I'd normally reach the MCP server at port 8006 to answer that.",
    })
  }, 400)
}
</script>

<style scoped>
.agent-flow {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}
.agent-header {
  max-width: 720px;
}
.agent-title-row {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  margin-bottom: 0.4rem;
}
.agent-icon { font-size: 1.7rem; }
.agent-header .ghost {
  margin-top: 0.8rem;
}

.agent-card {
  display: flex;
  flex-direction: column;
  gap: 0.8rem;
  min-height: 420px;
}

.chat-log {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 0.8rem;
  overflow-y: auto;
  padding: 0.2rem;
}
.msg {
  max-width: 80%;
  padding: 0.7rem 0.9rem;
  border-radius: var(--radius-sm);
}
.msg.user {
  align-self: flex-end;
  background: var(--accent-soft);
}
.msg.assistant {
  align-self: flex-start;
  background: var(--surface-alt);
}
.msg-role {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: 0.2rem;
}
.msg-body {
  white-space: pre-wrap;
  color: var(--text);
  line-height: 1.45;
}

.chat-input {
  display: flex;
  gap: 0.5rem;
  align-items: center;
  border-top: 1px solid var(--border);
  padding-top: 0.8rem;
}
.chat-input input {
  flex: 1;
}
</style>
