<template>
  <div class="search-wrap card">
    <textarea
      ref="textareaRef"
      v-model="localQuestion"
      class="search-input"
      :placeholder="placeholder"
      :disabled="disabled"
      rows="1"
      @keydown="onKeydown"
      @input="autoResize"
    />
    <div class="search-footer">
      <span class="search-hint">{{ localQuestion.length }}/1000 · ⌘↵ to send</span>
      <button
        class="search-btn"
        :class="{ loading: disabled }"
        :disabled="disabled || localQuestion.trim().length < 5"
        @click="handleSubmit"
      >
        <span v-if="!disabled" class="btn-text">Ask</span>
        <span v-else class="btn-spinner" aria-label="Loading…" />
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from "vue"

const props = defineProps({
  disabled: Boolean,
})

const emit = defineEmits(["submit"])

const localQuestion = ref("")
const textareaRef = ref(null)

const placeholder = "What product sells most on Fridays? Which waiter has the highest revenue?"

function autoResize() {
  const el = textareaRef.value
  if (!el) return
  el.style.height = "auto"
  el.style.height = Math.min(el.scrollHeight, 200) + "px"
}

function onKeydown(e) {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
    e.preventDefault()
    handleSubmit()
  }
}

function handleSubmit() {
  const q = localQuestion.value.trim()
  if (q.length >= 5 && !props.disabled) {
    emit("submit", q)
  }
}

onMounted(() => {
  textareaRef.value?.focus()
})
</script>

<style scoped>
.search-wrap {
  padding: 0;
}

.search-input {
  display: block;
  width: 100%;
  padding: 18px 20px 12px;
  font-family: var(--font-serif);
  font-size: 1.0625rem;
  font-weight: 400;
  color: var(--text);
  background: transparent;
  border: none;
  outline: none;
  resize: none;
  line-height: 1.55;
  min-height: 60px;
  max-height: 200px;
  overflow-y: auto;
}

.search-input::placeholder {
  color: var(--muted);
  font-style: italic;
  font-weight: 300;
}

.search-input:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

.search-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 16px 12px;
  border-top: 1px solid var(--border-light);
}

.search-hint {
  font-size: 0.6875rem;
  color: var(--muted);
  font-family: var(--font-mono);
}

.search-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 7px 20px;
  background: var(--accent);
  color: #fff;
  border: none;
  border-radius: var(--radius-sm);
  font-family: var(--font-sans);
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: background 0.15s ease, opacity 0.15s ease;
  min-width: 64px;
  height: 34px;
}

.search-btn:hover:not(:disabled) {
  background: #3730A3;
}

.search-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.search-btn.loading {
  opacity: 1;
  cursor: default;
}

.btn-text {
  letter-spacing: 0.02em;
}

.btn-spinner {
  display: block;
  width: 16px;
  height: 16px;
  border: 2px solid rgba(255,255,255,0.3);
  border-top-color: #fff;
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
}
</style>
