<template>
  <div class="card answer-card">
    <div class="card-header">
      <span class="answer-dot" />
      Answer
      <span v-if="model" class="answer-model-badge">{{ model }}</span>
    </div>
    <div class="answer-body">
      <p class="answer-text">
        {{ answer }}<span v-if="phase === 'streaming'" class="cursor" />
      </p>
    </div>
  </div>
</template>

<script setup>
defineProps({
  answer: String,
  phase: String,
  model: String,
})
</script>

<style scoped>
.answer-card {
  border-left: 3px solid var(--accent);
}

.answer-dot {
  display: inline-block;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--accent);
  flex-shrink: 0;
}

.answer-model-badge {
  margin-left: auto;
  font-family: var(--font-mono);
  font-size: 0.6875rem;
  color: var(--muted);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 2px 6px;
  white-space: nowrap;
}

.answer-body {
  padding: 18px 20px 20px;
}

.answer-text {
  font-family: var(--font-serif);
  font-size: 1.125rem;
  font-weight: 400;
  line-height: 1.65;
  color: var(--text);
  letter-spacing: -0.01em;
}

.cursor {
  display: inline-block;
  width: 2px;
  height: 1.1em;
  background: var(--accent);
  margin-left: 2px;
  vertical-align: text-bottom;
  animation: blink 0.9s step-end infinite;
}
</style>
