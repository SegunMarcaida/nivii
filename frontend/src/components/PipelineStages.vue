<template>
  <div class="card pipeline">
    <div class="card-header">
      <span class="pipeline-icon" :class="{ spinning: phase === 'streaming' }">◎</span>
      Pipeline
    </div>
    <ul class="stage-list">
      <li
        v-for="(stage, i) in stages"
        :key="i"
        class="stage-row"
        :class="[`stage--${stage.event}`, { 'stage--has-content': stage.hasContent, 'stage--expanded': stage.expanded }]"
        :style="{ animationDelay: `${i * 30}ms` }"
      >
        <!-- Clickable header row -->
        <button
          class="stage-header"
          :class="{ 'stage-header--clickable': stage.hasContent }"
          :disabled="!stage.hasContent"
          @click="stage.hasContent && $emit('toggle-stage', i)"
        >
          <span class="stage-ts">{{ formatTime(stage.ts) }}</span>
          <span class="stage-dot" :class="dotClass(stage)" />
          <span class="stage-label" :class="{ 'stage-label--active': stage.isActive }">{{ label(stage) }}</span>
          <span
            v-if="stage.event === 'sql_attempt' || stage.event === 'answer_start'"
            class="stage-badge"
          >
            {{ stage.payload.model }}
          </span>
          <span v-if="stage.hasContent" class="stage-chevron" :class="{ 'stage-chevron--open': stage.expanded }">›</span>
        </button>

        <!-- Expandable inline content -->
        <Transition name="expand">
          <div v-if="stage.expanded && stage.hasContent" class="stage-content">
            <!-- sql_attempt: dark terminal with <think> parsing -->
            <div v-if="stage.event === 'sql_attempt'" class="stage-stream">
              <div class="stream-body" :ref="el => { if (el) streamRefs[i] = el }">
                <pre class="stream-pre"><code><span
                  v-for="(seg, j) in parseSegments(stage.streamContent)"
                  :key="j"
                  :class="seg.cls"
                >{{ seg.text }}</span><span v-if="stage.isActive" class="cursor" /></code></pre>
              </div>
            </div>

            <!-- answer_start: prose answer preview -->
            <div v-else-if="stage.event === 'answer_start'" class="stage-answer-preview">
              <span class="answer-preview-text">{{ stage.streamContent }}</span><span v-if="stage.isActive" class="cursor-prose" />
            </div>
          </div>
        </Transition>
      </li>

      <!-- Live cursor while streaming (no new stage yet) -->
      <li v-if="phase === 'streaming'" class="stage-row stage--pending">
        <button class="stage-header" disabled>
          <span class="stage-ts">—</span>
          <span class="stage-dot stage-dot--pulse" />
          <span class="stage-label stage-label--dim">waiting for model…</span>
        </button>
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref, watch, nextTick } from "vue"

const props = defineProps({
  stages: Array,
  phase: String,
})

defineEmits(["toggle-stage"])

// Refs to scroll stream bodies to bottom on new tokens
const streamRefs = ref({})

// Auto-scroll active stream to bottom when streamContent changes
watch(
  () => props.stages.map(s => s.streamContent),
  async (newContents, oldContents) => {
    await nextTick()
    props.stages.forEach((stage, i) => {
      if (stage.isActive && streamRefs.value[i]) {
        streamRefs.value[i].scrollTop = streamRefs.value[i].scrollHeight
      }
    })
  }
)

const LABELS = {
  thinking_start:  "Generating SQL",
  sql_attempt:     "Calling model",
  sql_generated:   "SQL ready",
  query_running:   "Executing query",
  fallback_start:  "Retrying with correction prompt",
  answer_start:    "Generating answer",
  done:            "Complete",
  error:           "Failed",
}

function label(stage) {
  if (stage.event === "sql_attempt") {
    return `Calling model (attempt ${stage.payload.attempt})`
  }
  if (stage.event === "fallback_start") {
    const fromModel = stage.payload.model ?? null
    const toModel = stage.payload.to_model ?? null
    if (toModel && fromModel && toModel !== fromModel) {
      return `Escalating → ${toModel}`
    }
    if (stage.payload.attempt) {
      return `Retrying with correction prompt (attempt ${stage.payload.attempt})`
    }
    return "Retrying with correction prompt"
  }
  return LABELS[stage.event] ?? stage.event
}

function dotClass(stage) {
  if (stage.isActive) return "stage-dot--pulse"
  if (stage.event === "done") return "stage-dot--done"
  if (stage.event === "error") return "stage-dot--error"
  return "stage-dot--default"
}

function formatTime(date) {
  return date.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" })
}

// Parse model output into colored segments: think-tag markers, think content, SQL output
function parseSegments(text) {
  if (!text) return []

  const parts = []
  const thinkOpen = text.indexOf("<think>")
  const thinkClose = text.indexOf("</think>")

  if (thinkOpen === -1) {
    // No think tags — all SQL/raw output
    parts.push({ text, cls: "tok-sql" })
  } else if (thinkClose === -1) {
    // Think tag opened but not closed yet — still reasoning
    if (thinkOpen > 0) parts.push({ text: text.slice(0, thinkOpen), cls: "tok-sql" })
    parts.push({ text: "<think>", cls: "tok-tag" })
    parts.push({ text: text.slice(thinkOpen + 7), cls: "tok-think" })
  } else {
    // Both tags present — reasoning complete, SQL after
    if (thinkOpen > 0) parts.push({ text: text.slice(0, thinkOpen), cls: "tok-sql" })
    parts.push({ text: "<think>", cls: "tok-tag" })
    parts.push({ text: text.slice(thinkOpen + 7, thinkClose), cls: "tok-think" })
    parts.push({ text: "</think>", cls: "tok-tag" })
    const afterThink = text.slice(thinkClose + 8)
    if (afterThink) parts.push({ text: afterThink, cls: "tok-sql" })
  }

  return parts.filter(p => p.text)
}
</script>

<style scoped>
.pipeline {
  overflow: visible;
}

.pipeline-icon {
  font-size: 0.875rem;
  color: var(--accent-dim);
  display: inline-block;
  transition: color 0.2s;
}
.pipeline-icon.spinning {
  animation: spin 1.5s linear infinite;
  color: var(--accent);
}

.stage-list {
  list-style: none;
  padding: 6px 0 8px;
}

.stage-row {
  animation: fadeUp 0.25s ease both;
}

/* ── Stage header (clickable row) ──────────────────────────────────────── */
.stage-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 5px 18px;
  width: 100%;
  background: none;
  border: none;
  text-align: left;
  font-size: 0.8125rem;
  font-family: inherit;
  cursor: default;
  border-radius: 0;
  transition: background 0.15s;
}

.stage-header--clickable {
  cursor: pointer;
}
.stage-header--clickable:hover {
  background: var(--accent-light);
}

.stage--pending .stage-header {
  opacity: 0.5;
  cursor: default;
}

/* ── Stage row elements ─────────────────────────────────────────────────── */
.stage-ts {
  font-family: var(--font-mono);
  font-size: 0.6875rem;
  color: var(--muted);
  min-width: 62px;
  flex-shrink: 0;
}

.stage-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  flex-shrink: 0;
}
.stage-dot--default {
  background: var(--accent-dim);
}
.stage-dot--done {
  background: var(--success);
}
.stage-dot--error {
  background: var(--error);
}
.stage-dot--pulse {
  background: var(--accent);
  animation: pulse-dot 1.2s ease infinite;
}

.stage-label {
  color: var(--text-2);
  font-weight: 400;
  line-height: 1.4;
  flex: 1;
}
.stage-label--active {
  color: var(--text);
  font-weight: 500;
}
.stage-label--dim {
  color: var(--muted);
  font-style: italic;
}

.stage-badge {
  font-family: var(--font-mono);
  font-size: 0.6875rem;
  color: var(--muted);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 2px 6px;
  white-space: nowrap;
}

.stage-chevron {
  font-size: 1rem;
  color: var(--muted);
  flex-shrink: 0;
  display: inline-block;
  transition: transform 0.2s ease;
  line-height: 1;
}
.stage-chevron--open {
  transform: rotate(90deg);
}

/* ── Accordion expand/collapse ─────────────────────────────────────────── */
.expand-enter-active,
.expand-leave-active {
  transition: max-height 0.28s ease, opacity 0.2s ease;
  overflow: hidden;
}
.expand-enter-from,
.expand-leave-to {
  max-height: 0;
  opacity: 0;
}
.expand-enter-to,
.expand-leave-from {
  max-height: 600px;
  opacity: 1;
}

/* ── Inline stream content (sql_attempt) ───────────────────────────────── */
.stage-content {
  border-top: 1px solid rgba(255,255,255,0.04);
}

.stage-stream {
  border-left: 3px solid var(--accent-dim);
  margin: 0 18px 10px 80px;
  border-radius: 0 4px 4px 0;
  overflow: hidden;
}

.stream-body {
  max-height: 280px;
  overflow-y: auto;
  scroll-behavior: smooth;
}

.stream-pre {
  margin: 0;
  padding: 14px 16px;
  background: #1C1917;
  font-family: var(--font-mono);
  font-size: 0.7813rem;
  line-height: 1.7;
  white-space: pre-wrap;
  word-break: break-word;
  tab-size: 2;
  min-height: 40px;
}

.stream-pre code {
  color: #D4D0CC;
}

/* Think-block reasoning — dimmed */
.tok-think {
  color: #6B6560;
}

/* Think tags — very dim, italic */
.tok-tag {
  color: #4A4540;
  font-style: italic;
}

/* SQL output — bright */
.tok-sql {
  color: #D4D0CC;
}

/* Blinking cursor (monospace terminal) */
.cursor {
  display: inline-block;
  width: 7px;
  height: 1em;
  background: var(--accent);
  margin-left: 2px;
  vertical-align: text-bottom;
  animation: blink 0.9s step-end infinite;
  opacity: 0.8;
}

/* ── Inline answer preview (answer_start) ───────────────────────────────── */
.stage-answer-preview {
  margin: 0 18px 10px 80px;
  padding: 12px 16px;
  background: var(--accent-light);
  border-left: 3px solid var(--accent);
  border-radius: 0 4px 4px 0;
  font-family: var(--font-serif);
  font-size: 0.9375rem;
  line-height: 1.65;
  color: var(--text);
}

.answer-preview-text {
  white-space: pre-wrap;
  word-break: break-word;
}

/* Prose cursor (underline style) */
.cursor-prose {
  display: inline-block;
  width: 2px;
  height: 1.1em;
  background: var(--accent);
  margin-left: 1px;
  vertical-align: text-bottom;
  animation: blink 0.9s step-end infinite;
}
</style>
