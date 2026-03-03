<template>
  <div class="app">
    <!-- Header -->
    <header class="header">
      <div class="header-inner">
        <span class="logo">nivii</span>
        <span class="logo-sub">natural language analytics</span>
      </div>
    </header>

    <!-- Main content -->
    <main class="main">
      <div class="container">

        <!-- Hero headline (idle only) -->
        <div v-if="phase === 'idle'" class="hero">
          <h1 class="hero-title">Ask anything<br /><em>about your data.</em></h1>
          <p class="hero-sub">
            Natural language questions, instant SQL — powered by local AI.
          </p>
        </div>

        <!-- Search bar -->
        <SearchBar
          :disabled="phase === 'streaming'"
          @submit="handleSubmit"
        />

        <!-- Error state -->
        <div v-if="phase === 'error'" class="error-card">
          <span class="error-icon">⚠</span>
          <span>{{ error }}</span>
        </div>

        <!-- Pipeline stages (visible while streaming or done) -->
        <!-- Each step is an accordion: expands inline while active, collapses on next step -->
        <PipelineStages
          v-if="phase !== 'idle'"
          :stages="stages"
          :phase="phase"
          @toggle-stage="toggleStage"
        />

        <!-- Answer card (visible once done — answer was already shown inline during streaming) -->
        <AnswerCard
          v-if="answer && phase === 'done'"
          :answer="answer"
          :phase="phase"
          :model="answerModel"
        />

        <!-- Results table (visible once done) -->
        <ResultsTable
          v-if="phase === 'done' && results.length > 0"
          :results="results"
          :row-count="rowCount"
        />

        <!-- Clean SQL block (visible once done) -->
        <SQLBlock
          v-if="sql && phase === 'done'"
          :sql="sql"
          :model="model"
          :attempts="attempts"
        />

      </div>
    </main>
  </div>
</template>

<script setup>
import { useQueryStream } from "./composables/useQueryStream.js"
import SearchBar from "./components/SearchBar.vue"
import PipelineStages from "./components/PipelineStages.vue"
import AnswerCard from "./components/AnswerCard.vue"
import SQLBlock from "./components/SQLBlock.vue"
import ResultsTable from "./components/ResultsTable.vue"

const {
  phase, stages, sql, answer,
  results, rowCount, attempts, model, answerModel, error,
  submit,
  toggleStage,
} = useQueryStream()

function handleSubmit(question) {
  submit(question)
}
</script>

<style>
/* ── Design tokens ─────────────────────────────────────────────────────── */
:root {
  --bg:           #FAFAF8;
  --surface:      #FFFFFF;
  --border:       #E8E4DF;
  --border-light: #F0EDE9;
  --accent:       #4338CA;
  --accent-light: #EEF2FF;
  --accent-dim:   #818CF8;
  --muted:        #9B9189;
  --text:         #1C1917;
  --text-2:       #57534E;
  --success:      #16A34A;
  --warning:      #D97706;
  --error:        #DC2626;

  --font-serif:   'Lora', Georgia, 'Times New Roman', serif;
  --font-sans:    'DM Sans', system-ui, sans-serif;
  --font-mono:    'DM Mono', 'Fira Code', 'Cascadia Code', monospace;

  --radius:       10px;
  --radius-sm:    6px;
  --shadow-sm:    0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
  --shadow:       0 4px 12px rgba(0,0,0,0.08), 0 1px 3px rgba(0,0,0,0.05);
}

/* ── Global resets ─────────────────────────────────────────────────────── */
* { box-sizing: border-box; }

/* ── App shell ─────────────────────────────────────────────────────────── */
.app {
  min-height: 100vh;
  background: var(--bg);
}

/* ── Header ────────────────────────────────────────────────────────────── */
.header {
  border-bottom: 1px solid var(--border-light);
  background: var(--surface);
  position: sticky;
  top: 0;
  z-index: 10;
}
.header-inner {
  max-width: 760px;
  margin: 0 auto;
  padding: 14px 24px;
  display: flex;
  align-items: baseline;
  gap: 10px;
}
.logo {
  font-family: var(--font-serif);
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--text);
  letter-spacing: -0.02em;
}
.logo-sub {
  font-size: 0.75rem;
  font-weight: 300;
  color: var(--muted);
  letter-spacing: 0.04em;
}

/* ── Main ──────────────────────────────────────────────────────────────── */
.main {
  padding: 40px 24px 80px;
}
.container {
  max-width: 760px;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  gap: 20px;
}

/* ── Hero ──────────────────────────────────────────────────────────────── */
.hero {
  padding: 32px 0 8px;
  animation: fadeUp 0.5s ease both;
}
.hero-title {
  font-family: var(--font-serif);
  font-size: clamp(2rem, 5vw, 2.75rem);
  font-weight: 400;
  line-height: 1.2;
  color: var(--text);
  letter-spacing: -0.02em;
  margin-bottom: 12px;
}
.hero-title em {
  font-style: italic;
  color: var(--accent);
}
.hero-sub {
  font-size: 0.9375rem;
  color: var(--text-2);
  font-weight: 300;
  line-height: 1.6;
}

/* ── Error card ────────────────────────────────────────────────────────── */
.error-card {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 14px 18px;
  background: #FEF2F2;
  border: 1px solid #FECACA;
  border-radius: var(--radius-sm);
  color: var(--error);
  font-size: 0.875rem;
  animation: fadeUp 0.25s ease both;
}
.error-icon {
  font-size: 1rem;
  flex-shrink: 0;
}

/* ── Shared card ───────────────────────────────────────────────────────── */
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  box-shadow: var(--shadow-sm);
  overflow: hidden;
  animation: fadeUp 0.3s ease both;
}
.card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 18px;
  border-bottom: 1px solid var(--border-light);
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--muted);
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

/* ── Animations ────────────────────────────────────────────────────────── */
@keyframes fadeUp {
  from { opacity: 0; transform: translateY(6px); }
  to   { opacity: 1; transform: translateY(0); }
}

@keyframes pulse-dot {
  0%, 100% { opacity: 1; }
  50%       { opacity: 0.3; }
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to   { transform: rotate(360deg); }
}

@keyframes blink {
  0%, 100% { opacity: 1; }
  50%       { opacity: 0; }
}
</style>
