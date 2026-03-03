<template>
  <div class="card sql-block">
    <div class="sql-header">
      <span class="summary-left">
        <span class="sql-icon">{ }</span>
        <span class="sql-title">SQL</span>
      </span>
      <span class="summary-meta">
        <span v-if="model" class="meta-tag">{{ model }}</span>
        <span v-if="attempts" class="meta-tag">attempt {{ attempts }}</span>
      </span>
    </div>
    <div class="sql-body">
      <pre class="sql-pre"><code v-html="highlighted" /></pre>
    </div>
  </div>
</template>

<script setup>
import { computed } from "vue"

const props = defineProps({
  sql: String,
  model: String,
  attempts: Number,
})

// Basic syntax highlighting without external library
function highlight(sql) {
  if (!sql) return ""
  const escaped = sql
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")

  return escaped
    // String literals
    .replace(/'([^']*)'/g, "<span class=\"tok-str\">'$1'</span>")
    // Numbers
    .replace(/\b(\d+(?:\.\d+)?)\b/g, "<span class=\"tok-num\">$1</span>")
    // SQL keywords
    .replace(
      /\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|ON|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|DISTINCT|AS|AND|OR|NOT|IN|IS|NULL|ASC|DESC|WITH|UNION|ALL|EXCEPT|INTERSECT|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|INDEX|VIEW|OVER|PARTITION\s+BY|ROWS|BETWEEN|UNBOUNDED|PRECEDING|FOLLOWING|CURRENT\s+ROW|CASE|WHEN|THEN|ELSE|END|EXTRACT|NULLIF|COALESCE|COUNT|SUM|AVG|MIN|MAX|RANK|ROW_NUMBER|DENSE_RANK|EXPLAIN)\b/gi,
      "<span class=\"tok-kw\">$1</span>"
    )
    // Table/column aliases after AS
    .replace(/\bAS\s+(\w+)/gi, "<span class=\"tok-kw\">AS</span> <span class=\"tok-alias\">$1</span>")
    // Function names
    .replace(/\b([a-z_]+)\s*\(/gi, "<span class=\"tok-fn\">$1</span>(")
    // Line comments
    .replace(/(--[^\n]*)/g, "<span class=\"tok-comment\">$1</span>")
}

const highlighted = computed(() => highlight(props.sql))
</script>

<style scoped>
.sql-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 18px;
  gap: 12px;
}

.summary-left {
  display: flex;
  align-items: center;
  gap: 8px;
}

.sql-icon {
  font-family: var(--font-mono);
  font-size: 0.75rem;
  color: var(--accent-dim);
}

.sql-title {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--muted);
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.summary-meta {
  display: flex;
  align-items: center;
  gap: 6px;
}

.meta-tag {
  font-family: var(--font-mono);
  font-size: 0.6875rem;
  color: var(--muted);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 2px 6px;
}

.sql-body {
  border-top: 1px solid var(--border-light);
}

.sql-pre {
  margin: 0;
  padding: 18px 20px;
  background: #1C1917;
  border-radius: 0 0 var(--radius) var(--radius);
  overflow-x: auto;
  font-family: var(--font-mono);
  font-size: 0.8125rem;
  line-height: 1.7;
  white-space: pre;
  tab-size: 2;
}

.sql-pre :deep(.tok-kw)      { color: #818CF8; font-weight: 500; }
.sql-pre :deep(.tok-str)     { color: #6EE7B7; }
.sql-pre :deep(.tok-num)     { color: #FCA5A5; }
.sql-pre :deep(.tok-fn)      { color: #FCD34D; }
.sql-pre :deep(.tok-alias)   { color: #A5F3FC; font-style: italic; }
.sql-pre :deep(.tok-comment) { color: #57534E; font-style: italic; }
/* default token color */
.sql-pre code { color: #D4D0CC; }
</style>
