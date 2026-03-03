<template>
  <div class="card results-card">
    <div class="card-header">
      <span class="results-icon">⊞</span>
      Results
      <span class="results-count">{{ rowCount }} row{{ rowCount !== 1 ? "s" : "" }}</span>
    </div>

    <div v-if="truncated" class="truncation-notice">
      Showing first {{ MAX_ROWS }} of {{ rowCount }} rows
    </div>

    <div class="table-wrap">
      <table class="results-table">
        <thead>
          <tr>
            <th v-for="col in columns" :key="col" :class="{ 'col-num': isNumericCol(col) }">
              {{ col }}
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, i) in displayRows" :key="i" :class="{ 'row-alt': i % 2 === 1 }">
            <td
              v-for="col in columns"
              :key="col"
              :class="{ 'col-num': isNumericCol(col) }"
            >
              {{ formatCell(row[col]) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { computed } from "vue"

const MAX_ROWS = 100

const props = defineProps({
  results: Array,
  rowCount: Number,
})

const columns = computed(() =>
  props.results.length > 0 ? Object.keys(props.results[0]) : []
)

const displayRows = computed(() =>
  props.results.slice(0, MAX_ROWS)
)

const truncated = computed(() =>
  props.results.length > MAX_ROWS
)

function isNumericCol(col) {
  const r = props.results[0]
  if (!r) return false
  const val = r[col]
  return typeof val === "number"
}

function formatCell(val) {
  if (val === null || val === undefined) return "—"
  if (typeof val === "number") {
    // Format integers normally, floats to 2 decimal places
    return Number.isInteger(val) ? val.toLocaleString() : val.toFixed(2)
  }
  return String(val)
}
</script>

<style scoped>
.results-icon {
  font-size: 0.875rem;
  color: var(--accent-dim);
}

.results-count {
  margin-left: auto;
  font-family: var(--font-mono);
  font-size: 0.6875rem;
  font-weight: 400;
  color: var(--muted);
  text-transform: none;
  letter-spacing: 0;
}

.truncation-notice {
  padding: 6px 18px;
  font-size: 0.75rem;
  color: var(--warning);
  background: #FFFBEB;
  border-bottom: 1px solid #FDE68A;
}

.table-wrap {
  overflow-x: auto;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.8125rem;
}

thead tr {
  background: var(--bg);
  border-bottom: 1px solid var(--border);
}

th {
  padding: 9px 16px;
  font-family: var(--font-mono);
  font-size: 0.6875rem;
  font-weight: 500;
  color: var(--muted);
  text-align: left;
  letter-spacing: 0.04em;
  white-space: nowrap;
}

th.col-num, td.col-num {
  text-align: right;
}

td {
  padding: 8px 16px;
  color: var(--text);
  border-bottom: 1px solid var(--border-light);
  white-space: nowrap;
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.row-alt td {
  background: var(--bg);
}

tbody tr:last-child td {
  border-bottom: none;
}

tbody tr:hover td {
  background: var(--accent-light);
}
</style>
