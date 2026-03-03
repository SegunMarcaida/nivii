import { ref, readonly } from "vue"

// phase: "idle" | "streaming" | "done" | "error"
export function useQueryStream() {
  const phase = ref("idle")
  const stages = ref([])
  const sql = ref(null)
  const answer = ref("")
  const results = ref([])
  const rowCount = ref(0)
  const attempts = ref(0)
  const model = ref(null)
  const answerModel = ref(null)
  const error = ref(null)

  let _es = null
  let _activeStageIdx = -1

  function _reset() {
    phase.value = "idle"
    stages.value = []
    sql.value = null
    answer.value = ""
    results.value = []
    rowCount.value = 0
    attempts.value = 0
    model.value = null
    answerModel.value = null
    error.value = null
    _activeStageIdx = -1
  }

  function _addStage(event, payload, { hasContent = false } = {}) {
    // Auto-collapse the previously active stage
    if (_activeStageIdx >= 0) {
      stages.value[_activeStageIdx].expanded = false
      stages.value[_activeStageIdx].isActive = false
    }

    stages.value.push({
      event,
      payload,
      ts: new Date(),
      streamContent: "",
      expanded: hasContent,   // Auto-expand stages that will receive content
      hasContent,
      isActive: hasContent,
    })
    _activeStageIdx = stages.value.length - 1
  }

  function toggleStage(idx) {
    const stage = stages.value[idx]
    if (stage && stage.hasContent) {
      stage.expanded = !stage.expanded
    }
  }

  function submit(question) {
    if (_es) {
      _es.close()
      _es = null
    }
    _reset()
    phase.value = "streaming"

    const url = `/query/stream?q=${encodeURIComponent(question)}`
    const es = new EventSource(url)
    _es = es

    es.addEventListener("thinking_start", (e) => {
      _addStage("thinking_start", JSON.parse(e.data))
    })

    es.addEventListener("sql_attempt", (e) => {
      const data = JSON.parse(e.data)
      attempts.value = data.attempt ?? 0
      model.value = data.model ?? null
      _addStage("sql_attempt", data, { hasContent: true })
    })

    es.addEventListener("model_token", (e) => {
      const data = JSON.parse(e.data)
      if (_activeStageIdx >= 0) {
        stages.value[_activeStageIdx].streamContent += data.token
      }
    })

    es.addEventListener("sql_generated", (e) => {
      const data = JSON.parse(e.data)
      sql.value = data.sql
      _addStage("sql_generated", data)
    })

    es.addEventListener("query_running", (e) => {
      _addStage("query_running", JSON.parse(e.data))
    })

    es.addEventListener("fallback_start", (e) => {
      _addStage("fallback_start", JSON.parse(e.data))
    })

    es.addEventListener("answer_start", (e) => {
      const data = JSON.parse(e.data)
      if (data.model) answerModel.value = data.model
      _addStage("answer_start", data, { hasContent: true })
    })

    es.addEventListener("answer_chunk", (e) => {
      const data = JSON.parse(e.data)
      answer.value += data.chunk
      if (_activeStageIdx >= 0) {
        stages.value[_activeStageIdx].streamContent += data.chunk
      }
    })

    es.addEventListener("done", (e) => {
      const data = JSON.parse(e.data)
      results.value = data.results ?? []
      rowCount.value = data.row_count ?? 0
      attempts.value = data.attempts ?? 0
      model.value = data.model ?? null
      if (!answer.value && data.answer) answer.value = data.answer
      _addStage("done", data)
      // Collapse all stages
      stages.value.forEach(s => { s.expanded = false; s.isActive = false })
      _activeStageIdx = -1
      phase.value = "done"
      es.close()
      _es = null
    })

    es.addEventListener("error", (e) => {
      let msg = "An error occurred."
      try {
        const data = JSON.parse(e.data)
        msg = data.message || msg
      } catch (_) {}
      error.value = msg
      _addStage("error", { message: msg })
      phase.value = "error"
      es.close()
      _es = null
    })

    // Network-level error (connection lost before done event)
    es.onerror = () => {
      if (phase.value === "streaming") {
        error.value = "Connection lost. Please try again."
        phase.value = "error"
        es.close()
        _es = null
      }
    }
  }

  return {
    phase: readonly(phase),
    stages: readonly(stages),
    sql: readonly(sql),
    answer: readonly(answer),
    results: readonly(results),
    rowCount: readonly(rowCount),
    attempts: readonly(attempts),
    model: readonly(model),
    answerModel: readonly(answerModel),
    error: readonly(error),
    submit,
    toggleStage,
  }
}
