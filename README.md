# nivii-challenge

Local NL2SQL system for confectionery sales data.

## Stack

- `ingestion`: loads `data.csv` into SQLite (`/app/data/confectionery.db`) and ensures Ollama models are available
- `api`: FastAPI service (`/query`, `/query/stream`, `/health`)
- `frontend`: Vue 3 + Vite UI

## Runtime Architecture

- Storage: SQLite (`sqlite+aiosqlite`)
- Models: Ollama (`qwen2.5-coder:3b` + Arctic fallback + answer models)
- API query flow:
  1. on-topic guard
  2. SQL generation with retries/fallback
  3. `EXPLAIN QUERY PLAN` validation
  4. SQL execution
  5. answer generation

## Prerequisites

- Docker / Docker Compose
- `data.csv` in project root
- Ollama running (recommended on host for macOS performance):

```bash
ollama serve
```

## Run

### Standard (host Ollama)

```bash
docker compose up --build
```

This uses `OLLAMA_BASE_URL=http://host.docker.internal:11434` from containers.

### Full Docker mode (Ollama container)

```bash
docker compose -f docker-compose.yml -f docker-compose.docker-ollama.yml up --build
```

## Endpoints

- Frontend: `http://localhost:3000`
- API docs: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`
- Query: `POST http://localhost:8000/query`
- Stream: `GET http://localhost:8000/query/stream?q=...`

## Tests

From `api/`:

```bash
PYTHONPATH=. pytest tests/unit -q
```

Integration tests require:

- SQLite DB populated by ingestion
- Ollama reachable at `localhost:11434`

```bash
PYTHONPATH=. pytest tests/integration -q
```

## Notes

- `debug=true` on query requests enables additional diagnostics fields.
- The SSE frontend currently consumes: `thinking_start`, `sql_attempt`, `model_token`, `sql_generated`, `query_running`, `fallback_start`, `answer_start`, `answer_chunk`, `done`, `error`.
