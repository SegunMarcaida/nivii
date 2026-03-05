NL2SQL Confectionery Chatbot

A fully-local, Dockerized natural language to SQL system for confectionery retail POS data. Ask questions in plain English; the system generates SQL, runs it against a SQLite database, and returns a human-readable answer — all without any cloud API calls.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Endpoints](#endpoints)
3. [Tests](#tests)
4. [Data & Ingestion](#data--ingestion)
5. [Architectural Decisions](#architectural-decisions)
6. [Production Roadmap](#production-roadmap)

---

## Quick Start

### System Requirements

| Requirement | Minimum | Notes |
|---|---|---|
| **RAM** | 16 GB | Models load into Metal GPU memory (~8 GB) |
| **Disk** | ~10 GB free | ~8 GB for Ollama models + ~200 MB for Docker images + 5 MB SQLite DB |
| **Docker Desktop** | v4.x+ | [Download](https://www.docker.com/products/docker-desktop/) |
| **Ollama** | v0.3+ | [Download](https://ollama.com/download) |

### Step 1 — Install prerequisites (one-time)

```bash
# Docker Desktop — download and install from https://www.docker.com/products/docker-desktop/
# Then launch Docker Desktop from Applications.

# Ollama
brew install ollama
```

### Step 2 — Start Ollama on your host

```bash
ollama serve
```

> **Why on the host?** Docker Desktop on macOS runs containers inside a Linux VM that cannot access Apple Metal. Without Metal, inference takes 60–120s per query instead of 4–10s. See [details below](#why-ollama-runs-on-the-host-not-in-a-container-macos).

### Step 3 — Start all services

```bash
make start
```

This runs a prerequisite check, then `docker compose up --build`. On first run:
- **Ingestion pulls ~8 GB of models** (Arctic-Text2SQL-R1-7B, qwen2.5-coder:3b, llama3.2:1b, llama3.2:3b). This takes 5–15 minutes depending on internet speed.
- **CSV is loaded into SQLite** (~22k rows, takes a few seconds).
- Both steps are idempotent — subsequent starts skip them entirely.


Open the UI at **[http://localhost:3000](http://localhost:3000)**.

### Available Make targets

```
  make check             Verify all prerequisites (Docker, Ollama, data.csv)
  make start             Run checks, then start all services
  make stop              Stop all services
  make restart           Stop then start all services
  make logs              Tail logs from all services
  make test              Run unit tests (no Docker/Ollama needed)
  make test-integration  Run integration tests (requires stack running)
  make test-all          Run all tests
  make clean             Stop services and remove volumes
```

### Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `make check` says "Docker daemon NOT RUNNING" | Docker Desktop is not open | Launch Docker Desktop from Applications, wait for the whale icon to stop animating |
| Ingestion exits with "Ollama not reachable after 120s" | `ollama serve` was never started | Run `ollama serve` in a separate terminal, then `make start` again |
| First query takes 30+ seconds | Model cold-loading into Metal memory | Normal on first query after startup. Subsequent queries take 4–10s |
| `make test` fails with `ModuleNotFoundError` | Missing Python dependencies | Run `pip install -r api/requirements.txt` (or use a virtualenv) |
| Frontend shows "Network Error" | API not ready yet | Wait for `make logs-api` to show "Application startup complete" |
| Ingestion pulls models again after `make clean` | `make clean` removes Docker volumes | Expected — only use `make clean` when you want a full reset |

---

### Why Ollama runs on the host, not in a container (macOS)

When Docker Desktop runs on macOS, it actually runs all containers inside a lightweight Linux VM. That VM does not have access to **Apple Metal** — the framework Ollama uses to run inference on the Mac GPU (M1/M2/M3/M4).

The consequence is stark: without Metal, Ollama falls back to CPU-only inference. Arctic-Text2SQL-R1-7B on CPU takes **60–120 seconds per query** on a modern MacBook. On Metal it takes **4–10 seconds**. Running Ollama on the host and pointing the containers at `http://host.docker.internal:11434` is the only way to get usable performance on macOS.

### Running Ollama in Docker (Linux / Linux VMs)

On Linux, Docker can access NVIDIA GPUs via the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacuda/container-toolkit/install-guide.html) and the `--gpus all` flag. There is no Metal limitation. If you are on a Linux machine — including a Linux VM running on any hypervisor — you can run everything in Docker:

```bash
docker compose -f docker-compose.yml -f docker-compose.docker-ollama.yml up --build
```

The override file (`docker-compose.docker-ollama.yml`) adds an `ollama` service and rewires `OLLAMA_BASE_URL` from `host.docker.internal` to the container hostname. The `ingestion` service waits for Ollama to be ready before pulling models.

> **Note:** The Docker Ollama image runs CPU-only unless you add `deploy.resources.reservations.devices` (NVIDIA) or equivalent in the override. See the [Ollama Docker docs](https://hub.docker.com/r/ollama/ollama) for GPU passthrough configuration.

---

## Endpoints


| Service  | URL                                                      |
| -------- | -------------------------------------------------------- |
| Frontend | [http://localhost:3000](http://localhost:3000)           |
| API docs | [http://localhost:8000/docs](http://localhost:8000/docs) |
| Health   | `GET http://localhost:8000/health`                       |
| Query    | `POST http://localhost:8000/query`                       |
| Stream   | `GET http://localhost:8000/query/stream?q=...`           |


**Query request body:**

```json
{
  "question": "What were the top 5 products by revenue last month?",
  "debug": false
}
```

`debug: true` adds stage-level timing breakdowns (`stage_durations_ms`) and the SQL reasoning trace to the response.

---

## Tests

Unit tests run without Docker, without a database, without Ollama:

```bash
cd api
PYTHONPATH=. pytest tests/unit -q
```

Integration tests require the full stack (`docker compose up`):

```bash
PYTHONPATH=. pytest tests/integration -q
```

Integration tests auto-skip if the SQLite DB is unreachable, so running `pytest tests/` in CI without the stack is safe.

Or use the Makefile shortcuts:

```bash
make test              # unit tests only
make test-integration  # integration (requires stack running)
make test-all          # both (integration auto-skips if stack is down)
```

---

## Data & Ingestion

The ingestion pipeline (`ingestion/ingest.py`) runs once at startup, transforms `data.csv` into a flat SQLite `sales` table, and then exits. It is idempotent — if the table already has rows it does nothing.

### Pipeline steps

1. **Read CSV** — 24,212 raw rows with columns: `date`, `hour`, `ticket_number`, `waiter`, `product_name`, `quantity`, `unitary_price`.
2. **Deduplicate** — `drop_duplicates()` removes ~2,214 exact duplicate rows. These are a known POS export bug: when a ticket contains many zero-price sample lines the export system emits the same row multiple times. They are not real double-sales. Result: ~21,998 rows inserted.
3. **Normalize dates** — `date` column arrives as `MM/DD/YYYY`; converted to ISO `YYYY-MM-DD` so SQLite's `strftime()` and date comparison operators work correctly.
4. **Rename `hour` → `sale_hour`** — `hour` is a reserved word in several SQL dialects; renaming avoids accidental parser collisions.
5. **Derive and enrich columns** — all derived columns are computed at load time so the API never needs to compute them at query time:

| Derived column | Source | Logic |
|---|---|---|
| `ticket_type` | `ticket_number` prefix | `FCB`/`FCA` = forward sales; `NCB`/`NCA` = credit notes |
| `waiter_name` | `waiter` integer | Lookup table; `0` → `"Desconocido"` (see below) |
| `total` | `quantity × unitary_price` | Pre-computed to avoid per-query arithmetic |
| `is_credit_note` | `ticket_type` | `1` if type is `NCB` or `NCA`, else `0` |
| `is_promotional` | `unitary_price` | `1` if price is `0.0`, else `0` |
| `is_manual_adj` | `product_name` | `1` if product is `ART. INEXISTENTE`, else `0` |
| `sale_month` | `sale_date` | First 7 chars: `"YYYY-MM"` — avoids `strftime` on every GROUP BY |
| `ticket_series` | `ticket_number` | Terminal ID extracted from `"FCB 0003-…"` → `3` |
| `product_category` | `product_name` | Hand-mapped dict with 100% coverage across all 68 products |
| `product_unit` | `product_name` | Packaging unit (`1u`, `6u`, `80g`, etc.) from same dict |

6. **Insert in chunks of 1,000 rows** using `executemany` inside a single transaction for performance.
7. **Pull Ollama models** — Arctic-Text2SQL-R1-7B, qwen2.5-coder:3b, llama3.2:1b, llama3.2:3b are pulled after the DB load completes. Already-present models are skipped.

### Business decisions & anomaly handling

**`waiter = 0` → `"Desconocido"`**
Waiter ID `0` is not a real employee — it represents an anonymous POS session opened without logging in (e.g., a self-service kiosk or a manager override). It is stored as `waiter_name = 'Desconocido'` rather than discarded, so sales attributed to it are visible in queries. Only `waiter_name` is excluded from the prompt DDL (the LLM sees the integer `waiter` column only) because the lookup table is not exposed to the model.

**Credit notes kept (negative quantities)**
Rows with `ticket_type` in `(NCB, NCA)` represent returns. They have negative `quantity` values and are valid accounting records, not data errors. They are kept and flagged with `is_credit_note = 1`. Revenue queries must filter `WHERE is_credit_note = 0` to avoid under-counting; this rule is included in the prompt's BUSINESS_RULES block.

**Zero-price rows kept (promotional tastings)**
Rows with `unitary_price = 0.0` are product samples handed out at the counter. They represent real inventory movement but no revenue. They are kept and flagged with `is_promotional = 1`. All revenue aggregations filter `WHERE is_promotional = 0`.

**`ART. INEXISTENTE` kept (manual adjustments)**
This is a cashier-entered placeholder product used when applying a manual price adjustment without a real SKU. It has no meaningful `product_name` or category. It is kept with `is_manual_adj = 1` and `product_category = 'Ajuste Manual'` so adjustment totals are visible. Revenue queries filter `WHERE is_manual_adj = 0`.

**Fractional quantity `0.5` kept**
Some products are sold by the half-box. `0.5` is a valid business value, not a data error. The `quantity` column is stored as `REAL` (not integer) to accommodate it.

**Product category mapping is exhaustive**
The `PRODUCT_CATEGORY` dict in `ingest.py` maps all 68 distinct product names to one of 10 categories. If any unmapped product appears in the CSV, the script raises `ValueError` and refuses to load. This is a hard safety check — a partial mapping would silently produce `NULL` category values that break category-level queries.

---

## Architectural Decisions

Every decision below was made in the context of this specific system: a read-only analytics chatbot over a static 24k-row POS dataset, running entirely on a developer laptop. 

---

### 1. SQLite + aiosqlite, not PostgreSQL

The entire dataset is a single CSV that becomes a read-only analytical store after ingestion. SQLite's file-based model maps directly to a shared Docker volume — no separate DB container, no network hop, no connection pooling configuration. `aiosqlite` provides non-blocking reads that integrate cleanly with FastAPI's async request handling.

The realistic alternative was PostgreSQL, which has proper window functions, MVCC, and concurrent writer support. But this database has exactly one writer (the ingestion script, which runs once) and one reader (the API). PostgreSQL would add a container, a TCP connection pool, a healthcheck dependency, and ~200 MB of additional memory for zero functional gain. `EXPLAIN QUERY PLAN` — used for SQL validation in this system — works identically in both engines.

One additional factor: Arctic-Text2SQL-R1-7B is fine-tuned on SQLite dialect. Its training examples use `strftime()`, `SUBSTR()`, and the SQLite stddev workaround. Targeting SQLite means the model's training distribution matches our runtime, which measurably improves first-attempt SQL accuracy.

---

### 2. Flat `sales` table, no normalization

The data could be normalized into products, categories, ticket headers, and waiter tables. We explicitly chose not to.

LLM-generated SQL accuracy degrades with JOINs. Every join is a new opportunity for the model to hallucinate an alias, get a foreign key direction wrong, or produce a Cartesian product. With a flat schema, every analytical question in this domain — top products by revenue, average ticket by day of week, month-over-month growth — is answerable with a single-table query. This significantly reduces the failure surface.

The cost is data redundancy: `product_name` and `category` are repeated across every row. At 22k rows and ~5 MB of SQLite storage, this is immaterial.

---

### 3. Arctic-Text2SQL-R1-7B as the SQL generator

This model ([arXiv:2505.20315](https://arxiv.org/abs/2505.20315)) is fine-tuned specifically for text-to-SQL on 29 public benchmarks. The critical property is its output format: every response wraps reasoning in `<think>...</think>` and SQL in `<answer>```sql...```</answer>`. This structure makes extraction deterministic — there is no ambiguity about where the SQL is. General-purpose 7B models (Mistral, Llama 3) produce inconsistent formatting: sometimes SQL appears inline, sometimes in a code block, sometimes bare. Each format requires a separate extraction heuristic, and heuristics fail on edge cases.

`qwen2.5-coder:7b` and `CodeLlama` were also evaluated — both produce valid SQL for simple queries but fail more frequently on the dataset-specific patterns (strftime grouping, SUBSTR hour extraction) that Arctic handles in its reasoning trace.

---

### 4. Complexity-based routing (SIMPLE → Qwen-3B first, HARD → Arctic direct)

`qwen2.5-coder:3b` is a 2 GB model that generates tokens roughly 2× faster than the 7B Arctic model. For simple aggregations — `COUNT(*)`, `SUM(total) WHERE product = ?`, basic `GROUP BY` — it produces correct SQL on the first attempt and returns in 2–4 seconds. Routing these questions to Arctic would be wasted capacity.

HARD queries are a different story. Questions requiring window functions (`ROW_NUMBER OVER PARTITION BY`), the SQLite stddev workaround (`SQRT(AVG(x*x) - AVG(x)*AVG(x))`), `strftime()` date grouping, period-over-period self-joins, or anti-joins (`WHERE x NOT IN (...)`) fail consistently on Qwen-3B in testing. Sending HARD queries through the Qwen path first wastes one full inference cycle (~3 seconds) before escalating to Arctic. The routing logic skips that wasted attempt by sending HARD queries directly to Arctic.

The classifier is a set of regex patterns (`_HARD_INTENT_PATTERNS` in `api/app/prompts/few_shot.py`) matched against a normalized question string. This is a heuristic, not a classifier. A misclassified question costs one extra attempt. We accept this because the patterns are grounded in observed failures during development — "monthly grouping", "busiest hour", "growth rate", "never sold" were all tested and confirmed to fail on Qwen before the routing was added. The alternative (a learned classifier) would require labeled data we don't have and would add latency on every request.

---

### 5. llama3.2:1b and llama3.2:3b for answer generation, not Arctic

Arctic is a SQL specialist. Its training optimizes for generating structured query text — not for writing "Total revenue in Q3 was $47,293, up 12% from Q2." Using Arctic for answer generation produces SQL-flavored or overly terse prose. Small general-purpose models (llama3.2:1b, 1.3 GB) are well-suited to this task.

The answer service (`api/app/services/answer.py`) classifies query results by shape before choosing a model. A scalar result (one row, one column) goes to the 1B model with a 40-token budget — no reasoning required, just format the number. A complex table (many rows, many columns) goes to the 3B model with a 120-token budget — more context is needed to produce a coherent summary. This prevents wasting 3B capacity on "how many products do we sell?" answers.

The service has a three-level fallback: 3B model → 1B model → deterministic `"Found N result(s)."` string. This ensures the answer step never crashes the API response, even if both Ollama models are unavailable.

---

### 6. `EXPLAIN QUERY PLAN` for SQL validation, not execution

The Arctic model occasionally produces syntactically invalid SQL: unclosed parentheses, references to columns that don't exist, malformed `strftime` calls. We need to detect these failures before executing the query against the database.

`EXPLAIN QUERY PLAN {sql}` in SQLite validates syntax and schema references without executing the query. There are no side effects, no savepoints needed, no temporary writes. This is cheaper and simpler than a `BEGIN; SELECT ... LIMIT 0; ROLLBACK` dry-run, which would require savepoint logic and an additional async DB call per retry.

The limitation is deliberate: EXPLAIN does not catch semantic errors. A query with `WHERE is_promotional = 'false'` (string instead of integer) passes EXPLAIN and returns 0 rows silently. This is handled at a different layer: the correction prompt (attempts 2 and 3) receives the previous SQL, the error message or empty result, and explicit feedback asking the model to revise. The validation step catches hard failures; the correction prompt handles soft failures.

---

### 7. SSE streaming for the query pipeline

Arctic with chain-of-thought reasoning generates 200–800 thinking tokens before producing the SQL. Without streaming, the frontend shows a spinner for 5–15 seconds with no feedback. This is a bad experience for a chatbot.

The SSE endpoint emits named events at each pipeline stage: `thinking_start` (model begins CoT), `model_token` (each streamed token), `sql_attempt` (attempt number), `sql_generated` (valid SQL found), `query_running` (DB execution started), `answer_start`, `answer_chunk`, `done`. The frontend renders the thinking trace and SQL in real time, so users see progress immediately.

The trade-off is complexity. The SSE endpoint (`/query/stream`) is harder to test than a plain POST — it requires an async event consumer on the client and careful error propagation through the stream. We kept the non-streaming `POST /query` endpoint for integration tests, API clients, and debugging with `curl`.

---

### 8. Business rules and few-shot examples inside the Arctic prompt

Arctic is trained on public SQL benchmarks (Spider, WikiSQL, BIRD, etc.). None of those benchmarks include the specific quirks of this POS dataset:

- The `hour` column stores `"HH:MM"` strings — not integers. Queries for time-of-day analysis must use `CAST(SUBSTR(hour, 1, 2) AS INTEGER)`. Without this rule, the model generates `WHERE hour > 12` which silently fails on string comparison.
- Revenue calculations require filtering three boolean flags: `WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0`. Without this, aggregate queries overcount by including refunds and promotional giveaways.
- SQLite has no `STDDEV()` function. Standard deviation questions require the manual formula: `SQRT(AVG(unit_price * unit_price) - AVG(unit_price) * AVG(unit_price))`. Arctic generates `STDDEV(unit_price)` on its first attempt without the rule.
- Month-over-month growth requires a self-join pairing each month's revenue with the next month's, using `strftime('%Y-%m', sale_month || '-01', '+1 month')` arithmetic. This pattern is not in the model's training distribution.

The five few-shot examples in the prompt are targeted at exactly these patterns. The trade-off is prompt length: the full DDL + business rules + examples adds ~1,500 tokens per request. At local inference with no per-token cost, this is irrelevant. The gain is that these examples convert multi-retry questions into first-attempt successes for the specific patterns that fail without them.

---

### 9. One-shot ingestion service, not a migration framework

The dataset is a static CSV. There are no schema migrations, no incremental loads, no multi-environment deployments. A single Python script that checks `SELECT COUNT(*) FROM sales`, exits early if data exists, or runs the full CSV-to-SQLite transform otherwise — is the appropriate tool.

Alembic (or any migration framework) would add a config file, a versions directory, upgrade/downgrade commands, and a migration state table. For a dataset that never changes, this is pure overhead. The ingestion container exits after completion; `depends_on: condition: service_completed_successfully` in the compose file ensures the API does not start until both the data and the models are ready.

---

## Production Roadmap

The current system is designed for a single developer on a laptop. The following outlines what would need to change to serve real users against a live POS feed. This is not theoretical — each item addresses a specific limitation of the current architecture.

---

### Before any real traffic

**Replace SQLite with PostgreSQL.** SQLite is a file-based, single-writer database. Multiple API replicas cannot safely read from the same SQLite file over a network volume. PostgreSQL supports concurrent connections, proper MVCC, and extensions like `pgvector` (needed for semantic caching). The schema migration is straightforward — the flat `sales` table translates directly. The query dialect changes: `strftime()` becomes `to_char()`, and the SQLite stddev workaround becomes `STDDEV()`. Both the few-shot examples and the BUSINESS_RULES constant in `few_shot.py` need updates for PostgreSQL dialect.

**Replace Ollama with vLLM or Text Generation Inference (TGI).** Ollama processes one request at a time — it has no batching. Two concurrent users means one waits for the other's full inference to finish. vLLM implements continuous batching: multiple requests share GPU memory across a single forward pass. Deploy Arctic and the llama3.2 answer models on a dedicated GPU node; the API containers become fully stateless and horizontally scalable.

**Add authentication.** The `/query` endpoint is currently open. Before any external exposure, add JWT middleware (FastAPI's `Depends` pattern with `python-jose`) and tie rate limits to API keys.

---

### Short-term (production baseline)

**Semantic query cache.** Many users ask the same question with minor phrasing variations ("revenue last month" vs "total sales in the previous month"). Embed each incoming question with a small embedding model, store `(embedding, sql, result)` in Redis, and check cosine similarity against the cache before hitting Ollama. A similarity threshold of 0.95 returns cached results in under 50 ms. This is the single highest-leverage latency improvement available.

**Read-only database user for SQL execution.** The API currently uses a full-access SQLite connection. In production, the API should connect to PostgreSQL as a role with `SELECT`-only privileges on the `sales` table. Arctic occasionally generates `UPDATE` or `DELETE` statements when confused — a read-only role makes these fail immediately rather than causing data loss. This is not a substitute for input validation but adds meaningful defense in depth.

**Rate limiting.** Protect against runaway clients and accidental infinite loops. `slowapi` integrates directly with FastAPI and supports per-IP and per-API-key limits. Alternatively, push rate limiting to an API gateway (Kong, Nginx) if one already exists in the infrastructure.

**Structured JSON logging.** The current `logging.info` calls emit unstructured text. Production observability requires structured logs with consistent fields: `trace_id`, `question_hash`, `model`, `complexity`, `attempts`, `fallback`, `latency_ms`, `row_count`. These feed into a log aggregation platform (Loki, Elasticsearch) for dashboards and alerting. The `trace_id` field already exists in query responses — extending it to logs enables end-to-end request tracing.

---

### Medium-term (scale and reliability)

**Horizontal API scaling.** Once Ollama is replaced with a remote inference server and SQLite is replaced with PostgreSQL, the FastAPI service is stateless. Run 3–5 replicas behind a load balancer (Kubernetes Deployment + ClusterIP Service). The model warmup that currently happens at API startup becomes a readiness probe — a pod is not marked ready until it can reach the inference server and the database.

**Query feedback loop and model fine-tuning.** Log every question, the generated SQL, and whether the user accepted or re-asked. After collecting a few hundred labeled examples specific to this dataset, fine-tune Arctic (or a smaller SQL model like `defog/sqlcoder-7b`) on the domain-specific patterns: `SUBSTR(hour, 1, 2)`, the three-flag revenue filter, the SQLite stddev formula. A fine-tuned model eliminates the dependency on the large few-shot prompt — reducing both prompt token cost and the chance of prompt injection through crafted questions.

**Model versioning and shadow evaluation.** When deploying a new model version or prompt revision, route a percentage of real traffic to the new version and compare metrics: first-attempt success rate, correction rate, fallback rate, mean latency. The golden query fixture (`api/tests/fixtures/golden_queries_coverage_v2.json`) provides a baseline — run it against both model versions and block deployment if accuracy regresses.

**CI/CD pipeline.** The 215 unit tests run in under 10 seconds with no external dependencies. Add a GitHub Actions workflow that runs `pytest tests/unit` on every pull request and the golden query evaluation on merge to main. Block merges where the golden query pass rate drops below the established threshold.

---

### Long-term (enterprise)

**Multi-tenant schema isolation.** Different business units (store chains, franchisees) cannot see each other's data. In PostgreSQL, implement row-level security policies tied to a tenant ID. The LLM prompt includes only the columns and value ranges the tenant's role can access — a `WHERE tenant_id = ?` predicate is injected into every generated query before execution, regardless of what the model produced.

**Human-in-the-loop review for anomalous queries.** Some questions have high business impact: "delete all transactions before 2024", "show all employee IDs and their sales", queries referencing PII-adjacent columns. Flag these for async human review before returning results. A simple rule engine on the generated SQL (checking for `DROP`, `DELETE`, `UPDATE`, or enumeration of sensitive columns) can route them to a review queue with minimal latency impact on normal queries.

**Incremental ingestion from a live POS feed.** The current ingestion model (one-shot CSV load) is not suitable for a live system. Replace it with a change-data-capture pipeline: a Debezium connector reads the POS system's database binlog, publishes events to Kafka, and an ingestion worker applies upserts to the analytics table. This requires the move to PostgreSQL (SQLite has no WAL-based CDC). The flat schema design remains valid — denormalized tables are standard practice in analytical pipelines.
