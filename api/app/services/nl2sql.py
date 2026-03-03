"""
nl2sql.py — Arctic-Text2SQL-R1-7B generation pipeline for the flat SQLite sales table.

Simplified single-model loop:
  1. Build Arctic prompt
  2. Call Arctic-Text2SQL-R1-7B via Ollama
  3. Extract SQL from <answer> tag (or code block fallback)
  4. Validate with EXPLAIN QUERY PLAN
  5. Execute if valid; build correction prompt and retry if not (up to 3 attempts)

No deterministic compiler stages, no multi-model routing, no alias validation.
SQLite EXPLAIN QUERY PLAN replaces PostgreSQL EXPLAIN.
"""

import logging
import os
import time
import uuid
from collections.abc import Awaitable, Callable

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.prompts.few_shot import (
    build_arctic_correction_prompt,
    build_arctic_prompt,
    build_qwen_correction_prompt,
    build_qwen_prompt,
    classify_complexity,
    extract_sql,
    is_on_topic,
    QueryComplexity,
)

OnEvent = Callable[[str, dict], Awaitable[None]] | None

log = logging.getLogger(__name__)

OLLAMA_BASE_URL  = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL     = os.environ.get("OLLAMA_MODEL", "a-kore/Arctic-Text2SQL-R1-7B")
OLLAMA_MODEL_BASE = os.environ.get("OLLAMA_MODEL_BASE", "qwen2.5-coder:3b")
BASE_ATTEMPTS        = 3   # 3B model gets this many attempts before Arctic escalation
ARCTIC_ATTEMPTS      = 1   # Arctic attempts after 3B exhausted (SIMPLE path)
MAX_ATTEMPTS         = BASE_ATTEMPTS + ARCTIC_ATTEMPTS  # 4 total (SIMPLE path)
ARCTIC_DIRECT_ATTEMPTS = 3  # Arctic attempts when HARD complexity is detected

# Per-model generation options injected into Ollama /api/generate.
# Arctic needs 2048 num_predict for CoT reasoning; Qwen only needs ~512 for plain SQL.
# Qwen stop sequences prevent document-continuation after the first ```sql block closes.
_MODEL_OPTIONS: dict[str, dict] = {
    OLLAMA_MODEL: {
        "num_ctx": 4096,
        "num_predict": 2048,
    },
    OLLAMA_MODEL_BASE: {
        "num_ctx": 4096,
        "num_predict": 512,
        "stop": ["\n```"],  # stop when code block closes — prevents Q+SQL continuation loop
    },
}

# Persistent HTTP client — reuse TCP connections across all Ollama calls.
_ollama_client: httpx.AsyncClient | None = None


def _get_ollama_client() -> httpx.AsyncClient:
    global _ollama_client
    if _ollama_client is None:
        _ollama_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
    return _ollama_client


async def close_ollama_client() -> None:
    global _ollama_client
    if _ollama_client is not None:
        await _ollama_client.aclose()
        _ollama_client = None


async def _call_ollama(
    prompt: str,
    model: str = OLLAMA_MODEL,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    """Send a prompt to Ollama using streaming and return the accumulated response."""
    import json as _json

    log.debug("Ollama prompt (%d chars, model=%s):\n%s", len(prompt), model, prompt)

    tokens: list[str] = []
    token_count = 0
    t_start = time.perf_counter()
    t_first_token: float | None = None

    client = _get_ollama_client()
    async with client.stream(
        "POST",
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": model,
            "prompt": prompt,
            "stream": True,
            "raw": True,
            "options": {
                "temperature": 0.0,
                "seed": 42,
                **_MODEL_OPTIONS.get(model, {"num_ctx": 4096, "num_predict": 2048}),
            },
        },
    ) as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if not line:
                continue
            try:
                chunk = _json.loads(line)
            except _json.JSONDecodeError as exc:
                log.debug("JSON decode error on chunk %r: %s", line, exc)
                continue

            token = chunk.get("response", "")
            if token:
                if t_first_token is None:
                    t_first_token = time.perf_counter()
                    log.debug("First token received (TTFT=%.2fs)", t_first_token - t_start)
                token_count += 1
                tokens.append(token)
                if on_token is not None:
                    await on_token(token)

            if chunk.get("done", False):
                elapsed = time.perf_counter() - t_start
                log.debug(
                    "Stream done. tokens=%d total_time=%.2fs done_reason=%s",
                    token_count, elapsed, chunk.get("done_reason"),
                )
                break

    full_response = "".join(tokens)
    log.debug("Full raw response (%d chars):\n%s", len(full_response), full_response)
    return full_response


async def _explain_sql(sql: str, session: AsyncSession) -> str | None:
    """
    Validate SQL using SQLite EXPLAIN QUERY PLAN.
    Returns None if valid, or the error message string if invalid.
    SQLite EXPLAIN QUERY PLAN does not execute the query — no savepoint needed.
    """
    try:
        await session.execute(text(f"EXPLAIN QUERY PLAN {sql}"))
        return None
    except Exception as exc:
        return str(exc)


async def generate_sql(
    question: str,
    db_session: AsyncSession,
    on_event: OnEvent = None,
    debug: bool = False,
) -> dict:
    """
    Generate SQL for a natural language question using OmniSQL-7B.

    Runs up to MAX_ATTEMPTS cycles of:
      generate → validate (EXPLAIN QUERY PLAN) → execute or retry with correction
    """
    log.info("Query received: %s", question)

    if not is_on_topic(question):
        raise ValueError(
            "This assistant only answers questions about sales data, products, revenue, and waiters. "
            "Please ask something related to the store's POS records."
        )

    complexity = classify_complexity(question)
    # HARD → Arctic directly; SIMPLE → Qwen-first with Arctic fallback.
    trace_id = str(uuid.uuid4())
    t_pipeline_start = time.perf_counter()

    last_sql = ""
    last_error = ""

    async with db_session.begin():
        if on_event:
            await on_event("thinking_start", {
                "message": "Generating SQL for your question...",
                "complexity": complexity.value,
                **({"trace_id": trace_id} if debug else {}),
            })

        max_attempts = ARCTIC_DIRECT_ATTEMPTS if complexity == QueryComplexity.HARD else MAX_ATTEMPTS

        for attempt in range(1, max_attempts + 1):
            if complexity == QueryComplexity.HARD:
                # HARD: Arctic from the first attempt — skip Qwen entirely.
                sql_model = OLLAMA_MODEL
                if attempt == 1:
                    prompt = build_arctic_prompt(question)
                    log.info("Attempt %d: fresh Arctic prompt (HARD routing, model=%s)", attempt, sql_model)
                else:
                    if on_event:
                        await on_event("fallback_start", {
                            "attempt": attempt,
                            "model": sql_model,
                            "reason": last_error,
                        })
                    prompt = build_arctic_correction_prompt(question, last_sql, last_error)
                    log.info("Attempt %d: Arctic correction (HARD, model=%s, last_error=%s)", attempt, sql_model, last_error)
            else:
                # SIMPLE: Qwen for attempts 1-3, Arctic fallback for attempt 4+.
                sql_model = OLLAMA_MODEL_BASE if attempt <= BASE_ATTEMPTS else OLLAMA_MODEL
                if attempt == 1:
                    prompt = build_qwen_prompt(question)
                    log.info("Attempt %d: fresh Qwen prompt (model=%s)", attempt, sql_model)
                elif attempt <= BASE_ATTEMPTS:
                    if on_event:
                        await on_event("fallback_start", {
                            "attempt": attempt,
                            "model": sql_model,
                            "reason": last_error,
                        })
                    prompt = build_qwen_correction_prompt(question, last_sql, last_error)
                    log.info("Attempt %d: Qwen correction (model=%s, last_error=%s)", attempt, sql_model, last_error)
                else:
                    if on_event:
                        await on_event("fallback_start", {
                            "attempt": attempt,
                            "model": sql_model,
                            "reason": f"3B model exhausted {BASE_ATTEMPTS} attempts. Last error: {last_error}",
                        })
                    prompt = build_arctic_correction_prompt(question, last_sql, last_error)
                    log.info("Attempt %d: Arctic correction (SIMPLE fallback, model=%s, last_error=%s)", attempt, sql_model, last_error)

            if on_event:
                await on_event("sql_attempt", {"attempt": attempt, "model": sql_model})

            async def _token_cb(t: str) -> None:
                if on_event:
                    await on_event("model_token", {"token": t})

            t_llm_start = time.perf_counter()
            try:
                raw_response = await _call_ollama(
                    prompt,
                    model=sql_model,
                    on_token=_token_cb if on_event else None,
                )
            except Exception as ollama_exc:
                elapsed = time.perf_counter() - t_llm_start
                log.warning(
                    "Ollama call failed (attempt %d, %.1fs): %s: %s",
                    attempt, elapsed, type(ollama_exc).__name__, ollama_exc,
                )
                last_error = f"{type(ollama_exc).__name__}: {ollama_exc}"
                if attempt == max_attempts:
                    break
                continue

            elapsed = time.perf_counter() - t_llm_start
            log.info(
                "Ollama responded (attempt %d, %.1fs). Raw output (first 300): %r",
                attempt, elapsed, raw_response[:300],
            )

            last_sql = extract_sql(raw_response)
            log.info("SQL after extraction (attempt %d):\n%s", attempt, last_sql)

            if on_event:
                await on_event("sql_generated", {
                    "sql": last_sql,
                    "attempt": attempt,
                    "model": sql_model,
                })

            last_error = await _explain_sql(last_sql, db_session)
            if last_error is None:
                log.info("EXPLAIN QUERY PLAN succeeded on attempt %d.", attempt)
                if on_event:
                    await on_event("query_running", {"message": "Executing SQL query..."})

                result_proxy = await db_session.execute(text(last_sql))
                rows = result_proxy.fetchall()
                results = [dict(row._mapping) for row in rows]

                pipeline_ms = int((time.perf_counter() - t_pipeline_start) * 1000)
                response: dict = {
                    "question": question,
                    "sql": last_sql,
                    "results": results,
                    "row_count": len(results),
                    "attempts": attempt,
                    "model": sql_model,
                    "complexity": complexity.value,
                    "fallback": complexity == QueryComplexity.SIMPLE and sql_model == OLLAMA_MODEL,
                }
                if debug:
                    response.update({
                        "trace_id": trace_id,
                        "status": "success",
                        "latency_ms": pipeline_ms,
                        "guardrail_events": [],
                        "validation_summary": {},
                        "stage_durations_ms": {},
                    })
                return response

            log.warning(
                "EXPLAIN failed (attempt %d). SQL:\n%s\nError: %s",
                attempt, last_sql, last_error,
            )

    attempts_desc = (
        f"{ARCTIC_DIRECT_ATTEMPTS} Arctic attempt(s) (HARD routing)"
        if complexity == QueryComplexity.HARD
        else f"{BASE_ATTEMPTS} Qwen attempt(s) + {ARCTIC_ATTEMPTS} Arctic fallback attempt(s)"
    )
    raise RuntimeError(
        f"SQL generation failed after {attempts_desc}. "
        f"Last error: {last_error}\nLast SQL: {last_sql}"
    )
