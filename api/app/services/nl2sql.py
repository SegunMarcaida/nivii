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
import time
import uuid
from collections.abc import Awaitable, Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import (
    ARCTIC_ATTEMPTS,
    ARCTIC_DIRECT_ATTEMPTS,
    BASE_ATTEMPTS,
    MAX_ATTEMPTS,
    OLLAMA_MODEL,
    OLLAMA_MODEL_BASE,
)
from app.prompts.builders import (
    build_arctic_correction_prompt,
    build_arctic_prompt,
    build_qwen_correction_prompt,
    build_qwen_plan_prompt,
    build_qwen_prompt,
)
from app.prompts.classification import classify_complexity, is_on_topic, QueryComplexity
from app.prompts.parsers import extract_sql
from app.services.ollama_client import (
    call_ollama_generate as _call_ollama,
    close_clients as close_ollama_client,
)

OnEvent = Callable[[str, dict], Awaitable[None]] | None

log = logging.getLogger(__name__)


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


def _guard_on_topic(question: str) -> None:
    """Raise ValueError for off-topic questions before any DB or LLM call."""
    if not is_on_topic(question):
        raise ValueError(
            "This assistant only answers questions about sales data, products, revenue, and waiters. "
            "Please ask something related to the store's POS records."
        )


async def _maybe_plan(question: str, complexity: QueryComplexity) -> str:
    """Run the lightweight Qwen planning call for SIMPLE questions.

    Returns a SQL-comment plan string, or "" if HARD or if the call fails.
    """
    if complexity == QueryComplexity.HARD:
        return ""
    plan_prompt = build_qwen_plan_prompt(question)
    try:
        plan_response = await _call_ollama(
            plan_prompt,
            model=OLLAMA_MODEL_BASE,
            on_token=None,  # planning is silent — no SSE streaming
            options_override={"num_predict": 80, "stop": ["SELECT", "\n\n"]},
        )
        raw_plan = ("-- Columns needed:" + plan_response).strip()
        if raw_plan:
            log.info("Qwen plan:\n%s", raw_plan)
            return raw_plan
    except Exception as plan_exc:
        log.warning(
            "Qwen planning call failed (non-fatal, continuing without plan): %s",
            plan_exc,
        )
    return ""


def _select_prompt(
    question: str,
    complexity: QueryComplexity,
    attempt: int,
    plan: str,
    last_sql: str,
    last_error: str,
) -> tuple[str, str]:
    """Pure function: return (prompt, model) for the current attempt.

    Encapsulates all branching logic for HARD vs SIMPLE routing and
    fresh vs correction prompts.
    """
    if complexity == QueryComplexity.HARD:
        model = OLLAMA_MODEL
        if attempt == 1 or not last_sql:
            return build_arctic_prompt(question), model
        return build_arctic_correction_prompt(question, last_sql, last_error), model

    # SIMPLE path
    model = OLLAMA_MODEL_BASE if attempt <= BASE_ATTEMPTS else OLLAMA_MODEL
    if attempt == 1 or not last_sql:
        if attempt <= BASE_ATTEMPTS:
            return build_qwen_prompt(question, plan=plan), model
        return build_arctic_prompt(question), model
    if attempt <= BASE_ATTEMPTS:
        return build_qwen_correction_prompt(question, last_sql, last_error), model
    return build_arctic_correction_prompt(question, last_sql, last_error), model


async def _retry_loop(
    question: str,
    complexity: QueryComplexity,
    plan: str,
    max_attempts: int,
    session: AsyncSession,
    on_event: OnEvent,
    trace_id: str,
    debug: bool,
) -> tuple[dict | None, str, str]:
    """Attempt loop: generate → extract → validate → execute, up to max_attempts.

    Returns (result_dict, last_sql, last_error).
    result_dict is None if all attempts are exhausted.
    """
    last_sql = ""
    last_error = ""
    t_pipeline_start = time.perf_counter()

    if on_event:
        await on_event("thinking_start", {
            "message": "Generating SQL for your question...",
            "complexity": complexity.value,
            **({"trace_id": trace_id} if debug else {}),
        })

    for attempt in range(1, max_attempts + 1):
        prompt, sql_model = _select_prompt(
            question, complexity, attempt, plan, last_sql, last_error
        )

        if on_event and attempt > 1 and last_sql:
            reason = (
                f"3B model exhausted {BASE_ATTEMPTS} attempts. Last error: {last_error}"
                if complexity == QueryComplexity.SIMPLE and attempt > BASE_ATTEMPTS
                else last_error
            )
            await on_event("fallback_start", {
                "attempt": attempt,
                "model": sql_model,
                "reason": reason,
            })

        log.info(
            "Attempt %d: model=%s complexity=%s",
            attempt, sql_model, complexity.value,
        )

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

        if not raw_response or not raw_response.strip():
            log.warning(
                "Empty response from model on attempt %d (model=%s). "
                "Will retry with original prompt.",
                attempt, sql_model,
            )
            last_error = "Model returned empty response"
            last_sql = ""
            continue

        last_sql = extract_sql(raw_response)
        log.info("SQL after extraction (attempt %d):\n%s", attempt, last_sql)

        if on_event:
            await on_event("sql_generated", {
                "sql": last_sql,
                "attempt": attempt,
                "model": sql_model,
            })

        last_error = await _explain_sql(last_sql, session)
        if last_error is None:
            log.info("EXPLAIN QUERY PLAN succeeded on attempt %d.", attempt)
            if on_event:
                await on_event("query_running", {"message": "Executing SQL query..."})

            result_proxy = await session.execute(text(last_sql))
            rows = result_proxy.fetchall()
            results = [dict(row._mapping) for row in rows]
            pipeline_ms = int((time.perf_counter() - t_pipeline_start) * 1000)

            return (
                _build_response(
                    question, last_sql, results, complexity, sql_model,
                    attempt, trace_id, pipeline_ms, debug,
                ),
                last_sql,
                "",
            )

        log.warning(
            "EXPLAIN failed (attempt %d). SQL:\n%s\nError: %s",
            attempt, last_sql, last_error,
        )

    return None, last_sql, last_error


def _build_response(
    question: str,
    sql: str,
    results: list,
    complexity: QueryComplexity,
    model: str,
    attempts: int,
    trace_id: str,
    pipeline_ms: int,
    debug: bool,
) -> dict:
    """Assemble the final result dict from a successful SQL execution."""
    response: dict = {
        "question": question,
        "sql": sql,
        "results": results,
        "row_count": len(results),
        "attempts": attempts,
        "model": model,
        "complexity": complexity.value,
        "fallback": complexity == QueryComplexity.SIMPLE and model == OLLAMA_MODEL,
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


async def generate_sql(
    question: str,
    db_session: AsyncSession,
    on_event: OnEvent = None,
    debug: bool = False,
) -> dict:
    """
    Generate SQL for a natural language question using Arctic-Text2SQL-R1-7B.

    Runs up to MAX_ATTEMPTS cycles of:
      generate → validate (EXPLAIN QUERY PLAN) → execute or retry with correction
    """
    log.info("Query received: %s", question)

    _guard_on_topic(question)

    complexity = classify_complexity(question)
    trace_id = str(uuid.uuid4())
    plan = await _maybe_plan(question, complexity)

    max_attempts = ARCTIC_DIRECT_ATTEMPTS if complexity == QueryComplexity.HARD else MAX_ATTEMPTS

    async with db_session.begin():
        result, last_sql, last_error = await _retry_loop(
            question, complexity, plan, max_attempts,
            db_session, on_event, trace_id, debug,
        )

    if result is not None:
        return result

    attempts_desc = (
        f"{ARCTIC_DIRECT_ATTEMPTS} Arctic attempt(s) (HARD routing)"
        if complexity == QueryComplexity.HARD
        else f"{BASE_ATTEMPTS} Qwen attempt(s) + {ARCTIC_ATTEMPTS} Arctic fallback attempt(s)"
    )
    raise RuntimeError(
        f"SQL generation failed after {attempts_desc}. "
        f"Last error: {last_error}\nLast SQL: {last_sql}"
    )
