import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from app.config import ANSWER_MODEL, ANSWER_MODEL_BIG
from app.services.ollama_client import (
    call_ollama_chat as _call_ollama_answer,
    close_clients as close_answer_client,
)

log = logging.getLogger(__name__)


# Token budgets per shape
TOKEN_BUDGETS: dict[str, int] = {
    "scalar": 40,
    "comparison": 60,
    "short_list": 80,
    "long_list": 100,
    "complex_table": 120,
}

# Minimal chat prompt — one-shot primes the sentence pattern.
# llama3.2:1b follows this reliably; no JSON schema needed.
ANSWER_SYSTEM_PROMPT = (
    "Answer data questions in one short sentence. No preamble. No explanation."
)

_ONE_SHOT: list[dict[str, str]] = [
    {
        "role": "user",
        "content": (
            "Question: What product sold most?\n"
            "Data: product=Alfajor Sin Azucar, qty=500\n"
            "Answer:"
        ),
    },
    {
        "role": "assistant",
        "content": "The best-selling product is Alfajor Sin Azucar with 500 units.",
    },
]


# Classifier

def _classify_answer_complexity(results: list[dict]) -> tuple[str, str]:
    """
    Classify result shape and complexity for model routing.

    Returns (shape, complexity) where:
      shape      - "empty" | "scalar" | "comparison" | "short_list" | "long_list" | "complex_table"
      complexity - "simple" (-> ANSWER_MODEL 1b) | "complex" (-> ANSWER_MODEL_BIG 3b)

    Boundaries:
      scalar        - 1 row, 1 col
      comparison    - 2-3 rows, any cols
      short_list    - 4-8 rows, <=3 cols
      long_list     - 9-30 rows, <=3 cols
      complex_table - >30 rows OR >=4 cols
    """
    if not results:
        return ("empty", "simple")

    n_rows = len(results)
    n_cols = len(results[0])

    if n_rows == 1 and n_cols == 1:
        return ("scalar", "simple")
    if n_cols >= 4 or n_rows > 30:
        return ("complex_table", "complex")
    if n_rows <= 3:
        return ("comparison", "simple")
    if n_rows <= 8:
        return ("short_list", "simple")
    return ("long_list", "complex")


# Helpers

def _format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    return str(value)


def _format_row(row: dict[str, Any]) -> str:
    return ", ".join(f"{key}={_format_value(value)}" for key, value in row.items())


def _one_line(text: str) -> str:
    return text.replace("\n", " ").replace("\r", " ").strip()


def _results_to_text(results: list[dict], shape: str) -> str:
    """Flatten SQL results into terse text for the chat prompt."""
    if shape == "scalar":
        val = next(iter(results[0].values()))
        return _format_value(val)
    top = results[:3]
    parts = [_format_row(row) for row in top]
    text = "; ".join(parts)
    if len(results) > 3:
        text += f"; {len(results)} total"
    return text


def _build_answer_messages(
    question: str, results: list[dict], shape: str
) -> list[dict[str, str]]:
    """Build /api/chat messages for natural-language answer generation."""
    data_text = _results_to_text(results, shape)
    user_content = f"Question: {question}\nData: {data_text}\nAnswer:"
    return [
        {"role": "system", "content": ANSWER_SYSTEM_PROMPT},
        *_ONE_SHOT,
        {"role": "user", "content": user_content},
    ]


def _render_minimal_from_results(results: list[dict]) -> str:
    """Deterministic fallback — used when the model returns empty or fails."""
    if not results:
        return "No results were found for that query."

    if len(results) == 1 and len(results[0]) == 1:
        value = next(iter(results[0].values()))
        return _one_line(f"{_format_value(value)}.")

    top_rows = results[:3]
    row_parts = [_format_row(row) for row in top_rows]
    return _one_line(f"Top rows: {'; '.join(row_parts)}; total_rows={len(results)}.")


# Public API

async def generate_answer(question: str, results: list[dict]) -> str:
    """
    Translate SQL result rows into a short natural-language sentence.

    Routes through a 5-shape complexity classifier:
    - Simple shapes (scalar, comparison, short_list) -> ANSWER_MODEL (llama3.2:1b)
    - Complex shapes (long_list, complex_table)      -> ANSWER_MODEL_BIG (llama3.2:3b)
      with automatic fallback to the small model when the big model call fails.
    """
    shape, complexity = _classify_answer_complexity(results)

    if shape == "empty":
        return "No results were found for that query."

    model = ANSWER_MODEL_BIG if complexity == "complex" else ANSWER_MODEL
    num_predict = TOKEN_BUDGETS[shape]
    messages = _build_answer_messages(question, results, shape)

    try:
        answer = await _call_ollama_answer(messages, model, num_predict)
        if answer:
            log.info("Answer generated (shape=%s, model=%s)", shape, model)
            return _one_line(answer)
        log.warning(
            "Empty answer (shape=%s, model=%s); using deterministic fallback",
            shape,
            model,
        )
    except Exception as exc:
        if complexity == "complex":
            log.warning(
                "Big model failed (shape=%s): %s - retrying with small model", shape, exc
            )
            try:
                answer = await _call_ollama_answer(messages, ANSWER_MODEL, num_predict)
                if answer:
                    log.info(
                        "Fallback answer generated (shape=%s, model=%s)",
                        shape,
                        ANSWER_MODEL,
                    )
                    return _one_line(answer)
                log.warning(
                    "Fallback answer empty (shape=%s); using deterministic fallback",
                    shape,
                )
            except Exception as fallback_exc:
                log.error("Small model fallback also failed: %s", fallback_exc)
        else:
            log.error("Answer generation failed (shape=%s): %s", shape, exc)

    return _render_minimal_from_results(results)


async def stream_answer_chunks(question: str, results: list[dict]) -> AsyncGenerator[str, None]:
    """
    Stream the final answer as a single chunk for deterministic UX.
    """
    if not results:
        yield "No results were found for that query."
        return

    answer = await generate_answer(question, results)
    if answer:
        yield answer
