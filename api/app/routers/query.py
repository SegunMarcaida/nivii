import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.services.answer import generate_answer
from app.services.nl2sql import generate_sql

log = logging.getLogger(__name__)

router = APIRouter()


class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=1000,
        json_schema_extra={"example": "What is the most bought product on Fridays?"},
    )
    debug: bool = Field(
        default=False,
        description="Include pipeline diagnostics fields when true.",
    )


class QueryResponse(BaseModel):
    question: str
    sql: str
    results: list[dict]
    row_count: int
    attempts: int
    answer: str
    model: str = ""
    complexity: str = ""
    fallback: bool = False
    trace_id: str | None = None
    status: str | None = None
    guardrail_events: list[dict[str, Any]] | None = None
    validation_summary: dict[str, Any] | None = None
    latency_ms: int | None = None
    stage_durations_ms: dict[str, int] | None = None


@router.post("/query", response_model=QueryResponse, response_model_exclude_none=True)
async def query_endpoint(
    request: QueryRequest,
    db: AsyncSession = Depends(get_db),
) -> QueryResponse:
    """
    Translate a natural language question into a PostgreSQL query and return results.
    """
    log.info("POST /query — question: %s", request.question)
    try:
        result = await generate_sql(request.question, db, debug=request.debug)
        summarize_started = time.perf_counter()
        try:
            answer = await generate_answer(request.question, result["results"])
        except Exception as ans_exc:
            log.warning("Answer generation failed: %s", ans_exc)
            row_count = result.get("row_count", len(result.get("results", [])))
            answer = f"Found {row_count} result{'s' if row_count != 1 else ''}." if row_count > 0 else "No results were found."
        summarize_ms = int((time.perf_counter() - summarize_started) * 1000)
        result["answer"] = answer
        if request.debug:
            stage_durations = result.get("stage_durations_ms") or {}
            stage_durations["SUMMARIZE"] = summarize_ms
            result["stage_durations_ms"] = stage_durations
            if isinstance(result.get("latency_ms"), int):
                result["latency_ms"] = result["latency_ms"] + summarize_ms
        return QueryResponse(**result)
    except Exception as exc:
        log.error("SQL generation failed: %s: %s", type(exc).__name__, exc, exc_info=True)
        raise HTTPException(
            status_code=422,
            detail={
                "detail": f"SQL generation failed: {exc}",
                "error": str(exc),
            },
        ) from exc
