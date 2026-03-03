import asyncio
import json
import logging
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.services.nl2sql import generate_sql
from app.services.answer import (
    stream_answer_chunks,
    _classify_answer_complexity,
    ANSWER_MODEL,
    ANSWER_MODEL_BIG,
)

log = logging.getLogger(__name__)

router = APIRouter(tags=["stream"])


def _sse(event: str, data: dict) -> str:
    """Format a single Server-Sent Event message."""
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


@router.get("/query/stream")
async def stream_query(
    q: Annotated[str, Query(min_length=5, max_length=1000, description="Natural language question")],
    debug: Annotated[bool, Query(description="Include debug-only diagnostics events", alias="debug")] = False,
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    """
    Stream the NL2SQL pipeline as Server-Sent Events.

    Connect with native EventSource or fetch + ReadableStream.
    Events: thinking_start, sql_attempt, sql_generated, query_running,
            fallback_start, answer_start, answer_chunk, done, error.
    When debug=true, includes debug_stage events.
    """

    async def event_stream() -> AsyncGenerator[str, None]:
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def on_event(name: str, payload: dict) -> None:
            await queue.put(_sse(name, payload))

        async def run_pipeline() -> None:
            try:
                result = await generate_sql(q, db, on_event=on_event, debug=debug)

                _shape, _complexity = _classify_answer_complexity(result.get("results", []))
                _answer_model = ANSWER_MODEL_BIG if _complexity == "complex" else ANSWER_MODEL
                await queue.put(_sse("answer_start", {
                    "message": "Generating natural language answer...",
                    "model": _answer_model,
                    "shape": _shape,
                }))

                result["answer"] = ""
                async for chunk in stream_answer_chunks(q, result["results"]):
                    result["answer"] += chunk
                    await queue.put(_sse("answer_chunk", {"chunk": chunk}))

                await queue.put(_sse("done", result))
                log.info("SSE stream completed for question: %s", q[:80])
            except Exception as exc:
                log.error("SSE stream error: %s", exc)
                await queue.put(_sse("error", {"message": str(exc)}))
            finally:
                await queue.put(None)  # sentinel to stop consumer

        task = asyncio.create_task(run_pipeline())

        while True:
            item = await queue.get()
            if item is None:
                break
            yield item

        # Propagate any exceptions from the pipeline task
        await task

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
