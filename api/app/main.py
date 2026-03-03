import asyncio
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.db.database import get_engine
from app.routers.query import router
from app.routers.stream import router as stream_router
from app.services.answer import close_answer_client
from app.services.nl2sql import close_ollama_client

_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logging.getLogger("httpx").setLevel(_log_level)
logging.getLogger("httpcore").setLevel(_log_level)
log = logging.getLogger(__name__)


async def _warmup_model(base_url: str, model: str) -> None:
    """Send a minimal prompt to load model weights into Metal/GPU memory."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=120.0, write=10.0)) as c:
            await c.post(
                f"{base_url}/api/generate",
                json={"model": model, "prompt": "1", "stream": False, "options": {"num_predict": 1}},
            )
        log.info("Model warmed up: %s", model)
    except Exception as exc:
        log.warning("Warmup skipped for %s: %s", model, exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ────────────────────────────────────────────────────────────────
    log.info("API starting up...")
    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        log.info("Database connectivity confirmed.")
    except Exception as exc:
        log.warning("Database connectivity check FAILED: %s", exc)

    ollama_base = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
    await asyncio.gather(
        _warmup_model(ollama_base, os.environ.get("OLLAMA_MODEL", "a-kore/Arctic-Text2SQL-R1-7B")),
        _warmup_model(ollama_base, os.environ.get("OLLAMA_MODEL_BASE", "qwen2.5-coder:3b")),
        _warmup_model(ollama_base, os.environ.get("ANSWER_MODEL", "llama3.2:1b")),
        _warmup_model(ollama_base, os.environ.get("ANSWER_MODEL_BIG", "llama3.2:3b")),
        return_exceptions=True,
    )

    yield

    # ── Shutdown ───────────────────────────────────────────────────────────────
    log.info("API shutting down. Disposing database engine...")
    await get_engine().dispose()
    await close_ollama_client()
    await close_answer_client()


app = FastAPI(
    title="NL2SQL Confectionery API",
    description="Query the confectionery sales database using natural language.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(stream_router)


@app.get("/health", tags=["monitoring"])
async def health_check() -> dict:
    """Liveness probe."""
    return {"status": "ok"}
