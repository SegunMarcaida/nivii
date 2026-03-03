"""
Shared Ollama HTTP client — single module for all LLM communication.

Exposes two focused interfaces:
  - call_ollama_generate(): streaming text completion (used by nl2sql)
  - call_ollama_chat(): synchronous chat completion (used by answer)
  - close_clients(): shutdown hook for graceful cleanup
"""

import json as _json
import logging
import time
from collections.abc import Awaitable, Callable

import httpx

from app.config import MODEL_OPTIONS, OLLAMA_BASE_URL, OLLAMA_MODEL

log = logging.getLogger(__name__)

# ── Persistent HTTP clients ──────────────────────────────────────────────────
# Separate clients for generate (long-lived streaming) and chat (short requests)
# with different timeout profiles.

_generate_client: httpx.AsyncClient | None = None
_chat_client: httpx.AsyncClient | None = None


def _get_generate_client() -> httpx.AsyncClient:
    global _generate_client
    if _generate_client is None:
        _generate_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
    return _generate_client


def _get_chat_client() -> httpx.AsyncClient:
    global _chat_client
    if _chat_client is None:
        _chat_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=5.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
    return _chat_client


async def close_clients() -> None:
    """Shut down all HTTP clients. Called once during app shutdown."""
    global _generate_client, _chat_client
    for client_ref in (_generate_client, _chat_client):
        if client_ref is not None:
            await client_ref.aclose()
    _generate_client = None
    _chat_client = None


# ── Streaming text completion ────────────────────────────────────────────────

async def call_ollama_generate(
    prompt: str,
    model: str = OLLAMA_MODEL,
    on_token: Callable[[str], Awaitable[None]] | None = None,
    options_override: dict | None = None,
) -> str:
    """Stream a raw text completion from Ollama /api/generate.

    *options_override* is merged on top of MODEL_OPTIONS for the given model,
    allowing callers (e.g. the planning call) to tighten num_predict or add
    custom stop sequences without changing the shared MODEL_OPTIONS config.
    """
    log.debug("Ollama prompt (%d chars, model=%s):\n%s", len(prompt), model, prompt)

    tokens: list[str] = []
    token_count = 0
    t_start = time.perf_counter()
    t_first_token: float | None = None

    client = _get_generate_client()
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
                **MODEL_OPTIONS.get(model, {"num_ctx": 4096, "num_predict": 2048}),
                **(options_override or {}),
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


# ── Synchronous chat completion ──────────────────────────────────────────────

async def call_ollama_chat(
    messages: list[dict],
    model: str,
    num_predict: int,
) -> str:
    """POST chat messages to Ollama /api/chat and return the response text."""
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": num_predict,
            "stop": ["\n\n"],
        },
    }
    client = _get_chat_client()
    r = await client.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload, timeout=60.0)
    r.raise_for_status()
    return r.json().get("message", {}).get("content", "").strip()
