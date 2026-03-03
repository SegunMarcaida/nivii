"""
Integration test fixtures.

Prerequisites:
  - docker compose up must be running with the SQLite DB populated.
  - Ollama must be running at localhost:11434 with models pulled:
    hf.co/mradermacher/OmniSQL-7B-GGUF:Q4_K_M, llama3.2:1b

If the SQLite DB or Ollama is unreachable, all integration tests are skipped automatically.
"""
import os
import socket
from pathlib import Path

# Set OLLAMA_BASE_URL before importing the app — nl2sql.py reads this at module level.
os.environ.setdefault("OLLAMA_BASE_URL", "http://localhost:11434")

# Default to local SQLite path for integration tests run outside Docker.
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./data/confectionery.db")

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app


# ─────────────────────────────────────────────────────────────────────────────
# Reachability guards
# ─────────────────────────────────────────────────────────────────────────────

def _sqlite_db_is_ready() -> bool:
    """Return True if the SQLite DB file exists and has the sales table."""
    db_url = os.environ.get("DATABASE_URL", "")
    if "sqlite" not in db_url:
        return False
    # Extract file path from URL (sqlite+aiosqlite:////abs/path or ///./rel/path)
    path_str = db_url.split("///", 1)[-1]
    db_path = Path(path_str)
    return db_path.exists() and db_path.stat().st_size > 0


def _ollama_is_reachable(host: str = "localhost", port: int = 11434, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_db():
    """Skip entire integration suite if SQLite DB is not ready."""
    if not _sqlite_db_is_ready():
        db_url = os.environ.get("DATABASE_URL", "not set")
        pytest.skip(
            f"SQLite DB is not ready (DATABASE_URL={db_url!r}). "
            "Run 'docker compose up' first and wait for ingestion to complete."
        )


@pytest.fixture(scope="session", autouse=True)
def require_ollama():
    """Skip entire integration suite if Ollama is not reachable."""
    if not _ollama_is_reachable():
        pytest.skip(
            "Ollama is not reachable at localhost:11434. "
            "Start with: ollama serve"
        )


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI async test client (mounts app in-process, no HTTP server required)
# ─────────────────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def client():
    """
    Yields an httpx AsyncClient that mounts the FastAPI app directly via ASGI.
    The app uses its normal get_db() → connects to localhost:5432.
    Ollama calls go to localhost:11434 (set via OLLAMA_BASE_URL env var).
    Timeout is generous for CPU inference (up to 10 minutes per request).
    """
    transport = ASGITransport(app=app)
    timeout = httpx.Timeout(connect=60.0, read=600.0, write=60.0, pool=30.0)
    async with AsyncClient(transport=transport, base_url="http://test", timeout=timeout) as ac:
        yield ac


# ─────────────────────────────────────────────────────────────────────────────
# Shared ask fixture — sends a question through the full pipeline
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def ask(client):
    """
    Returns an async callable that posts a question to /query and returns
    the full response body: {question, sql, results, row_count, attempts, answer}.
    Asserts HTTP 200 and row_count consistency.
    """
    async def _ask(question: str) -> dict:
        response = await client.post("/query", json={"question": question})
        assert response.status_code == 200, (
            f"Expected 200, got {response.status_code}: {response.text}"
        )
        body = response.json()
        assert body["row_count"] == len(body["results"]), (
            f"row_count ({body['row_count']}) != len(results) ({len(body['results'])})"
        )
        return body
    return _ask


# ─────────────────────────────────────────────────────────────────────────────
# Flexible column-matching helpers
# ─────────────────────────────────────────────────────────────────────────────

def find_col(row: dict, *patterns: str) -> str | None:
    """Find a column key matching any pattern (case-insensitive substring)."""
    for key in row:
        for pat in patterns:
            if pat.lower() in key.lower():
                return key
    return None


def numeric_col(row: dict, *patterns: str) -> float:
    """
    Get numeric value from the column matching any pattern.
    Falls back to the first numeric value in the row.
    """
    col = find_col(row, *patterns)
    if col is not None:
        return float(row[col])
    # Fallback: first numeric value
    for v in row.values():
        try:
            return float(v)
        except (ValueError, TypeError):
            continue
    raise AssertionError(f"No numeric value in row: {list(row.keys())}")


def str_col(row: dict, *patterns: str) -> str:
    """
    Get string value from the column matching any pattern.
    Falls back to the first string value in the row.
    """
    col = find_col(row, *patterns)
    if col is not None:
        return str(row[col])
    for v in row.values():
        if isinstance(v, str):
            return v
    raise AssertionError(f"No string column matching {patterns}: {list(row.keys())}")
