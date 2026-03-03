"""
Integration tests for POST /query endpoint shape and error handling.
All happy-path tests call real Ollama models (no mocks).
Only the generation-failure error-path test uses a mock to force broken SQL.
"""
from unittest.mock import AsyncMock

import pytest

VALID_QUESTION = "What are the top 5 best-selling products by total quantity sold?"


# ─────────────────────────────────────────────────────────────────────────────
# Happy-path tests — real Ollama, real DB
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_valid_question_returns_200(client):
    response = await client.post("/query", json={"question": VALID_QUESTION})
    assert response.status_code == 200


@pytest.mark.integration
async def test_response_has_correct_schema(client):
    response = await client.post("/query", json={"question": VALID_QUESTION})
    body = response.json()
    assert set(body.keys()) == {"question", "sql", "results", "row_count", "attempts", "answer", "model", "complexity", "fallback"}


@pytest.mark.integration
async def test_row_count_matches_results_length(client):
    response = await client.post("/query", json={"question": VALID_QUESTION})
    body = response.json()
    assert body["row_count"] == len(body["results"])


@pytest.mark.integration
async def test_attempts_is_positive_int(client):
    response = await client.post("/query", json={"question": VALID_QUESTION})
    attempts = response.json()["attempts"]
    assert isinstance(attempts, int)
    assert 1 <= attempts <= 5


@pytest.mark.integration
async def test_sql_field_has_no_markdown_fences(client):
    """Even if Ollama returns fenced SQL internally, the stored sql field must be clean."""
    response = await client.post("/query", json={"question": VALID_QUESTION})
    assert response.status_code == 200
    sql_field = response.json()["sql"]
    assert "`" not in sql_field


@pytest.mark.integration
async def test_answer_is_compact_and_robotic(client):
    response = await client.post("/query", json={"question": VALID_QUESTION})
    assert response.status_code == 200
    answer = response.json()["answer"]
    assert "\n" not in answer
    assert "```" not in answer
    assert "Based on the data" not in answer
    assert "According to the results" not in answer
    assert answer.startswith("Top rows:")
    assert "total_rows=" in answer


# ─────────────────────────────────────────────────────────────────────────────
# Validation tests — no Ollama involved
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_too_short_question_returns_422(client):
    response = await client.post("/query", json={"question": "hi"})
    assert response.status_code == 422


@pytest.mark.integration
async def test_too_long_question_returns_422(client):
    response = await client.post("/query", json={"question": "x" * 1001})
    assert response.status_code == 422


# ─────────────────────────────────────────────────────────────────────────────
# Error-path test — mock required to force generation failure
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_generation_failure_returns_422(client, mocker):
    """When SQL generation fails all retries, the endpoint returns 422 with structured error."""
    mocker.patch(
        "app.services.nl2sql._call_ollama",
        new=AsyncMock(return_value="SELCT FRMO broken_table;"),
    )
    response = await client.post("/query", json={"question": VALID_QUESTION})
    assert response.status_code == 422
    body = response.json()
    assert "detail" in body
