"""
SPEC-T06: Integration tests for GET /health.
Requires: docker compose up (SQLite ingestion complete + Ollama reachable).
"""
import pytest


@pytest.mark.integration
async def test_health_returns_200(client):
    response = await client.get("/health")
    assert response.status_code == 200


@pytest.mark.integration
async def test_health_response_body(client):
    response = await client.get("/health")
    assert response.json() == {"status": "ok"}
