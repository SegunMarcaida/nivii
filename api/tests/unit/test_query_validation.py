"""
SPEC-T03: Unit tests for QueryRequest and QueryResponse Pydantic models.
Zero I/O — pure model validation.
"""
import pytest
from pydantic import ValidationError

from app.routers.query import QueryRequest, QueryResponse


@pytest.mark.unit
def test_request_too_short():
    with pytest.raises(ValidationError):
        QueryRequest(question="hi")


@pytest.mark.unit
def test_request_min_length_boundary():
    req = QueryRequest(question="hello")
    assert req.question == "hello"
    assert req.debug is False


@pytest.mark.unit
def test_request_max_length_boundary():
    question = "x" * 1000
    req = QueryRequest(question=question)
    assert len(req.question) == 1000


@pytest.mark.unit
def test_request_too_long():
    with pytest.raises(ValidationError):
        QueryRequest(question="x" * 1001)


@pytest.mark.unit
def test_response_valid():
    resp = QueryResponse(
        question="What is 1+1?",
        sql="SELECT 2;",
        results=[],
        row_count=0,
        attempts=1,
        answer="The result is 2.",
    )
    assert resp.question == "What is 1+1?"
    assert resp.row_count == 0
    assert resp.attempts == 1
    assert resp.answer == "The result is 2."


@pytest.mark.unit
def test_response_missing_field():
    with pytest.raises(ValidationError):
        QueryResponse(
            question="What is 1+1?",
            # sql is missing
            results=[],
            row_count=0,
            attempts=1,
        )


@pytest.mark.unit
def test_results_accepts_list_of_dicts():
    resp = QueryResponse(
        question="Some question?",
        sql="SELECT 1 AS col;",
        results=[{"col": 1}, {"col": 2}],
        row_count=2,
        attempts=1,
        answer="There are 2 rows.",
    )
    assert len(resp.results) == 2
    assert resp.results[0]["col"] == 1


@pytest.mark.unit
def test_request_debug_flag_true():
    req = QueryRequest(question="hello world", debug=True)
    assert req.debug is True


@pytest.mark.unit
def test_response_default_schema_unchanged_with_exclude_none():
    """Default fields should serialize to legacy schema when debug fields are None."""
    resp = QueryResponse(
        question="What is 1+1?",
        sql="SELECT 2;",
        results=[],
        row_count=0,
        attempts=1,
        answer="The result is 2.",
    )
    payload = resp.model_dump(exclude_none=True)
    assert set(payload.keys()) == {
        "question",
        "sql",
        "results",
        "row_count",
        "attempts",
        "answer",
        "model",
        "complexity",
        "fallback",
    }


@pytest.mark.unit
def test_response_includes_debug_fields_when_present():
    resp = QueryResponse(
        question="Q",
        sql="SELECT 1;",
        results=[],
        row_count=0,
        attempts=1,
        answer="A",
        trace_id="abc123",
        status="success",
        guardrail_events=[{"stage": "STATIC_VALIDATE", "reason": "ok"}],
        validation_summary={"static": {"static_ok": True}},
        latency_ms=10,
    )
    payload = resp.model_dump(exclude_none=True)
    assert payload["trace_id"] == "abc123"
    assert payload["status"] == "success"
    assert "guardrail_events" in payload
    assert "validation_summary" in payload
    assert payload["latency_ms"] == 10
