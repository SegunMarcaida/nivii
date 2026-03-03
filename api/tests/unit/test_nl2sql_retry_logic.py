"""
SPEC-T04: Unit tests for generate_sql() retry loop with Arctic-Text2SQL-R1-7B.
All Ollama HTTP calls and database I/O are mocked.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.nl2sql import (
    OLLAMA_MODEL,
    OLLAMA_MODEL_BASE,
    MAX_ATTEMPTS,
    ARCTIC_DIRECT_ATTEMPTS,
    generate_sql,
)


# Valid flat-table SQL (no JOINs needed with the new schema)
VALID_SQL = (
    "SELECT product_name, SUM(total) AS total_revenue "
    "FROM sales "
    "WHERE is_credit_note = 0 AND is_promotional = 0 AND is_manual_adj = 0 "
    "GROUP BY product_name "
    "ORDER BY total_revenue DESC "
    "LIMIT 5"
)
BAD_SQL = "SELCT FRMO broken_table"

# Arctic-style response: <think> reasoning + SQL in <answer> tag
ARCTIC_RESPONSE = (
    f"<think>\nLet me analyze this step by step.\n</think>\n"
    f"<answer>\n```sql\n{VALID_SQL}\n```\n</answer>"
)
ARCTIC_BAD_RESPONSE = (
    f"<think>\nI'll try this.\n</think>\n"
    f"<answer>\n```sql\n{BAD_SQL}\n```\n</answer>"
)
# Keep aliases for existing tests that reference these names
OMNISQL_RESPONSE = ARCTIC_RESPONSE
OMNISQL_BAD_RESPONSE = ARCTIC_BAD_RESPONSE


def _make_mock_session(explain_fails_on_attempts: list[int] | None = None):
    """
    Build an AsyncMock that simulates AsyncSession behaviour.

    explain_fails_on_attempts: list of 1-based attempt numbers on which
    EXPLAIN QUERY PLAN should fail.
    """
    explain_call_count = 0

    async def fake_execute(stmt, *args, **kwargs):
        nonlocal explain_call_count
        stmt_str = str(stmt)
        if "EXPLAIN QUERY PLAN" in stmt_str.upper():
            explain_call_count += 1
            if explain_fails_on_attempts and explain_call_count in explain_fails_on_attempts:
                raise Exception(f"near 'SELCT': syntax error (mock, call {explain_call_count})")
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        return mock_result

    session = AsyncMock()
    session.execute = fake_execute

    begin_ctx = AsyncMock()
    begin_ctx.__aenter__ = AsyncMock(return_value=None)
    begin_ctx.__aexit__ = AsyncMock(return_value=False)
    session.begin = MagicMock(return_value=begin_ctx)

    return session


@pytest.mark.unit
async def test_succeeds_first_attempt():
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        result = await generate_sql("What are the top products by revenue?", session)
    assert result["attempts"] == 1


@pytest.mark.unit
async def test_succeeds_second_attempt():
    session = _make_mock_session(explain_fails_on_attempts=[1])
    call_count = 0

    async def alternating_ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        nonlocal call_count
        call_count += 1
        return OMNISQL_BAD_RESPONSE if call_count == 1 else OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=alternating_ollama):
        result = await generate_sql("What are the top products by revenue?", session)
    assert result["attempts"] == 2


@pytest.mark.unit
async def test_raises_after_max_attempts():
    session = _make_mock_session(explain_fails_on_attempts=list(range(1, MAX_ATTEMPTS + 1)))
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_BAD_RESPONSE)):
        with pytest.raises(RuntimeError):
            await generate_sql("What are the top products by revenue?", session)


@pytest.mark.unit
async def test_extracts_sql_from_code_block():
    """SQL is correctly extracted from the Arctic <think>/<answer> format."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        result = await generate_sql("What are the top products?", session)
    assert "`" not in result["sql"]
    assert result["sql"].upper().startswith("SELECT")


@pytest.mark.unit
async def test_result_dict_shape():
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        result = await generate_sql("What are the top products?", session)
    required_keys = {"question", "sql", "results", "row_count", "attempts", "model", "complexity"}
    assert required_keys.issubset(result.keys())
    assert isinstance(result["results"], list)
    assert isinstance(result["row_count"], int)
    assert isinstance(result["attempts"], int)


@pytest.mark.unit
async def test_simple_query_uses_base_model():
    """SIMPLE queries route to Qwen (OLLAMA_MODEL_BASE) on first attempt."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    captured_models: list[str] = []

    async def capture_ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        captured_models.append(model)
        return OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=capture_ollama):
        result = await generate_sql("What is the total revenue?", session)

    assert len(captured_models) == 1
    assert captured_models[0] == OLLAMA_MODEL_BASE
    assert result["model"] == OLLAMA_MODEL_BASE
    assert result["complexity"] == "simple"


@pytest.mark.unit
async def test_hard_query_uses_arctic_directly():
    """HARD queries must route directly to Arctic, skipping Qwen entirely."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    captured_models: list[str] = []

    async def capture_ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        captured_models.append(model)
        return OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=capture_ollama):
        result = await generate_sql("Rank products by cumulative revenue", session)

    assert captured_models[0] == OLLAMA_MODEL  # Arctic first, not Qwen
    assert result["complexity"] == "hard"


@pytest.mark.unit
async def test_complexity_in_result():
    """Result always includes complexity classification."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        simple_result = await generate_sql("What is the total revenue?", session)
        hard_result = await generate_sql("Rank products by cumulative revenue", session)

    assert simple_result["complexity"] == "simple"
    assert hard_result["complexity"] == "hard"


@pytest.mark.unit
async def test_fallback_true_when_arctic_used():
    """fallback=True only when Arctic actually handled the query (after 3B exhausted 3 attempts)."""
    # Fail all 3 base-model attempts, succeed on Arctic (attempt 4)
    session = _make_mock_session(explain_fails_on_attempts=[1, 2, 3])
    call_count = 0

    async def ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        nonlocal call_count
        call_count += 1
        return OMNISQL_BAD_RESPONSE if call_count <= 3 else OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=ollama):
        result = await generate_sql("What are the top products?", session)

    assert result.get("fallback") is True
    assert result["model"] == OLLAMA_MODEL  # Arctic handled it


@pytest.mark.unit
async def test_fallback_false_on_first_attempt():
    """When answer comes on attempt 1, fallback=False."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        result = await generate_sql("What is the total revenue?", session)

    assert result.get("fallback") is False


@pytest.mark.unit
async def test_correction_prompt_used_on_retry():
    """Second _call_ollama invocation receives a prompt containing the error."""
    session = _make_mock_session(explain_fails_on_attempts=[1])
    captured_prompts: list[str] = []

    async def capture_ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        captured_prompts.append(prompt)
        return OMNISQL_BAD_RESPONSE if len(captured_prompts) == 1 else OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=capture_ollama):
        await generate_sql("What are the top products?", session)

    assert len(captured_prompts) == 2
    # Second prompt is a correction — must contain the failing SQL and the error
    correction = captured_prompts[1]
    assert BAD_SQL in correction
    assert "syntax error" in correction.lower() or "mock" in correction


@pytest.mark.unit
async def test_correction_prompt_uses_qwen_template():
    """Correction prompt on retry must follow the Qwen template structure (no CoT tags)."""
    session = _make_mock_session(explain_fails_on_attempts=[1])
    captured_prompts: list[str] = []

    async def capture_ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        captured_prompts.append(prompt)
        return OMNISQL_BAD_RESPONSE if len(captured_prompts) == 1 else OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=capture_ollama):
        await generate_sql("What are the top products?", session)

    correction = captured_prompts[1]
    # Qwen correction template: no CoT tags, SQL comment style with -- Fixed query:
    assert "<think>" not in correction
    assert "<answer>" not in correction
    assert "-- Fixed query:" in correction
    assert "SQLite" in correction


@pytest.mark.unit
async def test_result_dict_model_field():
    """Result dict must always contain the 'model' key with a string value."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        result = await generate_sql("What are the top products?", session)
    assert "model" in result
    assert isinstance(result["model"], str)


@pytest.mark.unit
async def test_call_ollama_uses_2048_num_predict():
    """_call_ollama must use num_predict=2048 for Arctic CoT output."""
    import inspect
    import app.services.nl2sql as nl2sql_module
    source = inspect.getsource(nl2sql_module._call_ollama)
    assert "2048" in source, "_call_ollama must use num_predict=2048 for Arctic CoT output"


@pytest.mark.unit
async def test_debug_fields_only_when_enabled():
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        no_debug = await generate_sql("What are the top products?", session, debug=False)
        debug = await generate_sql("What are the top products?", session, debug=True)

    assert "trace_id" not in no_debug
    assert "status" not in no_debug
    assert "validation_summary" not in no_debug

    assert isinstance(debug.get("trace_id"), str)
    assert debug.get("status") in {"success", "fallback"}
    assert isinstance(debug.get("guardrail_events"), list)
    assert isinstance(debug.get("validation_summary"), dict)
    assert isinstance(debug.get("latency_ms"), int)


@pytest.mark.unit
async def test_debug_trace_id_in_thinking_start():
    """When debug=True, thinking_start event includes trace_id."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    events: list[tuple[str, dict]] = []

    async def on_event(name: str, payload: dict) -> None:
        events.append((name, payload))

    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        await generate_sql("What are the top products?", session, on_event=on_event, debug=True)

    thinking_payload = next(payload for name, payload in events if name == "thinking_start")
    assert "trace_id" in thinking_payload


@pytest.mark.unit
async def test_no_trace_id_in_thinking_start_without_debug():
    """When debug=False, thinking_start event must NOT include trace_id."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    events: list[tuple[str, dict]] = []

    async def on_event(name: str, payload: dict) -> None:
        events.append((name, payload))

    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        await generate_sql("What are the top products?", session, on_event=on_event, debug=False)

    thinking_payload = next(payload for name, payload in events if name == "thinking_start")
    assert "trace_id" not in thinking_payload


@pytest.mark.unit
async def test_hard_query_fallback_false():
    """HARD queries always have fallback=False — Arctic is the primary model, not a fallback."""
    session = _make_mock_session(explain_fails_on_attempts=[])
    with patch("app.services.nl2sql._call_ollama", new=AsyncMock(return_value=OMNISQL_RESPONSE)):
        result = await generate_sql("Rank products by cumulative revenue", session)
    assert result.get("fallback") is False
    assert result["model"] == OLLAMA_MODEL


@pytest.mark.unit
async def test_hard_query_arctic_correction_on_retry():
    """HARD retry must use Arctic correction template (contains <think>/<answer> tags)."""
    session = _make_mock_session(explain_fails_on_attempts=[1])
    captured_prompts: list[str] = []

    async def capture_ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        captured_prompts.append(prompt)
        return OMNISQL_BAD_RESPONSE if len(captured_prompts) == 1 else OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=capture_ollama):
        await generate_sql("Rank products by cumulative revenue", session)

    assert len(captured_prompts) == 2
    correction = captured_prompts[1]
    assert "<think>" in correction
    assert "<answer>" in correction


@pytest.mark.unit
async def test_hard_query_never_uses_qwen():
    """No Qwen call should appear anywhere in a HARD query's attempt sequence."""
    session = _make_mock_session(explain_fails_on_attempts=[1])
    captured_models: list[str] = []

    async def capture_ollama(prompt: str, model: str = OLLAMA_MODEL, **kwargs) -> str:
        captured_models.append(model)
        return OMNISQL_BAD_RESPONSE if len(captured_models) == 1 else OMNISQL_RESPONSE

    with patch("app.services.nl2sql._call_ollama", side_effect=capture_ollama):
        await generate_sql("Rank products by cumulative revenue", session)

    assert all(m == OLLAMA_MODEL for m in captured_models), (
        f"Expected all calls to use Arctic, got: {captured_models}"
    )
