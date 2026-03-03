"""
Unit tests for answer.py - 5-shape complexity classifier and minimal chat prompt.

All Ollama HTTP calls are mocked; no I/O is performed.
Uses pytest-mock (mocker fixture) and asyncio_mode=auto from pytest.ini.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config import ANSWER_MODEL, ANSWER_MODEL_BIG
from app.services.answer import (
    TOKEN_BUDGETS,
    _build_answer_messages,
    _classify_answer_complexity,
    _render_minimal_from_results,
    _results_to_text,
    generate_answer,
    stream_answer_chunks,
)


# Classifier tests

@pytest.mark.unit
def test_classify_empty():
    assert _classify_answer_complexity([]) == ("empty", "simple")


@pytest.mark.unit
def test_classify_scalar():
    assert _classify_answer_complexity([{"total_revenue": 12345.67}]) == ("scalar", "simple")


@pytest.mark.unit
def test_classify_comparison_two_rows():
    rows = [{"waiter": "Alice", "revenue": 5000}, {"waiter": "Bob", "revenue": 4000}]
    assert _classify_answer_complexity(rows) == ("comparison", "simple")


@pytest.mark.unit
def test_classify_comparison_three_rows():
    rows = [{"week": "Mon", "rev": 100}, {"week": "Tue", "rev": 200}, {"week": "Wed", "rev": 150}]
    assert _classify_answer_complexity(rows) == ("comparison", "simple")


@pytest.mark.unit
def test_classify_short_list():
    rows = [{"product": f"P{i}", "revenue": i * 1000} for i in range(5)]
    assert _classify_answer_complexity(rows) == ("short_list", "simple")


@pytest.mark.unit
def test_classify_short_list_boundary_8_rows():
    rows = [{"product": f"P{i}", "revenue": i} for i in range(8)]
    assert _classify_answer_complexity(rows) == ("short_list", "simple")


@pytest.mark.unit
def test_classify_long_list():
    rows = [{"product": f"P{i}", "revenue": i * 500} for i in range(10)]
    assert _classify_answer_complexity(rows) == ("long_list", "complex")


@pytest.mark.unit
def test_classify_complex_table_many_rows():
    rows = [{"date": f"2024-01-{i+1:02d}", "revenue": i * 100} for i in range(35)]
    assert _classify_answer_complexity(rows) == ("complex_table", "complex")


@pytest.mark.unit
def test_classify_complex_table_wide():
    rows = [{"a": 1, "b": 2, "c": 3, "d": 4}, {"a": 5, "b": 6, "c": 7, "d": 8}]
    assert _classify_answer_complexity(rows) == ("complex_table", "complex")


# _results_to_text tests

@pytest.mark.unit
def test_results_to_text_scalar_returns_bare_value():
    result = _results_to_text([{"total_revenue": 12345.67}], "scalar")
    assert result == "12345.67"


@pytest.mark.unit
def test_results_to_text_rows_top3_and_total():
    rows = [{"product": f"P{i}", "qty": i} for i in range(5)]
    result = _results_to_text(rows, "short_list")
    assert "P0" in result
    assert "P2" in result
    assert "P3" not in result
    assert "5 total" in result


@pytest.mark.unit
def test_results_to_text_three_rows_no_total():
    rows = [{"a": 1}, {"a": 2}, {"a": 3}]
    result = _results_to_text(rows, "comparison")
    assert "total" not in result
    assert "a=1" in result
    assert "a=3" in result


# _build_answer_messages tests

@pytest.mark.unit
def test_build_answer_messages_structure():
    msgs = _build_answer_messages("What is total revenue?", [{"total_revenue": 42}], "scalar")
    assert msgs[0]["role"] == "system"
    assert msgs[-1]["role"] == "user"
    assert "What is total revenue?" in msgs[-1]["content"]
    assert "42" in msgs[-1]["content"]
    assert "Answer:" in msgs[-1]["content"]


@pytest.mark.unit
def test_build_answer_messages_includes_one_shot():
    msgs = _build_answer_messages("Q?", [{"v": 1}], "scalar")
    roles = [m["role"] for m in msgs]
    # system, user (one-shot), assistant (one-shot), user (actual)
    assert roles == ["system", "user", "assistant", "user"]


# _render_minimal_from_results tests

@pytest.mark.unit
def test_render_minimal_from_results_scalar():
    rendered = _render_minimal_from_results([{"total_revenue": 123.45}])
    assert rendered == "123.45."


@pytest.mark.unit
def test_render_minimal_from_results_rows_top_3_and_count():
    rows = [{"product": f"P{i}", "qty": i} for i in range(5)]
    rendered = _render_minimal_from_results(rows)
    assert rendered.startswith("Top rows: ")
    assert "product=P0, qty=0" in rendered
    assert "product=P2, qty=2" in rendered
    assert "product=P3, qty=3" not in rendered
    assert rendered.endswith("total_rows=5.")


# Token budget coverage

@pytest.mark.unit
def test_token_budgets_all_shapes_present():
    for shape in ("scalar", "comparison", "short_list", "long_list", "complex_table"):
        assert shape in TOKEN_BUDGETS
        assert TOKEN_BUDGETS[shape] > 0


@pytest.mark.unit
def test_token_budget_increases_with_complexity():
    assert TOKEN_BUDGETS["scalar"] < TOKEN_BUDGETS["comparison"]
    assert TOKEN_BUDGETS["comparison"] < TOKEN_BUDGETS["short_list"]
    assert TOKEN_BUDGETS["short_list"] < TOKEN_BUDGETS["long_list"]
    assert TOKEN_BUDGETS["long_list"] < TOKEN_BUDGETS["complex_table"]


# generate_answer - empty

@pytest.mark.unit
async def test_empty_results_returns_without_calling_ollama(mocker):
    mock_get_client = mocker.patch("app.services.ollama_client._get_chat_client")
    result = await generate_answer("How many sales?", [])
    mock_get_client.assert_not_called()
    assert result == "No results were found for that query."


# generate_answer - model routing

def _setup_mock_client(mocker, response_text: str):
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"message": {"content": response_text}}

    mock_post = AsyncMock(return_value=mock_response)
    mock_client = MagicMock()
    mock_client.post = mock_post

    mocker.patch("app.services.ollama_client._get_chat_client", return_value=mock_client)
    return mock_post


@pytest.mark.unit
async def test_simple_shape_uses_small_model_and_returns_sentence(mocker):
    mock_post = _setup_mock_client(
        mocker,
        "The total revenue was 12345.67.",
    )
    result = await generate_answer("What is total revenue?", [{"total_revenue": 12345.67}])
    called_model = mock_post.call_args[1]["json"]["model"]
    assert called_model == ANSWER_MODEL
    assert result == "The total revenue was 12345.67."


@pytest.mark.unit
async def test_comparison_shape_uses_small_model(mocker):
    rows = [{"waiter": "Alice", "revenue": 5000}, {"waiter": "Bob", "revenue": 4000}]
    mock_post = _setup_mock_client(
        mocker,
        "Alice generated the most revenue with 5,000.",
    )
    result = await generate_answer("Who sold more?", rows)
    called_model = mock_post.call_args[1]["json"]["model"]
    assert called_model == ANSWER_MODEL
    assert result == "Alice generated the most revenue with 5,000."


@pytest.mark.unit
async def test_short_list_shape_uses_small_model(mocker):
    rows = [{"product": f"P{i}", "revenue": i * 1000} for i in range(5)]
    mock_post = _setup_mock_client(
        mocker,
        "The top product is P4 with 4,000 in revenue.",
    )
    await generate_answer("Top 5 products?", rows)
    called_model = mock_post.call_args[1]["json"]["model"]
    assert called_model == ANSWER_MODEL


@pytest.mark.unit
async def test_long_list_uses_big_model(mocker):
    rows = [{"product": f"P{i}", "revenue": i * 500} for i in range(10)]
    mock_post = _setup_mock_client(
        mocker,
        "The top product is P9 with 4,500 in revenue.",
    )
    result = await generate_answer("Top products?", rows)
    called_model = mock_post.call_args[1]["json"]["model"]
    assert called_model == ANSWER_MODEL_BIG
    assert result == "The top product is P9 with 4,500 in revenue."


@pytest.mark.unit
async def test_token_budget_per_shape_sent_to_ollama(mocker):
    rows = [{"product": f"P{i}", "revenue": i * 500} for i in range(10)]
    mock_post = _setup_mock_client(mocker, "P9 leads with 4,500.")
    await generate_answer("Top products?", rows)
    sent_num_predict = mock_post.call_args[1]["json"]["options"]["num_predict"]
    assert sent_num_predict == TOKEN_BUDGETS["long_list"]


@pytest.mark.unit
async def test_answer_calls_use_temperature_zero(mocker):
    mock_post = _setup_mock_client(mocker, "The answer is 1.")
    await generate_answer("Q", [{"c": 1}])
    sent_temperature = mock_post.call_args[1]["json"]["options"]["temperature"]
    assert sent_temperature == 0.0


@pytest.mark.unit
async def test_answer_calls_use_chat_endpoint(mocker):
    mock_post = _setup_mock_client(mocker, "The answer is 1.")
    await generate_answer("Q", [{"c": 1}])
    called_url = mock_post.call_args[0][0]
    assert called_url.endswith("/api/chat")


@pytest.mark.unit
async def test_answer_payload_uses_messages_not_prompt(mocker):
    mock_post = _setup_mock_client(mocker, "The answer is 1.")
    await generate_answer("Q", [{"c": 1}])
    payload = mock_post.call_args[1]["json"]
    assert "messages" in payload
    assert "prompt" not in payload


# generate_answer - fallback behavior

@pytest.mark.unit
async def test_fallback_on_big_model_failure(mocker):
    rows = [{"a": i, "b": i, "c": i, "d": i} for i in range(2)]  # complex_table
    call_count = 0

    fallback_response = MagicMock()
    fallback_response.raise_for_status = MagicMock()
    fallback_response.json.return_value = {
        "message": {"content": "The result has 2 rows."}
    }

    async def mock_post(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if kwargs["json"]["model"] == ANSWER_MODEL_BIG:
            raise Exception("Big model unavailable")
        return fallback_response

    mock_client = MagicMock()
    mock_client.post = mock_post

    mocker.patch("app.services.ollama_client._get_chat_client", return_value=mock_client)

    result = await generate_answer("Top products?", rows)
    assert result == "The result has 2 rows."
    assert call_count == 2


@pytest.mark.unit
async def test_empty_response_from_big_model_uses_deterministic_fallback(mocker):
    rows = [{"a": i, "b": i, "c": i, "d": i} for i in range(2)]  # complex_table
    call_models = []

    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json.return_value = {"message": {"content": ""}}  # empty

    async def mock_post(url, **kwargs):
        call_models.append(kwargs["json"]["model"])
        return response

    mock_client = MagicMock()
    mock_client.post = mock_post
    mocker.patch("app.services.ollama_client._get_chat_client", return_value=mock_client)

    result = await generate_answer("Top products?", rows)
    assert call_models == [ANSWER_MODEL_BIG]  # no retry on empty (only on exception)
    assert result.startswith("Top rows: ")
    assert result.endswith("total_rows=2.")


@pytest.mark.unit
async def test_both_models_fail_returns_deterministic_fallback(mocker):
    rows = [{"a": i, "b": i, "c": i, "d": i} for i in range(2)]  # complex_table

    async def mock_post(url, **kwargs):
        raise Exception("Ollama offline")

    mock_client = MagicMock()
    mock_client.post = mock_post

    mocker.patch("app.services.ollama_client._get_chat_client", return_value=mock_client)

    result = await generate_answer("Top products?", rows)
    assert result.startswith("Top rows: ")
    assert result.endswith("total_rows=2.")


# stream_answer_chunks - one final chunk

@pytest.mark.unit
async def test_stream_empty_results():
    chunks = []
    async for chunk in stream_answer_chunks("How many sales?", []):
        chunks.append(chunk)
    assert chunks == ["No results were found for that query."]


@pytest.mark.unit
async def test_stream_uses_big_model_for_complex_shape_and_emits_single_chunk(mocker):
    rows = [{"a": i, "b": i, "c": i, "d": i} for i in range(2)]  # complex_table
    mock_post = _setup_mock_client(
        mocker,
        "The table has 2 rows with 4 columns each.",
    )

    chunks = []
    async for chunk in stream_answer_chunks("Top products?", rows):
        chunks.append(chunk)

    called_model = mock_post.call_args[1]["json"]["model"]
    assert called_model == ANSWER_MODEL_BIG
    assert chunks == ["The table has 2 rows with 4 columns each."]


@pytest.mark.unit
async def test_stream_uses_big_model_for_long_list(mocker):
    rows = [{"product": f"P{i}", "revenue": i * 500} for i in range(10)]
    mock_post = _setup_mock_client(
        mocker,
        "P9 is the top product with 4,500 in revenue.",
    )

    chunks = []
    async for chunk in stream_answer_chunks("Top products?", rows):
        chunks.append(chunk)

    called_model = mock_post.call_args[1]["json"]["model"]
    assert called_model == ANSWER_MODEL_BIG
    assert chunks == ["P9 is the top product with 4,500 in revenue."]
