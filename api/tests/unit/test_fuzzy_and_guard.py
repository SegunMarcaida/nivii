"""
Unit tests for off-topic question guard and complexity classifier.
Zero I/O — pure function testing.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.prompts.classification import (
    QueryComplexity,
    classify_complexity,
    is_on_topic,
)


# ─────────────────────────────────────────────────────────────────────────────
# is_on_topic()
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_on_topic_waiter_friday_query():
    assert is_on_topic("which waiter sold the most last friday") is True

@pytest.mark.unit
def test_on_topic_revenue_query():
    assert is_on_topic("what is the total revenue") is True

@pytest.mark.unit
def test_on_topic_top_products():
    assert is_on_topic("show me top products by quantity") is True

@pytest.mark.unit
def test_on_topic_monthly_sales():
    assert is_on_topic("monthly sales breakdown by product") is True

@pytest.mark.unit
def test_on_topic_hourly_query():
    assert is_on_topic("which hour has the most transactions") is True

@pytest.mark.unit
def test_on_topic_never_returned():
    assert is_on_topic("products that were never returned") is True

@pytest.mark.unit
def test_on_topic_ticket_query():
    assert is_on_topic("how many tickets were processed today") is True

@pytest.mark.unit
def test_on_topic_spanish_query():
    assert is_on_topic("cual es el producto mas vendido por vendedor") is True

@pytest.mark.unit
def test_off_topic_poem():
    assert is_on_topic("write me a poem") is False

@pytest.mark.unit
def test_off_topic_geography():
    assert is_on_topic("what is the capital of France") is False

@pytest.mark.unit
def test_off_topic_recipe():
    assert is_on_topic("give me a recipe for pasta") is False

@pytest.mark.unit
def test_off_topic_coding():
    assert is_on_topic("how do I implement quicksort in Python") is False

@pytest.mark.unit
def test_off_topic_math():
    assert is_on_topic("what is the square root of 144") is False


# ─────────────────────────────────────────────────────────────────────────────
# classify_complexity()
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_simple_basic_revenue():
    assert classify_complexity("What is the total revenue?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_product_count():
    assert classify_complexity("How many products were sold?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_last_7_days_filter():
    assert classify_complexity("What is revenue in the last 7 days?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_hard_revenue_by_month_grouping():
    """'by month' requires strftime() — routes to Arctic."""
    assert classify_complexity("Revenue by month") == QueryComplexity.HARD

@pytest.mark.unit
def test_simple_each_day_this_week_grouping():
    assert classify_complexity("Sales for each day this week") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_top_products():
    """'top 5 products' = ORDER BY + LIMIT, no per-group qualifier → Qwen."""
    assert classify_complexity("What are the top 5 products by revenue?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_hard_rank_keyword():
    assert classify_complexity("Rank products by cumulative revenue") == QueryComplexity.HARD

@pytest.mark.unit
def test_simple_most_keyword():
    """'most' without per-group qualifier = ORDER BY + LIMIT → Qwen."""
    assert classify_complexity("Which product sold the most?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_per_waiter_grouping():
    assert classify_complexity("What is the revenue per waiter?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_each_keyword_grouping():
    assert classify_complexity("Show total sales for each product") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_percentage():
    """Percentage = SUM/SUM*100 math, no window needed → Qwen."""
    assert classify_complexity("What percentage of revenue comes from each product?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_hard_never():
    assert classify_complexity("Which products were never returned?") == QueryComplexity.HARD

@pytest.mark.unit
def test_hard_monthly_trends():
    """'monthly' requires strftime() — routes to Arctic."""
    assert classify_complexity("Show me monthly sales trends") == QueryComplexity.HARD

@pytest.mark.unit
def test_simple_bottom():
    """'bottom' without per-group qualifier = ORDER BY + LIMIT → Qwen."""
    assert classify_complexity("Show bottom selling products") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_highest():
    """'highest' without per-group qualifier = MAX/ORDER BY → Qwen."""
    assert classify_complexity("Which waiter had the highest revenue?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_hard_versus_last():
    assert classify_complexity("Compare sales this week vs last week") == QueryComplexity.HARD

@pytest.mark.unit
def test_simple_growth_no_period_ref():
    """'growth over time' without explicit last/previous reference → Qwen (GROUP BY month)."""
    assert classify_complexity("What is the revenue growth over time?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_simple_spanish_percentage():
    """Porcentaje alone (no per-group) = simple math → Qwen."""
    assert classify_complexity("Que porcentaje de ingresos aporta cada producto?") == QueryComplexity.SIMPLE

@pytest.mark.unit
def test_hard_spanish_anti_join():
    assert classify_complexity("Productos que nunca se vendieron") == QueryComplexity.HARD

@pytest.mark.unit
def test_hard_spanish_comparison():
    assert classify_complexity("Compara ventas de esta semana vs la semana pasada") == QueryComplexity.HARD


# ─────────────────────────────────────────────────────────────────────────────
# generate_sql guard integration
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
async def test_generate_sql_rejects_off_topic():
    """Off-topic question should raise ValueError before any DB call."""
    from app.services.nl2sql import generate_sql

    mock_session = MagicMock()
    mock_session.begin = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(), __aexit__=AsyncMock()))

    with pytest.raises(ValueError, match="only answers questions about sales"):
        await generate_sql("write me a poem about the ocean", mock_session)

    # DB session should not have been entered
    mock_session.begin.assert_not_called()

@pytest.mark.unit
async def test_generate_sql_passes_on_topic_to_ollama():
    """On-topic question should NOT raise ValueError (reaches Ollama call)."""
    from app.services.nl2sql import generate_sql

    mock_begin = AsyncMock()
    mock_begin.__aenter__ = AsyncMock(return_value=None)
    mock_begin.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.begin = MagicMock(return_value=mock_begin)

    # Raises at Ollama stage (expected), not at the guard stage
    with patch("app.services.nl2sql._call_ollama", side_effect=RuntimeError("test stop")):
        with pytest.raises(RuntimeError, match="test stop"):
            await generate_sql("what are the top products by revenue", mock_session)
