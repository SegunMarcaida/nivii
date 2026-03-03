"""
Unit tests for query complexity classification.

SIMPLE → Qwen (ORDER BY+LIMIT, basic GROUP BY, basic math, WHERE filters)
HARD   → Arctic (window functions, per-group top-N, period-over-period, anti-join absence,
                  strftime grouping, SUBSTR time analysis, stddev, growth rate/acceleration)
"""
import pytest

from app.prompts.classification import QueryComplexity, classify_complexity


# ── SIMPLE queries ────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_simple_total_revenue():
    assert classify_complexity("What is the total revenue?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_friday_sales():
    assert classify_complexity("How many sales on Friday?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_product_count():
    assert classify_complexity("How many products were sold?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_no_keywords():
    """No keyword matches defaults to SIMPLE."""
    assert classify_complexity("xyzzy frobnicator") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_yesterday():
    assert classify_complexity("What was the revenue yesterday?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_specific_date():
    assert classify_complexity("Show all sales on November 5th") == QueryComplexity.SIMPLE


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
def test_simple_spanish_grouping():
    assert classify_complexity("Ventas por mes") == QueryComplexity.SIMPLE


# ORDER BY + LIMIT — no window function needed, Qwen handles these

@pytest.mark.unit
def test_simple_top_n_products():
    """'top 5 products' = ORDER BY + LIMIT — no per-group qualifier."""
    assert classify_complexity("What are the top 5 products?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_bottom_products():
    assert classify_complexity("Show bottom selling products") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_most_sold_product():
    assert classify_complexity("Which product sold the most?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_least_sold_product():
    assert classify_complexity("Which product sold the least?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_highest_revenue_waiter():
    assert classify_complexity("Which waiter had the highest revenue?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_lowest_revenue_waiter():
    assert classify_complexity("Which waiter had the lowest sales?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_best_selling_product():
    assert classify_complexity("What is the best selling product?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_worst_selling_product():
    assert classify_complexity("What is the worst selling product?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_per_waiter_grouping():
    assert classify_complexity("What is the revenue per waiter?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_each_product_grouping():
    assert classify_complexity("Show total sales for each product") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_by_waiter_breakdown():
    assert classify_complexity("Revenue by waiter breakdown") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_hard_by_month_breakdown():
    """'by month' requires strftime() — routes to Arctic."""
    assert classify_complexity("Sales report by month") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_weekly_breakdown():
    """'weekly' requires strftime('%Y-%W') — routes to Arctic."""
    assert classify_complexity("Weekly revenue breakdown") == QueryComplexity.HARD


@pytest.mark.unit
def test_simple_daily_last_quarter_grouping():
    assert classify_complexity("Show daily sales for last quarter") == QueryComplexity.SIMPLE


# Ratio / share — simple math (SUM/SUM*100), no window needed

@pytest.mark.unit
def test_simple_percentage():
    assert classify_complexity("What percentage of revenue comes from each product?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_percent():
    assert classify_complexity("What percent of total does Alfajor represent?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_rate():
    assert classify_complexity("What is the return rate?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_ratio():
    assert classify_complexity("What is the ratio of B2B to B2C sales?") == QueryComplexity.SIMPLE


# WHERE-filter exclusion — not a subquery

@pytest.mark.unit
def test_simple_excluding_credit_notes():
    assert classify_complexity("Revenue excluding credit notes and promos") == QueryComplexity.SIMPLE


# Plain trend / growth — GROUP BY is sufficient

@pytest.mark.unit
def test_hard_monthly_trends():
    """'monthly' requires strftime() — routes to Arctic."""
    assert classify_complexity("Show monthly sales trends") == QueryComplexity.HARD


@pytest.mark.unit
def test_simple_revenue_growth_no_period_ref():
    assert classify_complexity("What is the revenue growth over time?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_revenue_trend():
    assert classify_complexity("Show revenue trend over the period") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_first_week_filter():
    assert classify_complexity("What was the first week's revenue?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_simple_top_products_no_group():
    """'top products' with no per-group qualifier = ORDER BY + LIMIT."""
    assert classify_complexity("What were the last month's top products?") == QueryComplexity.SIMPLE


# ── HARD queries ──────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_hard_rank():
    assert classify_complexity("Rank products by revenue") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_second_highest():
    assert classify_complexity("Second highest revenue product per cashier") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_top_bottom_per_waiter():
    assert classify_complexity("Top-selling and bottom-selling product for each waiter") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_least_per_waiter():
    """Least per group requires ROW_NUMBER OVER PARTITION."""
    assert classify_complexity("Least sold product per waiter") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_not_sold():
    assert classify_complexity("Products not sold in the last week") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_never():
    assert classify_complexity("Products that have never been returned") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_versus_last():
    assert classify_complexity("Compare sales this week vs last week") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_growth_vs_previous():
    assert classify_complexity("How did revenue change vs previous month?") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_month_over_month():
    assert classify_complexity("Month over month revenue breakdown") == QueryComplexity.HARD


@pytest.mark.unit
def test_simple_spanish_percentage_per_group():
    """Porcentaje por mes → GROUP BY + scalar subquery, Qwen can attempt it."""
    assert classify_complexity("Que porcentaje de ingresos aporta cada vendedor por mes?") == QueryComplexity.SIMPLE


@pytest.mark.unit
def test_hard_spanish_anti_join():
    assert classify_complexity("Productos que nunca se vendieron") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_spanish_period_comparison():
    assert classify_complexity("Compara ventas de esta semana vs la semana pasada") == QueryComplexity.HARD


# Date grouping — strftime() needed

@pytest.mark.unit
def test_hard_monthly_revenue():
    assert classify_complexity("What is the monthly revenue?") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_weekly_revenue():
    assert classify_complexity("Show weekly revenue totals") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_revenue_per_month():
    assert classify_complexity("Revenue per month breakdown") == QueryComplexity.HARD


# Time-of-day — SUBSTR(sale_hour, 1, 2) needed

@pytest.mark.unit
def test_hard_busiest_hour():
    assert classify_complexity("Which hour of the day has the highest sales?") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_morning_afternoon_evening():
    assert classify_complexity("Show morning vs afternoon vs evening revenue") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_time_range_filter():
    """Two HH:MM values in proximity → range filter needing SUBSTR."""
    assert classify_complexity("How many sales happened between 12:00 and 14:00?") == QueryComplexity.HARD


# Standard deviation — manual SQRT formula needed

@pytest.mark.unit
def test_hard_standard_deviation():
    assert classify_complexity("What is the standard deviation of daily revenue?") == QueryComplexity.HARD


# Growth rate / acceleration

@pytest.mark.unit
def test_hard_growth_rate():
    assert classify_complexity("Show the growth rate by month") == QueryComplexity.HARD


@pytest.mark.unit
def test_hard_acceleration():
    assert classify_complexity("Did revenue growth accelerate in October?") == QueryComplexity.HARD


# ── Edge cases ────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_mixed_hard_dominates():
    """When a question triggers both simple and hard categories, hard dominates."""
    assert classify_complexity("Rank the top selling products") == QueryComplexity.HARD


@pytest.mark.unit
def test_returns_enum_type():
    """classify_complexity returns a QueryComplexity enum."""
    result = classify_complexity("What are the top products?")
    assert isinstance(result, QueryComplexity)
    assert result.value in ("simple", "hard")
